/*
 Copyright 2013 Red Hat, Inc. and/or its affiliates.

 This file is part of lightblue.

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.redhat.lightblue.mongo.crud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.bson.types.ObjectId;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteError;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.CRUDUpdateResponse;
import com.redhat.lightblue.crud.ConstraintValidator;
import com.redhat.lightblue.crud.CrudConstants;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.eval.FieldAccessRoleEvaluator;
import com.redhat.lightblue.eval.Projector;
import com.redhat.lightblue.eval.Updater;
import com.redhat.lightblue.interceptor.InterceptPoint;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.PredefinedFields;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.Path;

/**
 * Non-atomic updater that evaluates the query, and updates the documents one by
 * one.
 */
public class IterateAndUpdate implements DocUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(IterateAndUpdate.class);

    private final int batchSize;

    private final JsonNodeFactory nodeFactory;
    private final ConstraintValidator validator;
    private final FieldAccessRoleEvaluator roleEval;
    private final Translator translator;
    private final Updater updater;
    private final Projector projector;
    private final Projector errorProjector;
    private final WriteConcern writeConcern;

    public IterateAndUpdate(JsonNodeFactory nodeFactory,
                            ConstraintValidator validator,
                            FieldAccessRoleEvaluator roleEval,
                            Translator translator,
                            Updater updater,
                            Projector projector,
                            Projector errorProjector,
                            WriteConcern writeConcern, int batchSize) {
        this.nodeFactory = nodeFactory;
        this.validator = validator;
        this.roleEval = roleEval;
        this.translator = translator;
        this.updater = updater;
        this.projector = projector;
        this.errorProjector = errorProjector;
        this.writeConcern = writeConcern;
        this.batchSize = batchSize;
    }
    
    @Override
    public void update(CRUDOperationContext ctx,
                       DBCollection collection,
                       EntityMetadata md,
                       CRUDUpdateResponse response,
                       DBObject query) {
        LOGGER.debug("iterateUpdate: start");
        LOGGER.debug("Computing the result set for {}", query);
        DBCursor cursor = null;
        int docIndex = 0;
        int numMatched = 0;
        int numFailed = 0;
        BsonMerge merge = new BsonMerge(md);
        BulkUpdateCtx bulkCtx=new BulkUpdateCtx();
        try {
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE_RESULTSET, ctx);
            cursor = collection.find(query, null);
            LOGGER.debug("Found {} documents", cursor.count());
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_UPDATE_RESULTSET, ctx);
            // read-update-write
            bulkCtx.reset(collection);
            
            while (cursor.hasNext()) {
                numMatched++;
                boolean hasErrors = false;
                DBObject mongoDocument = cursor.next();
                BulkUpdateCtx.UpdateDoc doc=new BulkUpdateCtx.UpdateDoc(mongoDocument,ctx.addDocument(translator.toJson(mongoDocument)));
                
                LOGGER.debug("Retrieved doc {}", docIndex);

                doc.docCtx.startModifications();
                // From now on: doc.docCtx contains the working copy, and doc.docCtx.originalDoc contains the original copy
                if (updater.update(doc.docCtx, md.getFieldTreeRoot(), Path.EMPTY)) {
                    LOGGER.debug("Document {} modified, updating", docIndex);
                    PredefinedFields.updateArraySizes(md, nodeFactory, doc.docCtx);
                    LOGGER.debug("Running constraint validations");
                    ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE_DOC_VALIDATION, ctx, doc.docCtx);
                    validator.clearErrors();
                    validator.validateDoc(doc.docCtx);
                    List<Error> errors = validator.getErrors();
                    if (errors != null && !errors.isEmpty()) {
                        ctx.addErrors(errors);
                        hasErrors = true;
                        LOGGER.debug("Doc has errors");
                    }
                    errors = validator.getDocErrors().get(doc.docCtx);
                    if (errors != null && !errors.isEmpty()) {
                        doc.docCtx.addErrors(errors);
                        hasErrors = true;
                        LOGGER.debug("Doc has data errors");
                    }
                    if (!hasErrors) {
                        Set<Path> paths = roleEval.getInaccessibleFields_Update(doc.docCtx, doc.docCtx.getOriginalDocument());
                        LOGGER.debug("Inaccesible fields during update={}" + paths);
                        if (paths != null && !paths.isEmpty()) {
                            doc.docCtx.addError(Error.get("update", CrudConstants.ERR_NO_FIELD_UPDATE_ACCESS, paths.toString()));
                            hasErrors = true;
                        }
                    }
                    if (!hasErrors) {
                        try {
                            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE_DOC, ctx, doc.docCtx);
                            DBObject updatedObject = translator.toBson(doc.docCtx);
                            merge.merge(doc.oldMongoDocument, updatedObject);
                            try {
                                Translator.populateCaseInsensitiveHiddenFields(updatedObject, md);
                            } catch (IOException e) {
                                throw new RuntimeException("Error populating document: \n" + updatedObject);
                            }
                            Translator.setDocVersion(updatedObject,bulkCtx.txid);

                            bulkCtx.addDoc(doc,updatedObject);
                            // update in batches
                            if (bulkCtx.updateDocs.size() >= batchSize) {
                                bulkCtx.execute(collection,writeConcern);
                            }
                        } catch (Exception e) {
                            LOGGER.warn("Update exception for document {}: {}", docIndex, e);
                            doc.docCtx.addError(Error.get(MongoCrudConstants.ERR_UPDATE_ERROR, e.toString()));
                            hasErrors = true;
                        }
                    }
                } else {
                    LOGGER.debug("Document {} was not modified", docIndex);
                }
                if (hasErrors) {
                    LOGGER.debug("Document {} has errors", docIndex);
                    numFailed++;
                    doc.docCtx.setOutputDocument(errorProjector.project(doc.docCtx, nodeFactory));
                } else if (projector != null) {
                    LOGGER.debug("Projecting document {}", docIndex);
                    doc.docCtx.setOutputDocument(projector.project(doc.docCtx, nodeFactory));
                }
                docIndex++;
            }
            // if we have any remaining items to update
            if (!bulkCtx.updateDocs.isEmpty()) {
                try {
                    bulkCtx.execute(collection,writeConcern);
                } catch (Exception e) {
                    LOGGER.warn("Update exception for documents for query: {}", query.toString());
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        List<DocCtx> docsWithoutErrors=ctx.getDocumentsWithoutErrors();
        docsWithoutErrors.stream().forEach(doc -> {
                ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_UPDATE_DOC, ctx, doc);
            });
        
        response.setNumUpdated(bulkCtx.numUpdated);
        // numFailed has the number of failed docs that did not even make it into the bulkCtx
        response.setNumFailed(bulkCtx.numErrors+numFailed);
        response.setNumMatched(numMatched);
    }

}
