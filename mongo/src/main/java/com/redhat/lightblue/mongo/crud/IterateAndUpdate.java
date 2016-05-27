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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
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


    private final List<DocCtx> docUpdateAttempts = new ArrayList<>();
    private final List<BulkWriteError> docUpdateErrors = new ArrayList<>();

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
        int numUpdating = 0;
        int numFailed = 0;
        BsonMerge merge = new BsonMerge(md);
        try {
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE_RESULTSET, ctx);
            cursor = collection.find(query, null);
            LOGGER.debug("Found {} documents", cursor.count());
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_UPDATE_RESULTSET, ctx);
            // read-update-write
            BulkWriteOperation bwo = collection.initializeUnorderedBulkOperation();
            while (cursor.hasNext()) {
                DBObject document = cursor.next();
                numMatched++;
                boolean hasErrors = false;
                LOGGER.debug("Retrieved doc {}", docIndex);
                DocCtx doc = ctx.addDocument(translator.toJson(document));
                doc.startModifications();
                // From now on: doc contains the working copy, and doc.originalDoc contains the original copy
                if (updater.update(doc, md.getFieldTreeRoot(), Path.EMPTY)) {
                    LOGGER.debug("Document {} modified, updating", docIndex);
                    PredefinedFields.updateArraySizes(md, nodeFactory, doc);
                    LOGGER.debug("Running constraint validations");
                    ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE_DOC_VALIDATION, ctx, doc);
                    validator.clearErrors();
                    validator.validateDoc(doc);
                    List<Error> errors = validator.getErrors();
                    if (errors != null && !errors.isEmpty()) {
                        ctx.addErrors(errors);
                        hasErrors = true;
                        LOGGER.debug("Doc has errors");
                    }
                    errors = validator.getDocErrors().get(doc);
                    if (errors != null && !errors.isEmpty()) {
                        doc.addErrors(errors);
                        hasErrors = true;
                        LOGGER.debug("Doc has data errors");
                    }
                    if (!hasErrors) {
                        Set<Path> paths = roleEval.getInaccessibleFields_Update(doc, doc.getOriginalDocument());
                        LOGGER.debug("Inaccesible fields during update={}" + paths);
                        if (paths != null && !paths.isEmpty()) {
                            doc.addError(Error.get("update", CrudConstants.ERR_NO_FIELD_UPDATE_ACCESS, paths.toString()));
                            hasErrors = true;
                        }
                    }
                    if (!hasErrors) {
                        try {
                            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE_DOC, ctx, doc);
                            DBObject updatedObject = translator.toBson(doc);
                            merge.merge(document, updatedObject);
                            try {
                                Translator.populateDocHiddenFields(updatedObject, md);
                            } catch (IOException e) {
                                throw new RuntimeException("Error populating document: \n" + updatedObject);
                            }

                            bwo.find(new BasicDBObject("_id", document.get("_id"))).replaceOne(updatedObject);
                            docUpdateAttempts.add(doc);
                            numUpdating++;
                            // update in batches
                            if (numUpdating >= batchSize) {
                                executeAndLogBulkErrors(bwo);
                                numUpdating = 0;
                            }
                            doc.setCRUDOperationPerformed(CRUDOperation.UPDATE);
                            doc.setUpdatedDocument(doc);
                        } catch (Exception e) {
                            LOGGER.warn("Update exception for document {}: {}", docIndex, e);
                            doc.addError(Error.get(MongoCrudConstants.ERR_UPDATE_ERROR, e.toString()));
                            hasErrors = true;
                        }
                    }
                } else {
                    LOGGER.debug("Document {} was not modified", docIndex);
                }
                if (hasErrors) {
                    LOGGER.debug("Document {} has errors", docIndex);
                    numFailed++;
                    doc.setOutputDocument(errorProjector.project(doc, nodeFactory));
                } else if (projector != null) {
                    LOGGER.debug("Projecting document {}", docIndex);
                    doc.setOutputDocument(projector.project(doc, nodeFactory));
                }
                docIndex++;
            }
            // if we have any remaining items to update
            if (numUpdating > 0) {
                try {
                    executeAndLogBulkErrors(bwo);
                    numFailed += (int) docUpdateErrors.stream().map(e -> e.getIndex()).distinct().count();
                } catch (Exception e) {
                    LOGGER.warn("Update exception for documents for query: {}", query.toString());
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        handleBulkWriteError(docUpdateErrors, docUpdateAttempts);

        ctx.getDocumentsWithoutErrors().stream().forEach(doc -> {
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_UPDATE_DOC, ctx, doc);
        });

        response.setNumUpdated(ctx.getDocumentsWithoutErrors().size());
        response.setNumFailed(numFailed);
        response.setNumMatched(numMatched);
    }

    private void handleBulkWriteError(List<BulkWriteError> errors, List<DocCtx> docs) {
        for (BulkWriteError e : errors) {
            DocCtx doc = docs.get(e.getIndex());
            if (e.getCode() == 11000 || e.getCode() == 11001) {
                doc.addError(Error.get("update", MongoCrudConstants.ERR_DUPLICATE, e.getMessage()));
            } else {
                doc.addError(Error.get("update", MongoCrudConstants.ERR_SAVE_ERROR, e.getMessage()));
            }
        }
    }

    private BulkWriteResult executeAndLogBulkErrors(BulkWriteOperation bwo) {
        BulkWriteResult result = null;
        try {
            if (writeConcern == null) {
                LOGGER.debug("Bulk updating docs");
                result = bwo.execute();
            } else {
                LOGGER.debug("Bulk deleting docs with writeConcern={} from execution", writeConcern);
                result = bwo.execute(writeConcern);
            }
        } catch (BulkWriteException e) {
            BulkWriteException bwe = e;
            result = bwe.getWriteResult();
            LOGGER.warn("Bulk update operation contains errors", bwe);
            docUpdateErrors.addAll(bwe.getWriteErrors());
        } catch (Exception e) {
            throw e;
        }
        return result;
    }
}
