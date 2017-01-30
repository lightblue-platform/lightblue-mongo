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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map;

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
import com.mongodb.ReadPreference;
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
import com.redhat.lightblue.metadata.Type;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.Measure;
import com.redhat.lightblue.util.JsonDoc;

/**
 * Non-atomic updater that evaluates the query, and updates the documents one by
 * one.
 */
public class IterateAndUpdate implements DocUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(IterateAndUpdate.class);
    private static final Logger METRICS = LoggerFactory.getLogger("metrics."+IterateAndUpdate.class.getName());

    private final int batchSize;

    private final JsonNodeFactory nodeFactory;
    private final ConstraintValidator validator;
    private final FieldAccessRoleEvaluator roleEval;
    private final DocTranslator translator;
    private final Updater updater;
    private final Projector projector;
    private final Projector errorProjector;
    private final WriteConcern writeConcern;
    private final ConcurrentModificationDetectionCfg concurrentModificationDetection;

    private class MongoSafeUpdateProtocolForUpdate extends MongoSafeUpdateProtocol {

        private final EntityMetadata md;
        private final Measure measure;
        private final BsonMerge merge;

        public MongoSafeUpdateProtocolForUpdate(DBCollection collection,
                                                WriteConcern writeConcern,
                                                DBObject query,
                                                ConcurrentModificationDetectionCfg cfg,
                                                EntityMetadata md,
                                                Measure measure) {
            super(collection,writeConcern,query,cfg);
            this.md=md;
            this.measure=measure;
            this.merge=new BsonMerge(md);
        }
        
        protected DBObject reapplyChanges(int docIndex,DBObject doc) {
            DocTranslator.TranslatedDoc jsonDoc=translator.toJson(doc);
            // We are bypassing validation here
            if(!updateDoc(md,jsonDoc.doc,measure))
                return null;
            return translate(md,jsonDoc.doc,doc,merge,measure).doc;
        }
    }
    
    public IterateAndUpdate(JsonNodeFactory nodeFactory,
                            ConstraintValidator validator,
                            FieldAccessRoleEvaluator roleEval,
                            DocTranslator translator,
                            Updater updater,
                            Projector projector,
                            Projector errorProjector,
                            WriteConcern writeConcern,
                            int batchSize,
                            ConcurrentModificationDetectionCfg concurrentModificationDetection) {
        this.nodeFactory = nodeFactory;
        this.validator = validator;
        this.roleEval = roleEval;
        this.translator = translator;
        this.updater = updater;
        this.projector = projector;
        this.errorProjector = errorProjector;
        this.writeConcern = writeConcern;
        this.batchSize = batchSize;
        this.concurrentModificationDetection = concurrentModificationDetection;
    }

    private BatchUpdate getUpdateProtocol(CRUDOperationContext ctx,
                                          DBCollection collection,
                                          DBObject query,
                                          EntityMetadata md,
                                          Measure measure) {
        if(ctx.isUpdateIfCurrent()) {
            // Retrieve doc versions from the context
            Type type=md.resolve(DocTranslator.ID_PATH).getType();
            Set<DocIdVersion> docVersions=DocIdVersion.getDocIdVersions(ctx.getUpdateDocumentVersions(),type);
            UpdateIfSameProtocol uis=new UpdateIfSameProtocol(collection,writeConcern);
            uis.addVersions(docVersions);
            return uis;
        } else {
            return new MongoSafeUpdateProtocolForUpdate(collection,
                                                        writeConcern,
                                                        query,
                                                        concurrentModificationDetection,
                                                        md,
                                                        measure);
        }
    }

    @Override
    public void update(CRUDOperationContext ctx,
                       DBCollection collection,
                       EntityMetadata md,
                       CRUDUpdateResponse response,
                       DBObject query) {
        LOGGER.debug("iterateUpdate: start");
        LOGGER.debug("Computing the result set for {}", query);
        Measure measure=new Measure();
        BatchUpdate sup=getUpdateProtocol(ctx,collection,query,md,measure);
        DBCursor cursor = null;
        int docIndex = 0;
        int numMatched = 0;
        int numUpdated =0;
        int numFailed =0;
        BsonMerge merge = new BsonMerge(md);
        List<DocCtx> docUpdateAttempts=new ArrayList<>();
        try {
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE_RESULTSET, ctx);
            measure.begin("collection.find");
            cursor = collection.find(query, null);
            // Read from primary for read-for-update operations
            cursor.setReadPreference(ReadPreference.primary());
            measure.end("collection.find");
            LOGGER.debug("Found {} documents", cursor.count());
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_UPDATE_RESULTSET, ctx);
            // read-update-write
            measure.begin("iteration");
            int batchStartIndex=0; // docUpdateAttempts[batchStartIndex] is the first doc in this batch
            while (cursor.hasNext()) {
                DBObject document = cursor.next();
                numMatched++;
                boolean hasErrors = false;
                LOGGER.debug("Retrieved doc {}", docIndex);
                measure.begin("ctx.addDocument");
                DocTranslator.TranslatedDoc translatedDoc=translator.toJson(document);
                DocCtx doc = ctx.addDocument(translatedDoc.doc,translatedDoc.rmd);
                doc.startModifications();
                measure.end("ctx.addDocument");
                // From now on: doc contains the working copy, and doc.originalDoc contains the original copy
                if (updateDoc(md,doc,measure)) {
                    LOGGER.debug("Document {} modified, updating", docIndex);
                    ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE_DOC_VALIDATION, ctx, doc);
                    LOGGER.debug("Running constraint validations");
                    measure.begin("validation");
                    validator.clearErrors();
                    validator.validateDoc(doc);
                    measure.end("validation");
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
                        hasErrors=accessCheck(doc,measure);
                    }
                    if (!hasErrors) {
                        try {
                            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE_DOC, ctx, doc);
                            DocTranslator.TranslatedBsonDoc updatedObject=translate(md,doc,document,merge,measure);

                            sup.addDoc(updatedObject.doc);
                            docUpdateAttempts.add(doc);
                            // update in batches
                            if (docUpdateAttempts.size()-batchStartIndex>= batchSize) {
                                measure.begin("bulkUpdate");
                                Map<Integer,Error> updateErrors=sup.commit();
                                measure.end("bulkUpdate");
                                for(Map.Entry<Integer,Error> entry:updateErrors.entrySet()) {
                                    docUpdateAttempts.get(entry.getKey()+batchStartIndex).addError(entry.getValue());
                                }
                                int k=updateErrors.size();
                                numFailed+=k;
                                numUpdated+=docUpdateAttempts.size()-batchStartIndex-k;
                                batchStartIndex=docUpdateAttempts.size();
                            }
                            doc.setCRUDOperationPerformed(CRUDOperation.UPDATE);
                            doc.setUpdatedDocument(doc);
                        } catch (Exception e) {
                            LOGGER.warn("Update exception for document {}: {}", docIndex, e);
                            doc.addError(Error.get(MongoCrudConstants.ERR_UPDATE_ERROR, e.toString()));
                            hasErrors = true;
                        }
                    } else {
                        numFailed++;
                    }
                } else {
                    LOGGER.debug("Document {} was not modified", docIndex);
                }
                if (hasErrors) {
                    LOGGER.debug("Document {} has errors", docIndex);
                    doc.setOutputDocument(errorProjector.project(doc, nodeFactory));
                } else if (projector != null) {
                    LOGGER.debug("Projecting document {}", docIndex);
                    doc.setOutputDocument(projector.project(doc, nodeFactory));
                }
                docIndex++;
            }
            measure.end("iteration");
            // if we have any remaining items to update
            if (docUpdateAttempts.size() > batchStartIndex) {
                Map<Integer,Error> updateErrors=sup.commit();
                for(Map.Entry<Integer,Error> entry:updateErrors.entrySet()) {
                    docUpdateAttempts.get(entry.getKey()+batchStartIndex).addError(entry.getValue());
                }
                int k=updateErrors.size();
                numFailed+=k;
                numUpdated+=docUpdateAttempts.size()-batchStartIndex-k;
           }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        ctx.getDocumentsWithoutErrors().stream().forEach(doc -> {
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_UPDATE_DOC, ctx, doc);
        });

        response.setNumUpdated(numUpdated);
        response.setNumFailed(numFailed);
        response.setNumMatched(numMatched);
        METRICS.debug("IterateAndUpdate:\n{}",measure);
    }


    private boolean updateDoc(EntityMetadata md,
                              JsonDoc doc,
                              Measure measure) {
        if (updater.update(doc, md.getFieldTreeRoot(), Path.EMPTY)) {
            // Remove any nulls from the document
            JsonDoc.filterNulls(doc.getRoot());
            measure.begin("updateArraySizes");
            PredefinedFields.updateArraySizes(md, nodeFactory, doc);
            measure.end("updateArraySizes");
            return true;
        } else {
            return false;
        }
    }

    private DocTranslator.TranslatedBsonDoc translate(EntityMetadata md,JsonDoc doc,DBObject document,BsonMerge merge,Measure measure) {
        measure.begin("toBsonAndMerge");
        DocTranslator.TranslatedBsonDoc updatedObject = translator.toBson(doc);
        merge.merge(document, updatedObject.doc);
        measure.end("toBsonAndMerge");
        measure.begin("populateHiddenFields");
        DocTranslator.populateDocHiddenFields(updatedObject.doc, md);
        measure.end("populateHiddenFields");
        return updatedObject;
    }

    // Returns true if there is access check error
    private boolean accessCheck(DocCtx doc, Measure measure) {
        measure.begin("accessCheck");
        Set<Path> paths = roleEval.getInaccessibleFields_Update(doc, doc.getOriginalDocument());
        measure.end("accessCheck");
        LOGGER.debug("Inaccesible fields during update={}" + paths);
        if (paths != null && !paths.isEmpty()) {
            doc.addError(Error.get("update", CrudConstants.ERR_NO_FIELD_UPDATE_ACCESS, paths.toString()));
            return true;
        }
        return false;
    }
}
