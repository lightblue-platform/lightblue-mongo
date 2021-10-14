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

import com.mongodb.client.model.DBCollectionFindOptions;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
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
import com.redhat.lightblue.crud.ListDocumentStream;
import com.redhat.lightblue.crud.CRUDDeleteResponse;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.interceptor.InterceptPoint;
import com.redhat.lightblue.util.Error;

/**
 * Basic document deletion with no transaction support
 */
public class BasicDocDeleter implements DocDeleter {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicDocDeleter.class);

    // TODO: move this to a better place
    public final int batchSize;

    // Use in tests. Set to false to test deletion with bulk operations
    public boolean hookOptimization=true;

    private final DocTranslator translator;
    private final WriteConcern writeConcern;

    public BasicDocDeleter(DocTranslator translator, WriteConcern writeConcern, int batchSize) {
        super();
        this.translator = translator;
        this.writeConcern = writeConcern;
        this.batchSize = batchSize;
    }

    @Override
    public void delete(CRUDOperationContext ctx,
                       DBCollection collection,
                       DBObject mongoQuery,
                       CRUDDeleteResponse response) {
        LOGGER.debug("Removing docs with {}", mongoQuery);

        int numDeleted = 0;

        if(!hookOptimization||ctx.getHookManager().hasHooks(ctx,CRUDOperation.DELETE)) {
            LOGGER.debug("There are hooks, retrieve-delete");
            try (DBCursor cursor = collection.find(mongoQuery)) {
                // Set read preference to primary for read-for-update operations
                cursor.setReadPreference(ReadPreference.primary());

                // All docs, to be put into the context
                ArrayList<DocCtx> contextDocs=new ArrayList<>();
                // ids to delete from the db
                List<Object> idsToDelete = new ArrayList<>(batchSize);                
                while (cursor.hasNext()) {

                    // We will use this index to access the documents deleted in this batch
                    int thisBatchIndex=contextDocs.size();
                    if (idsToDelete.size() < batchSize) {
                        // build batch
                        DBObject doc = cursor.next();
                        DocTranslator.TranslatedDoc tdoc=translator.toJson(doc);
                        DocCtx docCtx=new DocCtx(tdoc.doc,tdoc.rmd);
                        docCtx.setOriginalDocument(docCtx);
                        docCtx.setCRUDOperationPerformed(CRUDOperation.DELETE);
                        contextDocs.add(docCtx);
                        idsToDelete.add(doc.get(MongoCRUDController.ID_STR));
                    }
                    
                    if (idsToDelete.size() == batchSize || !cursor.hasNext()) {
                        // batch built or run out of documents                        
                        BulkWriteOperation bw = collection.initializeUnorderedBulkOperation();
                        
                        for(Object id: idsToDelete) {
                            // doing a bulk of single operations instead of removing by initial query
                            // that way we know which documents were not removed
                            bw.find(new BasicDBObject("_id", id)).remove();
                        }
                        
                        BulkWriteResult result = null;
                        try {
                            if (writeConcern == null) {
                                LOGGER.debug("Bulk deleting docs");
                                result = bw.execute();
                            } else {
                                LOGGER.debug("Bulk deleting docs with writeConcern={} from execution", writeConcern);
                                result = bw.execute(writeConcern);
                            }
                            LOGGER.debug("Bulk deleted docs - attempted {}, deleted {}", idsToDelete.size(), result.getRemovedCount());
                        } catch (BulkWriteException bwe) {
                            LOGGER.error("Bulk write exception", bwe);
                            handleBulkWriteError(bwe.getWriteErrors(), contextDocs.subList(thisBatchIndex,contextDocs.size()));
                            result = bwe.getWriteResult();
                        } catch (RuntimeException e) {
                            LOGGER.error("Exception", e);
                            throw e;
                        } finally {
                            
                            numDeleted+=result.getRemovedCount();
                            // clear list before processing next batch
                            idsToDelete.clear();
                        }
                    }
                }
                ctx.setDocumentStream(new ListDocumentStream<DocCtx>(contextDocs));                
            }
        } else {
            LOGGER.debug("There are no hooks, deleting in bulk");
            try {
                if(writeConcern==null) {
                    numDeleted=collection.remove(mongoQuery).getN();
                } else {
                    numDeleted=collection.remove(mongoQuery,writeConcern).getN();
                }
            } catch(MongoException e) {
                LOGGER.error("Deletion error",e);
                throw e;
            }
            ctx.setDocumentStream(new ListDocumentStream<DocCtx>(new ArrayList<DocCtx>()));
        }

        response.setNumDeleted(numDeleted);
    }

    private void handleBulkWriteError(List<BulkWriteError> errors, List<DocCtx> docs) {
        for (BulkWriteError e : errors) {
            DocCtx doc = docs.get(e.getIndex());
            doc.addError(Error.get("remove", MongoCrudConstants.ERR_DELETE_ERROR, e.getMessage()));
        }
    }
}
