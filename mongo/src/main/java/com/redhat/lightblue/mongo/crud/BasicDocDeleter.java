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
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteError;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
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
    private final int batchSize = 64;

    private Translator translator;

    public BasicDocDeleter(Translator translator) {
        super();
        this.translator = translator;
    }

    private final class DocInfo {
        public DocInfo(DocCtx docCtx, Object _id) {
            super();
            this.docCtx = docCtx;
            this._id = _id;
        }

        public DocInfo(DBObject doc, CRUDOperationContext ctx) {
            DocCtx docCtx = ctx.addDocument(translator.toJson(doc));
            docCtx.setOriginalDocument(docCtx);
            this.docCtx = docCtx;
            this._id = doc.get(MongoCRUDController.ID_STR);
        }

        public final DocCtx docCtx;
        public final Object _id;
    }

    @Override
    public void delete(CRUDOperationContext ctx,
                       DBCollection collection,
                       DBObject mongoQuery,
                       CRUDDeleteResponse response) {
        LOGGER.debug("Removing docs with {}", mongoQuery);

        int numDeleted = 0;

        try (DBCursor cursor = collection.find(mongoQuery, null)) {
            List<DocInfo> docsToDelete = new ArrayList<>();

            while (cursor.hasNext()) {

                if (docsToDelete.size() < batchSize) {
                    // build batch
                    DBObject doc = cursor.next();
                    DocInfo docInfo = new DocInfo(doc, ctx);

                    docsToDelete.add(docInfo);

                    ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_DELETE_DOC, ctx, docInfo.docCtx);
                }

                if (docsToDelete.size() == batchSize || !cursor.hasNext()) {
                    // batch built or run out of documents

                    // TODO: write concern
                    BulkWriteOperation bw = collection.initializeUnorderedBulkOperation();

                    List<Object> _ids = docsToDelete.stream().map(d -> d._id).collect(Collectors.toList());
                    bw.find(new BasicDBObject("_id", new BasicDBObject("$in", _ids))).remove();

                    docsToDelete.forEach(d -> d.docCtx.setCRUDOperationPerformed(CRUDOperation.DELETE));

                    try {
                        LOGGER.debug("Bulk deleting docs");
                        BulkWriteResult result = bw.execute();
                        LOGGER.debug("Bulk deleted docs - attempted {}, deleted {}", _ids.size(), result.getRemovedCount());
                        numDeleted+=result.getRemovedCount();
                    } catch (BulkWriteException bwe) {
                        LOGGER.error("Bulk write exception", bwe);
                        // TODO: will partially successful deletes work? Seems there is no BulkWriteResult if exception happens.
                        handleBulkWriteError(bwe.getWriteErrors(), docsToDelete);
                    } catch (RuntimeException e) {
                        LOGGER.error("Exception", e);
                        throw e;
                    } finally {
                        for (DocInfo doc : docsToDelete) {
                            if (!doc.docCtx.hasErrors()) {
                                ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_INSERT_DOC, ctx, doc.docCtx);
                            }
                        }
                    }

                    // clear list before processing next batch
                    docsToDelete.clear();
                }
            }
        }

        response.setNumDeleted(numDeleted);
    }

    private void handleBulkWriteError(List<BulkWriteError> errors, List<DocInfo> docs) {
        for (BulkWriteError e : errors) {
            DocInfo doc = docs.get(e.getIndex());
            // TODO: what error should we use here?
            doc.docCtx.addError(Error.get("remove", MongoCrudConstants.ERR_SAVE_ERROR, e.getMessage()));
        }
    }
}
