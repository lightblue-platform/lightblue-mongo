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

import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.types.ObjectId;

import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.BulkWriteError;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.WriteConcern;

import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.crud.CRUDOperation;

import com.redhat.lightblue.util.Error;

/**
 * Helper class to facilitate safe bulk updates
 *
 * Call reset before the bulk update operation. Then use addDoc to add
 * documents to the bulk update operation, Once there is enough number
 * of docs, call execute to update, which also resets the bulk operation.
 *
 * This class implements a timestamping scheme to prevent concurrent
 * updates. Each document has a unique @docver variable. When we read
 * the doc, this class saves that @docver value (which can be null),
 * and updates only those documents whose @docver is the same. During
 * the update, it also updates the @docver to a different value. Any
 * documents that are missed are the documents modified by someone
 * else after it is read by this instance, so those documents are
 * marked with concurrent modification errors.
 */
public class BulkUpdateCtx {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkUpdateCtx.class);
    
    public static class UpdateDoc {
        /**
         * Document id, populated from _id
         */
        final Object id;

        /**
         * The docver field read from the doc, coul dbe null
         */
        final String docTxId;
        /**
         * The old mongo document
         */
        final DBObject oldMongoDocument;
        final DocCtx docCtx;
        
        public UpdateDoc(DBObject mongoDocument,
                         DocCtx docCtx) {
            this.oldMongoDocument=mongoDocument;
            this.docCtx=docCtx;
            docTxId=Translator.getDocVersion(oldMongoDocument);
            id=oldMongoDocument.get(Translator.ID_STR);
        }
    }
    
    BulkWriteOperation bwo;
    /**
     * The new docver value
     */
    String txid;
    /**
     * Map of _id -> docver
     */
    Map<Object,String> docIdTxIdMap;
    List<UpdateDoc> updateDocs;
    int numUpdated;
    int numErrors;
    
    public void reset(DBCollection collection) {
        bwo=collection.initializeUnorderedBulkOperation();
        txid=ObjectId.get().toString();
        docIdTxIdMap=new HashMap<>();
        updateDocs=new ArrayList<>();
    }
    
    public void addDoc(UpdateDoc doc,DBObject replaceDoc) {
        bwo.find(new BasicDBObject("_id", doc.id).
                 append(Translator.DOC_VERSION_FULLPATH_STR,doc.docTxId)).
            replaceOne(replaceDoc);
        updateDocs.add(doc);
        docIdTxIdMap.put(doc.id,doc.docTxId);
        doc.docCtx.setCRUDOperationPerformed(CRUDOperation.UPDATE);
        doc.docCtx.setUpdatedDocument(doc.docCtx);
    }
    
    private void checkConcurrentModifications(DBCollection collection) {
        Set<Object> failedIds=Utils.checkFailedUpdates(collection,txid,docIdTxIdMap);
        for(Object id:failedIds) {
            for(UpdateDoc doc:updateDocs) {
                if(doc.id.equals(id)&&!doc.docCtx.hasErrors()) {
                    doc.docCtx.addError(Error.get("update",MongoCrudConstants.ERR_CONCURRENT_MODIFICATION,doc.id.toString()));
                    break;
                }
            }
        }
    }
    
    public void execute(DBCollection collection,WriteConcern writeConcern) {
        try {
            BulkWriteResult result;
            if (writeConcern == null) {
                LOGGER.debug("Bulk updating docs");
                result=bwo.execute();
            }
            else {
                LOGGER.debug("Bulk updating docs with writeConcern={} from execution", writeConcern);
                result=bwo.execute(writeConcern);
            }
            numUpdated+=result.getModifiedCount();
            if(result.getMatchedCount()<updateDocs.size()) {
                // There are docs that didn't match, concurrent modification
                checkConcurrentModifications(collection);
            }
        } catch (BulkWriteException bwe) {
            LOGGER.error("Bulk write exception", bwe);
            handleBulkWriteError(bwe.getWriteErrors(),"update",(int i)->updateDocs.get(i).docCtx);
            checkConcurrentModifications(collection);
            numUpdated+=bwe.getWriteResult().getModifiedCount();
        }
        for(UpdateDoc doc:updateDocs)
            if(doc.docCtx.hasErrors())
                numErrors++;
        reset(collection);
    }

    public interface GetCB {
        DocCtx getCtx(int index);
    }
    
    public static <T> void handleBulkWriteError(List<BulkWriteError> errors,String op,GetCB cb) {
        for (BulkWriteError e : errors) {
            cb.getCtx(e.getIndex()).addError(getError(e,op));
        }
    }

    public static Error getError(BulkWriteError e,String op) {
        if (e.getCode() == 11000 || e.getCode() == 11001) {
            return Error.get(op, MongoCrudConstants.ERR_DUPLICATE, e.getMessage());
        } else {
            return Error.get(op, MongoCrudConstants.ERR_SAVE_ERROR, e.getMessage());
        }
    }
}
