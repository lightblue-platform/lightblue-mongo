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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.slf4j.Logger;

import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteError;
import com.mongodb.WriteConcern;
import com.mongodb.ReadPreference;

import org.bson.types.ObjectId;

import com.redhat.lightblue.util.Error;

public interface BatchUpdate {

    public static final String DOCVER_FLD=DocTranslator.HIDDEN_SUB_PATH.toString()+"."+DocVerUtil.DOCVER;
    public static final String DOCVER_FLD0=DocTranslator.HIDDEN_SUB_PATH.toString()+"."+DocVerUtil.DOCVER+".0";


    public static class CommitInfo {
        // errors[i] gives the error for the document i in the current batch
        final public Map<Integer,Error> errors=new HashMap<>();
        // An element of lostDocs is an index to a document in the current batch that is lost
        final public Set<Integer> lostDocs=new HashSet<>();

        public boolean hasErrors() {
            return !errors.isEmpty()||!lostDocs.isEmpty();
        }
    }

    /**
     * Adds a document to the current batch. The document should
     * contain the original docver as read from the db
     */
    void addDoc(DBObject doc);
    
    /**
     * Returns the number of queued requests
     */
    int getSize();

    /**
     * Commits the current batch, and prepares for the next
     * iteration. If any update operations fail, this call will detect
     * errors and associate them with the documents using the document
     * index.
     */
    CommitInfo commit();

    /**
     * Runs a batch update using bwo
     *
     * @param bwo The bulk write operation
     * @param writeConcern
     * @param batchSize
     * @param results The results are populated during this call with an error for each failed doc
     * @param logger The logger
     *
     * @return If returns true, all docs are updated. Otherwise, there
     * are some failed docs, and concurrent update error detection
     * should be called
     */
    public static boolean batchUpdate(BulkWriteOperation bwo,
                                      WriteConcern writeConcern,
                                      int batchSize,
                                      Map<Integer,Error> results,
                                      Logger logger) {
        boolean ret=true;
        BulkWriteResult writeResult;
        logger.debug("attemptToUpdate={}",batchSize);
        try {
            if(writeConcern==null) {
                writeResult=bwo.execute();
            } else {
                writeResult=bwo.execute(writeConcern);
            }
            logger.debug("writeResult={}",writeResult);
            if(batchSize==writeResult.getMatchedCount()) {
                logger.debug("Successful update");
            } else {
                logger.warn("notUpdated={}",batchSize-writeResult.getMatchedCount());
                ret=false;
            }
        } catch (BulkWriteException e) {
            List<BulkWriteError> writeErrors=e.getWriteErrors();
            if(writeErrors!=null) {
                for(BulkWriteError we:writeErrors) {
                    if (MongoCrudConstants.isDuplicate(we.getCode())) {
                        results.put(we.getIndex(),
                                    Error.get("update", MongoCrudConstants.ERR_DUPLICATE, we.getMessage()));
                    } else {
                        results.put(we.getIndex(),
                                    Error.get("update", MongoCrudConstants.ERR_SAVE_ERROR, we.getMessage()));
                    }
                }
            }
            ret=false;
        }
        return ret;
    }

    /**
     * Returns the set of document ids that were not updated with docver
     *
     * @param docver The current document version
     * @param documentIds The document ids to scan
     *
     * @return The set of document ids that were not updated with docver
     */
    public static Set<Object> getFailedUpdates(DBCollection collection,
                                               ObjectId docver,
                                               List<Object> documentIds) {
        Set<Object> failedIds=new HashSet<>();
        if(!documentIds.isEmpty()) {
            // documents with the given _ids and whose docver contains our docVer are the ones we managed to update
            // others are failures
            BasicDBObject query=new BasicDBObject(DOCVER_FLD,new BasicDBObject("$ne",docver));
            query.append("_id",new BasicDBObject("$in",documentIds));
            try (DBCursor cursor = collection.find(query,new BasicDBObject("_id",1))
                 .setReadPreference(ReadPreference.primary())) {
                while(cursor.hasNext()) {
                    failedIds.add(cursor.next().get("_id"));
                }
            }
        }
        return failedIds;
    }
}
