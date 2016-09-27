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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Date;

import com.mongodb.ReadPreference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.WriteConcern;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteError;

import org.bson.types.ObjectId;

import com.redhat.lightblue.util.Error;

/**
 * This class implements a safe update protocol. Mongo updates are
 * read-update in memory-write algorithms, so during an update,
 * another thread can come and modify the doc. We would like to
 * prevent our updates overwriting the other thread's updates.
 *
 * To implement this, we add a 'docver' hidden field to all the
 * documents. The idea is to read the documents, perform the update in
 * memory, and replace the db document with the memory copy using _id
 * and only id dover is still the same. When done using bulk apis,
 * this scheme has a problem, becase there are two windows in which
 * another thread can modify the document, and we secure only one of
 * those windows. For example:
 * <pre>
 *      thread1:                        thread2:
 *  read: {_id:1,docver:a},
 *        {_id:2,docver:a}
 *
 *                                   read: {_id:1,docver:a}
 *                                   write: {_id:1,docver:a} update docver:b
 *
 *  write: {_id:1,docver:a} update docver:c,
 *         {_id:2,docver:a} update docver:c
 *    -> only 1 doc written, no doc with {_id:1,docver:a}
 *
 *                                   read: {_id:2,docver:c}
 *                                   write: {_id:2,docver:c} update docver:d
 *
 *  read: {_id:1,docver:b}
 *        {_id:2,docver:d}
 *    -> two modified documents, none with docver:c, but _id:1 has only thread2 modifications
 *       applied, but _id:2 has thread1 modifications applied, and then thread2.
 *       only _id:1 must be reported as update error
 *
 * </pre>
 *
 * If the document is modified by another thread, that update will not
 * appear as an update in the result. But, we do not know which
 * documents were failed to update this way, only the number of
 * documents actually updated. So, we read the database again to see
 * which docvers have been changed. Another thread may come an modify
 * the documents before this step but after the modifications are
 * applied, meaning we may find out more documents have been modified
 * than initially known. Here the question is which documents have our
 * modifications, and which don't. We would like to filter out the
 * documents that don't have our modifications, and mark them as
 * concurrent update failures, because any update performed after our
 * update is valid.
 *
 * This is important for incremental updates. Example:
 * <pre>
 *   concurrent update failure:
 *      thread1:                        thread2:
 *      read doc, ver=1, field=1
 *                                read doc, ver=1, field=1
 *      set field=2
 *      submit update with ver=2
 *                                set field=2
 *                                submit update with ver=2
 *                                perform db update
 *      update fails, ver!=1
 * </pre>
 *
 * In the above scenario, the increment operation for thread1 should
 * fail, because thread2 modified the document after thread1 read
 * it. This is returned as one missing document in the write result of
 * thread 1. At this point, thread1 doesn't know which document
 * failed, so it reads the db to find out:
 * <pre>
 *   thread1:                            thread2:
 *                                     modify another doc, ver=2, set ver=3
 *   read all docs with ver=2
 *   
 * </pre>
 *
 * At this point, thread1 sees two documents missing, instead of
 * one. The first document doesn't have thread1 updates, the second
 * document has thread1 updates, but thread2 applied its own updates
 * before thread1 can find out which document was modified. Returning
 * the second document as failed might result in the client retrying
 * the update, applying the changes twice,
 *
 * To prevent this scenario, we will keep the document versions in an
 * array, so we'll know which updates are applied to the document. To
 * prevent unbounded array growth, we will truncate the array using
 * the docver timestamp.
 *
 */



public abstract class MongoSafeUpdateProtocol {

    private static final Logger LOGGER=LoggerFactory.getLogger(MongoSafeUpdateProtocol.class);
    
    public static final String DOCVER="docver";

    public static final String DOCVER_FLD=Translator.HIDDEN_SUB_PATH.toString()+"."+DOCVER;
    public static final String DOCVER_FLD0=Translator.HIDDEN_SUB_PATH.toString()+"."+DOCVER+".0";

    private static final long TOO_OLD_MS=1l*60l*1000l; // Any docver older than 1 minute is to old

    private static final class BatchDoc {
        Object id;

        public BatchDoc(DBObject doc) {
            id=doc.get("_id");
        }
    }
    
    private final DBCollection collection;
    private final WriteConcern writeConcern;
    private ObjectId docVer;
    private BulkWriteOperation bwo;
    private final DBObject query;

    private List<BatchDoc> batch;
    private final ConcurrentModificationDetectionCfg cfg;

    /**
     * @param collection The DB collection
     * @param writeConcern Write concern to use. Can be null to use the default
     * @param cfg Concurrent modification detection options
     */
    public MongoSafeUpdateProtocol(DBCollection collection,
                                   WriteConcern writeConcern,
                                   ConcurrentModificationDetectionCfg cfg) {
        this(collection,writeConcern,null,cfg);
    }

    /**
     * @param collection The DB collection
     * @param writeConcern Write concern to use. Can be null to use the default
     * @param query The update query, if this is an update call
     * @param cfg Concurrent modification detection options
     */
    public MongoSafeUpdateProtocol(DBCollection collection,
                                   WriteConcern writeConcern,
                                   DBObject query,
                                   ConcurrentModificationDetectionCfg cfg) {
        this.collection=collection;
        this.writeConcern=writeConcern;
        this.cfg=cfg==null?new ConcurrentModificationDetectionCfg(null):cfg;
        this.query=query;
        reset();
    }

    public ConcurrentModificationDetectionCfg getCfg() {
        return cfg;
    }


    /**
     * Override this method to define how to deal with retries
     *
     * This is called when a concurrent modification is detected, and
     * the failed document is reloaded. The implementation should
     * reapply the changes to the new version of the document. 
     */
    protected abstract DBObject reapplyChanges(int docIndex,DBObject doc);


    /**
     * Adds a document to the current batch. The document should
     * contain the original docver as read from the db
     */
    public void addDoc(DBObject doc) {
        DBObject q=writeReplaceQuery(doc);
        cleanupOldDocVer(doc);
        setDocVer(doc,docVer);
        LOGGER.debug("replaceQuery={}",q);
        bwo.find(q).replaceOne(doc);        
        batch.add(new BatchDoc(doc));
    }

    /**
     * Returns the number of queued requests
     */
    public int getSize() {
        return batch.size();
    }

    /**
     * Commits the current batch, and prepares for the next
     * iteration. If any update operations fail, this call will detect
     * errors and associate them with the documents using the document
     * index.
     */
    public Map<Integer,Error> commit() {
        Map<Integer,Error> results=new HashMap<>();
        if(!batch.isEmpty()) {
            BulkWriteResult writeResult;
            LOGGER.debug("attemptToUpdate={}",batch.size());
            try {
                if(writeConcern==null) {
                    writeResult=bwo.execute();
                } else {
                    writeResult=bwo.execute(writeConcern);
                }
                LOGGER.debug("writeResult={}",writeResult);
                if(batch.size()==writeResult.getMatchedCount()) {
                    LOGGER.debug("Successful update");
                } else {
                    LOGGER.warn("notUpdated={}",batch.size()-writeResult.getMatchedCount());
                    findConcurrentModifications(results);
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
                findConcurrentModifications(results);
            }
        }
        retryConcurrentUpdateErrorsIfNeeded(results);
        reset();
        return results;
    }

    public void retryConcurrentUpdateErrorsIfNeeded(Map<Integer,Error> results) {
        int nRetries=cfg.getFailureRetryCount();
        while(nRetries-->0) {
            // Get the documents with concurrent modification errors
            List<Integer> failedDocs=getFailedDocIndexes(results);
            if(!failedDocs.isEmpty()) {
                failedDocs=retryFailedDocs(failedDocs,results);
            } else {
                break;
            }
        }
    }


    private List<Integer> retryFailedDocs(List<Integer> failedDocs,Map<Integer,Error> results) {
        List<Integer> newFailedDocs=new ArrayList<>(failedDocs.size());
        for(Integer index:failedDocs) {            
            BatchDoc doc=batch.get(index);
            // Read the doc
            DBObject findQuery=new BasicDBObject("_id",doc.id);
            if(cfg.isReevaluateQueryForRetry()) {
                if(query!=null) {
                    List<DBObject> list=new ArrayList<>(2);
                    list.add(findQuery);
                    list.add(query);
                    findQuery=new BasicDBObject("$and",list);
                }
            }
            DBObject updatedDoc=collection.findOne(findQuery);
            DBObject newDoc=reapplyChanges(index,updatedDoc);
            if(newDoc!=null) {
                // Update the doc ver to our doc ver. This doc is here
                // because its docVer is not set to our docver, so
                // this is ok
                setDocVer(newDoc,docVer);
                // Using bulkwrite here with one doc to use the
                // findAndReplace API, which is lacking in
                // DBCollection
                BulkWriteOperation nestedBwo=collection.initializeUnorderedBulkOperation();
                nestedBwo.find(writeReplaceQuery(updatedDoc)).replaceOne(newDoc);
                try {
                    if(nestedBwo.execute().getMatchedCount()==1) {
                        // Successful update
                        results.remove(index);
                    }
                } catch(Exception e) {
                    newFailedDocs.add(index);
                }
            } else {
                // reapllyChanges removed the doc from the resultset
                results.remove(index);
            }
        }
        return newFailedDocs;
    }

    /**
     * Returns a list of indexes into the current batch containing the docs that failed because of concurrent update errors
     */    
    private List<Integer> getFailedDocIndexes(Map<Integer,Error> results) {
        List<Integer> ret=new ArrayList<>(results.size());
        for(Map.Entry<Integer,Error> entry:results.entrySet()) {
            if(entry.getValue().getErrorCode().equals(MongoCrudConstants.ERR_CONCURRENT_UPDATE)) {
                ret.add(entry.getKey());
            }
        }
        return ret;
    }

    /**
     * This executes a query to find out documents with concurrent modification errors
     *
     * Returns true if there are concurrent modification errors
     */
    protected boolean findConcurrentModifications(Map<Integer,Error> results) {
        boolean ret=false;
        if(!cfg.isDetect())
            return ret;
        
        List<Object> updatedIds=new ArrayList<>(batch.size());
        // Collect all ids without errors
        int index=0;
        for(BatchDoc doc:batch) {
            if(!results.containsKey(index))
                updatedIds.add(doc.id);
            index++;
        }
        LOGGER.debug("checking for concurrent modifications:{}",updatedIds);
        if(!updatedIds.isEmpty()) {
            // documents with the given _ids and whose docver contains our docVer are the ones we managed to update
            // others are failures
            BasicDBObject query=new BasicDBObject(DOCVER_FLD,docVer);
            query.append("_id",new BasicDBObject("$in",updatedIds));
            try (DBCursor cursor = collection.find(query,new BasicDBObject("_id",1))
                    .setReadPreference(ReadPreference.primaryPreferred())) {
                Set<Object> successfulIds=new HashSet<>(updatedIds.size());
                while(cursor.hasNext()) {
                    DBObject obj=cursor.next();
                    Object id=obj.get("_id");
                    successfulIds.add(id);
                }
                index=0;
                for(BatchDoc doc:batch) {
                    if(!results.containsKey(index)) { // No other errors for this id
                        if(!successfulIds.contains(doc.id)) {
                            // concurrency errors for this id
                            results.put(index,Error.get("update",MongoCrudConstants.ERR_CONCURRENT_UPDATE,doc.id.toString()));
                            ret=true;
                        }
                    }
                    index++;
                }
            }
        }
        return ret;
    }

    /**
     * Writes a replace query, which is:
     * <pre>
     *  {_id:<docId>, "@mongoHidden.docver.0":ObjectId(<originalDocver>)}
     * </pre>
     */
    private DBObject writeReplaceQuery(DBObject doc) {
        DBObject hidden=getHidden(doc,false);
        ObjectId top=null;
        if(hidden!=null) {
            List<ObjectId> list=(List<ObjectId>)hidden.get(DOCVER);
            if(list!=null&&!list.isEmpty())
                top=list.get(0);
        }
        BasicDBObject query=new BasicDBObject("_id",doc.get("_id"));
        if(cfg.isDetect())
            query.append(DOCVER_FLD0,top);
        return query;
    }
    

    /**
     * Returns the @mongoHidden at the root level of the document. Adds one if necessary.
     */
    public static DBObject getHidden(DBObject doc,boolean addIfNotFound) {
        DBObject hidden=(DBObject)doc.get(Translator.HIDDEN_SUB_PATH.toString());
        if(hidden==null&&addIfNotFound) {
            doc.put(Translator.HIDDEN_SUB_PATH.toString(),hidden=new BasicDBObject());            
        }
        return hidden;
    }

    public static void overwriteDocVer(DBObject doc,ObjectId docver) {
        getHidden(doc,true).removeField(DOCVER);
        setDocVer(doc,docver);
    }
    
    public static void setDocVer(DBObject doc,ObjectId docver) {
        DBObject hidden=getHidden(doc,true);
        List<ObjectId> list=(List<ObjectId>)hidden.get(DOCVER);
        if(list==null) {
            list=new ArrayList<ObjectId>();
        }
        list.add(0,docver);
        hidden.put(DOCVER,list);
    }

    public static void copyDocVer(DBObject destDoc,DBObject sourceDoc) {
        DBObject hidden=getHidden(sourceDoc,false);
        if(hidden!=null) {
            destDoc.put(Translator.HIDDEN_SUB_PATH.toString(),hidden);
        }
    }

    /**
     * Removes old docvers from a document
     */
    private void cleanupOldDocVer(DBObject doc) {
        DBObject hidden=getHidden(doc,false);
        if(hidden!=null) {
            List<ObjectId> list=(List<ObjectId>)hidden.get(DOCVER);
            if(list!=null) {
                List<ObjectId> copy=new ArrayList<>(list.size());
                long now=docVer.getDate().getTime();
                for(ObjectId id:list) {
                    if(!id.equals(docVer)) {
                        Date d=id.getDate();
                        if(now-d.getTime()<TOO_OLD_MS) {
                            copy.add(id);
                        }
                    } else {
                        copy.add(id);
                    }
                }
                if(copy.size()!=list.size()) {
                    hidden.put(DOCVER,copy);
                    LOGGER.debug("cleanupInput={}, cleanupOutput={}",list,copy);
                }
            }
        }
    }

    private void reset() {
        docVer=new ObjectId();
        bwo=collection.initializeUnorderedBulkOperation();
        batch=new ArrayList<>(128);
    }
}
