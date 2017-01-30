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
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteError;

import org.bson.types.ObjectId;

import com.redhat.lightblue.util.Error;

/**
 * Updates documents in a batch only if they are unmodified from the
 * version they were read.
 */
public class UpdateIfSameProtocol implements BatchUpdate {

    private static final Logger LOGGER=LoggerFactory.getLogger(MongoSafeUpdateProtocol.class);

    /**
     * The document version to be assigned to the documents modified by this updater
     */
    public final ObjectId docVer=new ObjectId();

    /**
     * Set of documents and their versions to be updated.
     */
    private final Set<DocIdVersion> versions=new HashSet<>();

    /**
     * A map with docId key, and its corresponding version the value
     */
    private final Map<Object,DocIdVersion> id2VersionMap=new HashMap<>();

    private final List<BatchDoc> batch=new ArrayList<>(128);
    private BulkWriteOperation bwo;
    private DBCollection collection;
    private WriteConcern writeConcern;

    private final static class BatchDoc {
        final DBObject doc;
        final DocIdVersion version;

        BatchDoc(DBObject doc,DocIdVersion v) {
            this.doc=doc;
            this.version=v;
        }
    }
    
    public UpdateIfSameProtocol(DBCollection collection,
                                WriteConcern writeConcern) {
        this.collection=collection;
        this.writeConcern=writeConcern;
        bwo=collection.initializeUnorderedBulkOperation();
    }

    public void addVersion(DocIdVersion v) {
        versions.add(v);
        id2VersionMap.put(v.id,v);
    }

    public void addVersions(Collection<DocIdVersion> collection) {
        for(DocIdVersion v:collection) {
            addVersion(v);
        }
    }
    
    @Override
    public void addDoc(DBObject doc) {
        BatchDoc batchDoc=null;
        // Is this document in the versions set? If not, we cannot update it
        Object id=DocTranslator.createIdFrom(doc.get("_id"));
        if(id!=null) {
            DocIdVersion v=id2VersionMap.get(id);
            if(v!=null) {
                batchDoc=new BatchDoc(doc,v);
            }
        }
        if(batchDoc!=null) {
            // Include this doc in batch
            DocVerUtil.cleanupOldDocVer(batchDoc.doc,docVer);
            DocVerUtil.setDocVer(batchDoc.doc,docVer);
            batch.add(batchDoc);
            DBObject query=new BasicDBObject("_id",doc.get("_id")).
                append(MongoSafeUpdateProtocol.DOCVER_FLD0,batchDoc.version.version);
            LOGGER.debug("replaceQuery={}",query);
            bwo.find(query).replaceOne(batchDoc.doc);                    
        }
    }


    @Override
    public int getSize() {
        return batch.size();
    }

    @Override
    public Map<Integer,Error> commit() {
        Map<Integer,Error> results=new HashMap<>();
        if(!batch.isEmpty()) {
            if(!BatchUpdate.batchUpdate(bwo,writeConcern,batch.size(),results,LOGGER))
                findConcurrentModifications(results);
        }
        batch.clear();
        bwo=collection.initializeUnorderedBulkOperation();
        return results;
    }
    
     /**
     * This executes a query to find out documents with concurrent modification errors
     *
     * Returns true if there are concurrent modification errors
     */
    protected void findConcurrentModifications(Map<Integer,Error> results) {
        List<Object> updatedIds=new ArrayList<>(batch.size());
        // Collect all ids without errors
        int index=0;
        for(BatchDoc doc:batch) {
            if(!results.containsKey(index))
                updatedIds.add(doc.version.id);
            index++;
        }
        LOGGER.debug("checking for concurrent modifications:{}",updatedIds);
        if(!updatedIds.isEmpty()) {
            Set<Object> failedIds=BatchUpdate.getFailedUpdates(collection,docVer,updatedIds);
            if(!failedIds.isEmpty()) {
                index=0;
                for(BatchDoc doc:batch) {
                    if(!results.containsKey(index)) { // No other errors for this id
                        if(failedIds.contains(doc.version.id)) {
                            // concurrency errors for this id
                            results.put(index,Error.get("update",MongoCrudConstants.ERR_CONCURRENT_UPDATE,doc.version.id.toString()));
                        }
                    }
                    index++;
                }
            }
        }
    }


}
