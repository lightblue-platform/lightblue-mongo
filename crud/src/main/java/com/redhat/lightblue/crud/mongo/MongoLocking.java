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
package com.redhat.lightblue.crud.mongo;

import java.util.Date;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import com.mongodb.DBObject;

import com.redhat.lightblue.extensions.synch.InvalidLockException;

public class MongoLocking {

    public static final String CALLERID="own";
    public static final String RESOURCEID="rsc";
    public static final String TIMESTAMP="t";
    public static final String TTL="ttl";
    public static final String EXPIRATION="exp";
    public static final String COUNT="n";
    public static final String VERSION="ver";

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoLocking.class);
   
    private DBCollection coll;
    private long defaultTTL=60l*60l*1000l;// 1 hr

    public MongoLocking(DBCollection coll) {
        init(coll);
    }

    public void init(DBCollection coll) {
        // Make sure we have a unique index on resourceid
        this.coll=coll;
        BasicDBObject keys=new BasicDBObject(RESOURCEID,1);
        BasicDBObject options=new BasicDBObject("unique",1);
        this.coll.ensureIndex(keys,options);
    }

    public void setDefaultTTL(long l) {
        defaultTTL=l;
    }

    /**
     * Attempts to insert a lock record to the db
     *
     * @returns true if successful, false if lock already exists. Any other case would be an exception.
     */
    private boolean acquire(String callerId,String resourceId, Long ttl, Date now, Date expiration) {
        BasicDBObject update=new BasicDBObject().
            append(CALLERID,callerId).
            append(RESOURCEID,resourceId).
            append(TIMESTAMP,now).
            append(TTL, ttl).
            append(EXPIRATION,expiration).
            append(COUNT,1).
            append(VERSION,1);

        try {
            LOGGER.debug("insert: {}",update);
            coll.insert(update,WriteConcern.SAFE);
        } catch (MongoException.DuplicateKey e) {
            return false;
        }
        return true;
    }

    /**
     * Attempt to acquire a lock. If successful, return true, otherwise return false.
     */
    public boolean acquire(String callerId,String resourceId,Long ttl) {
        /*
          Creating an atomic acquire() method in mongodb is not
          easy. The key is to use the uniqueness of a unique index, in
          this case, a unique index on resourceId. There can be at
          most one lock record for a resource in the db at any given
          time. 

          The insertion operation is atomic: if it is successful, we
          acquire the lock. We assume the update operation is
          transactional: once a document is found to be matching to
          the query of the update operation, it is locked, and no
          other caller can modify that document until our update
          operation is complete.

          We will use a version number to make sure we are updating
          the correct doc.
         */
        LOGGER.debug("acquire({}/{},ttl={})",callerId,resourceId,ttl);
        // Try to insert doc
        Date now=new Date();
        Date expiration;
        if(ttl==null)
            ttl=defaultTTL;
        expiration=new Date(now.getTime()+ttl);
        LOGGER.debug("{}/{}: lock will expire on {}",callerId,resourceId,expiration);
        BasicDBObject query;
        BasicDBObject update;
        WriteResult wr;
        int readVer=-1;
        String readCallerId=null;
        int readCount=-1;
        boolean locked=acquire(callerId,resourceId,ttl,now,expiration);
        if(!locked) {
            // At this point, we can add "if expired" predicate to the
            // queries to filter expired locks, but it is not safe to
            // rely on timestamps. Not all nodes have the same
            // timestamp, and any node can wait an arbitrary amount of
            // time at any point. So, we read the existing lock at
            // this point, and use the version number for all the
            // updates. if anybody updates the lock before we do, the
            // version number will change, and we will fail.
            query=new BasicDBObject(RESOURCEID,resourceId);
            LOGGER.debug("find: {}",query);
            DBObject lockObject=coll.findOne(query);
            if(lockObject==null) {
                LOGGER.debug("{}/{}: lock cannot be read. Retrying to acquire",callerId,resourceId);
                locked=acquire(callerId,resourceId,ttl,now,expiration);
                LOGGER.debug("{}/{}: acquire result: {}",callerId,resourceId,locked);
                // No need to continue here. If insertion fails, that means someone else inserted a record
                return locked;
            }
            readVer=((Number)lockObject.get(VERSION)).intValue();
            readCallerId=(String)lockObject.get(CALLERID);
            readCount=((Number)lockObject.get(COUNT)).intValue();
            
            // Lock already exists
            // Possibilities:
            //  - lock is not expired, but ours : increment count
            //  - lock is not expired, but someone else owns it : fail
            //  - lock is expired : attempt to acquire
            //  - lock count is less than 1 : attempt to acquire

            // lock is not expired and we own it: increment lock count
            LOGGER.debug("{}/{} locked, assuming lock is ours, attempting to increment lock count",callerId,resourceId);
            if(readCallerId.equals(callerId)) {
                query=new BasicDBObject().
                    append(CALLERID,callerId).
                    append(RESOURCEID,resourceId).
                    append(EXPIRATION,new BasicDBObject("$gt",now)).
                    append(VERSION,readVer);
                update=new BasicDBObject().
                    append("$set",new BasicDBObject(TIMESTAMP,now).
                           append(EXPIRATION,expiration).
                           append(TTL,ttl)).
                    append("$inc",new BasicDBObject(VERSION,1).
                           append(COUNT,1));
                LOGGER.debug("update: {} {}",query,update);
                wr=coll.update(query,update,false,false,WriteConcern.SAFE);
                if(wr.getN()==1) {
                    LOGGER.debug("{}/{} locked again",callerId,resourceId);
                    locked=true;
                }
            }
        }
        if(!locked) {
            // assume lock is expired or count <=0, and try to acquire it
            LOGGER.debug("{}/{} lock is expired or count <= 0, attempting to reacquire expired lock", callerId, resourceId);
            query=new BasicDBObject().
                append(RESOURCEID,resourceId).
                append("$or",Arrays.asList(new BasicDBObject(EXPIRATION,new BasicDBObject("$lte",now)),
                                           new BasicDBObject(COUNT,new BasicDBObject("$lte",0)))).
                append(VERSION,readVer);
            update=new BasicDBObject().
                append("$set",new BasicDBObject(CALLERID,callerId).
                       append(TIMESTAMP,now).
                       append(EXPIRATION,expiration).
                       append(TTL,ttl).
                       append(COUNT,1)).
                append("$inc",new BasicDBObject(VERSION,1));
            LOGGER.debug("update: {} {}",query,update);
            wr=coll.update(query,update,false,false,WriteConcern.SAFE);
            if(wr.getN()==1) {
                LOGGER.debug("{}/{} locked",callerId,resourceId);
                locked=true;
            }
        }
        LOGGER.debug("{}/{}: {}",callerId,resourceId,locked?"locked":"not locked");
        return locked;
    }

    /**
     * Release the lock. Returns true if the lock is released by this call
     */
    public boolean release(String callerId,String resourceId) {
        LOGGER.debug("release({}/{})",callerId,resourceId);
        Date now=new Date();
        // If lock count is only one, we can remove the lock
        BasicDBObject query=new BasicDBObject().
            append(CALLERID,callerId).
            append(RESOURCEID,resourceId).
            append(EXPIRATION,new BasicDBObject("$gt",now)).
            append(COUNT,1);
        LOGGER.debug("remove {}",query);
        WriteResult wr=coll.remove(query,WriteConcern.SAFE);
        if(wr.getN()==1) {
            LOGGER.debug("{}/{} released",callerId,resourceId);
            return true;
        }
        // Retrieve the lock
        query=new BasicDBObject(RESOURCEID,resourceId).
            append(CALLERID,callerId);
        DBObject lock=coll.findOne(query);
        if(lock!=null) {
            long ttl=((Number)lock.get(TTL)).longValue();
            Date expiration=new Date(now.getTime()+ttl);
            // Try decrementing the lock count of our lock
            query=new BasicDBObject().
                append(CALLERID,callerId).
                append(RESOURCEID,resourceId).
                append(EXPIRATION,new BasicDBObject("$gt",now)).
                append(COUNT,new BasicDBObject("$gt",0));
            BasicDBObject update=new BasicDBObject().
                append("$set",new BasicDBObject(EXPIRATION,expiration).
                       append(TTL,ttl).
                       append(TIMESTAMP,now)).
                append("$inc",new BasicDBObject(COUNT,-1).
                       append(VERSION,1));
            wr=coll.update(query,update,false,false,WriteConcern.SAFE);
            if(wr.getN()==1) {
                LOGGER.debug("{}/{} lock count decremented, still locked",callerId,resourceId);
                return false;
            }
        }
        // Both attempts failed, Lock is no longer owned by us
        throw new InvalidLockException(resourceId);
    }

    public int getLockCount(String callerId,String resourceId) {
        Date now=new Date();
        BasicDBObject q=new BasicDBObject().
            append(CALLERID,callerId).
            append(RESOURCEID,resourceId).
            append(EXPIRATION,new BasicDBObject("$gt",now)).
            append(COUNT,new BasicDBObject("$gt",0));
        BasicDBObject field=new BasicDBObject(COUNT,1);
        DBObject lock=coll.findOne(q,field);
        if(lock!=null) {
            int cnt=((Number)lock.get(COUNT)).intValue();
            LOGGER.debug("{}/{} lockCount={}",callerId,resourceId,cnt);
            return cnt;
        } else
            throw new InvalidLockException(resourceId);
    }

    public void ping(String callerId,String resourceId) {
        Date now=new Date();
        BasicDBObject q=new BasicDBObject().
            append(CALLERID,callerId).
            append(RESOURCEID,resourceId).
            append(EXPIRATION,new BasicDBObject("$gt",now)).
            append(COUNT,new BasicDBObject("$gt",0));
        DBObject lock=coll.findOne(q);
        if(lock!=null) {
            Date expiration=new Date(now.getTime()+((Number)lock.get(TTL)).longValue());
            BasicDBObject update=new BasicDBObject().
                append("$set",new BasicDBObject(TIMESTAMP,now).
                       append(EXPIRATION,expiration));
            WriteResult wr=coll.update(q,update,false,false,WriteConcern.SAFE);
            if(wr.getN()!=1)
                throw new InvalidLockException(resourceId);
            LOGGER.debug("{}/{} pinged",callerId,resourceId);
        } else
            throw new InvalidLockException(resourceId);
    }

}
