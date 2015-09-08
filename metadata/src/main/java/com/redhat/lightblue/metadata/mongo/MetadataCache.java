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
package com.redhat.lightblue.metadata.mongo;

import java.util.Map;
import java.util.HashMap;

import java.lang.ref.WeakReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

import com.redhat.lightblue.EntityVersion;

import com.redhat.lightblue.metadata.EntityMetadata;

public class MetadataCache {
    
    private static Logger LOGGER=LoggerFactory.getLogger(MetadataCache.class);

    private static final String LITERAL_COLL_VER="collectionVersion";

    /**
     * This is the collection version number we expect to see in the
     * database. If this doesn't match the value in db, someone
     * updated metadata, we refresh
     */
    private long expectedCollectionVersion;

    /**
     * Last time we retrieved collection version
     */
    private volatile long lastVersionLookupTime=0l;

    /**
     * The collection version lookup period
     */
    private long versionLookupPeriodMsecs=10l*1000l;

    /**
     * Cache clear period
     */
    private long cacheTTLMsecs=10l*60l*1000l;

    /**
     * Last time we refreshed cache
     */
    private volatile long lastCacheRefreshTime=0l;
    
    private final Map<EntityVersion,WeakReference<EntityMetadata>> cache=new HashMap<>();

    /**
     * Sets cache parameters. If null is passed, that parameter is not changed.
     */
    public void setCacheParams(Long versionLookupPeriodMsecs,
                               Long cacheTTLMsecs) {
        if(versionLookupPeriodMsecs!=null)
            this.versionLookupPeriodMsecs=versionLookupPeriodMsecs;
        if(cacheTTLMsecs!=null)
            this.cacheTTLMsecs=cacheTTLMsecs;
    }
    
    public EntityMetadata lookup(DBCollection collection,String entityName,String version) {
        EntityMetadata md;
        EntityVersion v=new EntityVersion(entityName,version);
        WeakReference<EntityMetadata> ref=cache.get(v);
        if(ref!=null)
            md=ref.get();
        else
            md=null;

        long now=System.currentTimeMillis();
        if(lastCacheRefreshTime+cacheTTLMsecs<now)
            fullRefresh(collection,now);
        else if(lastVersionLookupTime+versionLookupPeriodMsecs<now) 
            refreshCollectionVersion(collection,now,false);
        
        return md;
    }
    
    public synchronized void put(EntityMetadata md) {
        cache.put(new EntityVersion(md.getName(),md.getVersion().getValue()),new WeakReference(md));
    }
    
    /**
     * Update the collection version in db, and invalidate cache
     */
    public synchronized void updateCollectionVersion(DBCollection collection) {
        BasicDBObject query=new BasicDBObject(MongoMetadata.LITERAL_ID,LITERAL_COLL_VER);
        BasicDBObject update=new BasicDBObject("$inc",new BasicDBObject(LITERAL_COLL_VER,1));
        int nUpdated;
        try {
            WriteResult r=collection.update(query,update);
            nUpdated=r.getN();
        } catch(Exception e) {
            nUpdated=0;
        }
        if(nUpdated==0) {
            // Try to ins
            BasicDBObject doc=new BasicDBObject(MongoMetadata.LITERAL_ID,LITERAL_COLL_VER);
            doc.put(LITERAL_COLL_VER,0l);
            try {
                collection.insert(doc);
            } catch (Exception e) {}
        }
        cache.clear();
    }
    
    /**
     * Load the cache version from the db.
     */
    private synchronized Long loadCacheVersion(DBCollection collection) {
        BasicDBObject query=new BasicDBObject("_id","collectionVersion");
        DBObject obj=collection.findOne(query);
        if(obj==null) {
            updateCollectionVersion(collection);
            obj=collection.findOne(query);
            if(obj==null) {
                // Leave it uninitialized
                LOGGER.error("Cannot initialize metadata cache");
            }
        }
        if(obj!=null) {
            return (Long)obj.get("collectionVersion");
        } else {
            return null;
        }
    }

    private synchronized void fullRefresh(DBCollection collection,long now) {
        if(lastCacheRefreshTime+cacheTTLMsecs<now) {
            if(!refreshCollectionVersion(collection,now,true))
                cache.clear();
            lastCacheRefreshTime=now;
        }
    }
                        

    /**
     * Refreshes the collection version. Returns true if the cache is invalidated
     */
    private synchronized boolean refreshCollectionVersion(DBCollection collection,long now,boolean bypassRecheck) {
        // Re-check if one of the timers really expired. One of the
        // other threads might have already initialized it
        boolean ret=false;
        if(bypassRecheck||lastVersionLookupTime+versionLookupPeriodMsecs<now) {
            Long v=loadCacheVersion(collection);
            if(v!=null) {
                if(v!=expectedCollectionVersion) {
                    cache.clear();
                    expectedCollectionVersion=v;
                    ret=true;
                }
                lastVersionLookupTime=now;
            }
        }
        return ret;
    }
}
