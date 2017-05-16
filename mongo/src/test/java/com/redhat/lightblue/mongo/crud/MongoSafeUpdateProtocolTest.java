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
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import com.redhat.lightblue.util.Error;

public class MongoSafeUpdateProtocolTest extends AbstractMongoCrudTest {

    private MongoSafeUpdateProtocol updater;

    private class TestUpdater extends MongoSafeUpdateProtocol {

        int numRetries=0;
        
        public TestUpdater(DBCollection coll) {
            super(coll,null,null);
        }
        
        protected DBObject reapplyChanges(int docIndex,DBObject doc) {
            numRetries++;
            System.out.println("Retrying");
            return doc;
        }
  }
    
    @Override
    @Before
    public void setup() throws Exception {
        super.setup();

        db.getCollection(COLL_NAME).drop();
        db.createCollection(COLL_NAME, null);
        coll=db.getCollection(COLL_NAME);
        updater=new TestUpdater(coll);
    }

    private void insertDoc(String id,String value) {
        coll.insert(new BasicDBObject("_id",id).append("field",value));
    }

    /**
     * insert 50 docs {_id:10,11,12..., field:field10,field11,field12,...}
     */
    private void insert50() {
        for(int i=0;i<50;i++) {
            insertDoc(Integer.toString(i+10),"field"+(i+10));
        }
    }
    
    @Test
    public void successfulUpdateTest() throws Exception {
        insert50();
        updater.getCfg().setFailureRetryCount(0);
        DBCursor cursor=coll.find();
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            updater.addDoc(doc);
            doc.put("field","updated"+doc.get("_id").toString());
            if(updater.getSize()>8)
                Assert.assertTrue(updater.commit().isEmpty());
        }
        cursor.close();
        
        Assert.assertTrue(updater.commit().isEmpty());
        cursor=coll.find();
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            String id=(String)doc.get("_id");
            Assert.assertEquals("updated"+id,doc.get("field").toString());
            Assert.assertNotNull(doc.get(DocTranslator.HIDDEN_SUB_PATH.toString()));
            Assert.assertEquals(1,((List)((DBObject)doc.get(DocTranslator.HIDDEN_SUB_PATH.toString())).get("docver")).size());
        }
        cursor.close();
    }

    @Test
    public void dupTest() throws Exception {
        // unique index on field
        coll.createIndex(new BasicDBObject("field",1),"fieldindex",true);
        insert50();
        updater.getCfg().setFailureRetryCount(0);
        DBCursor cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","19")));
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            // Create a dup value
            if(doc.get("_id").equals("15"))
                doc.put("field","field10");            
            updater.addDoc(doc);
        }
        cursor.close();
        Map<Integer,Error> err=updater.commit();
        Assert.assertEquals(1,err.size());
        Assert.assertTrue(err.containsKey(5));
        Assert.assertEquals(MongoCrudConstants.ERR_DUPLICATE,err.get(5).getErrorCode());        
    }

    @Test
    public void concurrentUpdateTest() throws Exception {
        insert50();

        updater.getCfg().setFailureRetryCount(0);
        // Thread1 reads first 10 docs
        DBCursor cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","19")));
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            doc.put("field","updated1"+doc.get("_id").toString());
            updater.addDoc(doc);
        }
        cursor.close();

        // Thread2 reads first 5 docs
        MongoSafeUpdateProtocol updater2=new TestUpdater(coll);
        updater2.getCfg().setFailureRetryCount(0);
        cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","15")));
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            doc.put("field","updated2"+doc.get("_id").toString());            
            updater2.addDoc(doc);
        }
        cursor.close();

        // Thread2 updates first 5 docs
        Map<Integer,Error> err=updater2.commit();
        Assert.assertTrue(err.isEmpty());

        err=updater.commit();
        Assert.assertEquals(6,err.size());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(0).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(1).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(2).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(3).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(4).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(5).getErrorCode());
    }
    
    @Test
    public void dupAndConcurrentUpdateTest() throws Exception {
        // unique index on field
        coll.createIndex(new BasicDBObject("field",1),"fieldindex",true);
        insert50();
        updater.getCfg().setFailureRetryCount(0);

        // Thread1 reads first 10 docs
        DBCursor cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","19")));
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            // Create a dup value
            if(doc.get("_id").equals("16"))
                doc.put("field","field19");
            else
                doc.put("field","updated1"+doc.get("_id").toString());
            updater.addDoc(doc);
        }
        cursor.close();

        // Thread2 reads first 5 docs
        MongoSafeUpdateProtocol updater2=new TestUpdater(coll);
        updater2.getCfg().setFailureRetryCount(0);
        cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","15")));
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            doc.put("field","updated2"+doc.get("_id").toString());            
            updater2.addDoc(doc);
        }
        cursor.close();

        // Thread2 updates first 5 docs
        Map<Integer,Error> err=updater2.commit();
        Assert.assertTrue(err.isEmpty());

        err=updater.commit();
        Assert.assertEquals(7,err.size());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(0).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(1).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(2).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(3).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(4).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(5).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_DUPLICATE,err.get(6).getErrorCode());
    }

    @Test
    public void retryTest() throws Exception {
        insert50();
        TestUpdater updater=new TestUpdater(coll) {
                protected DBObject reapplyChanges(int docIndex,DBObject doc) {
                    numRetries++;
                    System.out.println("Retrying");
                    BasicDBObject newDoc=new BasicDBObject();
                    newDoc.putAll(doc);
                    // Emulating merging behavior here. When we merge
                    // documents, we put references to missing items
                    // from the old document into the new
                    // document. One of these references is
                    // the @mongoHidden element at the root. This
                    // contains the document versions. So after
                    // merging, we have two copies of the same
                    // document, sharing the same instance of document
                    // versions.
                    newDoc.put("@mongoHidden",doc.get("@mongoHidden"));
                    newDoc.put("field","updated1"+doc.get("_id").toString());                    
                    return newDoc;
                }
            };
        updater.getCfg().setFailureRetryCount(1);

        // Thread1 reads first 10 docs
        DBCursor cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","19")));
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            // Create a dup value
            doc.put("field","updated1"+doc.get("_id").toString());
            updater.addDoc(doc);
        }
        cursor.close();

        // Thread2 reads first 5 docs
        MongoSafeUpdateProtocol updater2=new TestUpdater(coll);
        updater2.getCfg().setFailureRetryCount(0);
        cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","15")));
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            doc.put("field","updated2"+doc.get("_id").toString());            
            updater2.addDoc(doc);
        }
        cursor.close();
        

        // Thread2 updates first 5 docs
        Map<Integer,Error> err=updater2.commit();
        Assert.assertTrue(err.isEmpty());
        System.out.println("All docs:");
        cursor=coll.find();
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            System.out.println(doc);
        }
        cursor.close();

        err=updater.commit();
        Assert.assertEquals(0,err.size());

        // Check if the updates worked
        cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","19")));
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            String id=doc.get("_id").toString();
            Assert.assertEquals(doc.get("field"),"updated1"+id);
        }
        cursor.close();
    }

    @Test
    public void dupAndConcurrentUpdateTest2() throws Exception {
        // unique index on field
        coll.createIndex(new BasicDBObject("field",1),"fieldindex",true);
        insert50();
        updater.getCfg().setFailureRetryCount(0);

        // Thread1 reads first 10 docs
        DBCursor cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","19")));
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            // Create a dup value
            if(doc.get("_id").equals("13"))
                doc.put("field","field19");
            else
                doc.put("field","updated1"+doc.get("_id").toString());
            updater.addDoc(doc);
        }
        cursor.close();

        // Thread2 reads first 5 docs
        MongoSafeUpdateProtocol updater2=new TestUpdater(coll);
        updater2.getCfg().setFailureRetryCount(0);
        cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","15")));
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            doc.put("field","updated2"+doc.get("_id").toString());            
            updater2.addDoc(doc);
        }
        cursor.close();

        // Thread2 updates first 5 docs
        Map<Integer,Error> err=updater2.commit();
        Assert.assertTrue(err.isEmpty());

        err=updater.commit();
        Assert.assertEquals(6,err.size());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(0).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(1).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(2).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(3).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(4).getErrorCode());
        Assert.assertEquals(MongoCrudConstants.ERR_CONCURRENT_UPDATE,err.get(5).getErrorCode());
    }


    private abstract class Updater extends TestUpdater {
        public Updater() {
            super(coll);
        }

        protected boolean findConcurrentModifications(Map<Integer,Error> results) {
            intercept();
            return super.findConcurrentModifications(results);
        }

        public abstract void intercept();
    }
    
    @Test
    public void secondThreadUpdatesAfterFirst() throws Exception {

        // unique index on field
        coll.createIndex(new BasicDBObject("field",1),"fieldindex",true);
        insert50();
        // Modify records after thread1 updates, but before it finds out the failures
        Updater u=new Updater() {
                @Override public void intercept() {
                    // Thread2 reads first 5 docs
                    MongoSafeUpdateProtocol updater2=new TestUpdater(coll);
                    DBCursor cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","15")));
                    while(cursor.hasNext()) {
                        DBObject doc=cursor.next();
                        doc.put("field","updated2"+doc.get("_id").toString());            
                        updater2.addDoc(doc);
                    }
                    cursor.close();                   
                }
            }; 
        updater.getCfg().setFailureRetryCount(0);

        // Thread1 reads first 10 docs
        DBCursor cursor=coll.find(new BasicDBObject("_id",new BasicDBObject("$lte","19")));
        while(cursor.hasNext()) {
            DBObject doc=cursor.next();
            // Create a dup value
            if(doc.get("_id").equals("13"))
                doc.put("field","field19");
            else
                doc.put("field","updated1"+doc.get("_id").toString());
            u.addDoc(doc);
        }        
        cursor.close();
        Map<Integer,Error> errors=u.commit();
        Assert.assertEquals(1,errors.size());
        Assert.assertEquals(MongoCrudConstants.ERR_DUPLICATE,errors.get(3).getErrorCode());
        
    }
}

