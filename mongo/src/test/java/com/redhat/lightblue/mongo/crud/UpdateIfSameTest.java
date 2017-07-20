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
import java.util.Arrays;
import java.util.ArrayList;

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
import com.mongodb.WriteConcern;

import org.bson.types.ObjectId;

import com.redhat.lightblue.util.Error;

public class UpdateIfSameTest extends AbstractMongoCrudTest {

    private UpdateIfSameProtocol updater;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();

        db.getCollection(COLL_NAME).drop();
        db.createCollection(COLL_NAME, null);
        coll=db.getCollection(COLL_NAME);
        updater=new UpdateIfSameProtocol(coll,WriteConcern.ACKNOWLEDGED);
    }

    private BasicDBObject getDoc(String id,String value,ObjectId...versions) {
        BasicDBObject doc=new BasicDBObject("_id",id).append("field",value);
        if(versions!=null&&versions.length>0) {
            doc.append(DocTranslator.HIDDEN_SUB_PATH.toString(),
                       new BasicDBObject(DocVerUtil.DOCVER,new ArrayList<ObjectId>(Arrays.asList(versions))));
        }
        return doc;
    }
    
    @Test
    public void successfulUpdateTest() throws Exception {

        DBObject doc;

        doc=getDoc("1","1",new ObjectId());
        DocIdVersion ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());

        doc=getDoc("2","2",new ObjectId());
        ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());
        
        doc=getDoc("3","3",new ObjectId());
        ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());
        
        doc=getDoc("4","4",new ObjectId());
        ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());
        
        Assert.assertTrue(updater.commit().errors.isEmpty());
        Assert.assertTrue(updater.commit().lostDocs.isEmpty());

        DBCursor cursor=coll.find();
        while(cursor.hasNext()) {
            doc=cursor.next();
            Assert.assertEquals("updated"+doc.get("_id").toString(),doc.get("field").toString());
            Assert.assertEquals(updater.docVer.toString(),((List<ObjectId>)((DBObject)doc.get(DocTranslator.HIDDEN_SUB_PATH.toString())).
                                                           get(DocVerUtil.DOCVER)).get(0).toString());
        }
        cursor.close();
        
    }

    @Test
    public void dontUpdateIfVersionUnknown() throws Exception {

        DBObject doc;

        doc=getDoc("1","1",new ObjectId());
        DocIdVersion ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());

        doc=getDoc("2","2",new ObjectId());
        ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());
        
        doc=getDoc("3","3",new ObjectId());
        ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());

        // Id:4 will not be updated, its version is unknown
        doc=getDoc("4","4",new ObjectId());
        ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());
        
        Assert.assertTrue(updater.commit().errors.isEmpty());
        Assert.assertTrue(updater.commit().lostDocs.isEmpty());

        DBCursor cursor=coll.find();
        while(cursor.hasNext()) {
            doc=cursor.next();
            if(doc.get("_id").toString().equals("4")) {
                Assert.assertEquals("4",doc.get("field").toString());
            } else {
                Assert.assertEquals("updated"+doc.get("_id").toString(),doc.get("field").toString());
                Assert.assertEquals(updater.docVer.toString(),((List<ObjectId>)((DBObject)doc.get(DocTranslator.HIDDEN_SUB_PATH.toString())).
                                                               get(DocVerUtil.DOCVER)).get(0).toString());
            }
        }
        cursor.close();
        
    }

    @Test
    public void nullIsValidVersion() throws Exception {

        DBObject doc;

        doc=getDoc("1","1");
        DocIdVersion ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());

        doc=getDoc("2","2",new ObjectId());
        ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());
        
        doc=getDoc("3","3",new ObjectId());
        ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());
        
        Assert.assertTrue(updater.commit().errors.isEmpty());
        Assert.assertTrue(updater.commit().lostDocs.isEmpty());

        DBCursor cursor=coll.find();
        while(cursor.hasNext()) {
            doc=cursor.next();
            Assert.assertEquals("updated"+doc.get("_id").toString(),doc.get("field").toString());
            Assert.assertEquals(updater.docVer.toString(),((List<ObjectId>)((DBObject)doc.get(DocTranslator.HIDDEN_SUB_PATH.toString())).
                                                           get(DocVerUtil.DOCVER)).get(0).toString());
        }
        cursor.close();
        
    }

    @Test
    public void concurrentModification() throws Exception {

        DBObject doc;

        doc=getDoc("1","1",new ObjectId());
        DocIdVersion ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());

        ObjectId id2=new ObjectId();
        doc=getDoc("2","2",id2);
        ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());
        
        doc=getDoc("3","3",new ObjectId());
        ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());

        doc=getDoc("4","4",new ObjectId());
        ver=DocIdVersion.getDocumentVersion(doc);
        coll.insert(doc);
        updater.addVersion(ver);
        updater.addDoc(doc);
        doc.put("field","updated"+doc.get("_id").toString());

        // Modify id:2
        // simulate an update
        ArrayList<ObjectId> elems=new ArrayList<>();
        elems.add(new ObjectId());
        elems.add(id2);
        coll.update(new BasicDBObject("_id","2"),
                    new BasicDBObject("$set",new BasicDBObject(DocTranslator.HIDDEN_SUB_PATH.toString()+"."+DocVerUtil.DOCVER,elems)));
        

        Map<Integer,Error> errors=updater.commit().errors;
        Assert.assertEquals(1,errors.size());
        Error e=errors.get(1);
        Assert.assertNotNull(e);

        DBCursor cursor=coll.find();
        while(cursor.hasNext()) {
            doc=cursor.next();
            if(doc.get("_id").toString().equals("2")) {
                Assert.assertEquals("2",doc.get("field").toString());
            } else {
                Assert.assertEquals("updated"+doc.get("_id").toString(),doc.get("field").toString());
                Assert.assertEquals(updater.docVer.toString(),((List<ObjectId>)((DBObject)doc.get(DocTranslator.HIDDEN_SUB_PATH.toString())).
                                                               get(DocVerUtil.DOCVER)).get(0).toString());
            }
        }
        cursor.close();
        
    }

}

