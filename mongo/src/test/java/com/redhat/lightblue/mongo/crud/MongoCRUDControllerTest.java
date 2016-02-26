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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.redhat.lightblue.crud.CRUDDeleteResponse;
import com.redhat.lightblue.crud.CRUDInsertionResponse;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.CRUDFindResponse;
import com.redhat.lightblue.crud.CRUDSaveResponse;
import com.redhat.lightblue.crud.CRUDUpdateResponse;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.metadata.CompositeMetadata;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.FieldConstraint;
import com.redhat.lightblue.metadata.FieldCursor;
import com.redhat.lightblue.metadata.Index;
import com.redhat.lightblue.metadata.MetadataStatus;
import com.redhat.lightblue.metadata.ObjectField;
import com.redhat.lightblue.metadata.SimpleField;
import com.redhat.lightblue.metadata.Version;
import com.redhat.lightblue.metadata.constraints.IdentityConstraint;
import com.redhat.lightblue.metadata.types.IntegerType;
import com.redhat.lightblue.metadata.types.StringType;
import com.redhat.lightblue.mongo.common.DBResolver;
import com.redhat.lightblue.mongo.common.MongoDataStore;
import com.redhat.lightblue.mongo.config.MongoConfiguration;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.SortKey;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.Path;

public class MongoCRUDControllerTest extends AbstractMongoCrudTest {

    private MongoCRUDController controller;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();

        final DB dbx = db;
        // Cleanup stuff
        dbx.getCollection(COLL_NAME).drop();
        dbx.createCollection(COLL_NAME, null);

        controller = new MongoCRUDController(null,new DBResolver() {
                @Override
                public DB get(MongoDataStore store) {
                    return dbx;
                }
                @Override
                public MongoConfiguration getConfiguration(MongoDataStore store) {return null;}
      });
    }

    @Test
    public void insertTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);
        System.out.println(ctx.getDataErrors());
        Assert.assertEquals(1, ctx.getDocuments().size());
        Assert.assertTrue(ctx.getErrors() == null || ctx.getErrors().isEmpty());
        Assert.assertTrue(ctx.getDataErrors() == null || ctx.getDataErrors().isEmpty());
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), response.getNumInserted());
        String id = ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();
        try (DBCursor c = coll.find(new BasicDBObject("_id", Translator.createIdFrom(id)))) {
            Assert.assertEquals(1, c.count());
        }
    }

    @Test
    public void insertTest_nullReqField() throws Exception {
        EntityMetadata md = getMd("./testMetadata-requiredFields2.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        doc.modify(new Path("field1"),null,false);
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);
        System.out.println(ctx.getDataErrors());
        Assert.assertEquals(1, ctx.getDocuments().size());
        Assert.assertTrue(ctx.getErrors() == null || ctx.getErrors().isEmpty());
        Assert.assertTrue(ctx.getDataErrors() == null || ctx.getDataErrors().isEmpty());
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), response.getNumInserted());
        String id = ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();
        try (DBCursor c = coll.find(new BasicDBObject("_id", Translator.createIdFrom(id)))) {
            Assert.assertEquals(1, c.count());
        }
    }


    @Test @Ignore
    public void insertTest_empty_array() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata_empty_array.json"));
        Projection projection = projection("{'field':'field7'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);
        System.out.println(ctx.getDataErrors());
        Assert.assertEquals(1, ctx.getDocuments().size());
        Assert.assertTrue(ctx.getErrors() == null || ctx.getErrors().isEmpty());
        Assert.assertTrue(ctx.getDataErrors() == null || ctx.getDataErrors().isEmpty());
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), response.getNumInserted());
        JsonNode field7Node = ctx.getDocuments().get(0).getOutputDocument().get(new Path("field7"));
        Assert.assertNotNull("empty array was not inserted", field7Node);
        Assert.assertTrue("field7 should be type ArrayNode", field7Node instanceof ArrayNode);
        String field7 = field7Node.asText();
        Assert.assertNotNull("field7");
    }

    @Test
    public void insertTest_nullProjection() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        Projection projection = null;
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

        // basic checks
        Assert.assertEquals(1, ctx.getDocuments().size());
        Assert.assertTrue(ctx.getErrors() == null || ctx.getErrors().isEmpty());
        Assert.assertTrue(ctx.getDataErrors() == null || ctx.getDataErrors().isEmpty());
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), response.getNumInserted());

        // verify there is nothing projected
        Assert.assertNotNull(ctx.getDocuments().get(0).getOutputDocument());
    }

    @Test
    public void saveTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        System.out.println("Write doc:" + doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);
        String id = ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
                projection("{'field':'*','recursive':1}"), null, null, null);
        JsonDoc readDoc = ctx.getDocuments().get(0);
        // Change some fields
        System.out.println("Read doc:" + readDoc);
        readDoc.modify(new Path("field1"), nodeFactory.textNode("updated"), false);
        readDoc.modify(new Path("field7.0.elemf1"), nodeFactory.textNode("updated too"), false);
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), response.getNumInserted());

        // Save it back
        ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
        ctx.add(md);
        ctx.addDocument(readDoc);
        CRUDSaveResponse saveResponse = controller.save(ctx, false, projection);

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        // Read it back
        controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
                projection("{'field':'*','recursive':1}"), null, null, null);
        JsonDoc r2doc = ctx.getDocuments().get(0);
        Assert.assertEquals(readDoc.get(new Path("field1")).asText(), r2doc.get(new Path("field1")).asText());
        Assert.assertEquals(readDoc.get(new Path("field7.0.elemf1")).asText(), r2doc.get(new Path("field7.0.elemf1")).asText());
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), saveResponse.getNumSaved());
    }

    @Test
    public void saveTest_duplicateKey() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        System.out.println("Write doc:" + doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);
        String id = ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();

        doc.modify(new Path("_id"),nodeFactory.textNode(id),false);

        ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        ctx.addDocument(doc);
        controller.insert(ctx,projection);

        Assert.assertEquals(1,ctx.getDocuments().get(0).getErrors().size());
        Assert.assertEquals(MongoCrudConstants.ERR_DUPLICATE,ctx.getDocuments().get(0).getErrors().get(0).getErrorCode());

    }

    @Test
    public void saveIdTypeUidTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata4.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata4.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        System.out.println("Write doc:" + doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);
        String id = ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
                projection("{'field':'*','recursive':1}"), null, null, null);
        JsonDoc readDoc = ctx.getDocuments().get(0);
        // Change some fields
        System.out.println("Read doc:" + readDoc);
        readDoc.modify(new Path("field1"), nodeFactory.textNode("updated"), false);
        readDoc.modify(new Path("field7.0.elemf1"), nodeFactory.textNode("updated too"), false);
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), response.getNumInserted());

        // Save it back
        ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
        ctx.add(md);
        ctx.addDocument(readDoc);
        CRUDSaveResponse saveResponse = controller.save(ctx, false, projection);

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        // Read it back
        controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
                projection("{'field':'*','recursive':1}"), null, null, null);
        JsonDoc r2doc = ctx.getDocuments().get(0);
        Assert.assertEquals(readDoc.get(new Path("field1")).asText(), r2doc.get(new Path("field1")).asText());
        Assert.assertEquals(readDoc.get(new Path("field7.0.elemf1")).asText(), r2doc.get(new Path("field7.0.elemf1")).asText());
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), saveResponse.getNumSaved());
    }

    @Test
    public void saveTestForInvisibleFields() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        // Insert a doc
        System.out.println("Write doc:" + doc);
        controller.insert(ctx, projection);
        String id = ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();
        System.out.println("Saved id:" + id);

        // Read doc using mongo
        DBObject dbdoc = coll.findOne(new BasicDBObject("_id", Translator.createIdFrom(id)));
        Assert.assertNotNull(dbdoc);
        // Add some fields
        dbdoc.put("invisibleField", "invisibleValue");
        // Save doc back
        coll.save(dbdoc);
        System.out.println("Saved doc:" + dbdoc);

        // Read the doc
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
                projection("{'field':'*','recursive':1}"), null, null, null);
        JsonDoc readDoc = ctx.getDocuments().get(0);

        // Now we save the doc, and expect that the invisible field is still there
        readDoc.modify(new Path("field1"), nodeFactory.textNode("updated"), false);
        System.out.println("To update:" + readDoc);
        ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
        ctx.add(md);
        ctx.addDocument(readDoc);
        controller.save(ctx, false, projection);

        // Make sure doc is modified, and invisible field is there
        dbdoc = coll.findOne(new BasicDBObject("_id", Translator.createIdFrom(id)));
        System.out.println("Loaded doc:" + dbdoc);
        Assert.assertEquals("updated", dbdoc.get("field1"));
        Assert.assertEquals("invisibleValue", dbdoc.get("invisibleField"));
    }

    @Test
    public void upsertTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        ctx.addDocument(doc);
        System.out.println("Write doc:" + doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
        String id = ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
                projection("{'field':'*','recursive':1}"), null, null, null);
        JsonDoc readDoc = ctx.getDocuments().get(0);
        // Remove id, to force re-insert
        readDoc.modify(new Path("_id"), null, false);
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), response.getNumInserted());

        ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
        ctx.add(md);
        ctx.addDocument(readDoc);
        // This should not insert anything
        CRUDSaveResponse sr = controller.save(ctx, false, projection("{'field':'_id'}"));
        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals(1, c.count());
        }

        ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
        ctx.add(md);
        ctx.addDocument(readDoc);
        sr = controller.save(ctx, true, projection("{'field':'_id'}"));
        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals(2, c.count());
        }
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), sr.getNumSaved());
    }

    @Test
    public void upsertWithIdsTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata_ids.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        ctx.addDocument(doc);
        System.out.println("Write doc:" + doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
        String id = ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
                        projection("{'field':'*','recursive':1}"), null, null, null);
        JsonDoc readDoc = ctx.getDocuments().get(0);

        // Remove _id from the doc
        readDoc.modify(new Path("_id"),null,false);
        // Change a field
        Assert.assertEquals(1,readDoc.get(new Path("field3")).asInt());
        readDoc.modify(new Path("field3"),JsonNodeFactory.instance.numberNode(2),true);
        ctx=new TestCRUDOperationContext(CRUDOperation.SAVE);
        ctx.add(md);
        ctx.addDocument(readDoc);
        controller.save(ctx,false, projection("{'field':'_id'}"));

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
                        projection("{'field':'*','recursive':1}"), null, null, null);
        readDoc = ctx.getDocuments().get(0);

        Assert.assertEquals(2,readDoc.get(new Path("field3")).asInt());
    }

    @Test
    public void upsertWithIdsTest2() throws Exception {
        EntityMetadata md = getMd("./testMetadata_ids_id_not_id.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        ctx.addDocument(doc);
        System.out.println("Write doc:" + doc);
        controller.updatePredefinedFields(ctx,doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
        String id = ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();

        //There should be 1 doc in the db
        Assert.assertEquals(1,controller.find(ctx,null, projection("{'field':'*','recursive':1}"), null, null, null).getSize());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
                        projection("{'field':'*','recursive':1}"), null, null, null);
        JsonDoc readDoc = ctx.getDocuments().get(0);

        //Remove _id from the doc
        readDoc.modify(new Path("_id"),null,false);
        //Change a field
        Assert.assertEquals(1,readDoc.get(new Path("field3")).asInt());
        readDoc.modify(new Path("field3"),JsonNodeFactory.instance.numberNode(2),true);
        ctx=new TestCRUDOperationContext(CRUDOperation.SAVE);
        ctx.add(md);
        ctx.addDocument(readDoc);
        controller.updatePredefinedFields(ctx,readDoc);
        CRUDSaveResponse sresponse=controller.save(ctx,true, projection("{'field':'_id'}"));
        Assert.assertEquals(1,sresponse.getNumSaved());
        //There should be 1 doc in the db
        Assert.assertEquals(1,controller.find(ctx,null, projection("{'field':'*','recursive':1}"), null, null, null).getSize());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
                        projection("{'field':'*','recursive':1}"), null, null, null);
        readDoc = ctx.getDocuments().get(0);

        Assert.assertEquals(2,readDoc.get(new Path("field3")).asInt());
    }


    @Test
    public void updateTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        // Generate some docs
        List<JsonDoc> docs = new ArrayList<>();
        int numDocs = 20;
        for (int i = 0; i < numDocs; i++) {
            JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
            doc.modify(new Path("field1"), nodeFactory.textNode("doc" + i), false);
            doc.modify(new Path("field3"), nodeFactory.numberNode(i), false);
            docs.add(doc);
        }
        ctx.addDocuments(docs);
        CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals(numDocs, c.count());
        }
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), response.getNumInserted());

        // Single doc update
        ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
        ctx.add(md);
        CRUDUpdateResponse upd = controller.update(ctx, query("{'field':'field3','op':'$eq','rvalue':10}"),
                update("{ '$set': { 'field3' : 1000 } }"),
                projection("{'field':'_id'}"));
        Assert.assertEquals(1, upd.getNumUpdated());
        Assert.assertEquals(0, upd.getNumFailed());
        //Assert.assertEquals(AtomicIterateUpdate.class, ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
        Assert.assertEquals(IterateAndUpdate.class, ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
        try (DBCursor c = coll.find(new BasicDBObject("field3", 1000), new BasicDBObject("_id", 1))) {
            DBObject obj = c.next();
            Assert.assertNotNull(obj);
            System.out.println("DBObject:" + obj);
            System.out.println("Output doc:" + ctx.getDocuments().get(0).getOutputDocument());
            Assert.assertEquals(ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText(),
                    obj.get("_id").toString());
        }
        try (DBCursor c = coll.find(new BasicDBObject("field3", 1000))) {
            Assert.assertEquals(1, c.count());
        }

        // Bulk update
        ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
        ctx.add(md);
        upd = controller.update(ctx, query("{'field':'field3','op':'>','rvalue':10}"),
                update("{ '$set': { 'field3' : 1000 } }"),
                projection("{'field':'_id'}"));
        //Assert.assertEquals(AtomicIterateUpdate.class, ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
        Assert.assertEquals(IterateAndUpdate.class, ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
        Assert.assertEquals(10, upd.getNumUpdated());
        Assert.assertEquals(0, upd.getNumFailed());
        try (DBCursor c = coll.find(new BasicDBObject("field3", new BasicDBObject("$gt", 10)))) {
            Assert.assertEquals(10, c.count());
        }

        // Bulk direct update
        ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
        ctx.add(md);
        upd = controller.update(ctx, query("{'field':'field3','op':'>','rvalue':10}"),
                update("{ '$set': { 'field3' : 1000 } }"), null);
        //Assert.assertEquals(AtomicIterateUpdate.class, ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
        Assert.assertEquals(IterateAndUpdate.class, ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
        Assert.assertEquals(10, upd.getNumUpdated());
        Assert.assertEquals(0, upd.getNumFailed());
        try (DBCursor c = coll.find(new BasicDBObject("field3", new BasicDBObject("$gt", 10)))) {
            Assert.assertEquals(10, c.count());
        }

        // Iterate update
        ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
        ctx.add(md);
        // Updating an array field will force use of IterateAndupdate
        upd = controller.update(ctx, query("{'field':'field3','op':'>','rvalue':10}"),
                update("{ '$set': { 'field7.0.elemf1' : 'blah' } }"), projection("{'field':'_id'}"));
        Assert.assertEquals(IterateAndUpdate.class, ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
        Assert.assertEquals(10, upd.getNumUpdated());
        Assert.assertEquals(0, upd.getNumFailed());
        try (DBCursor c = coll.find(new BasicDBObject("field7.0.elemf1", "blah"))) {
            Assert.assertEquals(10, c.count());
        }
    }

    @Test
    public void updateTest_nullReqField() throws Exception {
        EntityMetadata md = getMd("./testMetadata-requiredFields2.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);

        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        doc.modify(new Path("field1"),JsonNodeFactory.instance.nullNode(),false);
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

        ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
        ctx.add(md);
        CRUDUpdateResponse upd = controller.update(ctx, query("{'field':'field2','op':'=','rvalue':'f2'}"),
                                                   update(" {'$set' : {'field3':3 }}"),
                                                   projection("{'field':'*','recursive':1}"));
        Assert.assertEquals(0, upd.getNumUpdated());
        Assert.assertEquals(1, upd.getNumFailed());
    }


    @Test
    public void updateTest_PartialFailure() throws Exception {
        EntityMetadata md = getMd("./testMetadata-requiredFields.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        // Generate some docs
        List<JsonDoc> docs = new ArrayList<>();
        int numDocs = 20;
        for (int i = 0; i < numDocs; i++) {
            JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
            doc.modify(new Path("field1"), nodeFactory.textNode("doc" + i), false);
            doc.modify(new Path("field3"), nodeFactory.numberNode(i), false);
            docs.add(doc);
        }
        ctx.addDocuments(docs);
        CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals(numDocs, c.count());
        }
        Assert.assertEquals(ctx.getDocumentsWithoutErrors().size(), response.getNumInserted());

        // Add element to array
        ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
        ctx.add(md);
        CRUDUpdateResponse upd = controller.update(ctx, query("{'field':'field1','op':'=','rvalue':'doc1'}"),
                                                   update("[ {'$append' : {'field7':{}} }, { '$set': { 'field7.-1.elemf1':'test'} } ]"),
                                                   projection("{'field':'*','recursive':1}"));
        Assert.assertEquals(1, upd.getNumUpdated());
        Assert.assertEquals(0, upd.getNumFailed());
        Assert.assertEquals(1,ctx.getDocuments().size());
        Assert.assertEquals(3,ctx.getDocuments().get(0).getOutputDocument().get(new Path("field7")).size());

        // Add another element, with violated constraint
        ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
        ctx.add(md);
        upd = controller.update(ctx, query("{'field':'field1','op':'=','rvalue':'doc1'}"),
                                update("[ {'$append' : {'field7':{}} }, { '$set': { 'field7.-1.elemf2':'$null'} } ]"),
                                projection("{'field':'*','recursive':1}"));
        Assert.assertEquals(0, upd.getNumUpdated());
        Assert.assertEquals(1, upd.getNumFailed());
    }


    @Test
    public void findUsingBigdecimalTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);

        ctx.add(md);
        // Generate some docs
        List<JsonDoc> docs = new ArrayList<>();
        int numDocs = 20;
        for (int i = 0; i < numDocs; i++) {
            JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
            doc.modify(new Path("field1"), nodeFactory.textNode("doc" + i), false);
            doc.modify(new Path("field3"), nodeFactory.numberNode(i), false);
            doc.modify(new Path("field4"), nodeFactory.numberNode(new BigDecimal(100)),false);
            docs.add(doc);
        }
        ctx.addDocuments(docs);
        controller.insert(ctx, projection("{'field':'_id'}"));

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field4','op':'=','rvalue':'100'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(numDocs,ctx.getDocuments().size());

    }

    @Test
    public void findInq() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);

        ctx.add(md);
        // Generate some docs
        List<JsonDoc> docs = new ArrayList<>();
        int numDocs = 20;
        for (int i = 0; i < numDocs; i++) {
            JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
            doc.modify(new Path("field1"), nodeFactory.textNode("doc" + i), false);
            doc.modify(new Path("field3"), nodeFactory.numberNode(i), false);
            doc.modify(new Path("field4"), nodeFactory.numberNode(new BigDecimal(100)),false);
            docs.add(doc);
        }
        ctx.addDocuments(docs);
        controller.insert(ctx, projection("{'field':'_id'}"));

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field4','op':'$in','values':[100]}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(numDocs,ctx.getDocuments().size());

    }

    @Test
    public void findInqStr() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);

        ctx.add(md);
        // Generate some docs
        List<JsonDoc> docs = new ArrayList<>();
        int numDocs = 20;
        for (int i = 0; i < numDocs; i++) {
            JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
            doc.modify(new Path("field1"), nodeFactory.textNode("" + i), false);
            doc.modify(new Path("field3"), nodeFactory.numberNode(i), false);
            doc.modify(new Path("field4"), nodeFactory.numberNode(new BigDecimal(100)),false);
            docs.add(doc);
        }
        ctx.addDocuments(docs);
        controller.insert(ctx, projection("{'field':'_id'}"));

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field1','op':'$in','values':[0]}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());

    }

    @Test
    public void findNullProjection() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);

        ctx.add(md);
        // Generate some docs
        List<JsonDoc> docs = new ArrayList<>();
        int numDocs = 20;
        for (int i = 0; i < numDocs; i++) {
            JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
            doc.modify(new Path("field1"), nodeFactory.textNode("" + i), false);
            doc.modify(new Path("field3"), nodeFactory.numberNode(i), false);
            doc.modify(new Path("field4"), nodeFactory.numberNode(new BigDecimal(100)),false);
            docs.add(doc);
        }
        ctx.addDocuments(docs);
        controller.insert(ctx, projection("{'field':'_id'}"));

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field1','op':'$in','values':[0]}"),
                        null,null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());

    }

    @Test
    public void findInqId() throws Exception {
        EntityMetadata md = getMd("./testMetadata_idInt.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);

        ctx.add(md);
        // Generate some docs
        List<JsonDoc> docs = new ArrayList<>();
        int numDocs = 20;
        for (int i = 0; i < numDocs; i++) {
            JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
            doc.modify(new Path("_id"), nodeFactory.textNode("" + i), false);
            docs.add(doc);
        }
        ctx.addDocuments(docs);
        controller.insert(ctx, projection("{'field':'_id'}"));

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'_id','op':'$in','values':[1]}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());

    }

    @Test
    public void sortAndPageTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);

        ctx.add(md);
        // Generate some docs
        List<JsonDoc> docs = new ArrayList<>();
        int numDocs = 20;
        for (int i = 0; i < numDocs; i++) {
            JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
            doc.modify(new Path("field1"), nodeFactory.textNode("doc" + i), false);
            doc.modify(new Path("field3"), nodeFactory.numberNode(i), false);
            docs.add(doc);
        }
        ctx.addDocuments(docs);
        controller.insert(ctx, projection("{'field':'_id'}"));

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx, query("{'field':'field3','op':'>=','rvalue':0}"),
                projection("{'field':'*','recursive':1}"),
                sort("{'field3':'$desc'}"), null, null);
        Assert.assertEquals(numDocs, ctx.getDocuments().size());
        int lastValue = -1;
        for (DocCtx doc : ctx.getDocuments()) {
            int value = doc.getOutputDocument().get(new Path("field3")).asInt();
            if (value < lastValue) {
                Assert.fail("wrong order");
            }
        }

        for (int k = 0; k < 15; k++) {
            ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
            ctx.add(md);
            controller.find(ctx, query("{'field':'field3','op':'>=','rvalue':0}"),
                    projection("{'field':'*','recursive':1}"),
                    sort("{'field3':'$asc'}"), new Long(k), new Long(k + 5));

            int i = 0;
            for (DocCtx doc : ctx.getDocuments()) {
                int value = doc.getOutputDocument().get(new Path("field3")).asInt();
                Assert.assertEquals(i + k, value);
                i++;
            }
        }
    }

    @Test
    public void limitTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);

        ctx.add(md);
        // Generate some docs
        List<JsonDoc> docs = new ArrayList<>();
        int numDocs = 20;
        for (int i = 0; i < numDocs; i++) {
            JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
            doc.modify(new Path("field1"), nodeFactory.textNode("doc" + i), false);
            doc.modify(new Path("field3"), nodeFactory.numberNode(i), false);
            docs.add(doc);
        }
        ctx.addDocuments(docs);
        controller.insert(ctx, projection("{'field':'_id'}"));

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        CRUDFindResponse response=controller.find(ctx, query("{'field':'field3','op':'>=','rvalue':0}"),
                                                  projection("{'field':'*','recursive':1}"),
                                                  sort("{'field3':'$desc'}"), null, null);
        Assert.assertEquals(numDocs, ctx.getDocuments().size());
        Assert.assertEquals(numDocs,response.getSize());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        response=controller.find(ctx, query("{'field':'field3','op':'>=','rvalue':0}"),
                                                  projection("{'field':'*','recursive':1}"),
                                                  sort("{'field3':'$desc'}"), 0l, 1l);
        Assert.assertEquals(2, ctx.getDocuments().size());
        Assert.assertEquals(numDocs,response.getSize());
    }

    @Test
    public void fieldArrayComparisonTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf3','op':'=','rfield':'field6.nf5'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf3','op':'!=','rfield':'field6.nf5'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf3','op':'<','rfield':'field6.nf5'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());

    }

    @Test
    public void arrayArrayComparisonTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'=','rfield':'field6.nf5'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'!=','rfield':'field6.nf5'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'<=','rfield':'field6.nf5'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'<','rfield':'field6.nf5'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'>=','rfield':'field6.nf5'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'>','rfield':'field6.nf5'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());
    }

    @Test
    public void arrayArrayComparisonTest_gt() throws Exception {
        EntityMetadata md = getMd("./testMetadata5.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata5.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'>','rfield':'field6.nf8'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'>=','rfield':'field6.nf8'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'<','rfield':'field6.nf8'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'<=','rfield':'field6.nf8'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'=','rfield':'field6.nf8'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'!=','rfield':'field6.nf8'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());
    }

    @Test
    public void arrayArrayComparisonTest_ne() throws Exception {
        EntityMetadata md = getMd("./testMetadata5.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata5.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'>','rfield':'field6.nf9'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'>=','rfield':'field6.nf9'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'<','rfield':'field6.nf9'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'<=','rfield':'field6.nf9'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'=','rfield':'field6.nf9'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'!=','rfield':'field6.nf9'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());
    }

    @Test
    public void arrayArrayComparisonTest_lt() throws Exception {
        EntityMetadata md = getMd("./testMetadata5.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata5.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'>','rfield':'field6.nf10'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'>=','rfield':'field6.nf10'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'<','rfield':'field6.nf10'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'<=','rfield':'field6.nf10'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'=','rfield':'field6.nf10'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf5','op':'!=','rfield':'field6.nf10'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());
        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field6.nf10','op':'>=','rfield':'field6.nf5'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(0,ctx.getDocuments().size());
    }

    @Test
    public void nullqTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata5.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata5.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,null,
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());
    }

    @Test
    public void objectTypeIsAlwaysProjected() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);

        ctx.add(md);
        // Generate some docs
        List<JsonDoc> docs = new ArrayList<>();
        int numDocs = 20;
        for (int i = 0; i < numDocs; i++) {
            JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
            doc.modify(new Path("field1"), nodeFactory.textNode("doc" + i), false);
            doc.modify(new Path("field3"), nodeFactory.numberNode(i), false);
            docs.add(doc);
        }
        ctx.addDocuments(docs);
        controller.insert(ctx, projection("{'field':'*','recursive':true}"));

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx, query("{'field':'field3','op':'>=','rvalue':0}"),
                projection("{'field':'field3'}"),null, null, null);
        // The fact that there is no exceptions means objectType was included
    }

    @Test
    public void deleteTest() throws Exception {
        EntityMetadata md = getMd("./testMetadata.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        // Generate some docs
        List<JsonDoc> docs = new ArrayList<>();
        int numDocs = 20;
        for (int i = 0; i < numDocs; i++) {
            JsonDoc jsonDOc = new JsonDoc(loadJsonNode("./testdata1.json"));
            jsonDOc.modify(new Path("field1"), nodeFactory.textNode("doc" + i), false);
            jsonDOc.modify(new Path("field3"), nodeFactory.numberNode(i), false);
            docs.add(jsonDOc);
        }
        ctx.addDocuments(docs);
        controller.insert(ctx, projection("{'field':'_id'}"));
        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals(numDocs, c.count());
        }

        // Single doc delete
        ctx = new TestCRUDOperationContext(CRUDOperation.DELETE);
        ctx.add(md);
        CRUDDeleteResponse del = controller.delete(ctx, query("{'field':'field3','op':'$eq','rvalue':10}"));
        Assert.assertEquals(1, del.getNumDeleted());
        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals(numDocs - 1, c.count());
        }

        // Bulk delete
        ctx = new TestCRUDOperationContext(CRUDOperation.DELETE);
        ctx.add(md);
        del = controller.delete(ctx, query("{'field':'field3','op':'>','rvalue':10}"));
        Assert.assertEquals(9, del.getNumDeleted());
        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals(10, c.count());
        }
    }

    @Test
    public void elemMatchTest_Not() throws Exception {
        EntityMetadata md = getMd("./testMetadata5.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdata5.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'array':'field7','elemMatch':{'$not':{'field':'elemf3','op':'$eq','rvalue':0}}}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());
     }

   @Test
    public void entityIndexCreationTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex1"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        e.getEntityInfo().setDefaultVersion("1.0.0");
        Index index = new Index();
        index.setName("testIndex");
        index.setUnique(true);
        List<SortKey> indexFields = new ArrayList<>();
        //TODO actually parse $asc/$desc here
        indexFields.add(new SortKey(new Path("field1"), true));
        index.setFields(indexFields);
        List<Index> indexes = new ArrayList<>();
        indexes.add(index);
        e.getEntityInfo().getIndexes().setIndexes(indexes);
        controller.afterUpdateEntityInfo(null, e.getEntityInfo(),false);

        DBCollection entityCollection = db.getCollection("testCollectionIndex1");

        boolean foundIndex = false;

        for (DBObject mongoIndex : entityCollection.getIndexInfo()) {
            if ("testIndex".equals(mongoIndex.get("name"))) {
                if (mongoIndex.get("key").toString().contains("field1")) {
                    foundIndex = true;
                }
            }
        }
        Assert.assertTrue(foundIndex);
    }

   @Test
    public void entityIndexRemovalTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex1"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        e.getEntityInfo().setDefaultVersion("1.0.0");
        Index index = new Index();
        index.setName("testIndex");
        index.setUnique(true);
        List<SortKey> indexFields = new ArrayList<>();
        //TODO actually parse $asc/$desc here
        indexFields.add(new SortKey(new Path("field1"), true));
        index.setFields(indexFields);
        List<Index> indexes = new ArrayList<>();
        indexes.add(index);
        e.getEntityInfo().getIndexes().setIndexes(indexes);
        controller.afterUpdateEntityInfo(null, e.getEntityInfo(),false);

        DBCollection entityCollection = db.getCollection("testCollectionIndex1");

        boolean foundIndex = false;

        for (DBObject mongoIndex : entityCollection.getIndexInfo()) {
            if ("testIndex".equals(mongoIndex.get("name"))) {
                if (mongoIndex.get("key").toString().contains("field1")) {
                    foundIndex = true;
                }
            }
        }
        Assert.assertTrue(foundIndex);

        e.getEntityInfo().getIndexes().setIndexes(new ArrayList<Index>());
        controller.afterUpdateEntityInfo(null, e.getEntityInfo(),false);
        entityCollection = db.getCollection("testCollectionIndex1");

        foundIndex = false;

        for (DBObject mongoIndex : entityCollection.getIndexInfo()) {
            if ("testIndex".equals(mongoIndex.get("name"))) {
                if (mongoIndex.get("key").toString().contains("field1")) {
                    foundIndex = true;
                }
            }
        }
        Assert.assertTrue(!foundIndex);
    }

    @Test
    public void entityIndexUpdateTest_notSparseUnique() throws Exception {
        // verify that if an index already exists as unique and NOT sparse lightblue will not recreate it as sparse

        // 1. create the index as unique but not sparse
        DBCollection entityCollection = db.getCollection("testCollectionIndex2");

        DBObject newIndex = new BasicDBObject();
        newIndex.put("field1", -1);
        BasicDBObject options = new BasicDBObject("unique", true);
        options.append("sparse", false);
        options.append("name", "testIndex");
        entityCollection.createIndex(newIndex, options);

        // 2. create metadata with same index
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex2"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        e.getEntityInfo().setDefaultVersion("1.0.0");
        Index index = new Index();
        index.setName("testIndex");
        index.setUnique(true);
        List<SortKey> indexFields = new ArrayList<>();
        indexFields.add(new SortKey(new Path("field1"), true));
        index.setFields(indexFields);
        List<Index> indexes = new ArrayList<>();
        indexes.add(index);
        e.getEntityInfo().getIndexes().setIndexes(indexes);
        controller.afterUpdateEntityInfo(null, e.getEntityInfo(),false);

        // 3. verify index in database is unique but not sparse
        entityCollection = db.getCollection("testCollectionIndex2");

        // should also have _id index
        Assert.assertEquals("Unexpected number of indexes", 2, entityCollection.getIndexInfo().size());
        DBObject mongoIndex = entityCollection.getIndexInfo().get(1);
        Assert.assertTrue("Keys on index unexpected", mongoIndex.get("key").toString().contains("field1"));
        Assert.assertEquals("Index is not unique", Boolean.TRUE, mongoIndex.get("unique"));
        Assert.assertEquals("Index is sparse", Boolean.FALSE, mongoIndex.get("sparse"));
    }

    @Test
    public void saneIndex() throws Exception {

        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex2"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        e.getEntityInfo().setDefaultVersion("1.0.0");
        Index index = new Index();
        index.setName("testIndex1");
        index.setUnique(true);
        List<SortKey> indexFields = new ArrayList<>();
        indexFields.add(new SortKey(new Path("field1"), true));
        index.setFields(indexFields);
        List<Index> indexes = new ArrayList<>();
        indexes.add(index);
        index = new Index();
        index.setName("testIndex2");
        index.setUnique(false);
        indexFields = new ArrayList<>();
        indexFields.add(new SortKey(new Path("field1"), true));
        index.setFields(indexFields);
        indexes.add(index);
        e.getEntityInfo().getIndexes().setIndexes(indexes);
        try {
            controller.beforeUpdateEntityInfo(null, e.getEntityInfo(),false);
            Assert.fail();
        } catch (Exception x) {}
    }


    @Test
    public void entityIndexUpdateTest_default() throws Exception {

        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex2"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        e.getEntityInfo().setDefaultVersion("1.0.0");
        Index index = new Index();
        index.setName("testIndex");
        index.setUnique(true);
        List<SortKey> indexFields = new ArrayList<>();
        indexFields.add(new SortKey(new Path("field1"), true));
        index.setFields(indexFields);
        List<Index> indexes = new ArrayList<>();
        indexes.add(index);
        e.getEntityInfo().getIndexes().setIndexes(indexes);
        controller.afterUpdateEntityInfo(null, e.getEntityInfo(),false);

        DBCollection entityCollection = db.getCollection("testCollectionIndex2");

        index = new Index();
        index.setName("testIndex");
        index.setUnique(false);
        indexFields = new ArrayList<>();
        indexFields.clear();
        indexFields.add(new SortKey(new Path("field1"), true));
        index.setFields(indexFields);
        indexes = new ArrayList<>();
        indexes.add(index);
        e.getEntityInfo().getIndexes().setIndexes(indexes);

        controller.afterUpdateEntityInfo(null, e.getEntityInfo(),false);

        boolean foundIndex = false;

        for (DBObject mongoIndex : entityCollection.getIndexInfo()) {
            if ("testIndex".equals(mongoIndex.get("name"))) {
                if (mongoIndex.get("key").toString().contains("field1")) {
                    if (mongoIndex.get("unique") != null) {
                        // if it is null it is not unique, so no check is required
                        Assert.assertEquals("Index is unique", Boolean.FALSE, mongoIndex.get("unique"));
                    }
                    if (mongoIndex.get("sparse") != null) {
                        // if it is null it is not sparse, so no check is required
                        Assert.assertEquals("Index is sparse", Boolean.FALSE, mongoIndex.get("sparse"));
                    }
                    foundIndex = true;
                }
            }
        }
        Assert.assertTrue(foundIndex);
    }

    @Test
    public void entityIndexUpdateTest_addFieldToCompositeIndex_190() throws Exception {

        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex2"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        e.getFields().put(new SimpleField("field2", StringType.TYPE));
        e.getFields().put(new SimpleField("field3", StringType.TYPE));
        e.getEntityInfo().setDefaultVersion("1.0.0");
        Index index = new Index();
        index.setName("modifiedIndex");
        index.setUnique(true);
        List<SortKey> indexFields = new ArrayList<>();
        indexFields.add(new SortKey(new Path("field1"), true));
        indexFields.add(new SortKey(new Path("field2"), true));
        index.setFields(indexFields);
        List<Index> indexes = new ArrayList<>();
        indexes.add(index);
        e.getEntityInfo().getIndexes().setIndexes(indexes);
        controller.afterUpdateEntityInfo(null, e.getEntityInfo(),false);

        indexFields = new ArrayList<>();
        indexFields.add(new SortKey(new Path("field1"), true));
        indexFields.add(new SortKey(new Path("field2"), true));
        indexFields.add(new SortKey(new Path("field3"), true));
        index.setFields(indexFields);
        e.getEntityInfo().getIndexes().setIndexes(indexes);

        controller.afterUpdateEntityInfo(null, e.getEntityInfo(),false);

        DBCollection entityCollection = db.getCollection("testCollectionIndex2");

        boolean foundIndex = false;

        for (DBObject mongoIndex : entityCollection.getIndexInfo()) {
            if ("modifiedIndex".equals(mongoIndex.get("name"))) {
                if (mongoIndex.get("key").toString().contains("field1")&&
                    mongoIndex.get("key").toString().contains("field2")&&
                    mongoIndex.get("key").toString().contains("field3")) {
                    if (mongoIndex.get("unique") != null) {
                        // if it is null it is not unique, so no check is required
                        Assert.assertEquals("Index is unique", Boolean.TRUE, mongoIndex.get("unique"));
                    }
                    if (mongoIndex.get("sparse") != null) {
                        // if it is null it is not sparse, so no check is required
                        Assert.assertEquals("Index is sparse", Boolean.TRUE, mongoIndex.get("sparse"));
                    }
                    foundIndex = true;
                }
            }
        }
        Assert.assertTrue(foundIndex);
    }

    @Test
    public void ensureIdFieldTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex2"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        e.getEntityInfo().setDefaultVersion("1.0.0");

        Assert.assertNull(e.getFields().getField("_id"));
        controller.beforeCreateNewSchema(null,e);

        SimpleField id=(SimpleField)e.getFields().getField("_id");
        Assert.assertNotNull(id);
        Assert.assertEquals(StringType.TYPE,id.getType());
        Assert.assertEquals(0,id.getConstraints().size());
    }

    @Test
    public void ensureIdIndexTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex2"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        e.getEntityInfo().setDefaultVersion("1.0.0");

        Assert.assertEquals(0,e.getEntityInfo().getIndexes().getIndexes().size());
        controller.beforeUpdateEntityInfo(null,e.getEntityInfo(),false);

        Assert.assertEquals(1,e.getEntityInfo().getIndexes().getIndexes().size());
        Assert.assertEquals("_id",e.getEntityInfo().getIndexes().getIndexes().get(0).getFields().get(0).getField().toString());
    }

    @Test
    public void indexFieldValidationTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex2"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        e.getEntityInfo().setDefaultVersion("1.0.0");

        List<Index> indexes=new ArrayList<>();
        Index ix=new Index();
        List<SortKey> fields=new ArrayList<>();
        fields.add(new SortKey(new Path("x.*.y"),false));
        ix.setFields(fields);
        indexes.add(ix);
        e.getEntityInfo().getIndexes().setIndexes(indexes);

        controller.beforeUpdateEntityInfo(null,e.getEntityInfo(),false);
        Assert.assertEquals("x.y",e.getEntityInfo().getIndexes().getIndexes().get(0).getFields().get(0).getField().toString());

        indexes=new ArrayList<>();
        ix=new Index();
        fields=new ArrayList<>();
        fields.add(new SortKey(new Path("x.1.y"),false));
        ix.setFields(fields);
        indexes.add(ix);
        e.getEntityInfo().getIndexes().setIndexes(indexes);

        try {
            controller.beforeUpdateEntityInfo(null,e.getEntityInfo(),false);
            Assert.fail();
        } catch (Error x) {
            // expected
        }

        indexes=new ArrayList<>();
        ix=new Index();
        fields=new ArrayList<>();
        fields.add(new SortKey(new Path("x.y"),false));
        ix.setFields(fields);
        indexes.add(ix);
        e.getEntityInfo().getIndexes().setIndexes(indexes);
        controller.beforeUpdateEntityInfo(null,e.getEntityInfo(),false);
        Assert.assertEquals("x.y",e.getEntityInfo().getIndexes().getIndexes().get(0).getFields().get(0).getField().toString());
    }

    @Test
    public void indexFieldsMatch() {
        // order of kesys in an index matters to mongo, this test exists to ensure this is accounted for in the controller

        DBCollection collection = db.getCollection("testIndexFieldMatch");
        {
            BasicDBObject dbIndex = new BasicDBObject();
            dbIndex.append("x", 1);
            dbIndex.append("y", 1);
            collection.createIndex(dbIndex);
        }

        {
            Index ix = new Index();
            List<SortKey> fields = new ArrayList<>();
            fields.add(new SortKey(new Path("x"), false));
            fields.add(new SortKey(new Path("y"), false));
            ix.setFields(fields);

            boolean verified = false;
            for (DBObject dbi: collection.getIndexInfo()) {
                if (((BasicDBObject)dbi.get("key")).entrySet().iterator().next().getKey().equals("_id")) {
                    continue;
                }
                // non _id index is the one we want to verify with
                Assert.assertTrue(controller.indexFieldsMatch(ix, dbi));
                verified = true;
            }
            Assert.assertTrue(verified);
        }

        {
            Index ix = new Index();
            List<SortKey> fields = new ArrayList<>();
            fields.add(new SortKey(new Path("y"), false));
            fields.add(new SortKey(new Path("x"), false));
            ix.setFields(fields);

            boolean verified = false;
            for (DBObject dbi: collection.getIndexInfo()) {
                if (((BasicDBObject)dbi.get("key")).entrySet().iterator().next().getKey().equals("_id")) {
                    continue;
                }
                // non _id index is the one we want to verify with
                Assert.assertFalse(controller.indexFieldsMatch(ix, dbi));
                verified = true;
            }
            Assert.assertTrue(verified);
        }
    }


    @Test
    public void testMigrationUpdate() throws Exception {
        EntityMetadata md = getMd("./migrationJob.json");
        TestCRUDOperationContext ctx = new TestCRUDOperationContext("migrationJob",CRUDOperation.INSERT);
        ctx.add(md);
        JsonDoc doc = new JsonDoc(loadJsonNode("./job1.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        System.out.println("Write doc:" + doc);
        CRUDInsertionResponse insresponse = controller.insert(ctx, projection);

        ctx = new TestCRUDOperationContext("migrationJob",CRUDOperation.UPDATE);
        ctx.add(md);
        CRUDUpdateResponse upd = controller.update(ctx, query("{'field':'_id','op':'=','rvalue':'termsAcknowledgementJob_6264'}"),
                                                   update("[{'$append':{'jobExecutions':{}}},{'$set':{'jobExecutions.-1.ownerName':'hystrix'}},{'$set':{'jobExecutions.-1.hostName':'$(hostname)'}},{'$set':{'jobExecutions.-1.pid':'32601@localhost.localdomain'}},{'$set':{'jobExecutions.-1.actualStartDate':'20150312T19:19:00.700+0000'}},{'$set':{'jobExecutions.-1.actualEndDate':'$null'}},{'$set':{'jobExecutions.-1.completedFlag':false}},{'$set':{'jobExecutions.-1.processedDocumentCount':0}},{'$set':{'jobExecutions.-1.consistentDocumentCount':0}},{'$set':{'jobExecutions.-1.inconsistentDocumentCount':0}},{'$set':{'jobExecutions.-1.overwrittenDocumentCount':0}}]"),
                                                   projection("{'field':'*','recursive':1}"));
        Assert.assertEquals(1,ctx.getDocuments().size());
        Assert.assertEquals(2,ctx.getDocuments().get(0).getOutputDocument().get(new Path("jobExecutions")).size());
    }

    @Test
    public void idIndexRewriteTest() throws Exception {
        DBCollection collection=db.getCollection("data");

        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "data"));
        SimpleField idField=new SimpleField("_id",StringType.TYPE);
        List<FieldConstraint> clist=new ArrayList<>();
        clist.add(new IdentityConstraint());
        idField.setConstraints(clist);
        e.getFields().put(idField);
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        e.getFields().put(new SimpleField("objectType", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        e.getEntityInfo().setDefaultVersion("1.0.0");
        e.getEntitySchema().getAccess().getInsert().setRoles("anyone");
        e.getEntitySchema().getAccess().getFind().setRoles("anyone");
        controller.beforeUpdateEntityInfo(null,e.getEntityInfo(),false);
        Assert.assertEquals(1,e.getEntityInfo().getIndexes().getIndexes().size());
        Assert.assertEquals("_id",e.getEntityInfo().getIndexes().getIndexes().get(0).getFields().get(0).getField().toString());
        controller.afterUpdateEntityInfo(null,e.getEntityInfo(),false);
        // We have our _id index
        // lets insert a doc in the collection, so we can test indexes
        TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity",CRUDOperation.INSERT);
        ctx.add(e);
        ObjectNode obj=JsonNodeFactory.instance.objectNode();
        obj.set("objectType",JsonNodeFactory.instance.textNode("testEntity"));
        obj.set("field1",JsonNodeFactory.instance.textNode("blah"));
        JsonDoc doc=new JsonDoc(obj);
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

       Assert.assertEquals(1,collection.find().count());

       Index ix2=new Index(new SortKey(new Path("field1"),false));
       e.getEntityInfo().getIndexes().add(ix2);
       // At this point, there must be an _id index in the collection
       Assert.assertEquals(1,collection.getIndexInfo().size());
       // Lets overwrite
       controller.afterUpdateEntityInfo(null,e.getEntityInfo(),false);
       // This should not fail
       Assert.assertEquals(2,collection.getIndexInfo().size());
    }

    @Test
    public void doubleidIndexRewriteTest() throws Exception {
        DBCollection collection=db.getCollection("data");

        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "data"));
        SimpleField idField=new SimpleField("_id",StringType.TYPE);
        List<FieldConstraint> clist=new ArrayList<>();
        clist.add(new IdentityConstraint());
        idField.setConstraints(clist);
        e.getFields().put(idField);
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        e.getFields().put(new SimpleField("objectType", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        e.getEntityInfo().setDefaultVersion("1.0.0");
        e.getEntitySchema().getAccess().getInsert().setRoles("anyone");
        e.getEntitySchema().getAccess().getFind().setRoles("anyone");

        Index ix1=new Index(new SortKey(new Path("field1"),false),new SortKey(new Path("field2"),false));
        ix1.setUnique(true);
        ix1.setName("main");


        Index ix2=new Index(new SortKey(new Path("_id"),false));
        ix2.setUnique(true);
        e.getEntityInfo().getIndexes().add(ix2);
        Index ix3=new Index(new SortKey(new Path("_id"),false));
        ix3.setUnique(false);
        ix3.setName("nonunique");
        e.getEntityInfo().getIndexes().add(ix3);

        controller.afterUpdateEntityInfo(null,e.getEntityInfo(),false);
        // lets insert a doc in the collection, so we can test indexes
        TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity",CRUDOperation.INSERT);
        ctx.add(e);
        ObjectNode obj=JsonNodeFactory.instance.objectNode();
        obj.set("objectType",JsonNodeFactory.instance.textNode("testEntity"));
        obj.set("field1",JsonNodeFactory.instance.textNode("blah"));
        JsonDoc doc=new JsonDoc(obj);
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

        Assert.assertEquals(1,collection.find().count());

        // At this point, there must be only one _id index in the collection
        Assert.assertEquals(1,collection.getIndexInfo().size());

        // Remove the nonunique index
        List<Index> ilist=new ArrayList<>();
        ilist.add(ix1);
        ilist.add(ix2);
        e.getEntityInfo().getIndexes().setIndexes(ilist);

        // Lets overwrite
        controller.afterUpdateEntityInfo(null,e.getEntityInfo(),false);
        // This should not fail
        Assert.assertEquals(2,collection.getIndexInfo().size());
    }

    @Test
    public void projectedRefComesNullTest() throws Exception {
        EntityMetadata md = getMd("./testMetadataRef.json");
        CompositeMetadata cmd=CompositeMetadata.buildCompositeMetadata(md,new CompositeMetadata.GetMetadata() {
                @Override
                public EntityMetadata getMetadata(Path injectionField,
                                                  String entityName,
                                                  String version) {
                    return null;
                }
            });

        FieldCursor cursor=cmd.getFieldCursor();
        while(cursor.next()) {
            System.out.println(cursor.getCurrentPath()+":"+cursor.getCurrentNode());
        }

        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
        ctx.add(cmd);
        JsonDoc doc = new JsonDoc(loadJsonNode("./testdataref.json"));
        Projection projection = projection("{'field':'_id'}");
        ctx.addDocument(doc);
        CRUDInsertionResponse response = controller.insert(ctx, projection);

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        ctx.add(md);
        controller.find(ctx,query("{'field':'field1','op':'=','rvalue':'f1'}"),
                        projection("{'field':'*','recursive':1}"),null,null,null);
        Assert.assertEquals(1,ctx.getDocuments().size());
        Assert.assertNull(ctx.getDocuments().get(0).get(new Path("field7.0.elemf1")));
    }

}
