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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.redhat.lightblue.DataError;
import com.redhat.lightblue.ExecutionOptions;
import com.redhat.lightblue.ResultMetadata;
import com.redhat.lightblue.config.ControllerConfiguration;
import com.redhat.lightblue.crud.CRUDDeleteResponse;
import com.redhat.lightblue.crud.CRUDFindResponse;
import com.redhat.lightblue.crud.CRUDHealth;
import com.redhat.lightblue.crud.CRUDInsertionResponse;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.CRUDSaveResponse;
import com.redhat.lightblue.crud.CRUDUpdateResponse;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.crud.DocumentStream;
import com.redhat.lightblue.metadata.ArrayField;
import com.redhat.lightblue.metadata.CompositeMetadata;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.Field;
import com.redhat.lightblue.metadata.FieldConstraint;
import com.redhat.lightblue.metadata.FieldCursor;
import com.redhat.lightblue.metadata.Index;
import com.redhat.lightblue.metadata.IndexSortKey;
import com.redhat.lightblue.metadata.MetadataStatus;
import com.redhat.lightblue.metadata.ObjectArrayElement;
import com.redhat.lightblue.metadata.ObjectField;
import com.redhat.lightblue.metadata.SimpleArrayElement;
import com.redhat.lightblue.metadata.SimpleField;
import com.redhat.lightblue.metadata.Version;
import com.redhat.lightblue.metadata.constraints.IdentityConstraint;
import com.redhat.lightblue.metadata.types.IntegerType;
import com.redhat.lightblue.metadata.types.StringType;
import com.redhat.lightblue.mongo.common.DBResolver;
import com.redhat.lightblue.mongo.common.MongoDataStore;
import com.redhat.lightblue.mongo.config.MongoConfiguration;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.JsonUtils;
import com.redhat.lightblue.util.Path;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class MongoCRUDControllerTest extends AbstractMongoCrudTest {

  private MongoCRUDController controller;
  private DBResolver dbResolver;

  @Override
  @Before
  public void setup() throws Exception {
    super.setup();

    final DB dbx = db;
    // Cleanup stuff
    dbx.getCollection(COLL_NAME).drop();
    dbx.createCollection(COLL_NAME, null);

    dbResolver = new DBResolver() {
      @Override
      public DB get(MongoDataStore store) {
        return dbx;
      }

      @Override
      public MongoConfiguration getConfiguration(MongoDataStore store) {
        MongoConfiguration configuration = new MongoConfiguration();
        try {
          configuration.addServerAddress("localhost", 27777);
          configuration.setDatabase("mongo");
        } catch (UnknownHostException e) {
          return null;
        }
        return configuration;
      }

      @Override
      public Collection<MongoConfiguration> getConfigurations() {

        List<MongoConfiguration> configs = new ArrayList<>();
        configs.add(getConfiguration(null));
        return configs;
      }
    };

    controller = new MongoCRUDController(null, dbResolver);
  }

  private void addDocument(CRUDOperationContext ctx, JsonDoc doc) {
    if (ctx.getInputDocuments() == null) {
      ArrayList<DocCtx> list = new ArrayList<>();
      ctx.setInputDocuments(list);
    }
    ctx.getInputDocuments().add(new DocCtx(doc));
  }

  private void addDocuments(CRUDOperationContext ctx, List<JsonDoc> docs) {
    for (JsonDoc doc : docs) {
      addDocument(ctx, doc);
    }
  }

  private List<DocCtx> streamToList(CRUDOperationContext ctx) {
    List<DocCtx> list = new ArrayList<>();
    DocumentStream<DocCtx> stream = ctx.getDocumentStream();
    while (stream.hasNext()) {
      list.add(stream.next());
    }
    return list;
  }

  @Test
  public void ensureIndexNotRecreated() throws IOException, InterruptedException {
    db.getCollection("testCollectionIndex1").drop();
    EntityMetadata md = createMetadata();
    controller.afterUpdateEntityInfo(null, md.getEntityInfo(), false);
    TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdataCI.json"));
    addDocument(ctx, doc);
    controller.insert(ctx, null);
    addCIIndexes(md);
    controller.afterUpdateEntityInfo(null, md.getEntityInfo(), false);

    Thread.sleep(5000);
    DBCollection coll = db.getCollection("testCollectionIndex1");
    DBCursor find = coll.find();
    DBObject obj = find.next();
    assertTrue(
        ((DBObject) obj.get(DocTranslator.HIDDEN_SUB_PATH.toString())).containsField("field3"));

    DBObject hidden = (DBObject) obj.get(DocTranslator.HIDDEN_SUB_PATH.toString());
    hidden.removeField("field3");
    obj.put(DocTranslator.HIDDEN_SUB_PATH.toString(), hidden);
    coll.save(obj);
    find.close();

    controller.afterUpdateEntityInfo(null, md.getEntityInfo(), false);
    coll = db.getCollection("testCollectionIndex1");
    find = coll.find();
    obj = find.next();
    assertFalse(
        ((DBObject) obj.get(DocTranslator.HIDDEN_SUB_PATH.toString())).containsField("field3"));
  }

  @Test(expected = Error.class)
  public void invalidArrayIndex() {
    db.getCollection("testCollectionIndex1").drop();
    EntityMetadata md = createMetadata();
    addCIIndexes(md);

    IndexSortKey newIndexKey = new IndexSortKey(new Path("new.*.primarray.*"), true, true);
    Index newIndex = new Index(newIndexKey);
    newIndex.setName("invalidArrayIndex");
    newIndex.setUnique(true);
    md.getEntityInfo().getIndexes().add(newIndex);
    md.getClass();

    controller.beforeUpdateEntityInfo(null, md.getEntityInfo(), false);
  }

  @Test
  public void findDocument_CI() throws Exception {
    db.getCollection("testCollectionIndex1").drop();
    EntityMetadata emd = addCIIndexes(createMetadata());
    controller.afterUpdateEntityInfo(null, emd.getEntityInfo(), false);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdataCI.json"));

    TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.INSERT);
    ctx.add(emd);
    addDocument(ctx, doc);
    controller.insert(ctx, null);

    ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.FIND);
    ctx.add(emd);
    controller.find(ctx,
        query("{'field':'field2.subArrayField.*', 'regex':'fieldTwoSubArrOne', 'options':'i'}"),
        projection("{'field':'field2.subArrayField.*'}"), null, null, null);

    List<DocCtx> documents = streamToList(ctx);
    assertEquals(1, documents.size());
    Assert.assertNotNull(documents.get(0).getResultMetadata());
    Assert.assertNotNull(documents.get(0).getResultMetadata().getDocumentVersion());
  }

  @Test
  public void findDocument_CI_docver() throws Exception {
    db.getCollection("testCollectionIndex1").drop();
    EntityMetadata emd = addCIIndexes(createMetadataWithDocVer());
    controller.afterUpdateEntityInfo(null, emd.getEntityInfo(), false);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdataCI.json"));

    TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.INSERT);
    ctx.add(emd);
    addDocument(ctx, doc);
    controller.insert(ctx, null);

    ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.FIND);
    ctx.add(emd);
    controller.find(ctx,
        query("{'field':'field2.subArrayField.*', 'regex':'fieldTwoSubArrOne', 'options':'i'}"),
        projection("{'field':'field2.subArrayField.*'}"), null, null, null);

    List<DocCtx> documents = streamToList(ctx);
    assertEquals(1, documents.size());
    Assert.assertEquals(documents.get(0).getResultMetadata().getDocumentVersion(),
        documents.get(0).get(new Path("docver")).asText());
    Assert.assertEquals(documents.get(0).getResultMetadata().getDocumentVersion(),
        documents.get(0).get(new Path("resultMetadata.documentVersion")).asText());
    Assert.assertNotNull(documents.get(0).getResultMetadata());
    Assert.assertNotNull(documents.get(0).getResultMetadata().getDocumentVersion());
  }

  @Test
  public void findDocumentElemMatch_CI() throws Exception {
    db.getCollection("testCollectionIndex1").drop();
    EntityMetadata emd = addCIIndexes(createMetadata());
    controller.afterUpdateEntityInfo(null, emd.getEntityInfo(), false);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdataCI.json"));

    TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.INSERT);
    ctx.add(emd);
    addDocument(ctx, doc);
    controller.insert(ctx, null);

    DBCollection collection = db.getCollection("testCollectionIndex1");

    BasicDBList hiddenList = new BasicDBList();
    hiddenList.put(0,
        new BasicDBObject("@mongoHidden",
            new BasicDBObject("x", "NEWVALUE")));

    collection.update(new BasicDBObject("field1", "fieldOne"),
        new BasicDBObject("$set",
            new BasicDBObject("arrayObj", hiddenList)));

    ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.FIND);
    ctx.add(emd);
    controller.find(ctx, query(
            "{'array': 'arrayObj', 'elemMatch': { 'field': 'x', 'regex': 'newValue', 'caseInsensitive': true}}"),
        projection("{'field':'_id'}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    assertEquals(1, documents.size());
    Assert.assertNotNull(documents.get(0).getResultMetadata());
    Assert.assertNotNull(documents.get(0).getResultMetadata().getDocumentVersion());
  }


  @Test
  public void updateDocument_CI() throws Exception {
    db.getCollection("testCollectionIndex1").drop();
    EntityMetadata emd = addCIIndexes(createMetadata());
    controller.afterUpdateEntityInfo(null, emd.getEntityInfo(), false);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdataCI.json"));

    TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.INSERT);
    ctx.add(emd);
    addDocument(ctx, doc);
    controller.insert(ctx, null);

    ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.UPDATE);
    ctx.add(emd);
    controller.update(ctx,
        query("{'field':'field1','op':'$eq','rvalue':'fieldOne'}"),
        update("{ '$set': { 'field3' : 'newFieldThree' } }"),
        projection("{'field':'field3'}"));

    try (DBCursor cursor = db.getCollection("testCollectionIndex1").find()) {
      assertTrue(cursor.hasNext());
      cursor.forEach(obj -> {
        DBObject hidden = (DBObject) obj.get(DocTranslator.HIDDEN_SUB_PATH.toString());
        assertEquals("NEWFIELDTHREE", hidden.get("field3"));
      });
      List<DocCtx> documents = streamToList(ctx);
      Assert.assertNotNull(documents.get(0).getResultMetadata());
      Assert.assertNotNull(documents.get(0).getResultMetadata().getDocumentVersion());
    }

  }

  @Test
  public void saveDocument_CI() throws Exception {
    db.getCollection("testCollectionIndex1").drop();
    EntityMetadata emd = addCIIndexes(createMetadata());
    controller.afterUpdateEntityInfo(null, emd.getEntityInfo(), false);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdataCI.json"));

    TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.INSERT);
    ctx.add(emd);
    addDocument(ctx, doc);
    controller.insert(ctx, null);

    DBCursor cursor = db.getCollection("testCollectionIndex1").find();
    String id = cursor.next().get("_id").toString();

    doc = new JsonDoc(loadJsonNode("./testdataCI2.json"));
    doc.modify(new Path("_id"), JsonNodeFactory.instance.textNode(id), true);
    ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.SAVE);
    ctx.add(emd);
    addDocument(ctx, doc);

    controller.save(ctx, true, projection("{'field':'field3'}"));

    cursor = db.getCollection("testCollectionIndex1").find();
    assertTrue(cursor.hasNext());
    cursor.forEach(obj -> {
      DBObject hidden = (DBObject) obj.get(DocTranslator.HIDDEN_SUB_PATH.toString());
      assertEquals("NEWFIELDTHREE", hidden.get("field3"));
    });
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertNotNull(documents.get(0).getResultMetadata());
    Assert.assertNotNull(documents.get(0).getResultMetadata().getDocumentVersion());
  }

  @Test
  public void addDataAfterIndexExists_CI() throws IOException {
    db.getCollection("testCollectionIndex1").drop();
    EntityMetadata emd = addCIIndexes(createMetadata());
    controller.afterUpdateEntityInfo(null, emd.getEntityInfo(), false);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdataCI.json"));

    TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.INSERT);
    ctx.add(emd);
    addDocument(ctx, doc);
    controller.insert(ctx, null);
    DBCursor cursor = db.getCollection("testCollectionIndex1").find();
    assertTrue(cursor.hasNext());
    cursor.forEach(obj -> {
      DBObject hidden = (DBObject) obj.get(DocTranslator.HIDDEN_SUB_PATH.toString());
      DBObject arrayObj0Hidden = (DBObject) ((DBObject) ((BasicDBList) obj.get("arrayObj")).get(
          0)).get(DocTranslator.HIDDEN_SUB_PATH.toString());
      DBObject arrayObj1Hidden = (DBObject) ((DBObject) ((BasicDBList) obj.get("arrayObj")).get(
          1)).get(DocTranslator.HIDDEN_SUB_PATH.toString());
      DBObject arrayObj2Hidden = (DBObject) ((DBObject) ((BasicDBList) ((DBObject) ((BasicDBList) obj.get(
          "arrayObj")).get(2)).get("arraySubObj2")).get(0)).get(
          DocTranslator.HIDDEN_SUB_PATH.toString());
      DBObject field2Hidden = (DBObject) ((DBObject) obj.get("field2")).get(
          DocTranslator.HIDDEN_SUB_PATH.toString());

      assertEquals("FIELDTHREE", hidden.get("field3"));
      assertEquals("ARRAYFIELDONE", ((BasicDBList) hidden.get("arrayField")).get(0));
      assertEquals("ARRAYFIELDTWO", ((BasicDBList) hidden.get("arrayField")).get(1));
      assertEquals("ARRAYOBJXONE", arrayObj0Hidden.get("x"));
      assertEquals("ARRAYOBJONESUBOBJONE",
          ((BasicDBList) arrayObj0Hidden.get("arraySubObj")).get(0));
      assertEquals("ARRAYOBJONESUBOBJTWO",
          ((BasicDBList) arrayObj0Hidden.get("arraySubObj")).get(1));
      assertEquals("ARRAYOBJXTWO", arrayObj1Hidden.get("x"));
      assertEquals("ARRAYOBJTWOSUBOBJONE",
          ((BasicDBList) arrayObj1Hidden.get("arraySubObj")).get(0));
      assertEquals("ARRAYOBJTWOSUBOBJTWO",
          ((BasicDBList) arrayObj1Hidden.get("arraySubObj")).get(1));
      assertEquals("FIELDTWOX", field2Hidden.get("x"));
      assertEquals("FIELDTWOSUBARRONE", ((BasicDBList) field2Hidden.get("subArrayField")).get(0));
      assertEquals("FIELDTWOSUBARRTWO", ((BasicDBList) field2Hidden.get("subArrayField")).get(1));
      assertEquals("ARRAYSUBOBJ2Y", arrayObj2Hidden.get("y"));

    });
  }

  @Test
  public void createIndexAfterDataExists_CI() throws IOException, InterruptedException {
    db.getCollection("testCollectionIndex1").drop();
    EntityMetadata md = createMetadata();
    controller.afterUpdateEntityInfo(null, md.getEntityInfo(), false);
    TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdataCI.json"));
    addDocument(ctx, doc);
    CRUDInsertionResponse insert = controller.insert(ctx, null);
    addCIIndexes(md);
    controller.afterUpdateEntityInfo(null, md.getEntityInfo(), false);

    // wait a couple of seconds because the update runs in a ind thread
    Thread.sleep(5000);
    DBCursor cursor = db.getCollection("testCollectionIndex1").find();
    assertTrue(cursor.hasNext());
    cursor.forEach(obj -> {
      DBObject hidden = (DBObject) obj.get(DocTranslator.HIDDEN_SUB_PATH.toString());
      DBObject arrayObj0Hidden = (DBObject) ((DBObject) ((BasicDBList) obj.get("arrayObj")).get(
          0)).get(DocTranslator.HIDDEN_SUB_PATH.toString());
      DBObject arrayObj1Hidden = (DBObject) ((DBObject) ((BasicDBList) obj.get("arrayObj")).get(
          1)).get(DocTranslator.HIDDEN_SUB_PATH.toString());
      DBObject arrayObj2Hidden = (DBObject) ((DBObject) ((BasicDBList) ((DBObject) ((BasicDBList) obj.get(
          "arrayObj")).get(2)).get("arraySubObj2")).get(0)).get(
          DocTranslator.HIDDEN_SUB_PATH.toString());
      DBObject field2Hidden = (DBObject) ((DBObject) obj.get("field2")).get(
          DocTranslator.HIDDEN_SUB_PATH.toString());

      assertEquals("FIELDTHREE", hidden.get("field3"));
      assertEquals("ARRAYFIELDONE", ((BasicDBList) hidden.get("arrayField")).get(0));
      assertEquals("ARRAYFIELDTWO", ((BasicDBList) hidden.get("arrayField")).get(1));
      assertEquals("ARRAYOBJXONE", arrayObj0Hidden.get("x"));
      assertEquals("ARRAYOBJONESUBOBJONE",
          ((BasicDBList) arrayObj0Hidden.get("arraySubObj")).get(0));
      assertEquals("ARRAYOBJONESUBOBJTWO",
          ((BasicDBList) arrayObj0Hidden.get("arraySubObj")).get(1));
      assertEquals("ARRAYOBJXTWO", arrayObj1Hidden.get("x"));
      assertEquals("ARRAYOBJTWOSUBOBJONE",
          ((BasicDBList) arrayObj1Hidden.get("arraySubObj")).get(0));
      assertEquals("ARRAYOBJTWOSUBOBJTWO",
          ((BasicDBList) arrayObj1Hidden.get("arraySubObj")).get(1));
      assertEquals("FIELDTWOX", field2Hidden.get("x"));
      assertEquals("FIELDTWOSUBARRONE", ((BasicDBList) field2Hidden.get("subArrayField")).get(0));
      assertEquals("FIELDTWOSUBARRTWO", ((BasicDBList) field2Hidden.get("subArrayField")).get(1));
      assertEquals("ARRAYSUBOBJ2Y", arrayObj2Hidden.get("y"));

    });
  }

  @Test
  public void modifyExistingIndex_CI() {
    EntityMetadata e = addCIIndexes(createMetadata());
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    List<Index> indexes = e.getEntityInfo().getIndexes().getIndexes();
    List<Index> newIndexes = new ArrayList<>();

    indexes.forEach(i -> {
      List<IndexSortKey> fields = i.getFields();
      List<IndexSortKey> newFields = new ArrayList<>();
      fields.forEach(
          f -> newFields.add(new IndexSortKey(f.getField(), f.isDesc(), !f.isCaseInsensitive())));
      i.setFields(newFields);
      newIndexes.add(i);
    });

    e.getEntityInfo().getIndexes().getIndexes().clear();
    e.getEntityInfo().getIndexes().setIndexes(newIndexes);

    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    DBCollection entityCollection = db.getCollection("testCollectionIndex1");

    List<String> indexInfo = entityCollection.getIndexInfo().stream()
        .map(j -> j.get("key"))
        .map(Object::toString)
        .collect(Collectors.toList());

    // make sure the indexes are there correctly
    assertFalse(indexInfo.toString().contains("\"field1\""));
    assertTrue(indexInfo.toString().contains("@mongoHidden.field1"));

    assertFalse(indexInfo.toString().contains("@mongoHidden.field3"));
    assertTrue(indexInfo.toString().contains("field3"));

    assertFalse(indexInfo.toString().contains("arrayObj.@mongoHidden.x"));
    assertTrue(indexInfo.toString().contains("arrayObj.x"));

    assertFalse(indexInfo.toString().contains("arrayObj.@mongoHidden.arraySubObj"));
    assertTrue(indexInfo.toString().contains("arrayObj.arraySubObj"));

    assertFalse(indexInfo.toString().contains("@mongoHidden.arrayField"));
    assertTrue(indexInfo.toString().contains("arrayField"));

    assertFalse(indexInfo.toString().contains("field2.@mongoHidden.x"));
    assertTrue(indexInfo.toString().contains("field2.x"));

    assertFalse(indexInfo.toString().contains("field2.@mongoHidden.subArrayField"));
    assertTrue(indexInfo.toString().contains("field2.subArrayField"));

  }

  @Test
  public void createNewIndex_CI() {
    EntityMetadata e = addCIIndexes(createMetadata());
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    DBCollection entityCollection = db.getCollection("testCollectionIndex1");

    List<String> indexInfo = entityCollection.getIndexInfo().stream()
        .map(j -> j.get("key"))
        .map(Object::toString)
        .collect(Collectors.toList());

    assertTrue(indexInfo.toString().contains("field1"));
    assertTrue(indexInfo.toString().contains("@mongoHidden.field3"));
    assertTrue(indexInfo.toString().contains("arrayObj.@mongoHidden.x"));
    assertTrue(indexInfo.toString().contains("arrayObj.@mongoHidden.arraySubObj"));
    assertTrue(indexInfo.toString().contains("@mongoHidden.arrayField"));
    assertTrue(indexInfo.toString().contains("field2.@mongoHidden.x"));
    assertTrue(indexInfo.toString().contains("field2.@mongoHidden.subArrayField"));
    assertTrue(indexInfo.toString().contains("arrayObj.arraySubObj2.@mongoHidden.y"));
  }

  @Test
  public void createPartialIndex_CI() throws Exception {
    EntityMetadata md = getMd("./testMetadata_partialIndex.json");

    // save metadata
    controller.afterUpdateEntityInfo(null, md.getEntityInfo(), true);

    DBCollection entityCollection = db.getCollection("data");
    DBObject indexCreated = entityCollection.getIndexInfo().get(1);
    Assert.assertEquals("testPartialIndex", indexCreated.get("name"));
    Assert.assertEquals(
        "{\"$and\": [{\"field6.nf7.nnf2\": {\"$gt\": 5}}, {\"field6.nf7.nnf2\": {\"$lt\": 100}}]}",
        indexCreated.get("partialFilterExpression").toString());

    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);

    JsonNode node = loadJsonNode("./testdata_partial_index.json");

    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx,
        new JsonDoc(node.get(0))); // field3: 1, partial unique index does not include it
    addDocument(ctx, new JsonDoc(node.get(1))); // field3: 6, partial unique index does include it

    CRUDInsertionResponse response = controller.insert(ctx, projection);

    // this would fail for a non-partial unique index
    Assert.assertEquals("Partial unique index should allow both docs to be inserted", 2,
        response.getNumInserted());

    ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    addDocument(ctx, new JsonDoc(node.get(2)));
    response = controller.insert(ctx, projection);

    List<DocCtx> documents = streamToList(ctx);
    // this would fail if there was no index
    Assert.assertEquals("Partial unique index should prevent document from being inserted", 0,
        response.getNumInserted());
    Assert.assertEquals(1, documents.get(0).getErrors().size());
    Assert.assertEquals(MongoCrudConstants.ERR_DUPLICATE,
        documents.get(0).getErrors().get(0).getErrorCode());

  }

  @Test
  public void indexOptionMatchTest() {
    Index i = new Index();
    i.getProperties().put("foo", "bar");
    Assert.assertFalse(i.isUnique());

    BasicDBObject dbI = new BasicDBObject();
    Assert.assertNull(dbI.get("unique"));

    Assert.assertTrue("unique: false should be the same as existing unique: null",
        MongoCRUDController.indexOptionsMatch(i, dbI));

    i.setUnique(true);

    Assert.assertFalse("unique: true should be different than existing unique: null",
        MongoCRUDController.indexOptionsMatch(i, dbI));

    dbI = new BasicDBObject();
    dbI.append("unique", true);

    Assert.assertTrue(MongoCRUDController.indexOptionsMatch(i, dbI));

    dbI.append("foo", "bar2");
    Assert.assertTrue("Property foo should be ignored, indexes equal",
        MongoCRUDController.indexOptionsMatch(i, dbI));
  }

  @Test
  public void indexOptionMatchTest_partialFilterExpression() {
    EntityMetadata md = getMd("./testMetadata_partialIndex.json");

    // save metadata
    controller.afterUpdateEntityInfo(null, md.getEntityInfo(), true);

    DBCollection entityCollection = db.getCollection("data");
    DBObject indexFromDb = entityCollection.getIndexInfo().get(1);
    Assert.assertEquals("testPartialIndex", indexFromDb.get("name"));

    Index metadataIndex = md.getEntityInfo().getIndexes().getIndexes().get(0);

    Assert.assertTrue("partialFilterExpression index option is the same, indexes should match",
        MongoCRUDController.indexOptionsMatch(metadataIndex, indexFromDb));

    // remove one element from the filter expression
    ((ArrayList) ((java.util.HashMap) metadataIndex.getProperties()
        .get(MongoCRUDController.PARTIAL_FILTER_EXPRESSION_OPTION_NAME)).get("$and")).remove(1);

    Assert.assertFalse(
        "partialFilterExpression index option is different, indexes should not match",
        MongoCRUDController.indexOptionsMatch(metadataIndex, indexFromDb));
  }

  @Test
  public void dropExistingIndex_CI() {
    EntityMetadata e = addCIIndexes(createMetadata());
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    e.getEntityInfo().getIndexes().getIndexes().clear();

    Index index = new Index();
    index.setName("testIndex");
    index.setUnique(true);
    List<IndexSortKey> indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));

    index.setFields(indexFields);
    List<Index> indexes = new ArrayList<>();
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);

    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    DBCollection entityCollection = db.getCollection("testCollectionIndex1");

    List<String> indexInfo = entityCollection.getIndexInfo().stream()
        .filter(i -> "testIndex".equals(i.get("name")))
        .map(j -> j.get("key"))
        .map(Object::toString)
        .collect(Collectors.toList());

    assertTrue(indexInfo.toString().contains("field1"));

    assertFalse(indexInfo.toString().contains("@mongoHidden.field3"));
    assertFalse(indexInfo.toString().contains("arrayObj.*.@mongoHidden.x"));
    assertFalse(indexInfo.toString().contains("arrayObj.*.@mongoHidden.arraySubObj"));
    assertFalse(indexInfo.toString().contains("@mongoHidden.arrayField"));
    assertFalse(indexInfo.toString().contains("field2.@mongoHidden.x"));
    assertFalse(indexInfo.toString().contains("field2.@mongoHidden.subArrayField"));
  }

  private EntityMetadata createMetadata() {
    EntityMetadata e = new EntityMetadata("testEntity");
    e.setVersion(new Version("1.0.0", null, "some text blah blah"));
    e.setStatus(MetadataStatus.ACTIVE);
    e.getFields().put(new SimpleField("_id", StringType.TYPE));
    e.getFields().put(new SimpleField("objectType", StringType.TYPE));

    e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex1"));
    e.getFields().put(new SimpleField("field1", StringType.TYPE));
    e.getFields().put(new SimpleField("field3", StringType.TYPE));

    ObjectField o = new ObjectField("field2");
    o.getFields().put(new SimpleField("x", StringType.TYPE));

    SimpleArrayElement saSub = new SimpleArrayElement(StringType.TYPE);
    ArrayField afSub = new ArrayField("subArrayField", saSub);
    o.getFields().put(afSub);

    ObjectArrayElement oaObject = new ObjectArrayElement();
    oaObject.getFields().put(new SimpleField("x", StringType.TYPE));

    SimpleArrayElement saSubSub = new SimpleArrayElement(StringType.TYPE);
    ArrayField afSubSub = new ArrayField("arraySubObj", saSubSub);
    oaObject.getFields().put(afSubSub);

    ObjectArrayElement objSubSub = new ObjectArrayElement();
    objSubSub.getFields().put(new SimpleField("y", StringType.TYPE));
    ArrayField objSubSubField = new ArrayField("arraySubObj2", objSubSub);
    oaObject.getFields().put(objSubSubField);

    ArrayField afObject = new ArrayField("arrayObj", oaObject);
    e.getFields().put(afObject);

    e.getFields().put(o);

    SimpleArrayElement sa = new SimpleArrayElement(StringType.TYPE);
    ArrayField af = new ArrayField("arrayField", sa);
    e.getFields().put(af);

    e.getEntityInfo().setDefaultVersion("1.0.0");
    e.getEntitySchema().getAccess().getInsert().setRoles("anyone");
    e.getEntitySchema().getAccess().getFind().setRoles("anyone");
    e.getEntitySchema().getAccess().getUpdate().setRoles("anyone");

    return e;
  }

  private EntityMetadata createMetadataWithDocVer() {
    EntityMetadata e = new EntityMetadata("testEntity");
    e.setVersion(new Version("1.0.0", null, "some text blah blah"));
    e.setStatus(MetadataStatus.ACTIVE);
    e.getFields().put(new SimpleField("_id", StringType.TYPE));
    e.getFields().put(new SimpleField("objectType", StringType.TYPE));

    e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex1"));
    e.getFields().put(new SimpleField("field1", StringType.TYPE));
    e.getFields().put(new SimpleField("field3", StringType.TYPE));

    SimpleField f = new SimpleField("docver", StringType.TYPE);
    f.getProperties().put(ResultMetadata.MD_PROPERTY_DOCVER, Boolean.TRUE);
    e.getFields().put(f);

    ObjectField x = new ObjectField("resultMetadata");
    x.getProperties().put(ResultMetadata.MD_PROPERTY_RESULT_METADATA, Boolean.TRUE);
    e.getFields().put(x);

    ObjectField o = new ObjectField("field2");
    o.getFields().put(new SimpleField("x", StringType.TYPE));

    SimpleArrayElement saSub = new SimpleArrayElement(StringType.TYPE);
    ArrayField afSub = new ArrayField("subArrayField", saSub);
    o.getFields().put(afSub);

    ObjectArrayElement oaObject = new ObjectArrayElement();
    oaObject.getFields().put(new SimpleField("x", StringType.TYPE));

    SimpleArrayElement saSubSub = new SimpleArrayElement(StringType.TYPE);
    ArrayField afSubSub = new ArrayField("arraySubObj", saSubSub);
    oaObject.getFields().put(afSubSub);

    ObjectArrayElement objSubSub = new ObjectArrayElement();
    objSubSub.getFields().put(new SimpleField("y", StringType.TYPE));
    ArrayField objSubSubField = new ArrayField("arraySubObj2", objSubSub);
    oaObject.getFields().put(objSubSubField);

    ArrayField afObject = new ArrayField("arrayObj", oaObject);
    e.getFields().put(afObject);

    e.getFields().put(o);

    SimpleArrayElement sa = new SimpleArrayElement(StringType.TYPE);
    ArrayField af = new ArrayField("arrayField", sa);
    e.getFields().put(af);

    e.getEntityInfo().setDefaultVersion("1.0.0");
    e.getEntitySchema().getAccess().getInsert().setRoles("anyone");
    e.getEntitySchema().getAccess().getFind().setRoles("anyone");
    e.getEntitySchema().getAccess().getUpdate().setRoles("anyone");

    return e;
  }

  private EntityMetadata addCIIndexes(EntityMetadata e) {
    Index index1 = new Index();
    index1.setName("testIndex1");
    index1.setUnique(true);
    List<IndexSortKey> indexFields1 = new ArrayList<>();
    indexFields1.add(new IndexSortKey(new Path("field1"), true));
    indexFields1.add(new IndexSortKey(new Path("field3"), true, true));
    indexFields1.add(new IndexSortKey(new Path("arrayField"), true, true));
    indexFields1.add(new IndexSortKey(new Path("field2.x"), true, true));
    index1.setFields(indexFields1);

    Index index2 = new Index();
    index2.setName("testIndex2");
    index2.setUnique(true);
    List<IndexSortKey> indexFields2 = new ArrayList<>();
    indexFields2.add(new IndexSortKey(new Path("arrayObj.*.x"), true, true));
    index2.setFields(indexFields2);

    Index index3 = new Index();
    index3.setName("testIndex3");
    index3.setUnique(true);
    List<IndexSortKey> indexFields3 = new ArrayList<>();
    indexFields3.add(new IndexSortKey(new Path("arrayObj.*.arraySubObj"), true, true));
    index3.setFields(indexFields3);

    Index index4 = new Index();
    index4.setName("testIndex4");
    index4.setUnique(true);
    List<IndexSortKey> indexFields4 = new ArrayList<>();
    indexFields4.add(new IndexSortKey(new Path("field2.subArrayField"), true, true));
    index4.setFields(indexFields4);

    Index index5 = new Index();
    index5.setName("testIndex5");
    index5.setUnique(true);
    List<IndexSortKey> indexFields5 = new ArrayList<>();
    indexFields5.add(new IndexSortKey(new Path("arrayObj.*.arraySubObj2.*.y"), true, true));
    index5.setFields(indexFields5);

    List<Index> indexes = new ArrayList<>();
    indexes.add(index1);
    indexes.add(index2);
    indexes.add(index3);
    indexes.add(index4);
    indexes.add(index5);
    e.getEntityInfo().getIndexes().setIndexes(indexes);
    return e;
  }

  private List<DataError> getDataErrors(CRUDOperationContext ctx) {
    List<DataError> l = new ArrayList<>();
    for (DocCtx d : ctx.getInputDocuments()) {
      if (d.hasErrors()) {
        l.add(d.getDataError());
      }
    }
    return l;
  }

  @Test
  public void insertTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);
    System.out.println(getDataErrors(ctx));
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
    Assert.assertTrue(ctx.getErrors() == null || ctx.getErrors().isEmpty());
    Assert.assertTrue(getDataErrors(ctx).isEmpty());
    Assert.assertEquals(ctx.getInputDocumentsWithoutErrors().size(), response.getNumInserted());
    String id = documents.get(0).getOutputDocument().get(new Path("_id")).asText();
    try (DBCursor c = coll.find(new BasicDBObject("_id", DocTranslator.createIdFrom(id)))) {
      Assert.assertEquals(1, c.count());
    }
    Assert.assertNotNull(documents.get(0).getResultMetadata());
    Assert.assertNotNull(documents.get(0).getResultMetadata().getDocumentVersion());
  }

  @Test
  public void insertTest_nullReqField() throws Exception {
    EntityMetadata md = getMd("./testMetadata-requiredFields2.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    doc.modify(new Path("field1"), null, false);
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);
    List<DocCtx> documents = streamToList(ctx);
    System.out.println(getDataErrors(ctx));
    Assert.assertEquals(1, documents.size());
    Assert.assertTrue(ctx.getErrors() == null || ctx.getErrors().isEmpty());
    Assert.assertTrue(getDataErrors(ctx).isEmpty());
    Assert.assertEquals(ctx.getInputDocumentsWithoutErrors().size(), response.getNumInserted());
    String id = documents.get(0).getOutputDocument().get(new Path("_id")).asText();
    try (DBCursor c = coll.find(new BasicDBObject("_id", DocTranslator.createIdFrom(id)))) {
      Assert.assertEquals(1, c.count());
    }
  }

  @Test
  @Ignore
  public void insertTest_empty_array() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata_empty_array.json"));
    Projection projection = projection("{'field':'field7'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);
    System.out.println(getDataErrors(ctx));
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
    Assert.assertTrue(ctx.getErrors() == null || ctx.getErrors().isEmpty());
    Assert.assertTrue(getDataErrors(ctx).isEmpty());
    Assert.assertEquals(ctx.getInputDocumentsWithoutErrors().size(), response.getNumInserted());
    JsonNode field7Node = documents.get(0).getOutputDocument().get(new Path("field7"));
    Assert.assertNotNull("empty array was not inserted", field7Node);
    Assert.assertTrue("field7 should be type ArrayNode", field7Node instanceof ArrayNode);
    String field7 = field7Node.asText();
    Assert.assertNotNull(field7);
  }

  @Test
  public void insertTest_nullProjection() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    Projection projection = null;
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);
    List<DocCtx> documents = streamToList(ctx);

    // basic checks
    Assert.assertEquals(1, documents.size());
    Assert.assertTrue(ctx.getErrors() == null || ctx.getErrors().isEmpty());
    Assert.assertTrue(getDataErrors(ctx).isEmpty());
    Assert.assertEquals(ctx.getInputDocumentsWithoutErrors().size(), response.getNumInserted());

    // verify there is nothing projected
    Assert.assertNotNull(documents.get(0).getOutputDocument());
  }

  @Test
  public void saveTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    System.out.println("Write doc:" + doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);
    List<DocCtx> documents = streamToList(ctx);
    String id = documents.get(0).getOutputDocument().get(new Path("_id")).asText();
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    JsonDoc readDoc = documents.get(0);
    // Change some fields
    System.out.println("Read doc:" + readDoc);
    readDoc.modify(new Path("field1"), nodeFactory.textNode("updated"), false);
    readDoc.modify(new Path("field7.0.elemf1"), nodeFactory.textNode("updated too"), false);
    Assert.assertEquals(documents.size(), response.getNumInserted());
    Assert.assertNotNull(documents.get(0).getResultMetadata());
    Assert.assertNotNull(documents.get(0).getResultMetadata().getDocumentVersion());

    // Save it back
    ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
    ctx.add(md);
    addDocument(ctx, readDoc);
    CRUDSaveResponse saveResponse = controller.save(ctx, false, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    // Read it back
    controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    JsonDoc r2doc = documents.get(0);
    Assert.assertEquals(readDoc.get(new Path("field1")).asText(),
        r2doc.get(new Path("field1")).asText());
    Assert.assertEquals(readDoc.get(new Path("field7.0.elemf1")).asText(),
        r2doc.get(new Path("field7.0.elemf1")).asText());
    Assert.assertEquals(documents.size(), saveResponse.getNumSaved());
  }

  @Test
  public void saveTest_ifsame() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    System.out.println("Write doc:" + doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);
    List<DocCtx> documents = streamToList(ctx);
    String id = documents.get(0).getOutputDocument().get(new Path("_id")).asText();

    QueryExpression q = query("{'field':'_id','op':'=','rvalue':'" + id + "'}");
    Projection p = projection("{'field':'*','recursive':1}");

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, q, p, null, null, null);
    documents = streamToList(ctx);
    String ver = documents.get(0).getResultMetadata().getDocumentVersion();
    JsonDoc readDoc = documents.get(0);

    // Save the doc back
    ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
    readDoc.modify(new Path("field1"), nodeFactory.textNode("updated1"), false);
    ctx.add(md);
    addDocument(ctx, readDoc);
    controller.save(ctx, false, projection);

    // Try saving it again with updateIfCurrent flag
    ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
    ctx.setUpdateIfCurrent(true);
    ctx.getUpdateDocumentVersions().add(ver);
    readDoc.modify(new Path("field1"), nodeFactory.textNode("updated2"), false);
    ctx.add(md);
    addDocument(ctx, readDoc);
    controller.save(ctx, false, projection);
    boolean hasDocumentErrors = false;
    for (DocCtx x : ctx.getInputDocuments()) {
      if (x.hasErrors()) {
        hasDocumentErrors = true;
      }
    }
    Assert.assertTrue(hasDocumentErrors);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    // Read it back
    controller.find(ctx, q, p, null, null, null);
    documents = streamToList(ctx);
    JsonDoc r2doc = documents.get(0);
    Assert.assertEquals("updated1", r2doc.get(new Path("field1")).asText());
  }

  @Test
  public void saveTest_duplicateKey() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    System.out.println("Write doc:" + doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);
    String id = ctx.getInputDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();

    doc.modify(new Path("_id"), nodeFactory.textNode(id), false);

    ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    addDocument(ctx, doc);
    controller.insert(ctx, projection);

    Assert.assertEquals(1, ctx.getInputDocuments().get(0).getErrors().size());
    Assert.assertEquals(MongoCrudConstants.ERR_DUPLICATE,
        ctx.getInputDocuments().get(0).getErrors().get(0).getErrorCode());

  }

  @Test
  public void saveIdTypeUidTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata4.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata4.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    System.out.println("Write doc:" + doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);
    String id = ctx.getInputDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    JsonDoc readDoc = documents.get(0);
    // Change some fields
    System.out.println("Read doc:" + readDoc);
    readDoc.modify(new Path("field1"), nodeFactory.textNode("updated"), false);
    readDoc.modify(new Path("field7.0.elemf1"), nodeFactory.textNode("updated too"), false);
    Assert.assertEquals(documents.size(), response.getNumInserted());

    // Save it back
    ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
    ctx.add(md);
    addDocument(ctx, readDoc);
    CRUDSaveResponse saveResponse = controller.save(ctx, false, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    // Read it back
    controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    JsonDoc r2doc = documents.get(0);
    Assert.assertEquals(readDoc.get(new Path("field1")).asText(),
        r2doc.get(new Path("field1")).asText());
    Assert.assertEquals(readDoc.get(new Path("field7.0.elemf1")).asText(),
        r2doc.get(new Path("field7.0.elemf1")).asText());
    Assert.assertEquals(documents.size(), saveResponse.getNumSaved());
  }

  @Test
  public void saveTestForInvisibleFields() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    // Insert a doc
    System.out.println("Write doc:" + doc);
    controller.insert(ctx, projection);
    List<DocCtx> documents = streamToList(ctx);
    String id = documents.get(0).getOutputDocument().get(new Path("_id")).asText();
    System.out.println("Saved id:" + id);

    // Read doc using mongo
    DBObject dbdoc = coll.findOne(new BasicDBObject("_id", DocTranslator.createIdFrom(id)));
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
    documents = streamToList(ctx);
    JsonDoc readDoc = documents.get(0);

    // Now we save the doc, and expect that the invisible field is still there
    readDoc.modify(new Path("field1"), nodeFactory.textNode("updated"), false);
    System.out.println("To update:" + readDoc);
    ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
    ctx.add(md);
    addDocument(ctx, readDoc);
    controller.save(ctx, false, projection);

    // Make sure doc is modified, and invisible field is there
    dbdoc = coll.findOne(new BasicDBObject("_id", DocTranslator.createIdFrom(id)));
    System.out.println("Loaded doc:" + dbdoc);
    Assert.assertEquals("updated", dbdoc.get("field1"));
    Assert.assertEquals("invisibleValue", dbdoc.get("invisibleField"));
  }

  @Test
  public void saveTestForUpdatedCopyOfDoc() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    // Insert a doc
    System.out.println("Write doc:" + doc);
    controller.insert(ctx, projection);
    String id = ctx.getInputDocuments().get(0).getOutputDocument().get(new Path("_id")).asText();
    System.out.println("Saved id:" + id);

    // Read the doc
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    JsonDoc readDoc = documents.get(0);

    // Now we save the doc, and expect that the invisible field is still there
    readDoc.modify(new Path("field1"), nodeFactory.textNode("updated"), false);
    System.out.println("To update:" + readDoc);
    ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
    ctx.add(md);
    addDocument(ctx, readDoc);
    controller.save(ctx, false, projection);

    // Make sure ctx has the modified doc
    Assert.assertEquals("updated",
        ctx.getInputDocuments().get(0).getUpdatedDocument().get(new Path("field1")).asText());
    Assert.assertNull(ctx.getInputDocuments().get(0).getOutputDocument().get(new Path("field1")));
  }

  @Test
  public void upsertTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    addDocument(ctx, doc);
    System.out.println("Write doc:" + doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
    List<DocCtx> documents = streamToList(ctx);
    String id = documents.get(0).getOutputDocument().get(new Path("_id")).asText();
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    JsonDoc readDoc = documents.get(0);
    // Remove id, to force re-insert
    readDoc.modify(new Path("_id"), null, false);
    Assert.assertEquals(documents.size(), response.getNumInserted());

    ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
    ctx.add(md);
    addDocument(ctx, readDoc);
    // This should not insert anything
    controller.save(ctx, false, projection("{'field':'_id'}"));
    CRUDSaveResponse sr;
    try (DBCursor c = coll.find(null)) {
      Assert.assertEquals(1, c.count());
    }

    ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
    ctx.add(md);
    addDocument(ctx, readDoc);
    sr = controller.save(ctx, true, projection("{'field':'_id'}"));
    try (DBCursor c = coll.find(null)) {
      Assert.assertEquals(2, c.count());
    }
    documents = streamToList(ctx);
    Assert.assertEquals(ctx.getInputDocumentsWithoutErrors().size(), sr.getNumSaved());
    Assert.assertNotNull(documents.get(0).getResultMetadata());
    Assert.assertNotNull(documents.get(0).getResultMetadata().getDocumentVersion());
  }

  @Test
  public void upsertWithIdsTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata_ids.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    addDocument(ctx, doc);
    System.out.println("Write doc:" + doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
    List<DocCtx> documents = streamToList(ctx);
    String id = documents.get(0).getOutputDocument().get(new Path("_id")).asText();

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    JsonDoc readDoc = documents.get(0);

    // Remove _id from the doc
    readDoc.modify(new Path("_id"), null, false);
    // Change a field
    Assert.assertEquals(1, readDoc.get(new Path("field3")).asInt());
    readDoc.modify(new Path("field3"), JsonNodeFactory.instance.numberNode(2), true);
    ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
    ctx.add(md);
    addDocument(ctx, readDoc);
    controller.save(ctx, false, projection("{'field':'_id'}"));

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    readDoc = documents.get(0);

    Assert.assertEquals(2, readDoc.get(new Path("field3")).asInt());
  }

  @Test
  public void upsertWithIdsTest2() throws Exception {
    EntityMetadata md = getMd("./testMetadata_ids_id_not_id.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    addDocument(ctx, doc);
    System.out.println("Write doc:" + doc);
    controller.updatePredefinedFields(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
    List<DocCtx> documents = streamToList(ctx);
    String id = documents.get(0).getOutputDocument().get(new Path("_id")).asText();

    //There should be 1 doc in the db
    Assert.assertEquals(1,
        controller.find(ctx, null, projection("{'field':'*','recursive':1}"), null, null, null)
            .getSize());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    JsonDoc readDoc = documents.get(0);

    //Remove _id from the doc
    readDoc.modify(new Path("_id"), null, false);
    //Change a field
    Assert.assertEquals(1, readDoc.get(new Path("field3")).asInt());
    readDoc.modify(new Path("field3"), JsonNodeFactory.instance.numberNode(2), true);
    ctx = new TestCRUDOperationContext(CRUDOperation.SAVE);
    ctx.add(md);
    addDocument(ctx, readDoc);
    controller.updatePredefinedFields(ctx, readDoc);
    CRUDSaveResponse sresponse = controller.save(ctx, true, projection("{'field':'_id'}"));
    Assert.assertEquals(1, sresponse.getNumSaved());
    //There should be 1 doc in the db
    Assert.assertEquals(1,
        controller.find(ctx, null, projection("{'field':'*','recursive':1}"), null, null, null)
            .getSize());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'_id','op':'=','rvalue':'" + id + "'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    readDoc = documents.get(0);

    Assert.assertEquals(2, readDoc.get(new Path("field3")).asInt());
  }

  @Test
  public void updateTest_SingleFailure() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    coll.createIndex(new BasicDBObject("field1", 1), "unique_test_index", true);
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
    addDocuments(ctx, docs);
    CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
    try (DBCursor c = coll.find(null)) {
      Assert.assertEquals(numDocs, c.count());
    }
    Assert.assertEquals(ctx.getInputDocumentsWithoutErrors().size(), response.getNumInserted());

    ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
    ctx.add(md);

    CRUDUpdateResponse upd = controller.update(ctx,
        query("{'field':'field3','op':'>','rvalue':-1}"),
        update("{ '$set': { 'field1' : '100' } }"),
        projection("{'field':'_id'}"));
    assertEquals(1, upd.getNumUpdated());
    assertEquals(20, upd.getNumMatched());
    assertEquals(19, upd.getNumFailed());
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertNotNull(documents.get(0).getResultMetadata());
    Assert.assertNotNull(documents.get(0).getResultMetadata().getDocumentVersion());
  }

  @Test
  public void update_MultiBatch() throws Exception {
    int batch = controller.getBatchSize() + 2;
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);

    // Generate some docs
    List<JsonDoc> docs = new ArrayList<>();
    for (int i = 0; i < batch; i++) {
      JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
      doc.modify(new Path("field1"), nodeFactory.textNode("doc" + i), false);
      doc.modify(new Path("field3"), nodeFactory.numberNode(i), false);
      docs.add(doc);
    }
    addDocuments(ctx, docs);
    CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
    try (DBCursor c = coll.find(null)) {
      Assert.assertEquals(batch, c.count());
    }
    Assert.assertEquals(ctx.getInputDocumentsWithoutErrors().size(), response.getNumInserted());
    ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
    ctx.add(md);
    CRUDUpdateResponse upd = controller.update(ctx,
        query("{'field':'field3','op':'>','rvalue':-1}"),
        update("{ '$set': { 'field1' : '100' } }"),
        projection("{'field':'_id'}"));
    // just make sure all get inserted when we're multi batching.  difficult to spy from here
    List<DocCtx> documents = streamToList(ctx);
    assertEquals(batch, upd.getNumUpdated());
    Assert.assertNotNull(documents.get(0).getResultMetadata());
    Assert.assertNotNull(documents.get(0).getResultMetadata().getDocumentVersion());

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
    addDocuments(ctx, docs);
    CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
    try (DBCursor c = coll.find(null)) {
      Assert.assertEquals(numDocs, c.count());
    }
    Assert.assertEquals(ctx.getInputDocumentsWithoutErrors().size(), response.getNumInserted());

    // Single doc update
    ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
    ctx.add(md);
    CRUDUpdateResponse upd = controller.update(ctx,
        query("{'field':'field3','op':'$eq','rvalue':10}"),
        update("{ '$set': { 'field3' : 1000 } }"),
        projection("{'field':'_id'}"));
    Assert.assertEquals(1, upd.getNumUpdated());
    Assert.assertEquals(0, upd.getNumFailed());
    //Assert.assertEquals(AtomicIterateUpdate.class, ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
    Assert.assertEquals(IterateAndUpdate.class,
        ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
    List<DocCtx> documents = streamToList(ctx);
    try (DBCursor c = coll.find(new BasicDBObject("field3", 1000), new BasicDBObject("_id", 1))) {
      DBObject obj = c.next();
      Assert.assertNotNull(obj);
      System.out.println("DBObject:" + obj);
      System.out.println("Output doc:" + documents.get(0).getOutputDocument());
      Assert.assertEquals(documents.get(0).getOutputDocument().get(new Path("_id")).asText(),
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
    documents = streamToList(ctx);
    Assert.assertEquals(IterateAndUpdate.class,
        ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
    Assert.assertEquals(10, upd.getNumMatched()); // Doesn't update the one with field3:1000
    Assert.assertEquals(9, upd.getNumUpdated()); // Doesn't update the one with field3:1000
    Assert.assertEquals(0, upd.getNumFailed());
    try (DBCursor c = coll.find(new BasicDBObject("field3", new BasicDBObject("$gt", 10)))) {
      Assert.assertEquals(10, c.count());
    }

    // Bulk direct update
    ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
    ctx.add(md);
    upd = controller.update(ctx, query("{'field':'field3','op':'>','rvalue':10}"),
        update("{ '$set': { 'field3' : 1001 } }"), null);
    //Assert.assertEquals(AtomicIterateUpdate.class, ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
    Assert.assertEquals(IterateAndUpdate.class,
        ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
    Assert.assertEquals(10, upd.getNumMatched());
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
    Assert.assertEquals(IterateAndUpdate.class,
        ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
    Assert.assertEquals(10, upd.getNumUpdated());
    Assert.assertEquals(0, upd.getNumFailed());
    try (DBCursor c = coll.find(new BasicDBObject("field7.0.elemf1", "blah"))) {
      Assert.assertEquals(10, c.count());
    }
  }

  @Test
  public void updatingButNotModifyingReturnsProjection() throws Exception {
    // issue #391
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    addDocument(ctx, new JsonDoc(loadJsonNode("./testdata1.json")));
    controller.insert(ctx, projection("{'field':'_id'}"));

    ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
    ctx.add(md);
    // Updating an array field will force use of IterateAndupdate
    CRUDUpdateResponse upd = controller.update(ctx, query("{'field':'field3','op':'>','rvalue':0}"),
        update("{ '$set': { 'field7.0.elemf1' : 'value0_1' } }"), projection("{'field':'_id'}"));
    Assert.assertEquals(IterateAndUpdate.class,
        ctx.getProperty(MongoCRUDController.PROP_UPDATER).getClass());
    Assert.assertEquals(0, upd.getNumUpdated());
    Assert.assertEquals(1, upd.getNumMatched());
    Assert.assertEquals(0, upd.getNumFailed());
    Assert.assertTrue(ctx.getDocumentStream().hasNext());
  }

  @Test
  public void updateTest_nullReqField() throws Exception {
    EntityMetadata md = getMd("./testMetadata-requiredFields2.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);

    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    doc.modify(new Path("field1"), JsonNodeFactory.instance.nullNode(), false);
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
    ctx.add(md);
    CRUDUpdateResponse upd = controller.update(ctx,
        query("{'field':'field2','op':'=','rvalue':'f2'}"),
        update(" {'$set' : {'field3':3 }}"),
        projection("{'field':'*','recursive':1}"));
    Assert.assertEquals(0, upd.getNumUpdated());
    Assert.assertEquals(1, upd.getNumFailed());

    Assert.assertEquals(1, getDataErrors(ctx).size());
    Assert.assertEquals(1, getDataErrors(ctx).get(0).getErrors().size());
    Error error = getDataErrors(ctx).get(0).getErrors().get(0);
    Assert.assertEquals("crud:Required", error.getErrorCode());
    Assert.assertEquals("field1", error.getMsg());
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
    addDocuments(ctx, docs);
    CRUDInsertionResponse response = controller.insert(ctx, projection("{'field':'_id'}"));
    try (DBCursor c = coll.find(null)) {
      Assert.assertEquals(numDocs, c.count());
    }
    Assert.assertEquals(ctx.getInputDocumentsWithoutErrors().size(), response.getNumInserted());

    // Add element to array
    ctx = new TestCRUDOperationContext(CRUDOperation.UPDATE);
    ctx.add(md);
    CRUDUpdateResponse upd = controller.update(ctx,
        query("{'field':'field1','op':'=','rvalue':'doc1'}"),
        update("[ {'$append' : {'field7':{}} }, { '$set': { 'field7.-1.elemf1':'test'} } ]"),
        projection("{'field':'*','recursive':1}"));
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, upd.getNumUpdated());
    Assert.assertEquals(0, upd.getNumFailed());
    Assert.assertEquals(1, documents.size());
    Assert.assertEquals(3, documents.get(0).getOutputDocument().get(new Path("field7")).size());

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
      doc.modify(new Path("field4"), nodeFactory.numberNode(new BigDecimal(100)), false);
      docs.add(doc);
    }
    addDocuments(ctx, docs);
    controller.insert(ctx, projection("{'field':'_id'}"));

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field4','op':'=','rvalue':'100'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(numDocs, documents.size());
    Assert.assertNotNull(documents.get(0).getResultMetadata());
    Assert.assertNotNull(documents.get(0).getResultMetadata().getDocumentVersion());

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
      doc.modify(new Path("field4"), nodeFactory.numberNode(new BigDecimal(100)), false);
      docs.add(doc);
    }
    addDocuments(ctx, docs);
    controller.insert(ctx, projection("{'field':'_id'}"));

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field4','op':'$in','values':[100]}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(numDocs, documents.size());

  }

  @Test
  public void explainTest() throws Exception {
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
      doc.modify(new Path("field4"), nodeFactory.numberNode(new BigDecimal(100)), false);
      docs.add(doc);
    }
    addDocuments(ctx, docs);
    controller.insert(ctx, projection("{'field':'_id'}"));

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(JsonNodeFactory.instance.objectNode());
    controller.explain(ctx, query("{'field':'field4','op':'$in','values':[100]}"),
        projection("{'field':'*','recursive':1}"), null, null, null,
        doc);
    Assert.assertNotNull(doc.get(new Path("mongo")));
    System.out.println(doc);
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
      doc.modify(new Path("field4"), nodeFactory.numberNode(new BigDecimal(100)), false);
      docs.add(doc);
    }
    addDocuments(ctx, docs);
    controller.insert(ctx, projection("{'field':'_id'}"));

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field1','op':'$in','values':[0]}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());

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
      doc.modify(new Path("field4"), nodeFactory.numberNode(new BigDecimal(100)), false);
      docs.add(doc);
    }
    addDocuments(ctx, docs);
    controller.insert(ctx, projection("{'field':'_id'}"));

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field1','op':'$in','values':[0]}"),
        null, null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());

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
    addDocuments(ctx, docs);
    controller.insert(ctx, projection("{'field':'_id'}"));

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'_id','op':'$in','values':[1]}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());

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
    addDocuments(ctx, docs);
    controller.insert(ctx, projection("{'field':'_id'}"));

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field3','op':'>=','rvalue':0}"),
        projection("{'field':'*','recursive':1}"),
        sort("{'field3':'$desc'}"), null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(numDocs, documents.size());
    int lastValue = -1;
    for (DocCtx doc : documents) {
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
          sort("{'field3':'$asc'}"), (long) k, (long) (k + 5));
      documents = streamToList(ctx);

      int i = 0;
      for (DocCtx doc : documents) {
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
    addDocuments(ctx, docs);
    controller.insert(ctx, projection("{'field':'_id'}"));

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    CRUDFindResponse response = controller.find(ctx,
        query("{'field':'field3','op':'>=','rvalue':0}"),
        projection("{'field':'*','recursive':1}"),
        sort("{'field3':'$desc'}"), null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(numDocs, documents.size());
    Assert.assertEquals(numDocs, response.getSize());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    response = controller.find(ctx, query("{'field':'field3','op':'>=','rvalue':0}"),
        projection("{'field':'*','recursive':1}"),
        sort("{'field3':'$desc'}"), 0L, 1L);
    documents = streamToList(ctx);
    Assert.assertEquals(2, documents.size());
    Assert.assertEquals(numDocs, response.getSize());
  }

  @Test
  public void fieldArrayComparisonTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf3','op':'=','rfield':'field6.nf5'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf3','op':'!=','rfield':'field6.nf5'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf3','op':'<','rfield':'field6.nf5'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field4','op':'<','rfield':'field6.nf5'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());

  }

  @Test
  public void searchByArrayIndexTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field7.0.elemf1','op':'$eq','rvalue':'value0_1'}}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
  }

  @Test
  public void useThisInSameArrayTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx,
        query("{'array':'field6.nf6','elemMatch':{'field':'$this','op':'$in','rfield':'$parent'}}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
  }

  @Test
  public void useThisInArrayTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata7.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata7.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query(
            "{'array':'field6.nf6','elemMatch':{'field':'$this','op':'$in','rfield':'$parent.$parent.nf8'}}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query(
            "{'array':'field6.nf6','elemMatch':{'field':'$this','op':'$in','rfield':'$parent.$parent.nf9'}}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
  }

  @Test
  public void useThisInArrayWithRegexTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata7.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata7.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx,
        query("{'array':'field6.nf6','elemMatch':{'field':'$this','regex':'t.o'}}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
  }

  @Test
  public void useThisNinArrayTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata7.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata7.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query(
            "{'array':'field6.nf6','elemMatch':{'field':'$this','op':'$nin','rfield':'$parent.$parent.nf8'}}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query(
            "{'array':'field6.nf6','elemMatch':{'field':'$this','op':'$nin','rfield':'$parent.$parent.nf9'}}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
  }

  @Test
  public void arrayArrayComparisonTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata1.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'=','rfield':'field6.nf5'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'!=','rfield':'field6.nf5'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'<=','rfield':'field6.nf5'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'<','rfield':'field6.nf5'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'>=','rfield':'field6.nf5'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'>','rfield':'field6.nf5'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
  }

  @Test
  public void arrayArrayComparisonTest_gt() throws Exception {
    EntityMetadata md = getMd("./testMetadata5.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata5.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'>','rfield':'field6.nf8'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'>=','rfield':'field6.nf8'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'<','rfield':'field6.nf8'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'<=','rfield':'field6.nf8'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'=','rfield':'field6.nf8'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'!=','rfield':'field6.nf8'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
  }

  @Test
  public void arrayArrayComparisonTest_ne() throws Exception {
    EntityMetadata md = getMd("./testMetadata5.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata5.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'>','rfield':'field6.nf9'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'>=','rfield':'field6.nf9'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'<','rfield':'field6.nf9'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'<=','rfield':'field6.nf9'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'=','rfield':'field6.nf9'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'!=','rfield':'field6.nf9'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
  }

  @Test
  public void arrayArrayComparisonTest_lt() throws Exception {
    EntityMetadata md = getMd("./testMetadata5.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata5.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'>','rfield':'field6.nf10'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'>=','rfield':'field6.nf10'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'<','rfield':'field6.nf10'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'<=','rfield':'field6.nf10'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'=','rfield':'field6.nf10'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf5','op':'!=','rfield':'field6.nf10'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field6.nf10','op':'>=','rfield':'field6.nf5'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
  }

  @Test
  public void fieldfieldComparisonTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata5.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata5.json"));
    // This makes field1=field2
    doc.modify(new Path("field2"), JsonNodeFactory.instance.textNode("f1"), false);
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field1','op':'=','rfield':'field2'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
  }

  @Test
  public void fieldfieldComparisonTest_missingField() throws Exception {
    EntityMetadata md = getMd("./testMetadata5.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata5.json"));
    // This will remove field2
    doc.modify(new Path("field2"), null, false);
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field1','op':'=','rfield':'field2'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(0, documents.size());
    // should not throw exception
  }

  @Test
  public void nullqTest() throws Exception {
    EntityMetadata md = getMd("./testMetadata5.json");
    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdata5.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, null,
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
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
    addDocuments(ctx, docs);
    controller.insert(ctx, projection("{'field':'*','recursive':true}"));

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field3','op':'>=','rvalue':0}"),
        projection("{'field':'field3'}"), null, null, null);
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
    addDocuments(ctx, docs);
    controller.insert(ctx, projection("{'field':'_id'}"));
    try (DBCursor c = coll.find(null)) {
      Assert.assertEquals(numDocs, c.count());
    }

    // Single doc delete
    ctx = new TestCRUDOperationContext(CRUDOperation.DELETE);
    ctx.add(md);
    CRUDDeleteResponse del = controller.delete(ctx,
        query("{'field':'field3','op':'$eq','rvalue':10}"));
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
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx,
        query("{'array':'field7','elemMatch':{'$not':{'field':'elemf3','op':'$eq','rvalue':0}}}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
  }

  @Test
  public void entityIndexCreationTest() {
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
    List<IndexSortKey> indexFields = new ArrayList<>();
    //TODO actually parse $asc/$desc here
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    index.setFields(indexFields);
    List<Index> indexes = new ArrayList<>();
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

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
  public void entityIndexRemovalTest() {
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
    List<IndexSortKey> indexFields = new ArrayList<>();
    //TODO actually parse $asc/$desc here
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    index.setFields(indexFields);
    List<Index> indexes = new ArrayList<>();
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

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

    e.getEntityInfo().getIndexes().setIndexes(new ArrayList<>());
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);
    entityCollection = db.getCollection("testCollectionIndex1");

    foundIndex = false;

    for (DBObject mongoIndex : entityCollection.getIndexInfo()) {
      if ("testIndex".equals(mongoIndex.get("name"))) {
        if (mongoIndex.get("key").toString().contains("field1")) {
          foundIndex = true;
        }
      }
    }
    assertFalse(foundIndex);
  }

  @Test
  public void entityIndexUpdateTest_notSparseUnique() {
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
    List<IndexSortKey> indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    index.setFields(indexFields);
    List<Index> indexes = new ArrayList<>();
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    // 3. verify index in database is unique but not sparse
    entityCollection = db.getCollection("testCollectionIndex2");

    // should also have _id index
    Assert.assertEquals("Unexpected number of indexes", 2, entityCollection.getIndexInfo().size());
    DBObject mongoIndex = entityCollection.getIndexInfo().get(1);
    Assert.assertTrue("Keys on index unexpected",
        mongoIndex.get("key").toString().contains("field1"));
    Assert.assertEquals("Index is not unique", Boolean.TRUE, mongoIndex.get("unique"));
    Boolean sparse = (Boolean) mongoIndex.get("sparse");
    sparse = sparse == null ? Boolean.FALSE : sparse;
    Assert.assertEquals("Index is sparse", Boolean.FALSE, sparse);
  }

  @Test
  public void saneIndex() {

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
    List<IndexSortKey> indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    index.setFields(indexFields);
    List<Index> indexes = new ArrayList<>();
    indexes.add(index);
    index = new Index();
    index.setName("testIndex2");
    index.setUnique(false);
    indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    index.setFields(indexFields);
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);
    try {
      controller.beforeUpdateEntityInfo(null, e.getEntityInfo(), false);
      Assert.fail();
    } catch (Exception x) {
    }
  }

  @Test
  public void entityIndexUpdateTest_default() {
    db.getCollection("testCollectionIndex2").drop();

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
    List<IndexSortKey> indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    index.setFields(indexFields);
    List<Index> indexes = new ArrayList<>();
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    DBCollection entityCollection = db.getCollection("testCollectionIndex2");

    index = new Index();
    index.setName("testIndex");
    index.setUnique(false);
    indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    index.setFields(indexFields);
    indexes = new ArrayList<>();
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);

    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

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
  public void entityIndexUpdateTest_addFieldToCompositeIndex_190() {

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
    List<IndexSortKey> indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    indexFields.add(new IndexSortKey(new Path("field2"), true));
    index.setFields(indexFields);
    List<Index> indexes = new ArrayList<>();
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    indexFields.add(new IndexSortKey(new Path("field2"), true));
    indexFields.add(new IndexSortKey(new Path("field3"), true));
    index.setFields(indexFields);
    e.getEntityInfo().getIndexes().setIndexes(indexes);

    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    DBCollection entityCollection = db.getCollection("testCollectionIndex2");

    boolean foundIndex = false;

    for (DBObject mongoIndex : entityCollection.getIndexInfo()) {
      if ("modifiedIndex".equals(mongoIndex.get("name"))) {
        if (mongoIndex.get("key").toString().contains("field1")
            && mongoIndex.get("key").toString().contains("field2")
            && mongoIndex.get("key").toString().contains("field3")) {
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
  public void ensureIdFieldTest() {
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
    controller.beforeCreateNewSchema(null, e);

    SimpleField id = (SimpleField) e.getFields().getField("_id");
    Assert.assertNotNull(id);
    Assert.assertEquals(StringType.TYPE, id.getType());
    Assert.assertEquals(0, id.getConstraints().size());
  }

  @Test
  public void ensureIdIndexTest() {
    EntityMetadata e = new EntityMetadata("testEntity");
    e.setVersion(new Version("1.0.0", null, "some text blah blah"));
    e.setStatus(MetadataStatus.ACTIVE);
    e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex2"));
    e.getFields().put(new SimpleField("field1", StringType.TYPE));
    ObjectField o = new ObjectField("field2");
    o.getFields().put(new SimpleField("x", IntegerType.TYPE));
    e.getFields().put(o);
    e.getEntityInfo().setDefaultVersion("1.0.0");

    Assert.assertEquals(0, e.getEntityInfo().getIndexes().getIndexes().size());
    controller.beforeUpdateEntityInfo(null, e.getEntityInfo(), false);

    Assert.assertEquals(1, e.getEntityInfo().getIndexes().getIndexes().size());
    Assert.assertEquals("_id",
        e.getEntityInfo().getIndexes().getIndexes().get(0).getFields().get(0).getField()
            .toString());
  }

  @Test
  @Ignore
  public void indexFieldValidationTest() {
    EntityMetadata e = new EntityMetadata("testEntity");
    e.setVersion(new Version("1.0.0", null, "some text blah blah"));
    e.setStatus(MetadataStatus.ACTIVE);
    e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex2"));
    e.getFields().put(new SimpleField("field1", StringType.TYPE));
    ObjectField o = new ObjectField("field2");
    o.getFields().put(new SimpleField("x", IntegerType.TYPE));
    e.getFields().put(o);
    e.getEntityInfo().setDefaultVersion("1.0.0");

    List<Index> indexes = new ArrayList<>();
    Index ix = new Index();
    List<IndexSortKey> fields = new ArrayList<>();
    fields.add(new IndexSortKey(new Path("x.*.y"), false));
    ix.setFields(fields);
    indexes.add(ix);
    e.getEntityInfo().getIndexes().setIndexes(indexes);

    controller.beforeUpdateEntityInfo(null, e.getEntityInfo(), false);
    Assert.assertEquals("x.y",
        e.getEntityInfo().getIndexes().getIndexes().get(0).getFields().get(0).getField()
            .toString());

    indexes = new ArrayList<>();
    ix = new Index();
    fields = new ArrayList<>();
    fields.add(new IndexSortKey(new Path("x.1.y"), false));
    ix.setFields(fields);
    indexes.add(ix);
    e.getEntityInfo().getIndexes().setIndexes(indexes);

    try {
      controller.beforeUpdateEntityInfo(null, e.getEntityInfo(), false);
      Assert.fail();
    } catch (Error x) {
      // expected
    }

    indexes = new ArrayList<>();
    ix = new Index();
    fields = new ArrayList<>();
    fields.add(new IndexSortKey(new Path("x.y"), false));
    ix.setFields(fields);
    indexes.add(ix);
    e.getEntityInfo().getIndexes().setIndexes(indexes);
    controller.beforeUpdateEntityInfo(null, e.getEntityInfo(), false);
    Assert.assertEquals("x.y",
        e.getEntityInfo().getIndexes().getIndexes().get(0).getFields().get(0).getField()
            .toString());
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
      List<IndexSortKey> fields = new ArrayList<>();
      fields.add(new IndexSortKey(new Path("x"), false));
      fields.add(new IndexSortKey(new Path("y"), false));
      ix.setFields(fields);

      boolean verified = false;
      for (DBObject dbi : collection.getIndexInfo()) {
        if (((BasicDBObject) dbi.get("key")).entrySet().iterator().next().getKey().equals("_id")) {
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
      List<IndexSortKey> fields = new ArrayList<>();
      fields.add(new IndexSortKey(new Path("y"), false));
      fields.add(new IndexSortKey(new Path("x"), false));
      ix.setFields(fields);

      boolean verified = false;
      for (DBObject dbi : collection.getIndexInfo()) {
        if (((BasicDBObject) dbi.get("key")).entrySet().iterator().next().getKey().equals("_id")) {
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
    TestCRUDOperationContext ctx = new TestCRUDOperationContext("migrationJob",
        CRUDOperation.INSERT);
    ctx.add(md);
    JsonDoc doc = new JsonDoc(loadJsonNode("./job1.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    System.out.println("Write doc:" + doc);
    CRUDInsertionResponse insresponse = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext("migrationJob", CRUDOperation.UPDATE);
    ctx.add(md);
    CRUDUpdateResponse upd = controller.update(ctx,
        query("{'field':'_id','op':'=','rvalue':'termsAcknowledgementJob_6264'}"),
        update(
            "[{'$append':{'jobExecutions':{}}},{'$set':{'jobExecutions.-1.ownerName':'hystrix'}},{'$set':{'jobExecutions.-1.hostName':'$(hostname)'}},{'$set':{'jobExecutions.-1.pid':'32601@localhost.localdomain'}},{'$set':{'jobExecutions.-1.actualStartDate':'20150312T19:19:00.700+0000'}},{'$set':{'jobExecutions.-1.actualEndDate':'$null'}},{'$set':{'jobExecutions.-1.completedFlag':false}},{'$set':{'jobExecutions.-1.processedDocumentCount':0}},{'$set':{'jobExecutions.-1.consistentDocumentCount':0}},{'$set':{'jobExecutions.-1.inconsistentDocumentCount':0}},{'$set':{'jobExecutions.-1.overwrittenDocumentCount':0}}]"),
        projection("{'field':'*','recursive':1}"));
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
    Assert.assertEquals(2,
        documents.get(0).getOutputDocument().get(new Path("jobExecutions")).size());
  }

  @Test
  public void idIndexRewriteTest() throws Exception {
    DBCollection collection = db.getCollection("data");

    EntityMetadata e = new EntityMetadata("testEntity");
    e.setVersion(new Version("1.0.0", null, "some text blah blah"));
    e.setStatus(MetadataStatus.ACTIVE);
    e.setDataStore(new MongoDataStore(null, null, "data"));
    SimpleField idField = new SimpleField("_id", StringType.TYPE);
    List<FieldConstraint> clist = new ArrayList<>();
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
    controller.beforeUpdateEntityInfo(null, e.getEntityInfo(), false);
    Assert.assertEquals(1, e.getEntityInfo().getIndexes().getIndexes().size());
    Assert.assertEquals("_id",
        e.getEntityInfo().getIndexes().getIndexes().get(0).getFields().get(0).getField()
            .toString());
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);
    // We have our _id index
    // lets insert a doc in the collection, so we can test indexes
    TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.INSERT);
    ctx.add(e);
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("objectType", JsonNodeFactory.instance.textNode("testEntity"));
    obj.set("field1", JsonNodeFactory.instance.textNode("blah"));
    JsonDoc doc = new JsonDoc(obj);
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    Assert.assertEquals(1, collection.find().count());

    Index ix2 = new Index(new IndexSortKey(new Path("field1"), false));
    e.getEntityInfo().getIndexes().add(ix2);
    // At this point, there must be an _id index in the collection
    Assert.assertEquals(1, collection.getIndexInfo().size());
    // Lets overwrite
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);
    // This should not fail
    Assert.assertEquals(2, collection.getIndexInfo().size());
  }

  @Test
  public void doubleidIndexRewriteTest() throws Exception {
    DBCollection collection = db.getCollection("data");

    EntityMetadata e = new EntityMetadata("testEntity");
    e.setVersion(new Version("1.0.0", null, "some text blah blah"));
    e.setStatus(MetadataStatus.ACTIVE);
    e.setDataStore(new MongoDataStore(null, null, "data"));
    SimpleField idField = new SimpleField("_id", StringType.TYPE);
    List<FieldConstraint> clist = new ArrayList<>();
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

    Index ix1 = new Index(new IndexSortKey(new Path("field1"), false),
        new IndexSortKey(new Path("field2"), false));
    ix1.setUnique(true);
    ix1.setName("main");

    Index ix2 = new Index(new IndexSortKey(new Path("_id"), false));
    ix2.setUnique(true);
    e.getEntityInfo().getIndexes().add(ix2);
    Index ix3 = new Index(new IndexSortKey(new Path("_id"), false));
    ix3.setUnique(false);
    ix3.setName("nonunique");
    e.getEntityInfo().getIndexes().add(ix3);

    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);
    // lets insert a doc in the collection, so we can test indexes
    TestCRUDOperationContext ctx = new TestCRUDOperationContext("testEntity", CRUDOperation.INSERT);
    ctx.add(e);
    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("objectType", JsonNodeFactory.instance.textNode("testEntity"));
    obj.set("field1", JsonNodeFactory.instance.textNode("blah"));
    JsonDoc doc = new JsonDoc(obj);
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    Assert.assertEquals(1, collection.find().count());

    // At this point, there must be only one _id index in the collection
    Assert.assertEquals(1, collection.getIndexInfo().size());

    // Remove the nonunique index
    List<Index> ilist = new ArrayList<>();
    ilist.add(ix1);
    ilist.add(ix2);
    e.getEntityInfo().getIndexes().setIndexes(ilist);

    // Lets overwrite
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);
    // This should not fail
    Assert.assertEquals(2, collection.getIndexInfo().size());
  }

  @Test
  public void projectedRefComesNullTest() throws Exception {
    EntityMetadata md = getMd("./testMetadataRef.json");
    CompositeMetadata cmd = CompositeMetadata.buildCompositeMetadata(md,
        (injectionField, entityName, version) -> null);

    FieldCursor cursor = cmd.getFieldCursor();
    while (cursor.next()) {
      System.out.println(cursor.getCurrentPath() + ":" + cursor.getCurrentNode());
    }

    TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.INSERT);
    ctx.add(cmd);
    JsonDoc doc = new JsonDoc(loadJsonNode("./testdataref.json"));
    Projection projection = projection("{'field':'_id'}");
    addDocument(ctx, doc);
    CRUDInsertionResponse response = controller.insert(ctx, projection);

    ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
    ctx.add(md);
    controller.find(ctx, query("{'field':'field1','op':'=','rvalue':'f1'}"),
        projection("{'field':'*','recursive':1}"), null, null, null);
    List<DocCtx> documents = streamToList(ctx);
    Assert.assertEquals(1, documents.size());
    Assert.assertNull(documents.get(0).get(new Path("field7.0.elemf1")));
  }

  @Test
  public void getMaxQueryTimeMS_noConfig_noOptions() {
    long expected = MongoConfiguration.DEFAULT_MAX_QUERY_TIME_MS;

    long maxQueryTimeMS = controller.getMaxQueryTimeMS(null, null);

    Assert.assertEquals(expected, maxQueryTimeMS);
  }

  @Test
  public void getMaxQueryTimeMS_Config_noOptions() {
    long expected = MongoConfiguration.DEFAULT_MAX_QUERY_TIME_MS * 2;

    MongoConfiguration config = new MongoConfiguration();
    config.setMaxQueryTimeMS(expected);
    long maxQueryTimeMS = controller.getMaxQueryTimeMS(config, null);

    Assert.assertEquals(expected, maxQueryTimeMS);
  }

  @Test
  public void getMaxQueryTimeMS_Config_Options() {
    long expected = MongoConfiguration.DEFAULT_MAX_QUERY_TIME_MS * 2;

    MongoConfiguration config = new MongoConfiguration();
    config.setMaxQueryTimeMS(expected * 3);
    ExecutionOptions options = new ExecutionOptions();
    options.getOptions()
        .put(MongoConfiguration.PROPERTY_NAME_MAX_QUERY_TIME_MS, String.valueOf(expected));
    CRUDOperationContext context = new CRUDOperationContext(CRUDOperation.SAVE, COLL_NAME, factory,
        null, options) {
      @Override
      public EntityMetadata getEntityMetadata(String entityName) {
        throw new UnsupportedOperationException(
            "Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };
    long maxQueryTimeMS = controller.getMaxQueryTimeMS(config, context);

    Assert.assertEquals(expected, maxQueryTimeMS);
  }

  @Test
  public void getMaxQueryTimeMS_noConfig_Options() {
    long expected = MongoConfiguration.DEFAULT_MAX_QUERY_TIME_MS * 2;

    ExecutionOptions options = new ExecutionOptions();
    options.getOptions()
        .put(MongoConfiguration.PROPERTY_NAME_MAX_QUERY_TIME_MS, String.valueOf(expected));
    CRUDOperationContext context = new CRUDOperationContext(CRUDOperation.SAVE, COLL_NAME, factory,
        null, options) {
      @Override
      public EntityMetadata getEntityMetadata(String entityName) {
        throw new UnsupportedOperationException(
            "Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };
    long maxQueryTimeMS = controller.getMaxQueryTimeMS(null, context);

    Assert.assertEquals(expected, maxQueryTimeMS);
  }

  @Test
  public void indexFieldsMatch_with_lightblue_path() {
    Index index = new Index(new IndexSortKey(new Path("a.*.b"), false, false));
    DBObject existingIndex = new BasicDBObject("key", new BasicDBObject("a.b", 1));
    Assert.assertTrue(controller.indexFieldsMatch(index, existingIndex));
  }

  @Test
  public void healthyIfControllerIsHealthy() {
    CRUDHealth healthCheck = controller.checkHealth();
    Assert.assertTrue(healthCheck.isHealthy());
  }


  @Test
  @SuppressWarnings("deprecation")
  public void unhealthyIfControllerIsUnhealthy() {

    /*
     * A deliberate attempt to initialize MongoCRUDController with wrong
     * port 27776 (in memory mongo is running on 27777). The ping to the
     * mongo should fail resulting in health not OK
     */
    try (MongoClient mongoClient = new MongoClient("localhost", 27776)) {
      final DB dbx = mongoClient.getDB("mongo");

      MongoCRUDController unhealthyController = new MongoCRUDController(null, new DBResolver() {
        @Override
        public DB get(MongoDataStore store) {
          return dbx;
        }

        @Override
        public MongoConfiguration getConfiguration(MongoDataStore store) {
          MongoConfiguration configuration = new MongoConfiguration();
          try {
            configuration.addServerAddress("localhost", 27776); // adding wrong server port
            configuration.setDatabase("mongo");
          } catch (UnknownHostException e) {
            return null;
          }
          return configuration;
        }

        @Override
        public Collection<MongoConfiguration> getConfigurations() {

          List<MongoConfiguration> configs = new ArrayList<>();
          configs.add(getConfiguration(null));
          return configs;
        }
      });

      CRUDHealth healthCheck = unhealthyController.checkHealth();
      Assert.assertFalse(healthCheck.isHealthy());
    }

  }

  @Test
  @SuppressWarnings("deprecation")
  public void unhealthyIfDatabaseIsUnhealthy() {
    MongoClient mongoClient = new MongoClient("localhost", 27777);
    final DB validDB = mongoClient.getDB("valid-mongo");
    MongoClient mongoClient2 = new MongoClient("localhost", 27776);
    final DB invalidDB = mongoClient2.getDB("invalid-mongo");

    MongoCRUDController unhealthyController = new MongoCRUDController(null, new DBResolver() {
      @Override
      public DB get(MongoDataStore store) {
        if (store.getDatabaseName().equalsIgnoreCase("valid-mongo")) {
          return validDB;
        } else {
          return invalidDB;
        }
      }

      @Override
      public MongoConfiguration getConfiguration(MongoDataStore store) {
        return null;
      }

      @Override
      public Collection<MongoConfiguration> getConfigurations() {

        List<MongoConfiguration> configs = new ArrayList<>();
        MongoConfiguration validMongoConfiguration = new MongoConfiguration();
        MongoConfiguration invalidMongoConfiguration = new MongoConfiguration();
        try {
          validMongoConfiguration.addServerAddress("localhost", 27777);
          validMongoConfiguration.setDatabase("valid-mongo");

          invalidMongoConfiguration.addServerAddress("localhost",
              27776); // adding wrong server port
          invalidMongoConfiguration.setDatabase("invalid-mongo");
        } catch (UnknownHostException e) {
          return null;
        }
        configs.add(validMongoConfiguration);
        configs.add(invalidMongoConfiguration);
        return configs;
      }
    });

    CRUDHealth healthCheck = unhealthyController.checkHealth();

    Map<String, Object> validMongoMap = (Map<String, Object>) healthCheck.details()
        .get("valid-mongo");
    Map<String, Object> invalidMongoMap = (Map<String, Object>) healthCheck.details()
        .get("invalid-mongo");

    Assert.assertTrue((boolean) validMongoMap.get("isHealthy"));
    Assert.assertNull(validMongoMap.get("exception"));

    Assert.assertFalse((boolean) invalidMongoMap.get("isHealthy"));
    Assert.assertNotNull(invalidMongoMap.get("exception"));

    Assert.assertFalse(healthCheck.isHealthy());
  }

  @Test
  public void updateInaccessibleFieldReturnsError() throws Exception {
    // define metadata with a field that cannot be updated
    EntityMetadata entityMetadata = new EntityMetadata("test");
    entityMetadata.setVersion(new Version("1.0.0", null, null));
    entityMetadata.setStatus(MetadataStatus.ACTIVE);
    entityMetadata.setDataStore(new MongoDataStore(null, null, "test"));
    SimpleField idField = new SimpleField("_id", StringType.TYPE);
    List<FieldConstraint> clist = new ArrayList<>();
    clist.add(new IdentityConstraint());
    idField.setConstraints(clist);
    entityMetadata.getFields().put(idField);

    Field inaccessibleField = new SimpleField("inaccessibleField", StringType.TYPE);
    inaccessibleField.getAccess().getUpdate().setRoles(Collections.singletonList("noone"));
    entityMetadata.getFields().put(inaccessibleField);
    entityMetadata.getFields().put(new SimpleField("objectType", StringType.TYPE));

    entityMetadata.getEntityInfo().setDefaultVersion("1.0.0");
    entityMetadata.getEntitySchema().getAccess().getUpdate().setRoles("anyone");

    // create test document directly in mongo
    DBObject testDoc = new BasicDBObject("inaccessibleField", "foo").append("objectType", "test");
    db.getCollection("test").insert(testDoc);

    // try to update inaccessibleField
    TestCRUDOperationContext ctx = new TestCRUDOperationContext("test", CRUDOperation.UPDATE);
    ctx.add(entityMetadata);

    CRUDUpdateResponse response = controller.update(ctx,
        QueryExpression.fromJson(JsonUtils.json(
            "{'field': 'objectType', 'op': '=', 'rvalue': 'test'}".replaceAll("'", "\""))),
        UpdateExpression.fromJson(
            JsonUtils.json("{'$set': { 'inaccessibleField': 'changed'}}".replaceAll("'", "\""))),
        projection("{'field':'_id'}"));

    Assert.assertEquals(1, response.getNumMatched());
    Assert.assertEquals(0, response.getNumUpdated());
    Assert.assertEquals(1, response.getNumFailed());

    Assert.assertTrue(
        "Expecting a document in the results, so the data error can be associated with it",
        ctx.getDocumentStream().hasNext());
    DocCtx docCtx = ctx.getDocumentStream().next();
    ctx.getDocumentStream().close();
    Assert.assertEquals("crud:update:NoFieldAccess", docCtx.getErrors().get(0).getErrorCode());
    Assert.assertEquals("[inaccessibleField]", docCtx.getErrors().get(0).getMsg());
  }

  @Test
  public void entityIndexUpdateTest_default_unmanagedByControllerOptions() throws Exception {
    db.getCollection("testCollectionIndex2").drop();

    ControllerConfiguration cfg = new ControllerConfiguration();
    JsonNode options = json("{'indexManagement': {'managedEntities': []}}");
    cfg.setOptions((ObjectNode) options);
    MongoCRUDController controller = new MongoCRUDController(cfg, dbResolver);

    db.getCollection("testCollectionIndex2").drop();

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
    List<IndexSortKey> indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    index.setFields(indexFields);
    List<Index> indexes = new ArrayList<>();
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    DBCollection entityCollection = db.getCollection("testCollectionIndex2");

    index = new Index();
    index.setName("testIndex");
    index.setUnique(false);
    indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    index.setFields(indexFields);
    indexes = new ArrayList<>();
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);

    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    assertEquals(0, entityCollection.getIndexInfo().size());
  }

  @Test
  public void entityIndexUpdateTest_default_unmanagedByEntityInfo() {
    db.getCollection("testCollectionIndex2").drop();

    EntityMetadata e = new EntityMetadata("testEntity");
    e.setVersion(new Version("1.0.0", null, "some text blah blah"));
    e.setStatus(MetadataStatus.ACTIVE);
    e.setDataStore(new MongoDataStore(null, null, "testCollectionIndex2"));
    e.getFields().put(new SimpleField("field1", StringType.TYPE));
    ObjectField o = new ObjectField("field2");
    o.getFields().put(new SimpleField("x", IntegerType.TYPE));
    e.getFields().put(o);
    e.getEntityInfo().setDefaultVersion("1.0.0");
    e.getEntityInfo().getProperties().put("manageIndexes", false);
    Index index = new Index();
    index.setName("testIndex");
    index.setUnique(true);
    List<IndexSortKey> indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    index.setFields(indexFields);
    List<Index> indexes = new ArrayList<>();
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);
    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    DBCollection entityCollection = db.getCollection("testCollectionIndex2");

    index = new Index();
    index.setName("testIndex");
    index.setUnique(false);
    indexFields = new ArrayList<>();
    indexFields.add(new IndexSortKey(new Path("field1"), true));
    index.setFields(indexFields);
    indexes = new ArrayList<>();
    indexes.add(index);
    e.getEntityInfo().getIndexes().setIndexes(indexes);

    controller.afterUpdateEntityInfo(null, e.getEntityInfo(), false);

    assertEquals(0, entityCollection.getIndexInfo().size());
  }
}
