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
package com.redhat.lightblue.mongo.metadata;

import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.mongodb.BasicDBObject;
import com.redhat.lightblue.OperationStatus;
import com.redhat.lightblue.Response;
import com.redhat.lightblue.mongo.common.MongoDataStore;
import com.redhat.lightblue.crud.*;
import com.redhat.lightblue.metadata.constraints.EnumConstraint;
import com.redhat.lightblue.metadata.*;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import com.redhat.lightblue.metadata.types.IntegerType;
import com.redhat.lightblue.metadata.types.StringType;
import com.redhat.lightblue.mongo.metadata.MongoDataStoreParser;
import com.redhat.lightblue.mongo.metadata.MongoMetadata;
import com.redhat.lightblue.mongo.metadata.MongoMetadataConstants;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.Sort;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.test.AbstractJsonNodeTest;
import org.bson.BSONObject;
import org.json.JSONException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import com.mongodb.DB;
import org.skyscreamer.jsonassert.JSONAssert;

import com.redhat.lightblue.mongo.test.MongoServerExternalResource;

import java.io.IOException;
import java.util.Iterator;

public class MongoMetadataTest {

    @ClassRule
    public static final MongoServerExternalResource mongo = new MongoServerExternalResource();

    private MongoMetadata md;
    private DB db;

    @Before
    public void setup() throws Exception {
        Factory factory = new Factory();
        factory.addCRUDController("mongo", new TestCRUDController());
        Extensions<BSONObject> x = new Extensions<>();
        x.addDefaultExtensions();
        x.registerDataStoreParser("mongo", new MongoDataStoreParser<BSONObject>());
        db=mongo.getConnection().getDB("mongo");
        md = new MongoMetadata(db, x, new DefaultTypes(), factory, null);
        BasicDBObject index = new BasicDBObject("name", 1);
        index.put("version.value", 1);
        db.getCollection(MongoMetadata.DEFAULT_METADATA_COLLECTION).createIndex(index, "name", true);
    }

    @After
    public void teardown() {
        db.getCollection(MongoMetadata.DEFAULT_METADATA_COLLECTION).remove(new BasicDBObject());
    }

    public static class TestCRUDController implements CRUDController {

        @Override
        public CRUDInsertionResponse insert(CRUDOperationContext ctx,
                                            Projection projection) {
            return null;
        }

        @Override
        public CRUDSaveResponse save(CRUDOperationContext ctx,
                                     boolean upsert,
                                     Projection projection) {
            return null;
        }

        @Override
        public CRUDUpdateResponse update(CRUDOperationContext ctx,
                                         QueryExpression query,
                                         UpdateExpression update,
                                         Projection projection) {
            return null;
        }

        @Override
        public CRUDDeleteResponse delete(CRUDOperationContext ctx,
                                         QueryExpression query) {
            return null;
        }

        @Override
        public CRUDFindResponse find(CRUDOperationContext ctx,
                                     QueryExpression query,
                                     Projection projection,
                                     Sort sort,
                                     Long from,
                                     Long to) {
            return null;
        }

        @Override
        public MetadataListener getMetadataListener() {
            return null;
        }

        @Override
        public void updatePredefinedFields(CRUDOperationContext ctx, JsonDoc doc) {
        }
    }

    @Test
    public void defaultVersionTest() throws Exception {

        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        e.getEntityInfo().setDefaultVersion("1.0.0");
        md.createNewMetadata(e);

        EntityMetadata g = md.getEntityMetadata("testEntity", null);
        Assert.assertEquals("1.0.0", g.getVersion().getValue());
    }

    @Test
    public void createMdTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.setDescription("description");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);
        EntityMetadata g = md.getEntityMetadata("testEntity", "1.0.0");
        Assert.assertNotNull("Can't retrieve entity", g);
        Assert.assertEquals(e.getName(), g.getName());
        Assert.assertEquals(e.getVersion().getValue(), g.getVersion().getValue());
        Assert.assertEquals(e.getVersion().getChangelog(), g.getVersion().getChangelog());
        Assert.assertEquals(e.getStatus(), g.getStatus());
        Assert.assertEquals((e.resolve(new Path("field1"))).getType(),
                (g.resolve(new Path("field1"))).getType());
        Assert.assertEquals((e.resolve(new Path("field2.x"))).getType(),
                (g.resolve(new Path("field2.x"))).getType());
        Assert.assertEquals("description not set", o.getDescription(), e.getFields().getField(o.getName()).getDescription());
        Version[] v = md.getEntityVersions("testEntity");
        Assert.assertEquals(1, v.length);
        Assert.assertEquals("1.0.0", v[0].getValue());

        String[] names = md.getEntityNames();
        Assert.assertEquals(1, names.length);
        Assert.assertEquals("testEntity", names[0]);
    }

    @Test
    public void createMdWithAndRefTest() throws Exception {
        Extensions<JsonNode> extensions = new Extensions<>();
        extensions.addDefaultExtensions();
        extensions.registerDataStoreParser("mongo", new MongoDataStoreParser<JsonNode>());
        JSONMetadataParser parser = new JSONMetadataParser(extensions, new DefaultTypes(), new JsonNodeFactory(true));

        // get JsonNode representing metadata
        JsonNode jsonMetadata = AbstractJsonNodeTest.loadJsonNode(getClass().getSimpleName() + "-qps-andquery.json");

        // parser into EntityMetadata
        EntityMetadata e = parser.parseEntityMetadata(jsonMetadata);

        // persist
        md.createNewMetadata(e);
        EntityMetadata g = md.getEntityMetadata("test", "1.0.0");
        // No exception=OK
    }

    /**
     * Issue #13: if you create it twice, the error thrown for the second one
     * cleans up the first
     */
    @Test
    public void createMd2Test() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);
        Assert.assertNotNull(md.getEntityMetadata("testEntity", "1.0.0"));
        try {
            md.createNewMetadata(e);
            Assert.fail();
        } catch (Exception x) {
        }
        Assert.assertNotNull(md.getEntityMetadata("testEntity", "1.0.0"));
    }

    @Test
    public void testCollectionName() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "test-Collection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        try {
            md.createNewMetadata(e);
            Assert.fail();
        } catch (Error x) {
        }

        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        md.createNewMetadata(e);
    }

    @Test
    public void updateStatusTest() throws Exception {
        EntityMetadata e2 = new EntityMetadata("testEntity");
        e2.setVersion(new Version("1.1.0", null, "some text blah blah"));
        e2.setStatus(MetadataStatus.ACTIVE);
        e2.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e2.getFields().put(new SimpleField("field1", StringType.TYPE));
        md.createNewMetadata(e2);
        EntityMetadata g = md.getEntityMetadata("testEntity", "1.1.0");
        Assert.assertEquals(MetadataStatus.ACTIVE, g.getStatus());

        md.setMetadataStatus("testEntity", "1.1.0", MetadataStatus.DEPRECATED, "disable testEntity");
        EntityMetadata g1 = md.getEntityMetadata("testEntity", "1.1.0");
        Assert.assertEquals(e2.getVersion().getValue(), g1.getVersion().getValue());
        Assert.assertEquals(MetadataStatus.DEPRECATED, g1.getStatus());
    }

    @Test
    public void disabledDefaultUpdateTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        e.getEntityInfo().setDefaultVersion("1.0.0");
        md.createNewMetadata(e);
        EntityMetadata g1 = md.getEntityMetadata("testEntity", "1.0.0");
        Assert.assertEquals(e.getVersion().getValue(), g1.getVersion().getValue());
        Assert.assertEquals(MetadataStatus.ACTIVE, g1.getStatus());
        try {
            md.setMetadataStatus("testEntity", "1.0.0", MetadataStatus.DISABLED, "disabling the default version");
            Assert.fail("expected " + MongoMetadataConstants.ERR_DISABLED_DEFAULT_VERSION);
        } catch (Error ex) {
            Assert.assertEquals(MongoMetadataConstants.ERR_DISABLED_DEFAULT_VERSION, ex.getErrorCode());
        }
    }

    @Test
    public void disabledDefaultCreationTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.DISABLED);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        e.getEntityInfo().setDefaultVersion("1.0.0");
        try {
            md.createNewMetadata(e);
            Assert.fail("expected " + MongoMetadataConstants.ERR_DISABLED_DEFAULT_VERSION);
        } catch (Error ex) {
            Assert.assertEquals(MongoMetadataConstants.ERR_DISABLED_DEFAULT_VERSION, ex.getErrorCode());
        }
    }

    @Test
    public void illegalArgumentTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        md.createNewMetadata(e);
        try {
            md.getEntityMetadata("testEntity", "");
            Assert.fail("expected " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            Assert.assertEquals("version", ex.getMessage());
        }

        try {
            md.getEntityMetadata("testEntity", null);
            Assert.fail("expected " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            Assert.assertEquals("version", ex.getMessage());
        }

    }

    @Test
    public void unknownVersionTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        md.createNewMetadata(e);
        try {
            md.getEntityMetadata("testEntity", "1.1.0");
            Assert.fail("expected " + MongoMetadataConstants.ERR_UNKNOWN_VERSION);
        } catch (Error ex) {
            Assert.assertEquals(MongoMetadataConstants.ERR_UNKNOWN_VERSION, ex.getErrorCode());
        }

    }

    @Test
    public void updateEntityInfo() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);

        EntityInfo ei = new EntityInfo("testEntity");
        ei.setDataStore(new MongoDataStore(null, null, "somethingelse"));
        md.updateEntityInfo(ei);
    }

    @Test
    public void updateEntityInfo_invalidates() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        com.redhat.lightblue.metadata.Enum enumdef = new com.redhat.lightblue.metadata.Enum("en");
        Set<String> envalues = new HashSet<>();
        envalues.add("value");
        enumdef.setValues(envalues);
        e.getEntityInfo().getEnums().addEnum(enumdef);

        SimpleField s = new SimpleField("z", StringType.TYPE);
        ArrayList<FieldConstraint> enumsc = new ArrayList<>();
        EnumConstraint enumc = new EnumConstraint();
        enumc.setName("en");
        enumsc.add(enumc);
        s.setConstraints(enumsc);
        e.getFields().put(s);

        md.createNewMetadata(e);

        e.getEntityInfo().getEnums().setEnums(new ArrayList());
        try {
            md.updateEntityInfo(e.getEntityInfo());
            Assert.fail();
        } catch (Error x) {
        }
    }

    @Test
    public void createEntityInfo_validates() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        SimpleField s = new SimpleField("z", StringType.TYPE);
        ArrayList<FieldConstraint> enumsc = new ArrayList<>();
        EnumConstraint enumc = new EnumConstraint();
        enumc.setName("en");
        enumsc.add(enumc);
        s.setConstraints(enumsc);
        e.getFields().put(s);
        try {
            md.createNewMetadata(e);
            Assert.fail();
        } catch (Error x) {
        }
    }

    @Test
    public void updateEntityInfo_noEntity() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);

        EntityInfo ei = new EntityInfo("NottestEntity");
        ei.setDataStore(new MongoDataStore(null, null, "somethingelse"));
        try {
            md.updateEntityInfo(ei);
            Assert.fail();
        } catch (Error ex) {
            Assert.assertEquals(MongoMetadataConstants.ERR_MISSING_ENTITY_INFO, ex.getErrorCode());
        }
    }

    @Test
    public void invalidDefaultVersionTest() throws Exception {
        //with non-existant default.
        EntityMetadata eDefault = new EntityMetadata("testDefaultEntity");
        eDefault.setVersion(new Version("1.0.0", null, "some text blah blah"));
        eDefault.setStatus(MetadataStatus.DISABLED);
        eDefault.setDataStore(new MongoDataStore(null, null, "testCollection"));
        eDefault.getFields().put(new SimpleField("field1", StringType.TYPE));
        eDefault.getEntityInfo().setDefaultVersion("blah");
        try {
            md.createNewMetadata(eDefault);
            Assert.fail("expected " + MetadataConstants.ERR_INVALID_DEFAULT_VERSION);
        } catch (Error ex) {
            Assert.assertEquals(MetadataConstants.ERR_INVALID_DEFAULT_VERSION, ex.getErrorCode());
        }
    }

    @Test
    public void multipleVersions() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);
        EntityMetadata g = md.getEntityMetadata("testEntity", "1.0.0");
        Assert.assertNotNull("Can't retrieve entity", g);
        Assert.assertEquals(e.getName(), g.getName());
        Assert.assertEquals(e.getVersion().getValue(), g.getVersion().getValue());
        Version[] v = md.getEntityVersions("testEntity");
        Assert.assertEquals(1, v.length);
        Assert.assertEquals("1.0.0", v[0].getValue());
        e.setVersion(new Version("2.0.0", null, "blahblahyadayada"));
        md.createNewSchema(e);
        v = md.getEntityVersions("testEntity");
        Assert.assertEquals(2, v.length);
        try {
            md.createNewMetadata(e);
            Assert.fail();
        } catch (Exception x) {
        }
    }

    @Test
    public void validateAllVersions_InvalidDisabled() throws Exception {
        String metadataCollectionName = "metadata";
        String disabledVersion = "1.0.0";
        // create two versions of metadata, disable the older one, then update it in mongo to make it invalid
        // confirm validateAllVersions doesn't fail
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version(disabledVersion, null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);

        e.setVersion(new Version("2.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.getFields().put(new SimpleField("field3", IntegerType.TYPE));
        md.createNewSchema(e);

        // confirm they're all good so far, expect no exception
        md.validateAllVersions(e.getEntityInfo());

        // invalidate the disabled version
        db.getCollection(metadataCollectionName).update(new BasicDBObject("_id", e.getName() + "|" + disabledVersion),
                new BasicDBObject("$set", new BasicDBObject("fields.field1.type", "INVALID")));

        // shouldn't be valid now
        try {
            md.validateAllVersions(e.getEntityInfo());
            Assert.fail("Expected validation to fail, active version is invalid!");
        } catch (Exception ex) {
            // expected to be invalid
        }

        // fix the broken metadata
        db.getCollection(metadataCollectionName).update(new BasicDBObject("_id", e.getName() + "|" + disabledVersion),
                new BasicDBObject("$set", new BasicDBObject("fields.field1.type", "string")));

        // disable 1.0.0
        md.setMetadataStatus(e.getName(), disabledVersion, MetadataStatus.DISABLED, "test");

        // is still valid
        md.validateAllVersions(e.getEntityInfo());

        // break the disabled metadata
        db.getCollection(metadataCollectionName).update(new BasicDBObject("_id", e.getName() + "|" + disabledVersion),
                new BasicDBObject("$set", new BasicDBObject("fields.field1.type", "INVALID")));

        // it should still be valid
        md.validateAllVersions(e.getEntityInfo());
    }

    @Test
    public void snapshotUpdates() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0-SNAPSHOT", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);
        EntityMetadata g = md.getEntityMetadata("testEntity", "1.0.0-SNAPSHOT");
        Assert.assertNotNull("Can't retrieve entity", g);
        Assert.assertEquals(e.getName(), g.getName());
        Assert.assertEquals(e.getVersion().getValue(), g.getVersion().getValue());
        Version[] v = md.getEntityVersions("testEntity");
        Assert.assertEquals(1, v.length);
        Assert.assertEquals("1.0.0-SNAPSHOT", v[0].getValue());
        e.setVersion(new Version("1.0.0-SNAPSHOT", null, "blahblahyadayada"));
        md.createNewSchema(e);
        v = md.getEntityVersions("testEntity");
        Assert.assertEquals(1, v.length);
        try {
            md.createNewMetadata(e);
            Assert.fail();
        } catch (Exception x) {
        }
    }

    @Test
    public void nonsnapshotUpdateFails() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);
        EntityMetadata g = md.getEntityMetadata("testEntity", "1.0.0");
        Assert.assertNotNull("Can't retrieve entity", g);
        Assert.assertEquals(e.getName(), g.getName());
        Assert.assertEquals(e.getVersion().getValue(), g.getVersion().getValue());
        Version[] v = md.getEntityVersions("testEntity");
        Assert.assertEquals(1, v.length);
        Assert.assertEquals("1.0.0", v[0].getValue());
        e.setVersion(new Version("1.0.0", null, "blahblahyadayada"));
        try {
            md.createNewSchema(e);
            Assert.fail();
        } catch (Exception x) {
        }
        v = md.getEntityVersions("testEntity");
        Assert.assertEquals(1, v.length);
    }

    @Test
    public void getWithStatus() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);
        EntityMetadata g = md.getEntityMetadata("testEntity", "1.0.0");
        e.setVersion(new Version("2.0.0", null, "blahblahyadayada"));
        md.createNewSchema(e);

        e = new EntityMetadata("testEntity2");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);
        g = md.getEntityMetadata("testEntity2", "1.0.0");
        e.setVersion(new Version("2.0.0", null, "sfr"));
        md.createNewSchema(e);

        Assert.assertEquals(2, md.getEntityNames().length);
        Assert.assertEquals(2, md.getEntityNames(MetadataStatus.ACTIVE).length);
        Assert.assertEquals(0, md.getEntityNames(MetadataStatus.DEPRECATED).length);

        md.setMetadataStatus("testEntity", "1.0.0", MetadataStatus.DEPRECATED, "x");
        Assert.assertEquals(2, md.getEntityNames().length);
        Assert.assertEquals(2, md.getEntityNames(MetadataStatus.ACTIVE).length);

        md.setMetadataStatus("testEntity2", "1.0.0", MetadataStatus.DEPRECATED, "x");
        Assert.assertEquals(2, md.getEntityNames().length);
        Assert.assertEquals(2, md.getEntityNames(MetadataStatus.ACTIVE).length);
        Assert.assertEquals(2, md.getEntityNames(MetadataStatus.ACTIVE, MetadataStatus.DEPRECATED).length);

        md.setMetadataStatus("testEntity2", "2.0.0", MetadataStatus.DEPRECATED, "x");
        Assert.assertEquals(2, md.getEntityNames().length);
        Assert.assertEquals(1, md.getEntityNames(MetadataStatus.ACTIVE).length);
        Assert.assertEquals(2, md.getEntityNames(MetadataStatus.ACTIVE, MetadataStatus.DEPRECATED).length);

    }

    @Test
    public void removal() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);
        EntityMetadata g = md.getEntityMetadata("testEntity", "1.0.0");
        e.setVersion(new Version("2.0.0", null, "blahblahyadayada"));
        md.createNewSchema(e);

        try {
            md.removeEntity("testEntity");
            Assert.fail();
        } catch (Exception x) {
        }

        md.setMetadataStatus("testEntity", "1.0.0", MetadataStatus.DEPRECATED, "x");
        try {
            md.removeEntity("testEntity");
            Assert.fail();
        } catch (Exception x) {
        }

        md.setMetadataStatus("testEntity", "2.0.0", MetadataStatus.DISABLED, "x");
        try {
            md.removeEntity("testEntity");
            Assert.fail();
        } catch (Exception x) {
        }
        md.setMetadataStatus("testEntity", "1.0.0", MetadataStatus.DISABLED, "x");
        md.removeEntity("testEntity");
        Assert.assertNull(md.getEntityInfo("testEntity"));
    }

    @Test
    public void getAccessEntityVersion() throws IOException, JSONException {
        // setup parser
        Extensions<JsonNode> extensions = new Extensions<>();
        extensions.addDefaultExtensions();
        extensions.registerDataStoreParser("mongo", new MongoDataStoreParser<JsonNode>());
        JSONMetadataParser parser = new JSONMetadataParser(extensions, new DefaultTypes(), new JsonNodeFactory(true));

        // get JsonNode representing metadata
        JsonNode jsonMetadata = AbstractJsonNodeTest.loadJsonNode(getClass().getSimpleName() + "-access-entity-version.json");

        // parser into EntityMetadata
        EntityMetadata e = parser.parseEntityMetadata(jsonMetadata);

        // persist
        md.createNewMetadata(e);

        // ready to test!
        Response response = md.getAccess(e.getName(), e.getVersion().getValue());

        Assert.assertNotNull(response);

        // verify response content
        Assert.assertEquals(OperationStatus.COMPLETE, response.getStatus());
        Assert.assertTrue(response.getDataErrors().isEmpty());

        // verify data
        Assert.assertNotNull(response.getEntityData());
        String jsonEntityData = response.getEntityData().toString();
        String jsonExpected = "[{\"role\":\"field.find\",\"find\":[\"test.name\"]},{\"role\":\"field.insert\",\"insert\":[\"test.name\"]},{\"role\":\"noone\",\"update\":[\"test.objectType\"]},{\"role\":\"field.update\",\"update\":[\"test.name\"]},{\"role\":\"anyone\",\"find\":[\"test.objectType\"]},{\"role\":\"entity.insert\",\"insert\":[\"test\"]},{\"role\":\"entity.update\",\"update\":[\"test\"]},{\"role\":\"entity.find\",\"find\":[\"test\"]},{\"role\":\"entity.delete\",\"delete\":[\"test\"]}]";
        JSONAssert.assertEquals(jsonExpected, jsonEntityData, false);
    }

    @Test
    public void getAccessEntityMissingDefaultVersion() throws IOException, JSONException {
        // setup parser
        Extensions<JsonNode> extensions = new Extensions<>();
        extensions.addDefaultExtensions();
        extensions.registerDataStoreParser("mongo", new MongoDataStoreParser<JsonNode>());
        JSONMetadataParser parser = new JSONMetadataParser(extensions, new DefaultTypes(), new JsonNodeFactory(true));

        // get JsonNode representing metadata
        JsonNode jsonMetadata = AbstractJsonNodeTest.loadJsonNode(getClass().getSimpleName() + "-access-entity-missing-default-version.json");

        // parser into EntityMetadata
        EntityMetadata e = parser.parseEntityMetadata(jsonMetadata);

        // persist
        md.createNewMetadata(e);

        // ready to test!
        Response response = md.getAccess(e.getName(), null);

        Assert.assertNotNull(response);

        // verify response content
        Assert.assertEquals(OperationStatus.ERROR, response.getStatus());
        Assert.assertFalse(response.getDataErrors().isEmpty());

        // verify data
        Assert.assertNull(response.getEntityData());
    }

    /**
     * TODO enable once mongo metadata allows falling back on default version in
     * getEntityMetadata()
     *
     * @throws IOException
     * @throws JSONException
     */
    @Test
    public void getAccessEntityDefaultVersion() throws IOException, JSONException {
        // setup parser
        Extensions<JsonNode> extensions = new Extensions<>();
        extensions.addDefaultExtensions();
        extensions.registerDataStoreParser("mongo", new MongoDataStoreParser<JsonNode>());
        JSONMetadataParser parser = new JSONMetadataParser(extensions, new DefaultTypes(), new JsonNodeFactory(true));

        // get JsonNode representing metadata
        JsonNode jsonMetadata = AbstractJsonNodeTest.loadJsonNode(getClass().getSimpleName() + "-access-entity-default-version.json");

        // parser into EntityMetadata
        EntityMetadata e = parser.parseEntityMetadata(jsonMetadata);

        // persist
        md.createNewMetadata(e);

        // ready to test!
        Response response = md.getAccess(e.getName(), null);

        Assert.assertNotNull(response);

        // verify response content
        Assert.assertEquals(OperationStatus.COMPLETE, response.getStatus());
        Assert.assertTrue(response.getDataErrors().isEmpty());

        // verify data
        Assert.assertNotNull(response.getEntityData());
        String jsonEntityData = response.getEntityData().toString();
        String jsonExpected = "[{\"role\":\"field.find\",\"find\":[\"test.name\"]},{\"role\":\"field.insert\",\"insert\":[\"test.name\"]},{\"role\":\"field.update\",\"update\":[\"test.name\"]},{\"role\":\"noone\",\"update\":[\"test.objectType\"]},{\"role\":\"anyone\",\"find\":[\"test.objectType\"]},{\"role\":\"entity.insert\",\"insert\":[\"test\"]},{\"role\":\"entity.update\",\"update\":[\"test\"]},{\"role\":\"entity.find\",\"find\":[\"test\"]},{\"role\":\"entity.delete\",\"delete\":[\"test\"]}]";
        JSONAssert.assertEquals(jsonExpected, jsonEntityData, false);
    }

    /**
     * TODO enable once mongo metadata allows falling back on default version in
     * getEntityMetadata()
     *
     * @throws IOException
     * @throws JSONException
     */
    @Test
    public void getAccessSingleEntityDefaultVersion() throws IOException, JSONException {
        // setup parser
        Extensions<JsonNode> extensions = new Extensions<>();
        extensions.addDefaultExtensions();
        extensions.registerDataStoreParser("mongo", new MongoDataStoreParser<JsonNode>());
        JSONMetadataParser parser = new JSONMetadataParser(extensions, new DefaultTypes(), new JsonNodeFactory(true));

        // get JsonNode representing metadata
        JsonNode jsonMetadata = AbstractJsonNodeTest.loadJsonNode(getClass().getSimpleName() + "-access-single-entity-default-version.json");

        // parser into EntityMetadata
        EntityMetadata e = parser.parseEntityMetadata(jsonMetadata);
        // persist
        md.createNewMetadata(e);

        // ready to test!
        Response response = md.getAccess(null, null);

        Assert.assertNotNull(response);

        // verify response content
        Assert.assertEquals(OperationStatus.COMPLETE, response.getStatus());
        Assert.assertTrue(response.getDataErrors().isEmpty());

        // verify data
        Assert.assertNotNull(response.getEntityData());
        String jsonEntityData = response.getEntityData().toString();
        String jsonExpected = "[{\"role\":\"field.find\",\"find\":[\"test.name\"]},{\"role\":\"field.insert\",\"insert\":[\"test.name\"]},{\"role\":\"field.update\",\"update\":[\"test.name\"]},{\"role\":\"noone\",\"update\":[\"test.objectType\"]},{\"role\":\"anyone\",\"find\":[\"test.objectType\"]},{\"role\":\"entity.insert\",\"insert\":[\"test\"]},{\"role\":\"entity.update\",\"update\":[\"test\"]},{\"role\":\"entity.find\",\"find\":[\"test\"]},{\"role\":\"entity.delete\",\"delete\":[\"test\"]}]";
        JSONAssert.assertEquals(jsonExpected, jsonEntityData, false);
    }

    /**
     * TODO enable once mongo metadata allows falling back on default version in
     * getEntityMetadata()
     *
     * @throws IOException
     * @throws JSONException
     */
    @Test
    public void getAccessMultipleEntitiesDefaultVersion() throws IOException, JSONException {
        // setup parser
        Extensions<JsonNode> extensions = new Extensions<>();
        extensions.addDefaultExtensions();
        extensions.registerDataStoreParser("mongo", new MongoDataStoreParser<JsonNode>());
        JSONMetadataParser parser = new JSONMetadataParser(extensions, new DefaultTypes(), new JsonNodeFactory(true));

        // get JsonNode representing metadata
        JsonNode jsonMetadata = AbstractJsonNodeTest.loadJsonNode(getClass().getSimpleName() + "-access-multiple-entities-default-version.json");

        ArrayNode an = (ArrayNode) jsonMetadata;
        Iterator<JsonNode> itr = an.iterator();
        while (itr.hasNext()) {
            // parser into EntityMetadata
            EntityMetadata e = parser.parseEntityMetadata(itr.next());

            // persist
            md.createNewMetadata(e);
        }

        // ready to test!
        Response response = md.getAccess(null, null);

        Assert.assertNotNull(response);

        // verify response content
        Assert.assertEquals(OperationStatus.PARTIAL, response.getStatus());
        Assert.assertFalse(response.getDataErrors().isEmpty());
        Assert.assertEquals(1, response.getDataErrors().size());
        String jsonErrorExpected = "[{\"data\":{\"name\":\"test2\"},\"errors\":[{\"objectType\":\"error\",\"errorCode\":\"ERR_NO_METADATA\",\"msg\":\"Could not get metadata for given input. Error message: version\"}]}]";
        JSONAssert.assertEquals(jsonErrorExpected, response.getDataErrors().toString(), false);

        // verify data
        Assert.assertNotNull(response.getEntityData());
        String jsonEntityData = response.getEntityData().toString();
        String jsonExpected = "[{\"role\":\"field.find\",\"find\":[\"test1.name\",\"test3.name\"]},{\"role\":\"field.insert\",\"insert\":[\"test1.name\",\"test3.name\"]},{\"role\":\"noone\",\"update\":[\"test1.objectType\",\"test3.objectType\"]},{\"role\":\"field.update\",\"update\":[\"test1.name\",\"test3.name\"]},{\"role\":\"anyone\",\"find\":[\"test1.objectType\",\"test3.objectType\"]},{\"role\":\"entity.insert\",\"insert\":[\"test1\",\"test3\"]},{\"role\":\"entity.update\",\"update\":[\"test1\",\"test3\"]},{\"role\":\"entity.find\",\"find\":[\"test1\",\"test3\"]},{\"role\":\"entity.delete\",\"delete\":[\"test1\",\"test3\"]}]";
        JSONAssert.assertEquals(jsonExpected, jsonEntityData, false);
    }

}
