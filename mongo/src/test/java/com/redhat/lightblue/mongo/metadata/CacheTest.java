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

import com.redhat.lightblue.crud.*;
import com.redhat.lightblue.metadata.*;
import com.redhat.lightblue.mongo.common.MongoDataStore;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import com.redhat.lightblue.metadata.types.IntegerType;
import com.redhat.lightblue.metadata.types.StringType;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.Sort;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.util.JsonDoc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DB;
import com.redhat.lightblue.mongo.metadata.MetadataCache;
import com.redhat.lightblue.mongo.metadata.MongoDataStoreParser;
import com.redhat.lightblue.mongo.metadata.MongoMetadata;

import com.redhat.lightblue.mongo.test.MongoServerExternalResource;

public class CacheTest {

    @ClassRule
    public static final MongoServerExternalResource mongo = new MongoServerExternalResource();

    private MongoMetadata md;
    private final MetadataCache cache = new MetadataCache();
    private DB db;

    @Before
    public void setup() throws Exception {
        Factory factory = new Factory();
        factory.addCRUDController("mongo", new TestCRUDController());
        Extensions<Object> x = new Extensions<>();
        x.addDefaultExtensions();
        x.registerDataStoreParser("mongo", new MongoDataStoreParser<Object>());
        // 50 msecs version lookup
        // 100 msecs cache refresh
        cache.setCacheParams(50l, 100l);
        db = mongo.getConnection().getDB("mongo");
        md = new MongoMetadata(db, x, new DefaultTypes(), factory, cache);
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

        @Override
        public CRUDHealth checkHealth() {
            return new CRUDHealth(true, "Return always healthy for test");
        }
    }

    @Test
    public void createTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);

        // At this point, there should not be a collectionVersion doc
        DBCollection coll = db.getCollection(MongoMetadata.DEFAULT_METADATA_COLLECTION);
        Assert.assertNull(coll.findOne(new BasicDBObject("_id", "collectionVersion")));

        md.createNewMetadata(e);

        Assert.assertNotNull(coll.findOne(new BasicDBObject("_id", "collectionVersion")));
    }

    @Test
    public void cacheTest() throws Exception {
        EntityMetadata e = new EntityMetadata("testEntity");
        e.setVersion(new Version("1.0.0", null, "some text blah blah"));
        e.setStatus(MetadataStatus.ACTIVE);
        e.setDataStore(new MongoDataStore(null, null, "testCollection"));
        e.getFields().put(new SimpleField("field1", StringType.TYPE));
        ObjectField o = new ObjectField("field2");
        o.getFields().put(new SimpleField("x", IntegerType.TYPE));
        e.getFields().put(o);
        md.createNewMetadata(e);
        DBCollection coll = db.getCollection(MongoMetadata.DEFAULT_METADATA_COLLECTION);

        // Lookup should fail
        Assert.assertNull(cache.lookup(coll, "testEntity", "1.0.0"));
        md.getEntityMetadata("testEntity", "1.0.0");
        // Lookup should not fail
        Assert.assertNotNull(cache.lookup(coll, "testEntity", "1.0.0"));
        Thread.sleep(51);
        // Lookup shoult not fail
        Assert.assertNotNull(cache.lookup(coll, "testEntity", "1.0.0"));
        // Update the version in db
        coll.update(new BasicDBObject("_id", "collectionVersion"), new BasicDBObject("$inc", new BasicDBObject("collectionVersion", 1)));
        Thread.sleep(51);
        // Lookup will fail,  detect change
        Assert.assertNull(cache.lookup(coll, "testEntity", "1.0.0"));
    }

}
