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

import com.redhat.lightblue.crud.*;
import com.redhat.lightblue.metadata.*;
import com.redhat.lightblue.common.mongo.MongoDataStore;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.types.IntegerType;
import com.redhat.lightblue.metadata.types.StringType;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.Sort;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonDoc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.redhat.lightblue.mongo.test.EmbeddedMongo;

public class CacheTest {

    private static final EmbeddedMongo mongo = EmbeddedMongo.getInstance();

    private MongoMetadata md;
    private MetadataCache cache=new MetadataCache();

    @Before
    public void setup() {
        Factory factory = new Factory();
        factory.addCRUDController("mongo", new TestCRUDController());
        Extensions<BSONObject> x = new Extensions<>();
        x.addDefaultExtensions();
        x.registerDataStoreParser("mongo", new MongoDataStoreParser<BSONObject>());
        // 50 msecs version lookup
        // 100 msecs cache refresh
        cache.setCacheParams(50l,100l);
        md = new MongoMetadata(mongo.getDB(), x, new DefaultTypes(), factory,cache);
        BasicDBObject index = new BasicDBObject("name", 1);
        index.put("version.value", 1);
        mongo.getDB().getCollection(MongoMetadata.DEFAULT_METADATA_COLLECTION).ensureIndex(index, "name", true);
    }

    @After
    public void teardown() {
        mongo.reset();
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
        DBCollection coll=mongo.getDB().getCollection(MongoMetadata.DEFAULT_METADATA_COLLECTION);
        Assert.assertNull(coll.findOne(new BasicDBObject("_id","collectionVersion")));
       
        md.createNewMetadata(e);

        Assert.assertNotNull(coll.findOne(new BasicDBObject("_id","collectionVersion")));
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
        DBCollection coll=mongo.getDB().getCollection(MongoMetadata.DEFAULT_METADATA_COLLECTION);

        // Lookup should fail
        Assert.assertNull(cache.lookup(coll,"testEntity","1.0.0"));
        md.getEntityMetadata("testEntity","1.0.0");
        // Lookup should not fail
        Assert.assertNotNull(cache.lookup(coll,"testEntity","1.0.0"));
        Thread.sleep(51);
        // Lookup shoult not fail
        Assert.assertNotNull(cache.lookup(coll,"testEntity","1.0.0"));
        // Update the version in db
        coll.update(new BasicDBObject("_id","collectionVersion"),new BasicDBObject("$inc",new BasicDBObject("collectionVersion",1)));
        Thread.sleep(51);
        // Lookup should not fail, but will detect change
        Assert.assertNotNull(cache.lookup(coll,"testEntity","1.0.0"));
        // Lookup should fail now
        Assert.assertNull(cache.lookup(coll,"testEntity","1.0.0"));        
    }
    

}
