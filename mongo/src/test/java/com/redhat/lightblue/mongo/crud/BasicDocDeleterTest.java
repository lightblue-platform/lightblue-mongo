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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.redhat.lightblue.crud.CRUDDeleteResponse;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.metadata.EntityMetadata;

/**
 *
 * @author nmalik, mpatercz
 */
public class BasicDocDeleterTest extends AbstractMongoCrudTest {

    private TestCRUDOperationContext ctx;
    private Translator translator;
    private DBCollection spiedCollection;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();

        ctx = new TestCRUDOperationContext(CRUDOperation.DELETE);
        // load metadata
        EntityMetadata md = getMd("./testMetadata.json");
        // and add it to metadata resolver (the context)
        ctx.add(md);
        // create translator with the context
        translator = new Translator(ctx, nodeFactory);

        spiedCollection = Mockito.spy(this.coll);
    }

    @Test
    public void deleteSingleBatch() {
        // setup data to delete
        String id = "deleteTest1";
        DBObject obj = new BasicDBObject();
        obj.put("_id", id);
        obj.put("objectType", "test");
        coll.insert(obj);

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 1, c.count());
        }

        // execute delete
        BasicDocDeleter deleter = new BasicDocDeleter(translator);
        CRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.DELETE);
        DBObject mongoQuery = new BasicDBObject();
        mongoQuery.put("_id", id);
        CRUDDeleteResponse response = new CRUDDeleteResponse();
        deleter.delete(ctx, spiedCollection, mongoQuery, response);

        Assert.assertEquals("num deleted", 1, response.getNumDeleted());

        // verify nothing left in collection
        Assert.assertEquals("count on collection", 0, coll.find(null).count());

        // the batch is called once
        Mockito.verify(spiedCollection, Mockito.times(1)).initializeUnorderedBulkOperation();
    }

    @Test
    public void deleteMultiBatch() {
        int docsToInsertCount = BasicDocDeleter.batchSize+2;
        // setup data to delete
        for (int i=0;i<docsToInsertCount;i++) {
            String id = "deleteTest1-"+i;
            DBObject obj = new BasicDBObject();
            obj.put("_id", id);
            obj.put("objectType", "test");
            coll.insert(obj);
        }

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", docsToInsertCount, c.count());
        }

        // execute delete
        BasicDocDeleter deleter = new BasicDocDeleter(translator);
        CRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.DELETE);
        DBObject mongoQuery = new BasicDBObject();
        mongoQuery.put("objectType", "test");
        CRUDDeleteResponse response = new CRUDDeleteResponse();
        deleter.delete(ctx, spiedCollection, mongoQuery, response);

        Assert.assertEquals("num deleted", docsToInsertCount, response.getNumDeleted());

        // verify nothing left in collection
        Assert.assertEquals("count on collection", 0, coll.find(null).count());

        // the batch is called twice
        Mockito.verify(spiedCollection, Mockito.times(2)).initializeUnorderedBulkOperation();
    }
}
