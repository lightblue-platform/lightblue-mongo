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

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import com.redhat.lightblue.crud.CRUDDeleteResponse;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.crud.CRUDOperation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author nmalik
 */
public class BasicDocDeleterTest extends AbstractMongoCrudTest {

    private TestCRUDOperationContext ctx;
    private Translator translator;

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
    }

    @Test
    public void delete() {
        // setup data to delete
        String id = "deleteTest1";
        DBObject obj = new BasicDBObject();
        obj.put("_id", id);
        obj.put("objectType", "test");
        WriteResult wr = coll.insert(obj);

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 1, c.count());
        }

        // execute delete
        BasicDocDeleter deleter = new BasicDocDeleter(translator);
        CRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.DELETE);
        DBObject mongoQuery = new BasicDBObject();
        mongoQuery.put("_id", id);
        CRUDDeleteResponse response = new CRUDDeleteResponse();
        deleter.delete(ctx, coll, mongoQuery, response);

        Assert.assertEquals("num deleted", 1, response.getNumDeleted());

        // verify nothing left in collection
        Assert.assertEquals("count on collection", 0, coll.find(null).count());
    }
}
