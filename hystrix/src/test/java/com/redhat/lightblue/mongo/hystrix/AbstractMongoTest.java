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
package com.redhat.lightblue.mongo.hystrix;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.redhat.lightblue.mongo.test.EmbeddedMongo;
import com.redhat.lightblue.util.test.AbstractJsonSchemaTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * @author nmalik
 */
public abstract class AbstractMongoTest extends AbstractJsonSchemaTest {
    private static EmbeddedMongo mongo = EmbeddedMongo.getInstance();
    protected static final String COLL_NAME = "data";
    protected static DB db;
    protected DBCollection coll;

    protected final String key1 = "name";
    protected final String key2 = "foo";

    @BeforeClass
    public static void setupClass() throws Exception {
        db = mongo.getDB();
    }

    @Before
    public void setup() {
        coll = db.getCollection(COLL_NAME);

        // setup data
        int count = 0;
        for (int i = 1; i < 5; i++) {
            for (int x = 1; x < i + 1; x++) {
                DBObject obj = new BasicDBObject(key1, "obj" + i);
                obj.put(key2, "bar" + x);
                coll.insert(obj);
                count++;
            }
        }

        Assert.assertEquals(count, coll.find().count());
    }

    @After
    public void teardown() {
        mongo.reset();
        coll = null;
    }
}
