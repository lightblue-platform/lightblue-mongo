/*
 Copyright 2015 Red Hat, Inc. and/or its affiliates.

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
package com.redhat.lightblue.mongo.test;

import com.mongodb.BasicDBObject;
import static org.junit.Assert.assertNotNull;

import java.net.UnknownHostException;

import org.junit.ClassRule;
import org.junit.Test;

import com.mongodb.MongoClient;
import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public class MongoServerExternalResourceTest {

    @ClassRule
    public static final MongoServerExternalResource mongoServer = new MongoServerExternalResource();

    @Before
    public void before() throws IOException {
        mongoServer.before();
    }

    @After
    public void after() throws IOException {
        mongoServer.after();
    }

    @Test
    public void testGetConnection() throws UnknownHostException {
        MongoClient client = mongoServer.getConnection();

        assertNotNull(client);
        assertNotNull(client.getDB("test"));
    }

    @Test
    public void testRestart() throws UnknownHostException, IOException {
        {
            MongoClient client = mongoServer.getConnection();

            Assert.assertNotNull(client.getDB("test"));
            Assert.assertNotNull(client.getDB("test").getCollection("collection"));
            Assert.assertEquals(0, client.getDB("test").getCollection("collection").count());
            client.getDB("test").getCollection("collection").insert(new BasicDBObject("foo", "bar"));
            Assert.assertEquals(1, client.getDB("test").getCollection("collection").count());
        }

        mongoServer.after();
        mongoServer.before();

        {
            MongoClient client = mongoServer.getConnection();

            Assert.assertNotNull(client.getDB("test"));
            Assert.assertNotNull(client.getDB("test").getCollection("collection"));
            Assert.assertEquals(0, client.getDB("test").getCollection("collection").count());
            client.getDB("test").getCollection("collection").insert(new BasicDBObject("foo", "bar"));
            Assert.assertEquals(1, client.getDB("test").getCollection("collection").count());
        }

        mongoServer.after();
        mongoServer.before();

        {
            MongoClient client = mongoServer.getConnection();

            Assert.assertNotNull(client.getDB("test"));
            Assert.assertNotNull(client.getDB("test").getCollection("collection"));
            Assert.assertEquals(0, client.getDB("test").getCollection("collection").count());
            client.getDB("test").getCollection("collection").insert(new BasicDBObject("foo", "bar"));
            Assert.assertEquals(1, client.getDB("test").getCollection("collection").count());
        }
    }
}
