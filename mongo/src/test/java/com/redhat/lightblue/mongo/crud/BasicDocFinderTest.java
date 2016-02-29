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

import java.io.IOException;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.JsonUtils;

/**
 *
 * @author nmalik
 */
public class BasicDocFinderTest extends AbstractMongoCrudTest {

    private TestCRUDOperationContext ctx;
    private Translator translator;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();

        ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        // load metadata
        EntityMetadata md = getMd("./testMetadata.json");
        // and add it to metadata resolver (the context)
        ctx.add(md);
        // create translator with the context
        translator = new Translator(ctx, nodeFactory);
    }

    private void insert(String jsonStringFormat, String formatArg) {
        insert(jsonStringFormat, new String[]{formatArg});
    }

    private void insert(String jsonStringFormat, String[] formatArgs) {
        try {
            JsonNode node = JsonUtils.json(String.format(jsonStringFormat, (Object[]) formatArgs));
            BasicDBObject dbObject = new BasicDBObject();
            for (Iterator<String> itr = node.fieldNames(); itr.hasNext();) {
                String fld = itr.next();
                dbObject.append(fld, node.get(fld).asText());
            }
            WriteResult wr = coll.insert(dbObject);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void findAll() throws IOException, ProcessingException {
        String id = "findBasic";
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "1");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "2");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "3");

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 3, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null,
                // DBObject (Projection)
                null,
                // DBObject (sort)
                null,
                // Long (from)
                null,
                // Long (to)
                null);

        Assert.assertEquals("find count", 3, count);
        Assert.assertEquals(3, ctx.getDocumentsWithoutErrors().size());
    }

    @Test
    public void findOneOfMany() throws IOException, ProcessingException {
        String id = "findOneOfMany";
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "1");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "2");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "3");

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 3, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        DBObject mongoQuery = new BasicDBObject();
        mongoQuery.put("_id", id + "1");

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                mongoQuery,
                // DBObject (projection)
                null,
                // DBObject (sort)
                null,
                // Long (from)
                null,
                // Long (to)
                null);

        Assert.assertEquals("find count", 1, count);
        Assert.assertEquals(1, ctx.getDocumentsWithoutErrors().size());
    }

    @Test
    public void findLimit() throws IOException, ProcessingException {
        String id = "findLimit";
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "1");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "2");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "3");

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 3, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null, // all
                null,
                // DBObject (sort)
                null,
                // Long (from)
                null,
                // Long (to)
                1l);

        Assert.assertEquals("find count", 3, count);
        Assert.assertEquals(2, ctx.getDocumentsWithoutErrors().size());
    }

    @Test
    public void testSkipLimit() throws IOException, ProcessingException {
    	String id = "findLimit";
        for (int i = 0; i < 20; i++) {
            insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + i);
        }

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 20, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null, // all
                null,
                // DBObject (sort)
                null,
                // Long (from)
                3l,
                // Long (to)
                9l);

        Assert.assertEquals("find count", 20, count);
        Assert.assertEquals(7, ctx.getDocumentsWithoutErrors().size());
        Assert.assertEquals(id + "3", ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "4", ctx.getDocuments().get(1).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "5", ctx.getDocuments().get(2).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "6", ctx.getDocuments().get(3).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "7", ctx.getDocuments().get(4).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "8", ctx.getDocuments().get(5).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "9", ctx.getDocuments().get(6).getOutputDocument().get(new Path("_id")).asText());



    }

    @Test
    public void testNullLimit() throws IOException, ProcessingException {
        String id = "findLimit";
        for(int i = 0; i < 20; i++) {
            insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + i);
        }

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 20, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null, // all
                null,
                // DBObject (sort)
                null,
                // Long (from)
                12l,
                // Long (to)
                null);

        Assert.assertEquals("find count", 20, count);
        Assert.assertEquals(8, ctx.getDocumentsWithoutErrors().size());
    }

    @Test
    public void testZeroLimit() throws IOException, ProcessingException {
        String id = "findLimit";
        for(int i = 0; i < 20; i++) {
            insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + i);
        }

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 20, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null, // all
                null,
                // DBObject (sort)
                null,
                // Long (from)
                0l,
                // Long (to)
                0l);

        Assert.assertEquals("find count", 20, count);
        Assert.assertEquals(1, ctx.getDocumentsWithoutErrors().size());
    }

    @Test
    public void testResultSetLimit() throws IOException, ProcessingException {
        String id = "findLimit";
        for(int i = 0; i < 20; i++) {
            insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + i);
        }

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 20, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);
        finder.setMaxResultSetSize(10);

        try {
            finder.find(
                        // CRUDOperationContext
                        ctx,
                        //DBCollection
                        coll,
                        // DBObject (query)
                        null, // all
                        null,
                        // DBObject (sort)
                        null,
                        // Long (from)
                        0l,
                        // Long (to)
                        null);
            Assert.fail();
        } catch(Exception e) {}
        long count = finder.find(
                                 // CRUDOperationContext
                                 ctx,
                                 //DBCollection
                                 coll,
                                 // DBObject (query)
                                 null, // all
                                 null,
                                 // DBObject (sort)
                                 null,
                                 // Long (from)
                                 0l,
                                 // Long (to)
                                 9l);

        Assert.assertEquals("find count", 20, count);
        Assert.assertEquals(10, ctx.getDocumentsWithoutErrors().size());
    }


    @Test
    public void testNegativeLimit() throws IOException, ProcessingException {
        String id = "findLimit";
        for(int i = 0; i < 20; i++) {
            insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + i);
        }

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 20, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null, // all
                null,
                // DBObject (sort)
                null,
                // Long (from)
                12l,
                // Long (to)
                -8l);

        Assert.assertEquals("find count", 20, count);
        Assert.assertEquals(0, ctx.getDocumentsWithoutErrors().size());
    }

    @Test
    public void testLimitLesserThanSkip() throws IOException, ProcessingException {
        String id = "findLimit";
        for(int i = 0; i < 20; i++) {
            insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + i);
        }

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 20, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null, // all
                null,
                // DBObject (sort)
                null,
                // Long (from)
                18l,
                // Long (to)
                8l);

        Assert.assertEquals("find count", 20, count);
        Assert.assertEquals(0, ctx.getDocumentsWithoutErrors().size());
    }

    @Test
    public void findSort() throws IOException, ProcessingException {
        String id = "findSort";
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "2");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "1");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "3");

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 3, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        DBObject sort = new BasicDBObject();
        sort.put("_id", -1);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null, // all
                null,
                // DBObject (sort)
                sort,
                // Long (from)
                null,
                // Long (to)
                null);

        Assert.assertEquals("find count", 3, count);
        Assert.assertEquals(3, ctx.getDocumentsWithoutErrors().size());

        // verify order
        Assert.assertEquals(id + "3", ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "2", ctx.getDocuments().get(1).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "1", ctx.getDocuments().get(2).getOutputDocument().get(new Path("_id")).asText());
    }

    @Test
    public void findSortAndLimit() throws IOException, ProcessingException {
        String id = "findSortAndLimit";
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "2");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "1");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "3");

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 3, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        DBObject sort = new BasicDBObject();
        sort.put("_id", -1);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null, // all
                null,
                // DBObject (sort)
                sort,
                // Long (from)
                null,
                // Long (to)
                1l);

        Assert.assertEquals("find count", 3, count);
        Assert.assertEquals(2, ctx.getDocumentsWithoutErrors().size());

        // verify order
        Assert.assertEquals(id + "3", ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "2", ctx.getDocuments().get(1).getOutputDocument().get(new Path("_id")).asText());
    }

    @Test
    public void findSkip() throws IOException, ProcessingException {
        String id = "findSkip";
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "1");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "2");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "3");

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 3, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null, // all
                null,
                // DBObject (sort)
                null,
                // Long (from)
                1l,
                // Long (to)
                null);

        Assert.assertEquals("find count", 3, count);
        Assert.assertEquals(2, ctx.getDocumentsWithoutErrors().size());

        // verify data
        Assert.assertEquals(id + "2", ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "3", ctx.getDocuments().get(1).getOutputDocument().get(new Path("_id")).asText());
    }

    @Test
    public void findSortAndSkip() throws IOException, ProcessingException {
        String id = "findSortAndSkip";
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "2");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "1");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "3");

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 3, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        DBObject sort = new BasicDBObject();
        sort.put("_id", -1);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null, // all
                null,
                // DBObject (sort)
                sort,
                // Long (from)
                1l,
                // Long (to)
                null);

        Assert.assertEquals("find count", 3, count);
        Assert.assertEquals(2, ctx.getDocumentsWithoutErrors().size());

        // verify order
        Assert.assertEquals(id + "2", ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "1", ctx.getDocuments().get(1).getOutputDocument().get(new Path("_id")).asText());
    }

    @Test
    public void findSortSkipAndLimit() throws IOException, ProcessingException {
        String id = "findSortSkipAndLimit";
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "2");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "1");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "4");
        insert("{\"_id\":\"%s\",\"objectType\":\"test\"}", id + "3");

        try (DBCursor c = coll.find(null)) {
            Assert.assertEquals("count on collection", 4, c.count());
        }

        BasicDocFinder finder = new BasicDocFinder(translator);

        DBObject sort = new BasicDBObject();
        sort.put("_id", 1);

        long count = finder.find(
                // CRUDOperationContext
                ctx,
                //DBCollection
                coll,
                // DBObject (query)
                null, // all
                null,
                // DBObject (sort)
                sort,
                // Long (from)
                1l,
                // Long (to)
                2l);

        Assert.assertEquals("find count", 4, count);
        Assert.assertEquals(2, ctx.getDocumentsWithoutErrors().size());

        // verify order
        Assert.assertEquals(id + "2", ctx.getDocuments().get(0).getOutputDocument().get(new Path("_id")).asText());
        Assert.assertEquals(id + "3", ctx.getDocuments().get(1).getOutputDocument().get(new Path("_id")).asText());
    }
}
