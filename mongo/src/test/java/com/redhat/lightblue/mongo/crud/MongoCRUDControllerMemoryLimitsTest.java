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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.redhat.lightblue.Response;
import com.redhat.lightblue.crud.CRUDInsertionResponse;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.CRUDUpdateResponse;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.crud.DocumentStream;
import com.redhat.lightblue.hooks.CRUDHook;
import com.redhat.lightblue.hooks.HookDoc;
import com.redhat.lightblue.hooks.HookResolver;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.HookConfiguration;
import com.redhat.lightblue.mongo.common.DBResolver;
import com.redhat.lightblue.mongo.common.MongoDataStore;
import com.redhat.lightblue.mongo.config.MongoConfiguration;
import com.redhat.lightblue.util.JsonDoc;

/**
 * Memory limit tests.
 *
 * @author mpatercz
 *
 */
public class MongoCRUDControllerMemoryLimitsTest extends AbstractMongoCrudTest {

    private MongoCRUDController controller;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();

        final DB dbx = db;
        // Cleanup stuff
        dbx.getCollection(COLL_NAME).drop();
        dbx.createCollection(COLL_NAME, null);

        controller = new MongoCRUDController(null, new DBResolver() {
            @Override
            public DB get(MongoDataStore store) {
                return dbx;
            }

            @Override
            public MongoConfiguration getConfiguration(MongoDataStore store) {
                MongoConfiguration configuration = new MongoConfiguration();
                try {
                    configuration.addServerAddress("localhost", 27777   );
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

        factory.setHookResolver(new HookResolver() {

            @Override
            public CRUDHook getHook(String name) {
                if ("updateHook".equals(name)) {
                    return new CRUDHook() {

                        @Override
                        public void processHook(EntityMetadata md, HookConfiguration cfg, List<HookDoc> processedDocuments) {

                        }

                        @Override
                        public String getName() {
                            return "updateHook";
                        }
                    };
                } else {
                    return null;
                }
            }
        });

    }

    private void setupTestDataAndMetadata() throws IOException {
        emd = getMd("./testMetadata_cap.json");
        controller.afterUpdateEntityInfo(null, emd.getEntityInfo(), false);
        testDoc = new JsonDoc(loadJsonNode("./testData_cap.json"));
    }

    private void setupTestDataAndMetadataWithHook() throws IOException {
        emd = getMd("./testMetadata_cap_hook.json");
        controller.afterUpdateEntityInfo(null, emd.getEntityInfo(), false);
        testDoc = new JsonDoc(loadJsonNode("./testData_cap.json"));
    }

    private void addDocument(CRUDOperationContext ctx,JsonDoc doc) {
        if(ctx.getInputDocuments()==null) {
            ArrayList<DocCtx> list=new ArrayList<>();
            ctx.setInputDocuments(list);
        }
        ctx.getInputDocuments().add(new DocCtx(doc));
    }

    private void addDocuments(CRUDOperationContext ctx,List<JsonDoc> docs) {
        for(JsonDoc doc:docs)
            addDocument(ctx,doc);
    }

    private List<DocCtx> streamToList(CRUDOperationContext ctx) {
        List<DocCtx> list=new ArrayList<>();
        DocumentStream<DocCtx> stream=ctx.getDocumentStream();
        while(stream.hasNext())
            list.add(stream.next());
        return list;
    }

    EntityMetadata emd;
    JsonDoc testDoc;

    private void insertDocs(int count) throws IOException {
        TestCRUDOperationContext ctx = new TestCRUDOperationContext("test", CRUDOperation.INSERT);
        ctx.add(emd);

        for (int i=0;i<count;i++) {
            addDocument(ctx,testDoc);
        }
        CRUDInsertionResponse crudResponse = controller.insert(ctx, null);
        Assert.assertEquals(count, crudResponse.getNumInserted());
    }

    /**
     * A baseline test, no limits on result set size.
     *
     * @throws Exception
     */
    @Test
    public void updateDocuments_NoLimits() throws Exception {
        setupTestDataAndMetadata();
        insertDocs(10);

        TestCRUDOperationContext ctx = new TestCRUDOperationContext("test", CRUDOperation.UPDATE);
        ctx.add(emd);

        ctx.getFactory().setMaxResultSetSizeForWritesB(-1);
        CRUDUpdateResponse response = controller.update(ctx,
                query("{'field':'field1','op':'$eq','rvalue':'f1'}"),
                update("{ '$set': { 'field2' : 'f2-updated' } }"),
                projection("{'field':'*'}"));

        Assert.assertEquals(10, response.getNumMatched());
        Assert.assertEquals(10, response.getNumUpdated());

        DBCursor cursor = db.getCollection("data").find();
        assertTrue(cursor.hasNext());
        cursor.forEach(obj -> {
            String field2value = ((DBObject) obj).get("field2").toString();
            assertEquals("f2-updated", field2value);
        });
        List<DocCtx> documents=streamToList(ctx);
        Assert.assertEquals(10, documents.size());

        Assert.assertTrue(ctx.getErrors().isEmpty());
    }

    /**
     * Limit is above the needs of this update.
     *
     * @throws Exception
     */
    @Test
    public void updateDocuments_LimitNotExceeded() throws Exception {
        setupTestDataAndMetadata();
        insertDocs(10);

        TestCRUDOperationContext ctx = new TestCRUDOperationContext("test", CRUDOperation.UPDATE);
        ctx.add(emd);

        // 74 docs is 25798, 11 docs is ~3835B
        ctx.getFactory().setMaxResultSetSizeForWritesB(3835);
        CRUDUpdateResponse response = controller.update(ctx,
                query("{'field':'field1','op':'$eq','rvalue':'f1'}"),
                update("{ '$set': { 'field2' : 'f2-updated' } }"),
                projection("{'field':'*'}"));

        Assert.assertEquals(10, response.getNumMatched());
        Assert.assertEquals(10, response.getNumUpdated());

        DBCursor cursor = db.getCollection("data").find();
        assertTrue(cursor.hasNext());
        cursor.forEach(obj -> {
            String field2value = ((DBObject) obj).get("field2").toString();
            assertEquals("f2-updated", field2value);
        });
        List<DocCtx> documents=streamToList(ctx);
        Assert.assertEquals(10, documents.size());

        Assert.assertTrue(ctx.getErrors().isEmpty());
    }

    /**
     * Update accumulates a list of docs which exceeds the threshold.
     *
     * @throws Exception
     */
    @Test
    public void updateDocuments_LimitExceeded() throws Exception {

        // 2 batches. One batch + 10 would be updated in full, because the memory limit is enforced after commit.
        int count = 2*MongoCRUDController.DEFAULT_BATCH_SIZE+10;

        setupTestDataAndMetadata();
        insertDocs(count);

        TestCRUDOperationContext ctx = new TestCRUDOperationContext("test", CRUDOperation.UPDATE);
        ctx.add(emd);

        // 74 docs is 25798, 66 docs is ~22310
        ctx.getFactory().setMaxResultSetSizeForWritesB(22310);
        CRUDUpdateResponse response = controller.update(ctx,
                query("{'field':'field1','op':'$eq','rvalue':'f1'}"),
                update("{ '$set': { 'field2' : 'f2-updated' } }"),
                projection("{'field':'*'}"));

        Assert.assertEquals(1, ctx.getErrors().size());
        Assert.assertEquals(MongoCrudConstants.ERROR_RESULT_SIZE_TOO_LARGE, ctx.getErrors().get(0).getErrorCode());

        // this is misleading - 2 batches was updated successfully
        // see IterateAndUpdate#enforceMemoryLimit for more info
        Assert.assertEquals(0, response.getNumMatched());
        Assert.assertEquals(0, response.getNumUpdated());
        Assert.assertEquals(0, response.getNumFailed());

        DBCursor cursor = db.getCollection("data").find();

        int i = 0;
        while (cursor.hasNext()) {
            String field2value = cursor.next().get("field2").toString();
            if (i < 2*MongoCRUDController.DEFAULT_BATCH_SIZE) {
                assertEquals("Expecting first batch to be updated successfully", "f2-updated", field2value);
            } else {
                assertEquals("Expecting 2nd batch not to be updated, because this is where memory limit was reached", "f2", field2value);
            }
            i++;
        }
        Assert.assertEquals(count, i);

    }

    /**
     * Hook processing exceeds threshold. The size of the list of docs accumulated for this update
     * does not.
     *
     * @throws Exception
     */
    @Test
    public void updateDocuments_LimitExceededByHookManager() throws Exception {

        int count = MongoCRUDController.DEFAULT_BATCH_SIZE+10;

        setupTestDataAndMetadataWithHook();
        insertDocs(count);

        TestCRUDOperationContext ctx = new TestCRUDOperationContext("test", CRUDOperation.UPDATE);
        ctx.add(emd);

        // 25798 B is the ctx.inputDocuments size after update, but before hooks
        ctx.getFactory().setMaxResultSetSizeForWritesB(25798+10);

        CRUDUpdateResponse response = controller.update(ctx,
                query("{'field':'field1','op':'$eq','rvalue':'f1'}"),
                update("{ '$set': { 'field2' : 'f2-updated' } }"),
                projection("{'field':'*'}"));

        // expecting memory limit to kick in during hook processing
        Assert.assertEquals(1, ctx.getErrors().size());
        Assert.assertEquals(Response.ERR_RESULT_SIZE_TOO_LARGE, ctx.getErrors().get(0).getErrorCode());

        // all updates applied successfully
        Assert.assertEquals(count, response.getNumMatched());
        Assert.assertEquals(count, response.getNumUpdated());
        Assert.assertEquals(0, response.getNumFailed());

        DBCursor cursor = db.getCollection("data").find();

        int i = 0;
        while (cursor.hasNext()) {
            String field2value = cursor.next().get("field2").toString();
            assertEquals("Expecting first batch to be updated successfully", "f2-updated", field2value);
            i++;
        }
        Assert.assertEquals(count, i);

    }



}
