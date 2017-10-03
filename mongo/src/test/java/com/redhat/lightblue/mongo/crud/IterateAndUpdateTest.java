package com.redhat.lightblue.mongo.crud;

import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.concurrent.Semaphore;

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.CRUDUpdateResponse;
import com.redhat.lightblue.crud.ConstraintValidator;
import com.redhat.lightblue.eval.FieldAccessRoleEvaluator;
import com.redhat.lightblue.eval.Projector;
import com.redhat.lightblue.eval.Updater;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.util.JsonUtils;

public class IterateAndUpdateTest extends AbstractMongoCrudTest {

    class TestIterateAndUpdate extends IterateAndUpdate {

        // 0 means a release is needed prior successful acquire
        Semaphore semaphore = new Semaphore(0);

        private boolean isWaitingForSemaphore = false;

        public TestIterateAndUpdate(JsonNodeFactory nodeFactory, ConstraintValidator validator, FieldAccessRoleEvaluator roleEval, DocTranslator translator,
                Updater updater, Projector projector, Projector errorProjector, WriteConcern writeConcern, int batchSize,
                ConcurrentModificationDetectionCfg concurrentModificationDetection) throws InterruptedException {
            super(nodeFactory, validator, roleEval, translator, updater, projector, errorProjector, writeConcern, batchSize, concurrentModificationDetection);
        }

        @Override
        public void preCommit() {
            try {
                isWaitingForSemaphore = true;
                semaphore.acquire();
                isWaitingForSemaphore = false;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public void waitUntilBatchIsReadAndUpdatedInMemory() throws InterruptedException {
            // in other words, wait until the thread reaches preCommit checkpoint and stops
            while (!isWaitingForSemaphore) {
                Thread.sleep(100);
            }
        }

        public void attemptToPersistUpdatedBatch(int allowBatches) {
            System.out.println("release");
            semaphore.release(allowBatches);
        }

    }

    EntityMetadata md = getMd("./testMetadata.json");

    class DefaultConcurrentModificationDetectionCfg extends ConcurrentModificationDetectionCfg {

        public DefaultConcurrentModificationDetectionCfg() {
            super(null);
        }

        @Override
        public boolean isDetect() {
            return true;
        }

        @Override
        public int getFailureRetryCount() {
            return 3;
        }

        @Override
        public boolean isReevaluateQueryForRetry() {
            return true;
        }

    }

    CRUDOperationContext ctx;

    TestIterateAndUpdate iterateAndUpdate;

    @Before
    public void setup() throws Exception {

        super.setup();

        // Cleanup stuff
        db.getCollection(COLL_NAME).drop();
        db.createCollection(COLL_NAME, null);

        ctx = new CRUDOperationContext(CRUDOperation.UPDATE, "test", factory, Collections.emptyList(), null) {
            @Override
            public EntityMetadata getEntityMetadata(String entityName) {
                return md;
            }
        };

        Updater updater = Updater.getInstance(nodeFactory, md,
                UpdateExpression.fromJson(JsonUtils.json("{'$set': { 'field1': 'changed-by-iterate-and-update'}}".replaceAll("'", "\""))));

        iterateAndUpdate = new TestIterateAndUpdate(
                nodeFactory,
                mock(ConstraintValidator.class),
                new FieldAccessRoleEvaluator(md, Collections.emptySet()),
                new DocTranslator(ctx, nodeFactory),
                updater, null, Projector.getInstance(MongoCRUDController.ID_PROJECTION, md), WriteConcern.ACKNOWLEDGED, 3,
                new DefaultConcurrentModificationDetectionCfg());
    }

    @Test
    public void update() throws Exception {

        DBCollection collection = db.getCollection(COLL_NAME);

        // insert 5 test docs
        for (int i = 0; i < 5; i++) {
            DBObject insertDoc = new BasicDBObject("field1", "initialValue").append("objectType", "test");
            collection.insert(insertDoc);
        }

        CRUDUpdateResponse response = new CRUDUpdateResponse();

        // start update using iterateAndUpdate, make sure it stops before commit
        Thread updateThread = new Thread(new Runnable() {
            @Override
            public void run() {
                iterateAndUpdate.update(ctx, collection, md, response, new BasicDBObject("field1", "initialValue"));
            }
        });
        updateThread.start();

        iterateAndUpdate.waitUntilBatchIsReadAndUpdatedInMemory();

        // simulate another thread changing the docs
        // so that the update query does not match them anymore
        DBCursor cursor = coll.find();
        ObjectId docVer = new ObjectId();
        for (int i = 0; i < 4; i++) {
            DBObject doc = cursor.next();
            DBObject modified = new BasicDBObject("_id", doc.get("_id")).append("objectType", "test").append("field1", "changed-by-other-thread");
            DocVerUtil.setDocVer(modified, docVer);
            coll.save(modified);
            System.out.println("Doc "+modified+" saved");
        }
        cursor.close();

        // batch size is 3, so it will take 2 batches to process all 5 documents
        iterateAndUpdate.attemptToPersistUpdatedBatch(2);

        // wait until update completes
        while(updateThread.isAlive()) {
            Thread.sleep(100);
        }

        // make sure counts in response (in CRUDOperationContext passed to IterateAndUpdate.update) are set properly
        Assert.assertEquals(0, response.getNumFailed());
        Assert.assertEquals("Expecting 1 docs to match eventually, even though initially 5 matched", 1, response.getNumMatched()); // https://github.com/lightblue-platform/lightblue-mongo/issues/386
        Assert.assertEquals(1, response.getNumUpdated());

        int docsUpdatedByIterateAndUpdate = 0, docsTotal = 0;

        cursor = coll.find();
        while (cursor.hasNext()) {
            DBObject doc = cursor.next();
            System.out.println(doc.get("_id") + " " + doc.get("field1"));

            if ("changed-by-iterate-and-update".equals(doc.get("field1"))) {
                docsUpdatedByIterateAndUpdate++;
            }

            docsTotal++;

        }
        cursor.close();

        Assert.assertEquals("5 docs should have been procesed", 5, docsTotal);
        Assert.assertEquals("Out of 5 processed docs, only 1 should have been updated due to concurrent update", 1, docsUpdatedByIterateAndUpdate);
    }

}
