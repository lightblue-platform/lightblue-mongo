package com.redhat.lightblue.mongo.test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.net.UnknownHostException;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.mongodb.MongoClient;
import com.redhat.lightblue.mongo.test.MongoServerExternalResource.InMemoryMongoServer;

@InMemoryMongoServer
public class MongoServerExternalResourceTest {

    @ClassRule
    public static final MongoServerExternalResource mongoServer = new MongoServerExternalResource();

    @Test(expected = IllegalStateException.class)
    public void testApply_WithoutAnnotation_onTestLevel() {
        new MongoServerExternalResource().apply(mock(Statement.class), mock(Description.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testApply_WithoutAnnotation_onClassLevel() {
        new MongoServerExternalResource().apply(
                mock(Statement.class),
                Description.createSuiteDescription(Object.class));
    }

    @Test
    public void testGetConnection() throws UnknownHostException {
        MongoClient client = mongoServer.getConnection();

        assertNotNull(client);
        assertNotNull(client.getDB("test"));
    }
}
