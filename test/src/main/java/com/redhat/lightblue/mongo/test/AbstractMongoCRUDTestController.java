package com.redhat.lightblue.mongo.test;

import static com.redhat.lightblue.util.JsonUtils.json;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.ClassRule;

import com.fasterxml.jackson.databind.JsonNode;
import com.redhat.lightblue.mongo.test.MongoServerExternalResource.InMemoryMongoServer;
import com.redhat.lightblue.test.AbstractCRUDTestController;

@InMemoryMongoServer
public abstract class AbstractMongoCRUDTestController extends AbstractCRUDTestController {

    @ClassRule
    public static MongoServerExternalResource mongoServer = new MongoServerExternalResource();

    @BeforeClass
    public static void prepareMongoDatasources() {
        System.setProperty("mongo.host", "localhost");
        System.setProperty("mongo.port", String.valueOf(mongoServer.getPort()));
        System.setProperty("mongo.database", "lightblue");
    }

    public AbstractMongoCRUDTestController() throws Exception {
        super(true);
    }

    @Override
    protected JsonNode getDatasourcesJson() {
        try {
            if (getDatasourcesResourceName() == null) {
                return json(loadResource("/mongo-datasources.json", true));
            } else {
                return json(loadResource(getDatasourcesResourceName(), false));
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to load resource '" + getDatasourcesResourceName() + "'", e);
        }
    }

}
