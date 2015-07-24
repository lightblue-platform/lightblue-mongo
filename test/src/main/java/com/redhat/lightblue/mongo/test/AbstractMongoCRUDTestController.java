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

import static com.redhat.lightblue.util.JsonUtils.json;
import static com.redhat.lightblue.util.test.AbstractJsonNodeTest.loadResource;

import java.net.UnknownHostException;

import org.junit.BeforeClass;
import org.junit.ClassRule;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.BasicDBObject;
import com.redhat.lightblue.mongo.test.MongoServerExternalResource.InMemoryMongoServer;
import com.redhat.lightblue.test.AbstractCRUDTestController;

/**
 * <p>Extension of {@link AbstractCRUDTestController} that adds built in mongo support.</p>
 * <p><b>NOTE:</b> At this time only restarting mongo before/after the suite is supported.
 * That said, if you need to clean the mongo database between tests then see {@link #cleanupMongoCollections(String...)}. </p>
 *
 * @author dcrissman
 */
@InMemoryMongoServer
public abstract class AbstractMongoCRUDTestController extends AbstractCRUDTestController {

    @ClassRule
    public static MongoServerExternalResource mongoServer = new MongoServerExternalResource();

    @BeforeClass
    public static void prepareMongoDatasources() {
        if (System.getProperty("mongo.datasource") == null) {
            System.setProperty("mongo.datasource", "mongo");
        }
        if (System.getProperty("mongo.database") == null) {
            System.setProperty("mongo.database", "testdb");
        }
        if (System.getProperty("mongo.host") == null) {
            System.setProperty("mongo.host", "localhost");
        }
        if (System.getProperty("mongo.port") == null) {
            System.setProperty("mongo.port", String.valueOf(mongoServer.getPort()));
        }
    }

    public AbstractMongoCRUDTestController() throws Exception {
        super(true);
    }

    @Override
    protected JsonNode getLightblueMetadataJson() throws Exception {
        return json(loadResource("/mongo-lightblue-metadata.json", getClass()));
    }

    @Override
    protected JsonNode getDatasourcesJson() throws Exception {
        return json(loadResource("/mongo-datasources.json", getClass()));
    }

    /**
     * Drop specified collections from the mongo database with the dbName from <code>System.getProperty("mongo.database")</code>.
     * Useful for cleaning up between tests.
     *
     * @param collectionName
     * @throws UnknownHostException
     */
    public void cleanupMongoCollections(String... collectionNames) throws UnknownHostException {
        cleanupMongoCollections(System.getProperty("mongo.database"), collectionNames);
    }

    /**
     * Drop specified collections. Useful for cleaning up between tests.
     *
     * @param dbName
     * @param collectionName
     * @throws UnknownHostException
     */
    public void cleanupMongoCollections(String dbName, String[] collectionNames) throws UnknownHostException {
        for (String collectionName : collectionNames) {
            mongoServer.getConnection().getDB(dbName).getCollection(collectionName).remove(new BasicDBObject());
        }
    }

}
