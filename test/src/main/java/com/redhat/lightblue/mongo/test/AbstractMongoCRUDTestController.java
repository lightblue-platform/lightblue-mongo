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
