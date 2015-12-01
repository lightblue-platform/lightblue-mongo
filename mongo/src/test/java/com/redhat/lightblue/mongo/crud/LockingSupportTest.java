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

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com.redhat.lightblue.config.LightblueFactory;
import com.redhat.lightblue.config.DataSourcesConfiguration;

import com.redhat.lightblue.extensions.synch.Locking;
import com.redhat.lightblue.mongo.crud.MongoLocking;

public class LockingSupportTest extends AbstractMongoCrudTest  {

    private LightblueFactory lbfactory;
    
    @Before
    public void setup() throws Exception {
        super.setup();
        JsonNode datasources = loadJsonNode("./datasources.json");
        DataSourcesConfiguration dscfg=new DataSourcesConfiguration(datasources);
        JsonNode crudnode=loadJsonNode("./lightblue-crud.json");
        JsonNode mdnode=loadJsonNode("./lightblue-metadata.json");
        lbfactory=new LightblueFactory(dscfg,crudnode,mdnode);
    }

    @Test
    public void testGetLocking() throws Exception {
        try {
            lbfactory.getLocking("blah");
            Assert.fail();
        } catch (Exception e) {}
        Locking locking=lbfactory.getLocking("test");
        Assert.assertTrue(locking instanceof MongoLocking);
    }
}
