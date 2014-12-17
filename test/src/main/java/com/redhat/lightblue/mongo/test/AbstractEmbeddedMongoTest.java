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
package com.redhat.lightblue.mongo.test;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;

/**
 * Simple base unit tests that uses EmbeddedMongo to manage in memory mongo database.
 *
 * @author nmalik
 */
public abstract class AbstractEmbeddedMongoTest {

    private static EmbeddedMongo mongo = EmbeddedMongo.getInstance();

    @After
    public void teardown() {
        mongo.reset();
    }
}
