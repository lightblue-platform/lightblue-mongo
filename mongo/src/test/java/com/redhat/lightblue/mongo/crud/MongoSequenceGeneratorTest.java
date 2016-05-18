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

import org.junit.Test;

import com.redhat.lightblue.mongo.crud.MongoSequenceGenerator;

import org.junit.Assert;

public class MongoSequenceGeneratorTest extends AbstractMongoCrudTest {

    @Test
    public void theTest() throws Exception {
        MongoSequenceGenerator g = new MongoSequenceGenerator(coll);

        Assert.assertEquals(1, g.getNextSequenceValue("s1", 1, 1));
        Assert.assertEquals(100, g.getNextSequenceValue("s2", 100, 1));
        Assert.assertEquals(-1000, g.getNextSequenceValue("s3", -1000, 10));
        Assert.assertEquals(2, g.getNextSequenceValue("s1", 123, 123));
        Assert.assertEquals(3, g.getNextSequenceValue("s1", 213, 123));
        Assert.assertEquals(101, g.getNextSequenceValue("s2", 1234, 123));
        Assert.assertEquals(-990, g.getNextSequenceValue("s3", 123, 123));
    }
}
