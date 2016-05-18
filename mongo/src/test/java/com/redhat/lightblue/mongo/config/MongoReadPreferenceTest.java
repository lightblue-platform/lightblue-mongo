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
package com.redhat.lightblue.mongo.config;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.mongodb.ReadPreference;
import com.mongodb.TaggableReadPreference;
import com.mongodb.BasicDBObject;
import com.mongodb.TagSet;
import com.mongodb.Tag;

public class MongoReadPreferenceTest {

    @Test
    public void testNearest() {
        ReadPreference pref = MongoReadPreference.parse("nearest");
        Assert.assertTrue(pref.equals(ReadPreference.nearest()));
    }

    @Test
    public void testNearestArgs() {
        TaggableReadPreference pref = (TaggableReadPreference) MongoReadPreference.parse("nearest ( {\"x\":1} )");
        Assert.assertTrue(pref.equals(ReadPreference.nearest(new TagSet(Arrays.asList(new Tag("x", "1"))))));
    }

    @Test
    public void testNearestArgs2() {
        TaggableReadPreference pref = (TaggableReadPreference) MongoReadPreference.parse("nearest ( [ {\"x\":1}, {\"y\":\"a\"}] )");
        Assert.assertTrue(pref.equals(ReadPreference.nearest(Arrays.asList(new TagSet(Arrays.asList(new Tag("x", "1"))),
                new TagSet(Arrays.asList(new Tag("y", "a")))))));
    }

    @Test
    public void testNearestArgs3() {
        TaggableReadPreference pref = (TaggableReadPreference) MongoReadPreference.parse("nearest([ {\"x\":1}, {\"y\":\"a\"}])");
        Assert.assertTrue(pref.equals(ReadPreference.nearest(Arrays.asList(new TagSet(Arrays.asList(new Tag("x", "1"))),
                new TagSet(Arrays.asList(new Tag("y", "a")))))));
    }

    @Test
    public void testPrimary() {
        ReadPreference pref = MongoReadPreference.parse("primary");
        Assert.assertTrue(pref.equals(ReadPreference.primary()));
    }

    @Test
    public void testPrimaryPreferred() {
        ReadPreference pref = MongoReadPreference.parse("primaryPreferred");
        Assert.assertTrue(pref.equals(ReadPreference.primaryPreferred()));
    }

    @Test
    public void testSecondary() {
        ReadPreference pref = MongoReadPreference.parse("secondary");
        Assert.assertTrue(pref.equals(ReadPreference.secondary()));
    }

    @Test
    public void testSecondaryPreferred() {
        ReadPreference pref = MongoReadPreference.parse("secondaryPreferred");
        Assert.assertTrue(pref.equals(ReadPreference.secondaryPreferred()));
    }

    @Test
    public void testWrong() {
        try {
            MongoReadPreference.parse("secondaryPreferredd");
            Assert.fail();
        } catch (Exception e) {
        }
    }
}
