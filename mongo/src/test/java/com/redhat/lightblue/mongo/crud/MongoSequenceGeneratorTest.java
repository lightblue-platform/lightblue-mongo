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

import java.util.Map;
import java.util.HashMap;

import org.junit.Test;
import org.junit.Before;

import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

import com.redhat.lightblue.mongo.crud.MongoSequenceGenerator;

import org.junit.Assert;

public class MongoSequenceGeneratorTest extends AbstractMongoCrudTest {

    @Before
    public void init() {
        coll.remove(new BasicDBObject());
        MongoSequenceGenerator.sequenceInfo.clear();
    }
    
    @Test
    public void zeroPoolTest() throws Exception {
        MongoSequenceGenerator g = new MongoSequenceGenerator(coll);

        Assert.assertEquals(1, g.getNextSequenceValue("s1", 1, 1,0));
        validateId("s1",2);
        Assert.assertEquals(100, g.getNextSequenceValue("s2", 100, 1,0));
        validateId("s2",101);
        Assert.assertEquals(-1000, g.getNextSequenceValue("s3", -1000, 10,0));
        validateId("s3",-990);
        Assert.assertEquals(2, g.getNextSequenceValue("s1", 123, 123,0));
        validateId("s1",3);
        Assert.assertEquals(3, g.getNextSequenceValue("s1", 213, 123,0));
        validateId("s1",4);
        Assert.assertEquals(101, g.getNextSequenceValue("s2", 1234, 123,0));
        validateId("s2",102);
        Assert.assertEquals(-990, g.getNextSequenceValue("s3", 123, 123,0));
        validateId("s3",-980);
    }

    @Test
    public void onePoolTest() throws Exception {
        MongoSequenceGenerator g = new MongoSequenceGenerator(coll);

        Assert.assertEquals(1, g.getNextSequenceValue("s1", 1, 1,1));
        validateId("s1",2);
        Assert.assertEquals(100, g.getNextSequenceValue("s2", 100, 1,1));
        validateId("s2",101);
        Assert.assertEquals(-1000, g.getNextSequenceValue("s3", -1000, 10,1));
        validateId("s3",-990);
        Assert.assertEquals(2, g.getNextSequenceValue("s1", 123, 123,1));
        validateId("s1",3);
        Assert.assertEquals(3, g.getNextSequenceValue("s1", 213, 123,1));
        validateId("s1",4);
        Assert.assertEquals(101, g.getNextSequenceValue("s2", 1234, 123,1));
        validateId("s2",102);
        Assert.assertEquals(-990, g.getNextSequenceValue("s3", 123, 123,1));
        validateId("s3",-980);
    }

    @Test
    public void bigPoolTest() throws Exception {
        MongoSequenceGenerator g = new MongoSequenceGenerator(coll);

        Assert.assertEquals(1, g.getNextSequenceValue("s1", 1, 1,2));
        validateId("s1",3);
        Assert.assertEquals(2, g.getNextSequenceValue("s1", 1, 1,2));
        validateId("s1",3);
        Assert.assertEquals(3, g.getNextSequenceValue("s1", 1, 1,2));
        validateId("s1",5);
        Assert.assertEquals(4, g.getNextSequenceValue("s1", 1, 1,2));
        validateId("s1",5);

        
        Assert.assertEquals(100, g.getNextSequenceValue("s2", 100, 1,3));
        validateId("s2",103);
        Assert.assertEquals(101, g.getNextSequenceValue("s2", 100, 1,3));
        validateId("s2",103);
        Assert.assertEquals(102, g.getNextSequenceValue("s2", 100, 1,3));
        validateId("s2",103);
        Assert.assertEquals(103, g.getNextSequenceValue("s2", 100, 1,3));
        validateId("s2",106);
        Assert.assertEquals(104, g.getNextSequenceValue("s2", 100, 1,3));
        validateId("s2",106);
        Assert.assertEquals(105, g.getNextSequenceValue("s2", 100, 1,3));
        validateId("s2",106);
        Assert.assertEquals(106, g.getNextSequenceValue("s2", 100, 1,3));
        validateId("s2",109);
                                                        
        Assert.assertEquals(-1000, g.getNextSequenceValue("s3", -1000, 10,3));
        validateId("s3",-970);
        Assert.assertEquals(-990, g.getNextSequenceValue("s3", -1000, 10,3));
        validateId("s3",-970);
        Assert.assertEquals(-980, g.getNextSequenceValue("s3", -1000, 10,3));
        validateId("s3",-970);
        Assert.assertEquals(-970, g.getNextSequenceValue("s3", -1000, 10,3));
        validateId("s3",-940);
    }

    @Test
    public void multipleGeneratorsTest() throws Exception {
        // Simulate two different machines by swapping sequenceInfo
        Map<String,MongoSequenceGenerator.SequenceInfo> s1=new HashMap<>();
        Map<String,MongoSequenceGenerator.SequenceInfo> s2=new HashMap<>();
        MongoSequenceGenerator g = new MongoSequenceGenerator(coll);

        g.sequenceInfo=s1;
        Assert.assertEquals(1, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",6);

        g.sequenceInfo=s2;
        Assert.assertEquals(6, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",11);

        g.sequenceInfo=s1;
        Assert.assertEquals(2, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",11);
        
        g.sequenceInfo=s2;
        Assert.assertEquals(7, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",11);

        g.sequenceInfo=s1;
        Assert.assertEquals(3, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",11);
        
        g.sequenceInfo=s2;
        Assert.assertEquals(8, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",11);

        g.sequenceInfo=s1;
        Assert.assertEquals(4, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",11);
        
        g.sequenceInfo=s2;
        Assert.assertEquals(9, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",11);

        g.sequenceInfo=s1;
        Assert.assertEquals(5, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",11);
        
        g.sequenceInfo=s2;
        Assert.assertEquals(10, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",11);
        
        g.sequenceInfo=s1;
        Assert.assertEquals(11, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",16);

        g.sequenceInfo=s2;
        Assert.assertEquals(16, g.getNextSequenceValue("s1", 1, 1,5));
        validateId("s1",21);
        
    }

    private void validateId(String seq,long expected) throws Exception {
        DBObject obj=coll.findOne(new BasicDBObject("name",seq));
        Long l=(Long)obj.get("value");
        Assert.assertEquals(expected, l.longValue());
    }
}
