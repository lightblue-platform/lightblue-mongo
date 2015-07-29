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
package com.redhat.lightblue.crud.mongo;

import org.junit.Test;
import org.junit.Assert;

public class MongoLockingTest extends AbstractMongoCrudTest {

    @Test
    public void acquireExclusionTest() throws Exception {
        // Make sure only one caller can lock
        MongoLocking locking=new MongoLocking(coll);
        Assert.assertTrue(locking.acquire("1","rsc1",null));
        Assert.assertFalse(locking.acquire("2","rsc1",null));
        Assert.assertTrue(locking.release("1","rsc1"));
        Assert.assertTrue(locking.acquire("2","rsc1",null));
        Assert.assertTrue(locking.release("2","rsc1"));
    }

    @Test
    public void acquireLockCountingTest() throws Exception {
        MongoLocking locking=new MongoLocking(coll);
        Assert.assertTrue(locking.acquire("1","rsc1",null));
        Assert.assertFalse(locking.acquire("2","rsc1",null));
        Assert.assertEquals(1,locking.getLockCount("1","rsc1"));
        Assert.assertTrue(locking.acquire("1","rsc1",null));
        Assert.assertFalse(locking.acquire("2","rsc1",null));
        Assert.assertEquals(2,locking.getLockCount("1","rsc1"));
        Assert.assertFalse(locking.release("1","rsc1"));
        Assert.assertFalse(locking.acquire("2","rsc1",null));
        Assert.assertEquals(1,locking.getLockCount("1","rsc1"));
        Assert.assertTrue(locking.release("1","rsc1"));
        Assert.assertTrue(locking.acquire("2","rsc1",null));
        try {
            locking.getLockCount("1","rsc1");
            Assert.fail();
        } catch (Exception e) {}
        locking.release("2","rsc1");
    }

    @Test
    public void expireTest() throws Exception {
        MongoLocking locking=new MongoLocking(coll);
        Assert.assertTrue(locking.acquire("1","rsc1",100l));
        Thread.sleep(110);
        try {
            locking.ping("1","rsc1");
            Assert.fail();
        } catch (Exception e) {}
        Assert.assertTrue(locking.acquire("2","rsc1",null));
        Assert.assertTrue(locking.release("2","rsc1"));
    }

    @Test
    public void pingTest() throws Exception {
        MongoLocking locking=new MongoLocking(coll);
        Assert.assertTrue(locking.acquire("1","rsc1",100l));
        locking.ping("1","rsc1");
        Thread.sleep(50);
        locking.ping("1","rsc1");
        Thread.sleep(50);
        locking.ping("1","rsc1");
        Thread.sleep(50);
        locking.ping("1","rsc1");
    }
           
}
