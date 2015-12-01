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
package com.redhat.lightblue.mongo.metadata;

import com.binarytweed.test.Quarantine;
import com.binarytweed.test.QuarantiningRunner;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoCredential;
import com.redhat.lightblue.crud.*;
import com.redhat.lightblue.metadata.*;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import com.redhat.lightblue.mongo.metadata.MongoDataStoreParser;
import com.redhat.lightblue.mongo.metadata.MongoMetadata;
import com.redhat.lightblue.mongo.test.EmbeddedMongo;
import com.redhat.lightblue.util.Error;
import org.bson.BSONObject;
import org.junit.*;
import org.junit.runner.RunWith;

@RunWith(QuarantiningRunner.class)
//Classloader isolation due EmbeddedMongo is a singleton instance
@Quarantine({"com.redhat.lightblue.mongo.test.EmbeddedMongo"})
public class MongoMetadataFailureTest {

    @Test
    // Ignored due https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo/issues/114, resume this test build after it have been solved and we update our dependency to de.flapdoodle
    @Ignore
    public void mongoAuthFailureTest() throws Exception {
        EmbeddedMongo.PORT = 2888;
        EmbeddedMongo.MONGO_CREDENTIALS.add(MongoCredential.createMongoCRCredential("usera", EmbeddedMongo.DATABASE_NAME, "passworda".toCharArray()));
        EmbeddedMongo mongo = EmbeddedMongo.getInstance();
        MongoMetadata md;
        Factory factory = new Factory();
        factory.addCRUDController("mongo", new MongoMetadataTest.TestCRUDController());
        Extensions<BSONObject> x = new Extensions<>();
        x.addDefaultExtensions();
        x.registerDataStoreParser("mongo", new MongoDataStoreParser<BSONObject>());
        md = new MongoMetadata(mongo.getDB(), x, new DefaultTypes(), factory,null);
        BasicDBObject index = new BasicDBObject("name", 1);
        index.put("version.value", 1);
        mongo.getDB().getCollection(MongoMetadata.DEFAULT_METADATA_COLLECTION).ensureIndex(index, "name", true);

        boolean passed = false;
        try {
            EntityMetadata g = md.getEntityMetadata("testEntity", null);
        } catch (Error e) {
            passed = true;
            Assert.assertEquals(MetadataConstants.ERR_AUTH_FAILED, e.getErrorCode());
        }
        Assert.assertTrue("Lightblue/mongodb didn't throw Error and that was expected to happen.", passed);
    }
}
