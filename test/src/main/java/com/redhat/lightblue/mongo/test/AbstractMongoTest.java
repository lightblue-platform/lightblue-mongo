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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;

import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.io.IStreamProcessor;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.runtime.Network;
import org.junit.After;

/**
 *
 * @author nmalik
 */
public abstract class AbstractMongoTest {
    protected static final JsonNodeFactory nodeFactory = JsonNodeFactory.withExactBigDecimals(true);

    // Copied from  https://github.com/tommysdk/showcase/blob/master/mongo-in-mem/src/test/java/tommysdk/showcase/mongo/TestInMemoryMongo.java
    protected static final String MONGO_HOST = "localhost";
    protected static final int MONGO_PORT = 27777;
    protected static final String IN_MEM_CONNECTION_URL = MONGO_HOST + ":" + MONGO_PORT;

    protected static final String DB_NAME = "test";
    protected static final String COLL_NAME = "data";

    protected static MongodExecutable mongodExe;
    protected static MongodProcess mongod;
    protected static Mongo mongo;
    protected static DB db;
    protected static DBCollection coll;
    protected static ReferenceQueue referenceQueue = new ReferenceQueue();

    static {
        try {
            IStreamProcessor mongodOutput = Processors.named("[mongod>]",
                    new FileStreamProcessor(File.createTempFile("mongod", "log")));
            IStreamProcessor mongodError = new FileStreamProcessor(File.createTempFile("mongod-error", "log"));
            IStreamProcessor commandsOutput = Processors.namedConsole("[console>]");

            IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                    .defaults(Command.MongoD)
                    .processOutput(new ProcessOutput(mongodOutput, mongodError, commandsOutput))
                    .build();

            MongodStarter runtime = MongodStarter.getInstance(runtimeConfig);
            MongodExecutable _mongoExe = runtime.prepare(
                    new MongodConfigBuilder()
                    .version(de.flapdoodle.embed.mongo.distribution.Version.V2_6_0)
                    .net(new Net(MONGO_PORT, Network.localhostIsIPv6()))
                    .build()
            );

            try {
                mongod = _mongoExe.start();
            } catch (IOException t) {
                // try again, could be killed breakpoint in IDE
                mongod = _mongoExe.start();
            }

            Mongo _mongo = new Mongo(IN_MEM_CONNECTION_URL);
            DB _db = _mongo.getDB(DB_NAME);

            coll = _db.createCollection(COLL_NAME, null);

            mongodExe = _mongoExe;
            mongo = _mongo;
            db = _db;

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // use run(), do not use start()
                    super.run();
                    clearDatabase();
                }

            });
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    public static void clearDatabase() {
        if (mongod != null) {
            mongod.stop();
            mongodExe.stop();
        }
        db = null;
        mongo = null;
        mongod = null;
        mongodExe = null;
    }

    @After
    public void teardown() throws Exception {
        if (mongod != null) {
            mongo.dropDatabase(DB_NAME);
        }
    }

    public static class FileStreamProcessor implements IStreamProcessor {
        private final FileOutputStream outputStream;

        public FileStreamProcessor(File file) throws FileNotFoundException {
            outputStream = new FileOutputStream(file);
        }

        @Override
        public void process(String block) {
            try {
                outputStream.write(block.getBytes());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void onProcessed() {
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
