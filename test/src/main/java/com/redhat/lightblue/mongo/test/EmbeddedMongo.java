package com.redhat.lightblue.mongo.test;

import com.mongodb.*;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.io.IStreamProcessor;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.runtime.Network;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test class that encapsulates the in memory mongo DB used for unit tests.
 *
 * Created by nmalik on 12/16/14.
 * 
 * Should be using {@link AbstractMongoCRUDTestController} and/or {@link MongoServerExternalResource}.
 */
@Deprecated
public final class EmbeddedMongo {

    /**
     * This field is used only once during the bootstrap of the singleton instance of this class
     */
    public static final List<MongoCredential> MONGO_CREDENTIALS = new CopyOnWriteArrayList<>();

    /**
     * This field is used only once during the bootstrap of the singleton instance of this class
     */
    public static final String HOSTNAME = "localhost";

    /**
     * This field is used only once during the bootstrap of the singleton instance of this class
     */
    public static int PORT = 27777;

    /**
     * This field is used only once during the bootstrap of the singleton instance of this class
     */
    public static final String DATABASE_NAME = "mongo";




    private static EmbeddedMongo instance;

    private final String mongoHostname;
    private final int mongoPort;
    private final String dbName;

    private EmbeddedMongo(String mongoHostname, int mongoPort, String dbName) {
        this.mongoHostname = mongoHostname;
        this.mongoPort = mongoPort;
        this.dbName = dbName;
    }

    protected static class FileStreamProcessor implements IStreamProcessor {
        private final FileOutputStream outputStream;

        public FileStreamProcessor(File file) throws FileNotFoundException {
            outputStream = new FileOutputStream(file);
        }

        @Override
        public void process(String block) {
            try {
                outputStream.write(block.getBytes("UTF-8"));
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

    private MongodExecutable mongodExe;
    private MongodProcess mongod;
    private MongoClient client;
    private DB db;

    public static EmbeddedMongo getInstance() {
        if (instance == null) {
            bootstrap();
        }
        return instance;
    }

    private static synchronized void bootstrap() {
        if (instance == null) {
            EmbeddedMongo temp = new EmbeddedMongo(HOSTNAME, PORT, DATABASE_NAME);
            temp.initialize();
            instance = temp;
        }
    }

    private void initialize() {
        if (db != null) {
            return;
        }

        System.setProperty("mongodb.database", dbName);
        System.setProperty("mongodb.host", mongoHostname);
        System.setProperty("mongodb.port", String.valueOf(mongoPort));

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
            mongodExe = runtime.prepare(
                    new MongodConfigBuilder()
                            .version(de.flapdoodle.embed.mongo.distribution.Version.V2_6_0)
                            .net(new Net(mongoPort, Network.localhostIsIPv6()))
                            .build()
            );
            try {
                mongod = mongodExe.start();
            } catch (Throwable t) {
                // try again, could be killed breakpoint in IDE
                mongod = mongodExe.start();
            }

            if(MONGO_CREDENTIALS.isEmpty()) {
                client = new MongoClient(mongoHostname + ":" + mongoPort);
            } else {
                client = new MongoClient(new ServerAddress(mongoHostname + ":" + mongoPort), MONGO_CREDENTIALS);
                client.getDB("admin").command("{ user: \"siteUserAdmin\", pwd: \"password\", roles: [ { role: \"userAdminAnyDatabase\", db: \"admin\" } , { role: \"userAdminAnyDatabase\", db: \""+dbName+"\" } ] }");
            }

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    super.start();
                    if (mongod != null) {
                        mongod.stop();
                        mongodExe.stop();
                    }
                    db = null;
                    client = null;
                    mongod = null;
                    mongodExe = null;
                }

            });

            db = client.getDB(dbName);
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    /**
     * Drops the database.  Use between test executions.
     */
    public void reset() {
        if (client != null) {
            client.dropDatabase(dbName);
        }
    }

    public void dropDatabase(String name) {
        client.dropDatabase(name);
    }

    public DB getDB() {
        return db;
    }

    public void dropCollection(String name) {
        DBCollection coll = db.getCollection(name);
        if (coll != null) {
            coll.drop();
        }
    }
}
