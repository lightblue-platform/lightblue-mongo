package com.redhat.lightblue.mongo.test;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.Defaults;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.RuntimeConfig;
import de.flapdoodle.embed.process.extract.DirectoryAndExecutableNaming;
import de.flapdoodle.embed.process.extract.UserTempNaming;
import de.flapdoodle.embed.process.io.StreamProcessor;
import de.flapdoodle.embed.process.io.directories.PlatformTempDir;
import de.flapdoodle.embed.process.runtime.Network;
import de.flapdoodle.embed.process.store.Downloader;
import de.flapdoodle.embed.process.store.ExtractedArtifactStore;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test class that encapsulates the in memory mongo DB used for unit tests.
 *
 * Created by nmalik on 12/16/14.
 *
 * Should be using {@link AbstractMongoCRUDTestController} and/or
 * {@link MongoServerExternalResource}.
 */
@Deprecated
public final class EmbeddedMongo {

    /**
     * This field is used only once during the bootstrap of the singleton
     * instance of this class
     */
    public static final List<MongoCredential> MONGO_CREDENTIALS = new CopyOnWriteArrayList<>();

    /**
     * This field is used only once during the bootstrap of the singleton
     * instance of this class
     */
    public static final String HOSTNAME = "localhost";

    /**
     * This field is used only once during the bootstrap of the singleton
     * instance of this class
     */
    public static int PORT = 27777;

    /**
     * This field is used only once during the bootstrap of the singleton
     * instance of this class
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

    protected static class FileStreamProcessor implements StreamProcessor {
        private final FileOutputStream outputStream;

        public FileStreamProcessor(File file) throws FileNotFoundException {
            outputStream = new FileOutputStream(file);
        }

        @Override
        public void process(String block) {
            try {
                outputStream.write(block.getBytes(StandardCharsets.UTF_8));
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
            RuntimeConfig runtimeConfig = Defaults.runtimeConfigFor(Command.MongoD)
                .artifactStore(ExtractedArtifactStore.builder()
                    .extraction(DirectoryAndExecutableNaming.builder()
                        .executableNaming(new UserTempNaming())
                        .directory(new PlatformTempDir()).build())
                    .downloader(Downloader.platformDefault())
                    .temp(DirectoryAndExecutableNaming.builder()
                        .executableNaming(new UserTempNaming())
                        .directory(new PlatformTempDir()).build())
                    .downloadConfig(Defaults.downloadConfigFor(Command.MongoD)
                        .fileNaming(new UserTempNaming()).build())
                    .build())
                .build();

            MongodStarter runtime = MongodStarter.getInstance(runtimeConfig);
            mongodExe = runtime.prepare(
                    MongodConfig.builder()
                    .version(Version.V5_0_2)
                    .net(new Net(mongoPort, Network.localhostIsIPv6()))
                    .build()
            );
            try {
                mongod = mongodExe.start();
            } catch (Throwable t) {
                // try again, could be killed breakpoint in IDE
                mongod = mongodExe.start();
            }

            if (MONGO_CREDENTIALS.isEmpty()) {
                client = new MongoClient(mongoHostname + ":" + mongoPort);
            } else {
                List<ServerAddress> serverAddresses = new ArrayList<>();
                serverAddresses.add(new ServerAddress(mongoHostname + ":" + mongoPort));
                client = new MongoClient(serverAddresses, MONGO_CREDENTIALS.get(0), new MongoClientOptions.Builder().build());
                client.getDB("admin").command("{ user: \"siteUserAdmin\", pwd: \"password\", roles: [ { role: \"userAdminAnyDatabase\", db: \"admin\" } , { role: \"userAdminAnyDatabase\", db: \"" + dbName + "\" } ] }");
            }

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    super.start();
                    if (mongodExe != null) {
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
     * Drops the database. Use between test executions.
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
