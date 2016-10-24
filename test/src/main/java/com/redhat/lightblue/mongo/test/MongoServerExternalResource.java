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

import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.UnknownHostException;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.mongodb.MongoClient;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

/**
 * <p>
 * JUnit {@link ExternalResource} that will handle standing/shutting down an
 * In-Memory Mongo instance for testing purposes.</p>
 * <p>
 * Example Usage:<br>
 * Create an instance of MongoServerExternalResource with a
 * {@link org.junit.Rule} or {@link org.junit.ClassRule} annotation.
 * <p>
 * <code>
 *      {@literal @}Rule<br>
 * public MongoServerExternalResource mongoServer = new
 * MongoServerExternalResource();
 * </code></p>
 * Then set the <code>{@literal @}InMemoryMongoServer</code> annotation on
 * either the Class or Method level. This annotation allows properties of the
 * Mongo instance to be configured, but is required even if only using the
 * default values.
 * </p>
 * <p>
 * On occasion for debugging purposes, it is useful to connect to the running
 * mongo instance. This can be achieved by placing a breakpoint in the unit test
 * and then running <code>mongo --host localhost --port 27777</code> from your
 * console.<p>
 *
 * @author dcrissman
 */
public class MongoServerExternalResource extends ExternalResource {

    public static final int DEFAULT_PORT = 27777;
    public static final Version DEFAULT_VERSION = Version.V3_1_6;

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.TYPE})
    @Inherited
    @Documented
    public @interface InMemoryMongoServer {
        /**
         * Port to run the Mongo instance on.
         */
        int port() default DEFAULT_PORT;

        /**
         * Version of Mongo to use.
         */
        Version version() default Version.V3_1_6;
    }

    private InMemoryMongoServer immsAnnotation = null;
    private MongodExecutable mongodExe;
    private MongodProcess mongod;
    private MongoClient client;

    public MongoServerExternalResource() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                super.run();
                after();
            }
        });
    }

    @Override
    public Statement apply(Statement base, Description description) {
        immsAnnotation = description.getAnnotation(InMemoryMongoServer.class);
        if ((immsAnnotation == null) && description.isTest()) {
            Class<?> clazz = description.getTestClass();
            if (clazz != null) {
                immsAnnotation = clazz.getAnnotation(InMemoryMongoServer.class);
            }
        }

        return super.apply(base, description);
    }

    @Override
    protected void before() throws IOException {
        MongodStarter runtime = MongodStarter.getDefaultInstance();
        IMongodConfig config = new MongodConfigBuilder().
                version(getMongoVersion()).
                net(new Net(getPort(), Network.localhostIsIPv6())).
                build();
        mongodExe = runtime.prepare(config);

        try {
            mongod = mongodExe.start();
        } catch (IOException e) {
            //Mongo failed to start for the previously stated reason. A single retry will be attempted.
            mongod = mongodExe.start();
        }
    }

    @Override
    protected void after() {
        if (mongodExe != null) {
            mongodExe.stop();
        }

        client = null;
        mongod = null;
        mongodExe = null;
    }

    /**
     * Provides a {@link MongoClient} for the running in-memory Mongo instance.
     *
     * @return {@link MongoClient}
     * @throws UnknownHostException
     */
    public MongoClient getConnection() throws UnknownHostException {
        if (client == null) {
            client = new MongoClient("localhost", getPort());
        }
        return client;
    }

    /**
     * @return the port the mongo instance is running on.
     */
    public int getPort() {
        return (immsAnnotation == null) ? DEFAULT_PORT : immsAnnotation.port();
    }

    /**
     * @return the mongo version being run.
     */
    public Version getMongoVersion() {
        return (immsAnnotation == null) ? DEFAULT_VERSION : immsAnnotation.version();
    }

}
