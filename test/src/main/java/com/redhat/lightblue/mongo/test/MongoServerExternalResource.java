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

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class MongoServerExternalResource extends ExternalResource{

    public static final int DEFAULT_PORT = 27777;

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.TYPE})
    @Inherited
    @Documented
    public @interface InMemoryMongoServer {
        Version version() default Version.V2_6_1;
        int port() default DEFAULT_PORT;
    }

    private InMemoryMongoServer immsAnnotation = null;
    private MongodExecutable mongodExe;
    private MongodProcess mongod;

    @Override
    public Statement apply(Statement base, Description description){
        immsAnnotation = description.getAnnotation(InMemoryMongoServer.class);
        if((immsAnnotation == null) && description.isTest()){
            immsAnnotation = description.getTestClass().getAnnotation(InMemoryMongoServer.class);
        }

        if(immsAnnotation == null){
            throw new IllegalStateException("@InMemoryMongoServer must be set on suite or test level.");
        }

        return super.apply(base, description);
    }

    @Override
    protected void before() throws UnknownHostException, IOException{
        MongodStarter runtime = MongodStarter.getDefaultInstance();
        IMongodConfig config = new MongodConfigBuilder().
                version(immsAnnotation.version()).
                net(new Net(immsAnnotation.port(), Network.localhostIsIPv6())).
                build();
        mongodExe = runtime.prepare(config);
        mongod = mongodExe.start();
    }

    @Override
    protected void after(){
        if (mongod != null) {
            mongod.stop();
            mongodExe.stop();
        }
    }

}
