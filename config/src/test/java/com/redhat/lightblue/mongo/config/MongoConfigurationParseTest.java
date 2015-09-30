package com.redhat.lightblue.mongo.config;

import java.io.IOException;

import static org.junit.Assert.*;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.redhat.lightblue.util.JsonUtils;

public class MongoConfigurationParseTest {

    @Test
    public void testReadPreference() throws IOException {

        JsonNode node = JsonUtils.json(Thread.currentThread().getContextClassLoader().getResourceAsStream("datasources.json"));

        MongoConfiguration metadataConfig = new MongoConfiguration();
        metadataConfig.initializeFromJson(node.get("metadata"));

        MongoConfiguration dataConfig = new MongoConfiguration();
        dataConfig.initializeFromJson(node.get("mongodata"));

        assertEquals(ReadPreference.nearest(), metadataConfig.getMongoClientOptions().getReadPreference());
        assertEquals(ReadPreference.secondary(), dataConfig.getMongoClientOptions().getReadPreference());
        assertEquals(WriteConcern.SAFE, metadataConfig.getWriteConcern());
    }

}
