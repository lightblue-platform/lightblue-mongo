package com.redhat.lightblue.mongo.config;

import java.io.IOException;

import static org.junit.Assert.*;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.redhat.lightblue.util.JsonUtils;
import java.io.InputStream;

public class MongoConfigurationParseTest {

    @Test
    public void readPreference() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("parse-test-datasources.json")) {
            JsonNode node = JsonUtils.json(is);

            MongoConfiguration metadataConfig = new MongoConfiguration();
            metadataConfig.initializeFromJson(node.get("metadata_readPreference"));

            MongoConfiguration dataConfig = new MongoConfiguration();
            dataConfig.initializeFromJson(node.get("mongodata_readPreference"));

            assertEquals(ReadPreference.nearest(), metadataConfig.getMongoClientOptions().getReadPreference());
            assertEquals(ReadPreference.secondary(), dataConfig.getMongoClientOptions().getReadPreference());
            assertEquals(WriteConcern.SAFE, metadataConfig.getWriteConcern());
        }
    }

    @Test
    public void maxQueryTimeMS() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("parse-test-datasources.json")) {
            JsonNode node = JsonUtils.json(is);

            MongoConfiguration dataConfig = new MongoConfiguration();
            dataConfig.initializeFromJson(node.get("mongodata_maxQueryTimeMS"));

            assertEquals(98765, dataConfig.getMaxQueryTimeMS());
        }
    }
    
    @Test
    public void writeConcern_deprecated() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("parse-test-datasources.json")) {
            JsonNode node = JsonUtils.json(is);

            MongoConfiguration dataConfig = new MongoConfiguration();
            dataConfig.initializeFromJson(node.get("mongodata_writeConcern_deprecated"));

            assertEquals(WriteConcern.SAFE, dataConfig.getWriteConcern());
        }
    }

    @Test
    public void maxResultSetSize_deprecated() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("parse-test-datasources.json")) {
            JsonNode node = JsonUtils.json(is);

            MongoConfiguration dataConfig = new MongoConfiguration();
            dataConfig.initializeFromJson(node.get("mongodata_maxResultSetSize_deprecated"));

            assertEquals(12345, dataConfig.getMaxResultSetSize());
        }
    }
    
    @Test
    public void writeConcern_deprecated_override() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("parse-test-datasources.json")) {
            JsonNode node = JsonUtils.json(is);

            MongoConfiguration dataConfig = new MongoConfiguration();
            dataConfig.initializeFromJson(node.get("mongodata_writeConcern_deprecated_override"));

            assertEquals(WriteConcern.W1, dataConfig.getWriteConcern());
        }
    }

    @Test
    public void maxResultSetSize_deprecated_override() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("parse-test-datasources.json")) {
            JsonNode node = JsonUtils.json(is);

            MongoConfiguration dataConfig = new MongoConfiguration();
            dataConfig.initializeFromJson(node.get("mongodata_maxResultSetSize_deprecated_override"));

            assertEquals(54321, dataConfig.getMaxResultSetSize());
        }
    }
    
    @Test
    public void writeConcern() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("parse-test-datasources.json")) {
            JsonNode node = JsonUtils.json(is);

            MongoConfiguration dataConfig = new MongoConfiguration();
            dataConfig.initializeFromJson(node.get("mongodata_writeConcern"));

            assertEquals(WriteConcern.SAFE, dataConfig.getWriteConcern());
        }
    }

    @Test
    public void maxResultSetSize() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("parse-test-datasources.json")) {
            JsonNode node = JsonUtils.json(is);

            MongoConfiguration dataConfig = new MongoConfiguration();
            dataConfig.initializeFromJson(node.get("mongodata_maxResultSetSize"));

            assertEquals(12345, dataConfig.getMaxResultSetSize());
        }
    }
}
