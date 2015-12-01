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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.redhat.lightblue.crud.Factory;
import com.redhat.lightblue.crud.validator.DefaultFieldConstraintValidators;
import com.redhat.lightblue.crud.validator.EmptyEntityConstraintValidators;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.PredefinedFields;
import com.redhat.lightblue.metadata.TypeResolver;
import com.redhat.lightblue.mongo.metadata.MongoDataStoreParser;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import com.redhat.lightblue.mongo.test.MongoServerExternalResource;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.Sort;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.util.JsonUtils;
import com.redhat.lightblue.util.test.AbstractJsonSchemaTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;

/**
 * @author nmalik
 */
@MongoServerExternalResource.InMemoryMongoServer(port=27777)
public abstract class AbstractMongoCrudTest extends AbstractJsonSchemaTest {
    protected static final JsonNodeFactory nodeFactory = JsonNodeFactory.withExactBigDecimals(true);

    @ClassRule
    public static final MongoServerExternalResource mongo = new MongoServerExternalResource();

    protected static final String COLL_NAME = "data";
    private static final String DATABADSE_NAME = "mongo";
    protected static Factory factory;
    protected DBCollection coll;
    protected static DB db;

    @BeforeClass
    public static void setupClass() throws Exception {
        factory = new Factory();
        factory.addFieldConstraintValidators(new DefaultFieldConstraintValidators());
        factory.addEntityConstraintValidators(new EmptyEntityConstraintValidators());

        db = mongo.getConnection().getDB(DATABADSE_NAME);
    }

    @Before
    public void setup() throws Exception {
        coll = db.getCollection(COLL_NAME);
    }

    @After
    public void teardown() throws Exception {
        if(coll!=null)
            coll.remove(new BasicDBObject());
    }

    protected Projection projection(String s) throws Exception {
        return Projection.fromJson(json(s));
    }

    protected QueryExpression query(String s) throws Exception {
        return QueryExpression.fromJson(json(s));
    }

    protected UpdateExpression update(String s) throws Exception {
        return UpdateExpression.fromJson(json(s));
    }

    protected Sort sort(String s) throws Exception {
        return Sort.fromJson(json(s));
    }

    protected JsonNode json(String s) throws Exception {
        return JsonUtils.json(s.replace('\'', '\"'));
    }

    public EntityMetadata getMd(String fname) throws IOException, ProcessingException {
        //runValidJsonTest("json-schema/metadata/metadata.json", fname);
        JsonNode node = loadJsonNode(fname);
        Extensions<JsonNode> extensions = new Extensions<>();
        extensions.addDefaultExtensions();
        extensions.registerDataStoreParser("mongo", new MongoDataStoreParser<JsonNode>());
        TypeResolver resolver = new DefaultTypes();
        JSONMetadataParser parser = new JSONMetadataParser(extensions, resolver, nodeFactory);
        EntityMetadata md = parser.parseEntityMetadata(node);
        PredefinedFields.ensurePredefinedFields(md);
        return md;
    }
}
