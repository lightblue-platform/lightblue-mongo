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

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DBObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import com.redhat.lightblue.crud.MetadataResolver;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.PredefinedFields;
import com.redhat.lightblue.metadata.TypeResolver;
import com.redhat.lightblue.mongo.metadata.MongoDataStoreParser;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.types.DefaultTypes;

import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.test.AbstractJsonSchemaTest;

public class BsonMergeTest extends AbstractJsonSchemaTest {

    private EntityMetadata md;
    private EntityMetadata md2;
    private EntityMetadata md3;
    private EntityMetadata md6;
    private BsonMerge merge;
    private BsonMerge merge2;
    private BsonMerge merge3;
    private BsonMerge merge6;
    private static final JsonNodeFactory nodeFactory = JsonNodeFactory.withExactBigDecimals(true);

    private class Resolver implements MetadataResolver {
        EntityMetadata md;

        public Resolver(EntityMetadata md) {
            this.md = md;
        }

        @Override
        public EntityMetadata getEntityMetadata(String entityName) {
            return md;
        }
    }

    public EntityMetadata getMd(String fname) throws Exception {
        JsonNode node = loadJsonNode(fname);
        Extensions<JsonNode> extensions = new Extensions<>();
        extensions.addDefaultExtensions();
        extensions.registerDataStoreParser("mongo", new MongoDataStoreParser<JsonNode>());
        TypeResolver resolver = new DefaultTypes();
        JSONMetadataParser parser = new JSONMetadataParser(extensions, resolver, nodeFactory);
        EntityMetadata emd = parser.parseEntityMetadata(node);
        PredefinedFields.ensurePredefinedFields(emd);
        return emd;
    }

    @Before
    public void init() throws Exception {
        md = getMd("./testMetadata.json");
        md2 = getMd("./testMetadata2.json");
        md3 = getMd("./testMetadata3.json");
        md6 = getMd("./testMetadata6.json");
        merge = new BsonMerge(md);
        merge2 = new BsonMerge(md2);
        merge3 = new BsonMerge(md3);
        merge6 = new BsonMerge(md6);
    }

    @Test
    public void merge_simple() throws Exception {
        Translator translator = new Translator(new Resolver(md), nodeFactory);
        DBObject oldDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata1.json")));
        DBObject newDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata1.json")));
        oldDoc.put("inv1", "val1");
        ((DBObject) oldDoc.get("field6")).put("inv2", "val2");
        merge.merge(oldDoc, newDoc);
        Assert.assertEquals(oldDoc.get("inv1"), newDoc.get("inv1"));
        Assert.assertEquals(((DBObject) oldDoc.get("field6")).get("inv2"), ((DBObject) newDoc.get("field6")).get("inv2"));
    }

    @Test
    public void merge_elemremove() throws Exception {
        Translator translator = new Translator(new Resolver(md), nodeFactory);
        DBObject oldDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata1.json")));
        DBObject newDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata1.json")));
        oldDoc.put("inv1", "val1");
        ((DBObject) oldDoc.get("field6")).put("inv2", "val2");
        newDoc.removeField("field6");
        merge.merge(oldDoc, newDoc);
        Assert.assertEquals(oldDoc.get("inv1"), newDoc.get("inv1"));
        Assert.assertNull(newDoc.get("field6"));
    }

    @Test
    public void merge_arr_mod() throws Exception {
        Translator translator = new Translator(new Resolver(md), nodeFactory);
        DBObject oldDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata1.json")));
        DBObject newDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata1.json")));
        ((List<DBObject>) oldDoc.get("field7")).get(0).put("inv1", "val1");
        merge.merge(oldDoc, newDoc);
        Assert.assertEquals("val1", ((List<DBObject>) newDoc.get("field7")).get(0).get("inv1"));
    }

    @Test
    public void merge_arr_mod_id() throws Exception {
        Translator translator = new Translator(new Resolver(md2), nodeFactory);
        DBObject oldDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata2.json")));
        DBObject newDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata2.json")));
        ((List<DBObject>) oldDoc.get("field7")).get(0).put("inv1", "val1");
        merge2.merge(oldDoc, newDoc);
        Assert.assertEquals("val1", ((List<DBObject>) newDoc.get("field7")).get(0).get("inv1"));
    }

    @Test
    public void merge_nested_objarr() throws Exception {
        Translator translator = new Translator(new Resolver(md6), nodeFactory);
        DBObject oldDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata6.json")));
        DBObject newDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata6.json")));
        ((List<DBObject>) oldDoc.get("field7")).get(0).put("inv1", "val1");
        merge6.merge(oldDoc, newDoc);
        Assert.assertEquals("val1", ((List<DBObject>) newDoc.get("field7")).get(0).get("inv1"));
    }

    @Test
    public void merge_duplicate_arrayid() throws Exception {
        Translator translator = new Translator(new Resolver(md6), nodeFactory);
        DBObject oldDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata6.json")));
        DBObject newDoc = translator.toBson(new JsonDoc(loadJsonNode("./testdata6.json")));
        // Create a duplicate ID
        ((List<DBObject>) oldDoc.get("field7")).get(1).put("id", "1");
        merge6.merge(oldDoc, newDoc);
        Assert.assertEquals("1",  ((List<DBObject>) oldDoc.get("field7")).get(0).get("id"));
        Assert.assertEquals("2",  ((List<DBObject>) newDoc.get("field7")).get(1).get("id"));
    }
}
