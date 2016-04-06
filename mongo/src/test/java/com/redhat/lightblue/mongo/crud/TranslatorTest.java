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

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.Set;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.mongo.crud.CannotTranslateException;
import com.redhat.lightblue.mongo.crud.Translator;
import com.redhat.lightblue.metadata.CompositeMetadata;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.RegexMatchExpression;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.JsonUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.bson.types.ObjectId;

/**
 *
 * @author nmalik
 */
public class TranslatorTest extends AbstractMongoCrudTest {
    private Translator translator;
    private EntityMetadata md;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();

        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        // load metadata
        md = getMd("./testMetadata.json");
        // and add it to metadata resolver (the context)
        ctx.add(md);
        // create translator with the context
        translator = new Translator(ctx, nodeFactory);
    }

    @Test
    public void populateHiddenFields() throws IOException, ProcessingException {
        // use a different md for this test
        TestCRUDOperationContext ctx = new TestCRUDOperationContext(CRUDOperation.FIND);
        md = getMd("./testMetadata_index.json");
        ctx.add(md);
        translator = new Translator(ctx, nodeFactory);

        ObjectNode obj = JsonNodeFactory.instance.objectNode();
        obj.put(Translator.OBJECT_TYPE_STR, "test")
            .put("field1", "testField1")
            .put("field2", "testField2")
            .put("field3", "testField3");

        DBObject bson = translator.toBson(new JsonDoc(obj));
        Translator.populateDocHiddenFields(bson, md);

        DBObject hidden = (DBObject) bson.get(Translator.HIDDEN_SUB_PATH.toString());

        Assert.assertEquals("TESTFIELD1", hidden.get("field1"));
        Assert.assertNull("testField2", hidden.get("field2"));
        Assert.assertEquals("TESTFIELD3", hidden.get("field3"));
    }

    @Test
    public void getHiddenForField() {
        assertEquals("objField.@mongoHidden.strField", Translator.getHiddenForField(new Path("objField.strField")).toString());
        assertEquals("objField.@mongoHidden.strField2", Translator.getHiddenForField(new Path("objField.strField2")).toString());
        assertEquals("@mongoHidden.strField", Translator.getHiddenForField(new Path("strField")).toString());
    }

    @Test
    public void getFieldForHidden() {
        assertEquals("objField.strField", Translator.getFieldForHidden(new Path("objField.@mongoHidden.strField")).toString());
        assertEquals("objField.strField2", Translator.getFieldForHidden(new Path("objField.@mongoHidden.strField2")).toString());
        assertEquals("strField", Translator.getFieldForHidden(new Path("@mongoHidden.strField")).toString());
    }

    @Test
    public void translateHiddenIndexesQuery() throws IOException, ProcessingException {
        String query = "{'field':'field1','regex':'value','caseInsensitive':'true'}";
        BasicDBObject expected = new BasicDBObject("@mongoHidden.field1", new BasicDBObject("$regex", "VALUE"));

        QueryExpression queryExp = RegexMatchExpression.fromJson(JsonUtils.json(query.replace('\'', '\"')));
        EntityMetadata indexMd = getMd("./testMetadata_index.json");

        BasicDBObject trans = (BasicDBObject) translator.translate(indexMd, queryExp);
        assertEquals(expected, trans);
    }

    @Test
    public void translateNullArray() throws Exception {
        JsonDoc doc=new JsonDoc(json(loadResource("./testdata1.json")));
        doc.modify(new Path("field7"),nodeFactory.nullNode(),true);
        DBObject bdoc=translator.toBson(doc);
        Assert.assertNull(bdoc.get("field7"));
    }

    @Test
    public void translateNullObject() throws Exception {
        JsonDoc doc=new JsonDoc(json(loadResource("./testdata1.json")));
        doc.modify(new Path("field6"),nodeFactory.nullNode(),true);
        DBObject bdoc=translator.toBson(doc);
        Assert.assertNull(bdoc.get("field6"));
    }

    @Test
    public void translateNullBsonObject() throws Exception {
        BasicDBObject obj=new BasicDBObject("field6",null).append("objectType","test");
        JsonDoc doc=translator.toJson(obj);
        Assert.assertNull(doc.get(new Path("field6")));
    }
    @Test
    public void translateNullBsonArray() throws Exception {
        BasicDBObject obj=new BasicDBObject("field7",null).append("objectType","test");
        JsonDoc doc=translator.toJson(obj);
        Assert.assertNull(doc.get(new Path("field7")));
    }

    @Test
    public void translateUpdateSetField() throws Exception {
        String updateQueryJson = loadResource(getClass().getSimpleName() + "-update-set-field.json");
        UpdateExpression ue = update(updateQueryJson);
        DBObject mongoUpdateExpr = translator.translate(md, ue);

        Assert.assertNotNull(mongoUpdateExpr);
    }

    @Test
    public void translateUpdateAddField() throws Exception {
        String updateQueryJson = loadResource(getClass().getSimpleName() + "-update-add-field.json");
        UpdateExpression ue = update(updateQueryJson);
        DBObject mongoUpdateExpr = translator.translate(md, ue);

        Assert.assertNotNull(mongoUpdateExpr);
    }

    @Test
    public void translateUpdateUnsetField() throws Exception {
        String updateQueryJson = loadResource(getClass().getSimpleName() + "-update-unset-field.json");
        UpdateExpression ue = update(updateQueryJson);
        DBObject mongoUpdateExpr = translator.translate(md, ue);

        Assert.assertNotNull(mongoUpdateExpr);
    }

    @Test(expected = CannotTranslateException.class)
    public void translateUpdateUnsetNestedArrayElement() throws Exception {
        String updateQueryJson = loadResource(getClass().getSimpleName() + "-update-unset-nested-array-element.json");
        UpdateExpression ue = update(updateQueryJson);
        translator.translate(md, ue);
    }

    @Test
    public void translateUpdateUnsetNestedField() throws Exception {
        String updateQueryJson = loadResource(getClass().getSimpleName() + "-update-unset-nested-field.json");
        UpdateExpression ue = update(updateQueryJson);
        DBObject mongoUpdateExpr = translator.translate(md, ue);

        Assert.assertNotNull(mongoUpdateExpr);
    }
    /*
     array_update_expression := { $append : { path : rvalue_expression } } |
     { $append : { path : [ rvalue_expression, ... ] }} |
     { $insert : { path : rvalue_expression } } |
     { $insert : { path : [ rvalue_expression,...] }} |
     { $foreach : { path : update_query_expression,
     $update : foreach_update_expression } }
     */

    @Test(expected = CannotTranslateException.class)
    public void translateUpdateAppendValue() throws Exception {
        String updateQueryJson = loadResource(getClass().getSimpleName() + "-update-append-value.json");
        UpdateExpression ue = update(updateQueryJson);
        translator.translate(md, ue);
    }

    @Test(expected = CannotTranslateException.class)
    public void translateUpdateAppendValues() throws Exception {
        String updateQueryJson = loadResource(getClass().getSimpleName() + "-update-append-values.json");
        UpdateExpression ue = update(updateQueryJson);
        translator.translate(md, ue);
    }

    @Test(expected = CannotTranslateException.class)
    public void translateUpdateInsertValue() throws Exception {
        String updateQueryJson = loadResource(getClass().getSimpleName() + "-update-insert-value.json");
        UpdateExpression ue = update(updateQueryJson);
        translator.translate(md, ue);
    }

    @Test(expected = CannotTranslateException.class)
    public void translateUpdateInsertValues() throws Exception {
        String updateQueryJson = loadResource(getClass().getSimpleName() + "-update-insert-values.json");
        UpdateExpression ue = update(updateQueryJson);
        translator.translate(md, ue);
    }

    @Test(expected = CannotTranslateException.class)
    public void translateUpdateForeachSimple() throws Exception {
        String updateQueryJson = loadResource(getClass().getSimpleName() + "-update-foreach-simple.json");
        UpdateExpression ue = update(updateQueryJson);
        translator.translate(md, ue);
    }

    @Test
    public void translateUpdateListSetField() throws Exception {
        String updateQueryJson = loadResource(getClass().getSimpleName() + "-update-list-set-field.json");
        UpdateExpression ue = update(updateQueryJson);
        DBObject mongoUpdateExpr = translator.translate(md, ue);

        Assert.assertNotNull(mongoUpdateExpr);
    }

    @Test
    public void translateReference_fail() throws Exception {
        String docStr = loadResource(getClass().getSimpleName() + "-data-with-ref.json");
        JsonNode jdoc=JsonUtils.json(docStr);
        JsonDoc doc=new JsonDoc(jdoc);
        try {
            translator.toBson(doc);
            Assert.fail();
        } catch(Exception e) {}

    }

    @Test
    public void translateEmptyReference() throws Exception {
        String docStr = loadResource(getClass().getSimpleName() + "-data-without-ref.json");
        JsonNode jdoc=JsonUtils.json(docStr);
        JsonDoc doc=new JsonDoc(jdoc);
        DBObject obj=translator.toBson(doc);
        Assert.assertNull(obj.get("ref"));
    }

    @Test
    public void translateNullReference() throws Exception {
        String docStr = loadResource(getClass().getSimpleName() + "-data-with-null-ref.json");
        JsonNode jdoc=JsonUtils.json(docStr);
        JsonDoc doc=new JsonDoc(jdoc);
        DBObject obj=translator.toBson(doc);
        Assert.assertNull(obj.get("ref"));
    }

    @Test
    public void transalteJS() throws Exception {
        DBObject obj = translator.translate(md, query("{'field':'field7.*.elemf1','op':'=','rfield':'field7.*.elemf2'}"));
        Assert.assertEquals("function() {for(var r0=0;r0<this.field7.length;r0++) {"
                            + "for(var l0=0;l0<this.field7.length;l0++) {if(this.field7[l0].elemf1 == this.field7[r0].elemf2) { return true;}}}return false;}",
                            obj.get("$where").toString().trim());

        obj = translator.translate(md, query("{'field':'field7.0.elemf1','op':'=','rfield':'field7.*.elemf2'}"));
        Assert.assertEquals("function() {for(var i0=0;i0<this.field7.length;i0++) {"
                            + "if(this.field7[0].elemf1 == this.field7[i0].elemf2) { return true;}}return false;}",
                            obj.get("$where").toString().trim());
    }

    @Test
    public void translateNullCmp() throws Exception {
        DBObject obj=translator.translate(md,query("{'field':'field6','op':'=','rvalue':null}"));
        Assert.assertEquals("{ \"field6\" :  null }",obj.toString());
    }

    @Test
    public void createIdFrom_null() {
        Object idObj = Translator.createIdFrom(null);
        Assert.assertNull(idObj);
    }

    @Test
    public void createIdFrom_isValid() {
        Object idObj = Translator.createIdFrom("abcdefABCDEF012345678912");
        Assert.assertTrue(idObj instanceof ObjectId);
    }

    @Test
    public void createIdFrom_notValid() {
        Object idObj = Translator.createIdFrom("abcdefABCDEF01234567891|");
        Assert.assertTrue(idObj instanceof String);
    }

    @Test
    public void createIdFrom_integer() {
        Object idObj = Translator.createIdFrom(1234);
        Assert.assertTrue(idObj instanceof Integer);
    }

    @Test
    public void createIdFrom_double() {
        Object idObj = Translator.createIdFrom(12.34);
        Assert.assertTrue(idObj instanceof Double);
    }

    @Test
    public void projectionFields() throws Exception {
        Set<Path> fields=Translator.getRequiredFields(md,projection("{'field':'*','recursive':1}"),null,null);
        System.out.println(fields);
        Assert.assertTrue(fields.contains(new Path("objectType")));
        Assert.assertTrue(fields.contains(new Path("_id")));
        Assert.assertTrue(fields.contains(new Path("field1")));
        Assert.assertTrue(fields.contains(new Path("field2")));
        Assert.assertTrue(fields.contains(new Path("field3")));
        Assert.assertTrue(fields.contains(new Path("field4")));
        Assert.assertTrue(fields.contains(new Path("field5")));
        Assert.assertTrue(fields.contains(new Path("field6.nf1")));
        Assert.assertTrue(fields.contains(new Path("field6.nf2")));
        Assert.assertTrue(fields.contains(new Path("field6.nf3")));
        Assert.assertTrue(fields.contains(new Path("field6.nf4")));
        Assert.assertTrue(fields.contains(new Path("field6.nf5")));
        Assert.assertTrue(fields.contains(new Path("field6.nf6")));
        Assert.assertTrue(fields.contains(new Path("field6.nf7.nnf1")));
        Assert.assertTrue(fields.contains(new Path("field6.nf7.nnf2")));
        Assert.assertTrue(fields.contains(new Path("field7.*.elemf1")));
        Assert.assertTrue(fields.contains(new Path("field7.*.elemf2")));
        Assert.assertTrue(fields.contains(new Path("field7.*.elemf3")));
    }
    @Test
    public void projectionFieldsWithRef() throws Exception {
        md = getMd("./testMetadataRef.json");
        CompositeMetadata cmd=CompositeMetadata.buildCompositeMetadata(md,new CompositeMetadata.GetMetadata() {
                public EntityMetadata getMetadata(Path injectionField,
                                                  String entityName,
                                                  String version) {
                    return null;
                }
            });
        Set<Path> fields=Translator.getRequiredFields(cmd,projection("{'field':'*','recursive':1}"),null,null);
        System.out.println(fields);
        Assert.assertTrue(fields.contains(new Path("objectType")));
        Assert.assertTrue(fields.contains(new Path("_id")));
        Assert.assertTrue(fields.contains(new Path("field1")));
        Assert.assertFalse(fields.contains(new Path("field2")));
        Assert.assertFalse(fields.contains(new Path("field2.*")));
        Assert.assertTrue(fields.contains(new Path("field3")));
        Assert.assertTrue(fields.contains(new Path("field4")));
        Assert.assertTrue(fields.contains(new Path("field5")));
        Assert.assertTrue(fields.contains(new Path("field6.nf1")));
        Assert.assertTrue(fields.contains(new Path("field6.nf2")));
        Assert.assertTrue(fields.contains(new Path("field6.nf3")));
        Assert.assertTrue(fields.contains(new Path("field6.nf4")));
        Assert.assertTrue(fields.contains(new Path("field6.nf5")));
        Assert.assertTrue(fields.contains(new Path("field6.nf6")));
        Assert.assertTrue(fields.contains(new Path("field6.nf7.nnf1")));
        Assert.assertTrue(fields.contains(new Path("field6.nf7.nnf2")));
        Assert.assertFalse(fields.contains(new Path("field7.*.elemf1")));
        Assert.assertFalse(fields.contains(new Path("field7.*.elemf1.*")));
        Assert.assertTrue(fields.contains(new Path("field7.*.elemf2")));
        Assert.assertTrue(fields.contains(new Path("field7.*.elemf3")));
    }
}
