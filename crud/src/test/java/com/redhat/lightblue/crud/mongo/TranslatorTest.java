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
package com.redhat.lightblue.crud.mongo;

import java.util.Set;
import com.fasterxml.jackson.databind.node.NullNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.JsonDoc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import org.bson.types.ObjectId;

/**
 *
 * @author nmalik
 */
public class TranslatorTest extends AbstractMongoCrudTest {
    private Translator translator;
    private EntityMetadata md;

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
        Assert.assertTrue(doc.get(new Path("field6")) instanceof NullNode);
    }
    @Test
    public void translateNullBsonArray() throws Exception {
        BasicDBObject obj=new BasicDBObject("field7",null).append("objectType","test");
        JsonDoc doc=translator.toJson(obj);
        Assert.assertTrue(doc.get(new Path("field7")) instanceof NullNode);
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
        Assert.assertTrue(idObj instanceof String);
    }

    @Test
    public void createIdFrom_double() {
        Object idObj = Translator.createIdFrom(12.34);
        Assert.assertTrue(idObj instanceof String);
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
}
