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
package com.redhat.lightblue.mongo.crud.js;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
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
import com.redhat.lightblue.query.*;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.JsonUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.bson.types.ObjectId;

import com.redhat.lightblue.mongo.crud.AbstractMongoCrudTest;

public class JSQueryTranslatorTest extends AbstractMongoCrudTest {
    private JSQueryTranslator translator;
    private EntityMetadata md;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        md = getMd("./testMetadata.json");
        translator = new JSQueryTranslator(md);
    }

    @Test
    public void testValueComparison() throws Exception {
        cmpjs("function() { var r0=false; {r0=this.field1==\"test\";} return r0;}",
              translator.translateQuery(query("{'field':'field1','op':'=','rvalue':'test'}")).toString());
    }

    @Test
    public void testAnyComparison() throws Exception {
        cmpjs("function() {var r0=[1,2,3];var r1=false;varr 3=false;"+
              "{for(var i2=0;i2<r0.length;i2++){"+
              "r3=false;"+
              "for(var j4=0;j4<this.field6.nf5.length;j4++){"+
              "if(this.field6.nf5[j4]==r0[i2]){r3=true;break;}}"+
              "if(r3){r1=true;break;}}}return r1;}",
              translator.translateQuery(query("{'array':'field6.nf5','contains':'$any','values':[1,2,3]}")).toString());
    }
    @Test
    public void testNoneComparison() throws Exception {
        cmpjs("function() {var r0=[1,2,3];var r1=true;varr 3=false;"+
              "{for(var i2=0;i2<r0.length;i2++){"+
              "r3=false;"+
              "for(var j4=0;j4<this.field6.nf5.length;j4++){"+
              "if(this.field6.nf5[j4]==r0[i2]){r3=true;break;}}"+
              "if(r3){r1=false;break;}}}return r1;}",
              translator.translateQuery(query("{'array':'field6.nf5','contains':'$none','values':[1,2,3]}")).toString());
    }

    @Test
    public void testIn() throws Exception {
        cmpjs("function() {var r0=[\"test1\",\"test2\",\"test3\"];var r1=false;var r3;{for(var i2=0;i2<r0.length;i2++){if(this.field1==r0[i2]) {r1=true;break;} }}return r1;}",
              translator.translateQuery(query("{'field':'field1','op':'$in','values':['test1','test2','test3']}")).toString());
    }

    @Test
    public void testInField() throws Exception {
        cmpjs("function(){var r0=false;{for(var i1=0;i1<this.field6.nf5.length;i1++){if(this.field6.nf5[i1]==this.field1){r0=true;break;}}}return r0;}",
              translator.translateQuery(query("{'field':'field1','op':'$in','rfield':'field6.nf5'}")).toString());
    }

    @Test
    public void testRegex() throws Exception {
        cmpjs("function() { var r0=new RegExp(\"test.*\",\"i\");var r1=false;{r1=r0.test(this.field1);}return r1;}",
              translator.translateQuery(query("{'field':'field1','regex':'test.*','caseInsensitive':true}")).toString());
    }

    @Test
    public void testNotValueComparison() throws Exception {
        cmpjs("function() { var r0=false; var r1=false; {{r1=this.field1==\"test\";} r0=!r1;} return r0;}",
              translator.translateQuery(query("{'$not': {'field':'field1','op':'=','rvalue':'test'}}")).toString());
    }

    @Test
    public void testAndValueComparison() throws Exception {
        cmpjs("function() { var r0=false;var r1=false; var r2=false;{{r1=this.field1==\"test\";} {r2=this.field2==\"test2\";} r0=r1&&r2;} return r0;}",
              translator.translateQuery(query("{'$and':[ {'field':'field1','op':'=','rvalue':'test'}, {'field':'field2','op':'=','rvalue':'test2'}]}")).toString());
    }

    @Test
    public void testArrayMatchValueComparison() throws Exception {
        cmpjs("function() { var r0=false; var r2=false; {for(var i1=0;i1<this.field7.length;i1++) {{r2=this.field7[i1].elemf1==\"test\";}if(r2){r0=true;break; }}} return r0;}",
              translator.translateQuery(query("{'array':'field7','elemMatch':{'field':'elemf1','op':'=','rvalue':'test'}}")).toString());
    }

    @Test
    public void testArrayMatchValueComparisonWithThis() throws Exception {
        cmpjs("function() { var r0=false; var r2=false; {for(var i1=0;i1<this.field6.nf5.length;i1++) {{r2=this.field6.nf5[i1]==1;}if(r2){r0=true;break;}}} return r0;}",
              translator.translateQuery(query("{'array':'field6.nf5','elemMatch':{'field':'$this','op':'=','rvalue':1}}")).toString());
    }
    

    private void cmpjs(String expected,String got) {
        String e=expected.replaceAll(" ","").replaceAll("\n","");
        String g=got.replaceAll(" ","").replaceAll("\n","");
        Assert.assertEquals("\n e:"+e+"\n g:"+g,e,g);
                                      
    }
}
