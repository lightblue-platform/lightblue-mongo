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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DB;

import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.PredefinedFields;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.TypeResolver;
import com.redhat.lightblue.metadata.Metadata;
import com.redhat.lightblue.metadata.MetadataStatus;
import com.redhat.lightblue.metadata.VersionInfo;
import com.redhat.lightblue.metadata.EntityInfo;
import com.redhat.lightblue.metadata.MetadataRole;

import com.redhat.lightblue.crud.Factory;
import com.redhat.lightblue.crud.FindRequest;
import com.redhat.lightblue.crud.CRUDFindResponse;
import com.redhat.lightblue.crud.CRUDFindRequest;
import com.redhat.lightblue.crud.MetadataResolver;

import com.redhat.lightblue.mediator.Mediator;

import com.redhat.lightblue.assoc.QueryPlan;

import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.Sort;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.query.ValueComparisonExpression;

import com.redhat.lightblue.util.test.AbstractJsonSchemaTest;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.JsonUtils;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.Error;

import com.redhat.lightblue.Response;
import com.redhat.lightblue.Request;
import com.redhat.lightblue.EntityVersion;
import com.redhat.lightblue.mongo.common.DBResolver;
import com.redhat.lightblue.mongo.common.MongoDataStore;
import com.redhat.lightblue.mongo.config.MongoConfiguration;

public class AssocTest extends AbstractMongoCrudTest {

    private static final JsonNodeFactory nodeFactory = JsonNodeFactory.withExactBigDecimals(false);

    public class MDResolver implements MetadataResolver {
        public EntityMetadata getEntityMetadata(String entityName) {
            return getMd(entityName+".json");
        }
    }

    public Mediator getMediator() {
        factory.addCRUDController("mongo", new MongoCRUDController(null,new DBResolver() {
                public DB get(MongoDataStore store) {
                    return db;
                }
                public MongoConfiguration getConfiguration(MongoDataStore store) {
                    return null;
                }
 
                @Override
                public Collection<MongoConfiguration> getConfigurations() {
                    return null;
                }                
            }));
        return new Mediator(new DatabaseMetadata(), factory);
    }

    public class DatabaseMetadata implements Metadata {
        
        @Override
        public EntityMetadata getEntityMetadata(String entityName, String version) {
            return getMd(entityName+".json");
    }

        @Override
        public String[] getEntityNames(MetadataStatus... statuses) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        @Override
        public VersionInfo[] getEntityVersions(String entityName) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        @Override
        public void createNewMetadata(EntityMetadata md) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        @Override
        public void setMetadataStatus(String entityName, String version, MetadataStatus newStatus, String comment) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        @Override
        public Response getDependencies(String entityName, String version) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        @Override
        public void updateEntityInfo(EntityInfo ei) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        @Override
        public void createNewSchema(EntityMetadata md) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        @Override
        public EntityInfo getEntityInfo(String entityName) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        @Override
        public Response getAccess(String entityName, String version) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
        @Override
        public void removeEntity(String entityName) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        
        @Override
        public Map<MetadataRole, List<String>> getMappedRoles() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }


    private void init_arr() throws Exception {
        DocTranslator translator=new DocTranslator(new MDResolver(),JsonNodeFactory.instance);
        ArrayNode node = (ArrayNode)loadJsonNode("arr_parent_data.json");
        for(int i=0;i<node.size();i++) {
            DBObject obj=translator.toBson(new JsonDoc(node.get(i))).doc;
            db.getCollection("arr_parent").insert(obj);
        }
        node = (ArrayNode)loadJsonNode("arr_child_data.json");
        for(int i=0;i<node.size();i++) {
            DBObject obj=translator.toBson(new JsonDoc(node.get(i))).doc;
            db.getCollection("arr_child").insert(obj);
        }
    }

    private void cleanup_arr() {
        db.getCollection("arr_parent").remove(new BasicDBObject());
        db.getCollection("arr_child").remove(new BasicDBObject());
    }

    @Test
    public void array_in_reference_fullarr() throws Exception {
        init_arr();
        FindRequest fr=new FindRequest();
        fr.setQuery(query("{'field':'_id','op':'=','rvalue':'1'}"));
        fr.setProjection(projection("[{'field':'*','recursive':1},{'field':'ref'}]"));
        fr.setEntityVersion(new EntityVersion("arr_parent","1.0.0"));
        Response response=getMediator().find(fr);
        System.out.println(response.getEntityData());
        Assert.assertEquals(1, response.getEntityData().size());
        Assert.assertEquals(3,response.getEntityData().get(0).get("ref").size());
        cleanup_arr();
    }

    @Test
    public void array_in_reference_emptyarr() throws Exception {
        init_arr();
        FindRequest fr=new FindRequest();
        fr.setQuery(query("{'field':'_id','op':'=','rvalue':'2'}"));
        fr.setProjection(projection("[{'field':'*','recursive':1},{'field':'ref'}]"));
        fr.setEntityVersion(new EntityVersion("arr_parent","1.0.0"));
        Response response=getMediator().find(fr);
        System.out.println(response.getEntityData());
        Assert.assertEquals(1, response.getEntityData().size());
        Assert.assertNull(response.getEntityData().get(0).get("ref"));
        cleanup_arr();
    }

    @Test
    public void array_in_reference_nullarr() throws Exception {
        init_arr();
        FindRequest fr=new FindRequest();
        fr.setQuery(query("{'field':'_id','op':'=','rvalue':'3'}"));
        fr.setProjection(projection("[{'field':'*','recursive':1},{'field':'ref'}]"));
        fr.setEntityVersion(new EntityVersion("arr_parent","1.0.0"));
        Response response=getMediator().find(fr);
        System.out.println(response.getEntityData());
        Assert.assertEquals(1, response.getEntityData().size());
        Assert.assertNull(response.getEntityData().get(0).get("ref"));
        cleanup_arr();
    }

}
