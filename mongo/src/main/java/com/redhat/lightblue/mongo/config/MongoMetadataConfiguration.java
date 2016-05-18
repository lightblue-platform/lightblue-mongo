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
package com.redhat.lightblue.mongo.config;

import org.bson.BSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.redhat.lightblue.config.AbstractMetadataConfiguration;
import com.redhat.lightblue.config.DataSourceConfiguration;
import com.redhat.lightblue.config.DataSourcesConfiguration;
import com.redhat.lightblue.config.LightblueFactory;
import com.redhat.lightblue.metadata.Metadata;
import com.redhat.lightblue.mongo.metadata.MongoDataStoreParser;
import com.redhat.lightblue.mongo.metadata.MongoMetadata;
import com.redhat.lightblue.mongo.metadata.MetadataCache;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.JSONMetadataParser;
import com.redhat.lightblue.metadata.types.DefaultTypes;
import com.redhat.lightblue.mongo.common.DBResolver;
import com.redhat.lightblue.mongo.common.MongoDataStore;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class MongoMetadataConfiguration extends AbstractMetadataConfiguration {

    private String datasource;
    private String collection;
    private Long cachePeekInterval;
    private Long cacheTTL;

    private static final MetadataCache metadataCache = new MetadataCache();

    @Override
    public Metadata createMetadata(DataSourcesConfiguration datasources,
                                   JSONMetadataParser jsonParser,
                                   LightblueFactory factory) {
        DataSourceConfiguration cfg = datasources.getDataSourceConfiguration(datasource);
        if (cfg != null) {
            DBResolver dbresolver = new MongoDBResolver(datasources);
            Extensions<Object> parserExtensions = new Extensions<>();
            parserExtensions.addDefaultExtensions();
            parserExtensions.registerDataStoreParser(MongoDataStoreParser.NAME, new MongoDataStoreParser<Object>());

            // register any of the common configuration bits from abstract parent
            registerWithExtensions(parserExtensions);

            try {
                factory.
                        getJSONParser().
                        getExtensions().
                        mergeWith(parserExtensions);

            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }

            DefaultTypes typeResolver = new DefaultTypes();
            MongoDataStore mdstore = new MongoDataStore();
            mdstore.setDatasourceName(datasource);

            metadataCache.setCacheParams(cachePeekInterval, cacheTTL);

            try {
                MongoMetadata mongoMetadata = null;
                if (collection == null) {
                    mongoMetadata = new MongoMetadata(dbresolver.get(mdstore), parserExtensions, typeResolver, factory.getFactory(), metadataCache);
                } else {
                    mongoMetadata = new MongoMetadata(dbresolver.get(mdstore), collection, parserExtensions, typeResolver, factory.getFactory(), metadataCache);
                }

                mongoMetadata.setRoleMap(getMappedRoles());

                return mongoMetadata;
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        } else {
            throw new IllegalArgumentException(datasource);
        }
    }

    /**
     * @return the datasource name
     */
    public String getDataSource() {
        return datasource;
    }

    /**
     * @param name the datasource name to set
     */
    public void setDataSource(String name) {
        this.datasource = name;
    }

    /**
     * @return the collection
     */
    public String getCollection() {
        return collection;
    }

    /**
     * @param collection the collection to set
     */
    public void setCollection(String collection) {
        this.collection = collection;
    }

    @Override
    public String toString() {
        return "dataSource:" + datasource + " collection:" + collection;
    }

    @Override
    public void initializeFromJson(JsonNode node) {
        // init from super (gets hook configuration parsers and anything else that's common)
        super.initializeFromJson(node);

        if (node != null) {
            JsonNode x = node.get("dataSource");
            if (x != null) {
                datasource = x.asText();
            }
            x = node.get("collection");
            if (x != null) {
                collection = x.asText();
            }
            x = node.get("cachePeekIntervalMsec");
            if (x != null) {
                cachePeekInterval = x.asLong();
            }
            x = node.get("cacheTTLMsec");
            if (x != null) {
                cacheTTL = x.asLong();
            }
        }
    }
}
