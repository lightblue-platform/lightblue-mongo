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
package com.redhat.lightblue.mongo.metadata;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.mongodb.BasicDBObject;
import com.redhat.lightblue.metadata.EntityInfo;
import com.redhat.lightblue.metadata.EntitySchema;
import com.redhat.lightblue.metadata.MetadataConstants;
import com.redhat.lightblue.metadata.TypeResolver;
import com.redhat.lightblue.metadata.parser.Extensions;
import com.redhat.lightblue.metadata.parser.MetadataParser;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.Sort;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonUtils;

@SuppressWarnings({"rawtypes", "unchecked"})
public class BSONParser extends MetadataParser<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BSONParser.class);

    public static final String DELIMITER_ID = "|";

    public BSONParser(Extensions<Object> ex,
                      TypeResolver resolver) {
        super(ex, resolver);
    }

    @Override
    public Object newMap() {
        return new BasicDBObject();
    }

    @Override
    public Object getMapProperty(Object map, String name) {
        return ((BSONObject) map).get(name);
    }

    @Override
    public Set<String> getMapPropertyNames(Object map) {
        return ((BSONObject) map).keySet();
    }

    @Override
    public void setMapProperty(Object map, String name, Object value) {
        ((BSONObject) map).put(name, value);
    }

    @Override
    public Object newList() {
        return new ArrayList<Object>();
    }

    @Override
    public int getListSize(Object list) {
        return ((List) list).size();
    }

    @Override
    public Object getListElement(Object list, int n) {
        return ((List) list).get(n);
    }

    @Override
    public void addListElement(Object list, Object element) {
        ((List) list).add(element);
    }

    @Override
    public Object asValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof BSONObject
                || value instanceof List) {
            throw Error.get(MetadataConstants.ERR_ILL_FORMED_METADATA, value.toString());
        } else {
            return value;
        }
    }

    @Override
    public Object asRepresentation(Object value) {
        return value;
    }

    @Override
    public MetadataParser.PropertyType getType(Object object) {
        if (object instanceof List) {
            return MetadataParser.PropertyType.LIST;
        } else if (object instanceof BSONObject) {
            return MetadataParser.PropertyType.MAP;
        } else if (object == null) {
            return MetadataParser.PropertyType.NULL;
        } else {
            return MetadataParser.PropertyType.VALUE;
        }
    }

    /**
     * Override to set _id appropriately.
     */
    @Override
    public BSONObject convert(EntityInfo info) {
        Error.push("convert[info|bson]");
        try {
            BSONObject doc = (BSONObject) super.convert(info);

            // entityInfo._id = {entityInfo.name}|
            putValue(doc, "_id", getStringProperty(doc, "name") + DELIMITER_ID);

            return doc;
        } catch (Error e) {
            // rethrow lightblue error
            throw e;
        } catch (Exception e) {
            // throw new Error (preserves current error context)
            LOGGER.error(e.getMessage(), e);
            throw Error.get(MetadataConstants.ERR_ILL_FORMED_METADATA, e.getMessage());
        } finally {
            Error.pop();
        }
    }

    /**
     * Override to set _id appropriately.
     */
    @Override
    public BSONObject convert(EntitySchema schema) {
        Error.push("convert[info|bson]");
        try {
            BSONObject doc = (BSONObject) super.convert(schema);
            putValue(doc, "_id", getStringProperty(doc, "name") + DELIMITER_ID + getRequiredStringProperty(getRequiredObjectProperty(doc, "version"), "value"));

            return doc;
        } catch (Error e) {
            // rethrow lightblue error
            throw e;
        } catch (Exception e) {
            // throw new Error (preserves current error context)
            LOGGER.error(e.getMessage(), e);
            throw Error.get(MetadataConstants.ERR_ILL_FORMED_METADATA, e.getMessage());
        } finally {
            Error.pop();
        }
    }

    @Override
    public Projection getProjection(Object object, String name) {
        String x = (String) ((BSONObject) object).get(name);
        return x == null ? null : Projection.fromJson(toJson(x));
    }

    @Override
    public QueryExpression getQuery(Object object, String name) {
        String x = (String) ((BSONObject) object).get(name);
        return x == null ? null : QueryExpression.fromJson(toJson(x));
    }

    @Override
    public Sort getSort(Object object, String name) {
        String x = (String) ((BSONObject) object).get(name);
        return x == null ? null : Sort.fromJson(toJson(x));
    }

    @Override
    public void putProjection(Object object, String name, Projection p) {
        if (p != null) {
            ((BSONObject) object).put(name, p.toJson().toString());
        }
    }

    @Override
    public void putQuery(Object object, String name, QueryExpression q) {
        if (q != null) {
            ((BSONObject) object).put(name, q.toJson().toString());
        }
    }

    @Override
    public void putSort(Object object, String name, Sort s) {
        if (s != null) {
            ((BSONObject) object).put(name, s.toJson().toString());
        }
    }

    private static JsonNode toJson(String object) {
        try {
            return JsonUtils.json(object);
        } catch (Exception e) {
            throw Error.get(MetadataConstants.ERR_ILL_FORMED_METADATA, object);
        }
    }
}
