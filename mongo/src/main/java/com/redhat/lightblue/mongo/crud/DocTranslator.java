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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Date;
import java.util.stream.Stream;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.redhat.lightblue.ResultMetadata;
import com.redhat.lightblue.crud.MetadataResolver;
import com.redhat.lightblue.metadata.MetadataObject;
import com.redhat.lightblue.metadata.ArrayElement;
import com.redhat.lightblue.metadata.ArrayField;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.FieldCursor;
import com.redhat.lightblue.metadata.FieldTreeNode;
import com.redhat.lightblue.metadata.Index;
import com.redhat.lightblue.metadata.IndexSortKey;
import com.redhat.lightblue.metadata.ObjectArrayElement;
import com.redhat.lightblue.metadata.ObjectField;
import com.redhat.lightblue.metadata.ReferenceField;
import com.redhat.lightblue.metadata.ResolvedReferenceField;
import com.redhat.lightblue.metadata.SimpleArrayElement;
import com.redhat.lightblue.metadata.SimpleField;
import com.redhat.lightblue.metadata.Type;
import com.redhat.lightblue.metadata.types.*;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.JsonNodeCursor;
import com.redhat.lightblue.util.MutablePath;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.Util;

/**
 * Translations between BSON and JSON. This class is thread-safe, and can be
 * shared between threads
 */
public class DocTranslator {

    public static final String OBJECT_TYPE_STR = "objectType";
    public static final Path OBJECT_TYPE = new Path(OBJECT_TYPE_STR);

    public static final Path ID_PATH = new Path("_id");
    public static final Path HIDDEN_SUB_PATH = new Path("@mongoHidden");

    public static final String ERR_NO_OBJECT_TYPE = "mongo-translation:no-object-type";
    public static final String ERR_INVALID_OBJECTTYPE = "mongo-translation:invalid-object-type";
    public static final String ERR_INVALID_FIELD = "mongo-translation:invalid-field";
    public static final String ERR_INVALID_COMPARISON = "mongo-translation:invalid-comparison";
    public static final String ERR_CANNOT_TRANSLATE_REFERENCE = "mongo-translation:cannot-translate-reference";

    private static final Logger LOGGER = LoggerFactory.getLogger(DocTranslator.class);

    private final MetadataResolver mdResolver;
    private final JsonNodeFactory factory;

    public static class TranslatedDoc {
        public final JsonDoc doc;
        public final ResultMetadata rmd;

        public TranslatedDoc(JsonDoc doc,ResultMetadata md) {
            this.doc=doc;
            this.rmd=md;
        }
    }

    /**
     * Constructs a translator using the given metadata resolver and factory
     */
    public DocTranslator(MetadataResolver mdResolver,
                         JsonNodeFactory factory) {
        this.mdResolver = mdResolver;
        this.factory = factory;
    }

    /**
     * Translates a list of JSON documents to DBObjects. Translation is metadata
     * driven.
     */
    public DBObject[] toBson(List<? extends JsonDoc> docs) {
        DBObject[] ret = new DBObject[docs.size()];
        int i = 0;
        for (JsonDoc doc : docs) {
            ret[i++] = toBson(doc);
        }
        return ret;
    }

    /**
     * Translates a JSON document to DBObject. Translation is metadata driven.
     */
    public DBObject toBson(JsonDoc doc) {
        LOGGER.debug("toBson() enter");
        JsonNode node = doc.get(OBJECT_TYPE);
        if (node == null) {
            throw Error.get(ERR_NO_OBJECT_TYPE);
        }
        EntityMetadata md = mdResolver.getEntityMetadata(node.asText());
        if (md == null) {
            throw Error.get(ERR_INVALID_OBJECTTYPE, node.asText());
        }
        DBObject ret = toBson(doc, md);
        LOGGER.debug("toBson() return");
        return ret;
    }

    /**
     * Traslates a DBObject document to Json document
     */
    public TranslatedDoc toJson(DBObject object) {
        LOGGER.debug("toJson() enter");
        Object type = object.get(OBJECT_TYPE_STR);
        if (type == null) {
            throw Error.get(ERR_NO_OBJECT_TYPE);
        }
        EntityMetadata md = mdResolver.getEntityMetadata(type.toString());
        if (md == null) {
            throw Error.get(ERR_INVALID_OBJECTTYPE, type.toString());
        }
        TranslatedDoc doc = toJson(object, md);
        LOGGER.debug("toJson() return");
        return doc;
    }

    /**
     * Translates DBObjects into Json documents
     */
    public List<TranslatedDoc> toJson(List<DBObject> objects) {
        List<TranslatedDoc> list = new ArrayList<>(objects.size());
        for (DBObject object : objects) {
            list.add(toJson(object));
        }
        return list;
    }

    public static Object getDBObject(DBObject start, Path p) {
        int n = p.numSegments();
        Object trc = start;
        for (int seg = 0; seg < n; seg++) {
            String segment = p.head(seg);
            if (segment.equals(Path.ANY)) {
                throw Error.get(MongoCrudConstants.ERR_TRANSLATION_ERROR, p.toString());
            } else if (trc != null && Util.isNumber(segment)) {
                trc = ((List) trc).get(Integer.valueOf(segment));
            } else if (trc != null) {
                trc = ((DBObject) trc).get(segment);
            }
            if (trc == null && seg + 1 < n) {
                //At least one element in the Path is optional and does not exist in the document. Just return null.
                LOGGER.debug("Error retrieving path {} with {} segments from {}", p, p.numSegments(), start);
                return null;
            }
        }
        return trc;
    }

    /**
     * Get a reference to the path's hidden sub-field.
     *
     * This does not guarantee the sub-path exists.
     *
     * @param path
     * @return
     */
    public static Path getHiddenForField(Path path) {
        if (path.getLast().equals(Path.ANY)) {
            return path.prefix(-2).mutableCopy().push(HIDDEN_SUB_PATH).push(path.suffix(2));
        }
        return path.prefix(-1).mutableCopy().push(HIDDEN_SUB_PATH).push(path.getLast());
    }

    /**
     * Get a reference to the hidden path's actual field.
     *
     * This does not guarantee the sub-path exists.
     *
     * @param path
     * @return
     */
    public static Path getFieldForHidden(Path hiddenPath) {
        return hiddenPath.prefix(-2).mutableCopy().push(hiddenPath.getLast());
    }


    public static void populateCaseInsensitiveField(Object doc, Path field) {
        if (doc == null) {
            return;
        } else if (field.numSegments() == 1) {
            DBObject docObj = (DBObject) doc;
            if (docObj.get(field.head(0)) == null) {
                // no value, so nothing to populate
                DBObject dbo = (DBObject) docObj.get(HIDDEN_SUB_PATH.toString());
                if(dbo != null && dbo.get(field.head(0)) != null) {
                    dbo.removeField(field.head(0));
                }
                return;
            } else if (docObj.get(field.head(0)) instanceof List) {
                // primitive list - add hidden field to doc and populate list
                List<String> objList = (List<String>) docObj.get(field.head(0));
                BasicDBList hiddenList = new BasicDBList();
                objList.forEach(s -> hiddenList.add(s.toUpperCase()));
                DBObject dbo = (DBObject) docObj.get(HIDDEN_SUB_PATH.toString());
                if (dbo == null) {
                    docObj.put(HIDDEN_SUB_PATH.toString(), new BasicDBObject(field.head(0), hiddenList));
                } else {
                    dbo.put(field.head(0), hiddenList);
                }
            } else {
                // add hidden field to doc, populate field
                DBObject dbo = (DBObject) docObj.get(HIDDEN_SUB_PATH.toString());
                if (dbo == null) {
                    docObj.put(HIDDEN_SUB_PATH.toString(), new BasicDBObject(field.head(0), docObj.get(field.head(0)).toString().toUpperCase()));
                } else {
                    dbo.put(field.head(0), docObj.get(field.head(0)).toString().toUpperCase());
                }
            }
        } else if (field.head(0).equals(Path.ANY)) {
            // doc is a list
            List<?> docList = ((List<?>) doc);
            docList.forEach(key -> populateCaseInsensitiveField(key, field.suffix(-1)));
        } else {
            DBObject docObj = (DBObject) doc;
            populateCaseInsensitiveField(docObj.get(field.head(0)), field.suffix(-1));
        }
    }

    public static void populateDocHiddenFields(DBObject doc, EntityMetadata md){
        Stream<IndexSortKey> ciIndexes = getCaseInsensitiveIndexes(md.getEntityInfo().getIndexes().getIndexes());
        ciIndexes.forEach(i -> populateCaseInsensitiveField(doc, i.getField()));
    }

    public static void populateDocHiddenFields(DBObject doc, List<Path> fields) {
        fields.forEach(f -> populateCaseInsensitiveField(doc, f));
    }

    public static ResultMetadata getDocMetadata(DBObject obj) {
        ResultMetadata md=new ResultMetadata();
        List<ObjectId> list=MongoSafeUpdateProtocol.getVersionList(obj);
        if(list!=null&&!list.isEmpty())
            md.setDocumentVersion(getDocVer(obj,list.get(0)));
        return md;
    }


    private TranslatedDoc toJson(DBObject object, EntityMetadata md) {
        // Translation is metadata driven. We don't know how to
        // translate something that's not defined in metadata.
        FieldCursor cursor = md.getFieldCursor();
        if (cursor.firstChild()) {
            return new TranslatedDoc(new JsonDoc(objectToJson(object,object, md, cursor)),getDocMetadata(object));
        } else {
            return null;
        }
    }

    private void injectResultMetadata(DBObject root,ObjectNode parent,String fieldName) {
        ObjectNode node=factory.objectNode();
        injectDocumentVersion(root,node,"documentVersion");
        parent.set(fieldName,node);
    }
    
    private void injectDocumentVersion(DBObject root,ObjectNode parent,String fieldName) {
        List<ObjectId> list=MongoSafeUpdateProtocol.getVersionList(root);
        if(list!=null&&!list.isEmpty()) {
            parent.set(fieldName,factory.textNode(getDocVer(root,list.get(0))));
        }
    }

    public static String getDocVer(DBObject doc,ObjectId ver) {
        return String.format("%s:%s",doc.get("_id"),ver);
    }
    
    /**
     * Called after firstChild is called on cursor
     */
    private ObjectNode objectToJson(DBObject root, DBObject object, EntityMetadata md, FieldCursor mdCursor) {
        ObjectNode node = factory.objectNode();
        do {
            Path p = mdCursor.getCurrentPath();
            FieldTreeNode field = mdCursor.getCurrentNode();
            String fieldName = field.getName();
            LOGGER.debug("{}", p);
            boolean translate=true;
            Object x=((MetadataObject)field).getProperties().get(ResultMetadata.MD_PROPERTY_RESULT_METADATA);
            if(x!=null&&x instanceof Boolean&&((Boolean)x).booleanValue()) {
                injectResultMetadata(root,node,fieldName);
                translate=false;
            } else {
                x=((MetadataObject)field).getProperties().get(ResultMetadata.MD_PROPERTY_DOCVER);
                if(x!=null&&x instanceof Boolean&&((Boolean)x).booleanValue()) {
                    injectDocumentVersion(root,node,fieldName);
                    translate=false;
                }
            }
            if(translate) {
                // Retrieve field value
                Object value = object.get(fieldName);
                if (value != null) {
                    if (field instanceof SimpleField) {
                        convertSimpleFieldToJson(root, node, field, value, fieldName);
                    } else if (field instanceof ObjectField) {
                        convertObjectFieldToJson(root, node, fieldName, md, mdCursor, value, p);
                    } else if (field instanceof ResolvedReferenceField) {
                        // This should not happen
                    } else if (field instanceof ArrayField && value instanceof List && mdCursor.firstChild()) {
                        convertArrayFieldToJson(root, node, fieldName, md, mdCursor, value);
                    } else if (field instanceof ReferenceField) {
                        convertReferenceFieldToJson(root, value);
                    }
                }
            } // Don't add any null values to the document
        } while (mdCursor.nextSibling());
        return node;
    }

    private void convertSimpleFieldToJson(DBObject root,ObjectNode node, FieldTreeNode field, Object value, String fieldName) {
        JsonNode valueNode = field.getType().toJson(factory, value);
        if (valueNode != null && !(valueNode instanceof NullNode)) {
            node.set(fieldName, valueNode);
        }
    }

    private void convertObjectFieldToJson(DBObject root,ObjectNode node, String fieldName, EntityMetadata md, FieldCursor mdCursor, Object value, Path p) {
        if (value instanceof DBObject) {
            if (mdCursor.firstChild()) {
                JsonNode valueNode = objectToJson(root,(DBObject) value, md, mdCursor);
                if (valueNode != null && !(valueNode instanceof NullNode)) {
                    node.set(fieldName, valueNode);
                }
                mdCursor.parent();
            }
        } else {
            LOGGER.error("Expected DBObject, found {} for {}", value.getClass(), p);
        }
    }

    @SuppressWarnings("rawtypes")
    private void convertArrayFieldToJson(DBObject root,ObjectNode node, String fieldName, EntityMetadata md, FieldCursor mdCursor, Object value) {
        ArrayNode valueNode = factory.arrayNode();
        node.set(fieldName, valueNode);
        // We must have an array element here
        FieldTreeNode x = mdCursor.getCurrentNode();
        if (x instanceof ArrayElement) {
            for (Object item : (List) value) {
                valueNode.add(arrayElementToJson(root, item, (ArrayElement) x, md, mdCursor));
            }
        }
        mdCursor.parent();
    }

    private void convertReferenceFieldToJson(DBObject root,Object value) {
        //TODO
        LOGGER.debug("Converting reference field: ");
    }

    private JsonNode arrayElementToJson(DBObject root,
                                        Object value,
                                        ArrayElement el,
                                        EntityMetadata md,
                                        FieldCursor mdCursor) {
        JsonNode ret = null;
        if (el instanceof SimpleArrayElement) {
            if (value != null) {
                ret = el.getType().toJson(factory, value);
            }
        } else if (value != null) {
            if (value instanceof DBObject) {
                if (mdCursor.firstChild()) {
                    ret = objectToJson(root,(DBObject) value, md, mdCursor);
                    mdCursor.parent();
                }
            } else {
                LOGGER.error("Expected DBObject, got {}", value.getClass().getName());
            }
        }
        return ret;
    }

    private BasicDBObject toBson(JsonDoc doc, EntityMetadata md) {
        LOGGER.debug("Entity: {}", md.getName());
        BasicDBObject ret = null;
        JsonNodeCursor cursor = doc.cursor();
        if (cursor.firstChild()) {
            ret = objectToBson(cursor, md);
        }
        return ret;
    }

    private Object toValue(Type t, JsonNode node) {
        if (node == null || node instanceof NullNode) {
            return null;
        } else {
            return filterBigNumbers(t.fromJson(node));
        }
    }

    public static Object filterBigNumbers(Object value) {
        // Store big values as string. Mongo does not support big values
        if (value instanceof BigDecimal || value instanceof BigInteger) {
            return value.toString();
        } else {
            return value;
        }
    }

    private void toBson(BasicDBObject dest,
                        SimpleField fieldMd,
                        Path path,
                        JsonNode node, EntityMetadata md) {
        Object value = toValue(fieldMd.getType(), node);
        // Should we add fields with null values to the bson doc? Answer: no
        if (value != null) {
            if (path.equals(ID_PATH)) {
                value = createIdFrom(value);
            }
            dest.append(path.tail(0), value);
        }
    }

    /**
     * @param cursor The cursor, pointing to the first element of the object
     */
    private BasicDBObject objectToBson(JsonNodeCursor cursor, EntityMetadata md) {
        BasicDBObject ret = new BasicDBObject();
        do {
            Path path = cursor.getCurrentPath();
            JsonNode node = cursor.getCurrentNode();
            LOGGER.debug("field: {}", path);
            FieldTreeNode fieldMdNode = md.resolve(path);

            // Do not translate result metadata fields
            if(((MetadataObject)fieldMdNode).getProperties().get(ResultMetadata.MD_PROPERTY_RESULT_METADATA)==null&&
               ((MetadataObject)fieldMdNode).getProperties().get(ResultMetadata.MD_PROPERTY_DOCVER)==null) {
                if (fieldMdNode instanceof SimpleField) {
                    toBson(ret, (SimpleField) fieldMdNode, path, node, md);
                } else if (fieldMdNode instanceof ObjectField) {
                    convertObjectFieldToBson(node, cursor, ret, path, md);
                } else if (fieldMdNode instanceof ArrayField) {
                    convertArrayFieldToBson(node, cursor, ret, fieldMdNode, path, md);
                } else if (fieldMdNode instanceof ReferenceField) {
                    convertReferenceFieldToBson(node, path);
                }
            }
        } while (cursor.nextSibling());
        return ret;
    }

    private void convertObjectFieldToBson(JsonNode node, JsonNodeCursor cursor, BasicDBObject ret, Path path, EntityMetadata md) {
        if (node != null) {
            if (node instanceof ObjectNode) {
                if (cursor.firstChild()) {
                    ret.append(path.tail(0), objectToBson(cursor, md));
                    cursor.parent();
                }
            } else if (node instanceof NullNode) {
                ret.append(path.tail(0), null);
            } else {
                throw Error.get(ERR_INVALID_FIELD, path.toString());
            }
        }
    }

    private void convertArrayFieldToBson(JsonNode node, JsonNodeCursor cursor, BasicDBObject ret, FieldTreeNode fieldMdNode, Path path, EntityMetadata md) {
        if (node != null) {
            if (node instanceof ArrayNode) {
                if (cursor.firstChild()) {
                    ret.append(path.tail(0), arrayToBson(cursor, ((ArrayField) fieldMdNode).getElement(), md));
                    cursor.parent();
                } else {
                    // empty array! add an empty list.
                    ret.append(path.tail(0), new ArrayList());
                }
            } else if (node instanceof NullNode) {
                ret.append(path.tail(0), null);
            } else {
                throw Error.get(ERR_INVALID_FIELD, path.toString());
            }
        }
    }

    private void convertReferenceFieldToBson(JsonNode node, Path path) {
        if (node instanceof NullNode || node.size() == 0) {
            return;
        }
        //TODO
        throw Error.get(ERR_CANNOT_TRANSLATE_REFERENCE, path.toString());
    }

    /**
     * @param cursor The cursor, pointing to the first element of the array
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private List arrayToBson(JsonNodeCursor cursor, ArrayElement el, EntityMetadata md) {
        List l = new ArrayList();
        if (el instanceof SimpleArrayElement) {
            Type t = el.getType();
            do {
                Object value = toValue(t, cursor.getCurrentNode());
                l.add(value);
            } while (cursor.nextSibling());
        } else {
            do {
                JsonNode node = cursor.getCurrentNode();
                if (node == null || node instanceof NullNode) {
                    l.add(null);
                } else if (cursor.firstChild()) {
                    l.add(objectToBson(cursor, md));
                    cursor.parent();
                } else {
                    l.add(null);
                }
            } while (cursor.nextSibling());
        }
        return l;
    }

    /**
     * Creates appropriate identifier object given source data. If the source
     * can be converted to an ObjectId it is, else it is returned unmodified
     *
     * @param source input data
     * @return ObjectId if possible else String
     */
    public static Object createIdFrom(Object source) {
        if (source == null) {
            return null;
        } else if (ObjectId.isValid(source.toString())) {
            return new ObjectId(source.toString());
        } else {
            return source;
        }
    }

    public static int size(JsonNode node) {
        int size = 0;
        if (node instanceof ArrayNode) {
            for (Iterator<JsonNode> elements = ((ArrayNode) node).elements(); elements.hasNext();) {
                size += size(elements.next());
            }
        } else if (node instanceof ObjectNode) {
            for (Iterator<Map.Entry<String, JsonNode>> fields = ((ObjectNode) node).fields(); fields.hasNext();) {
                Map.Entry<String, JsonNode> field = fields.next();
                size += field.getKey().length();
                size += size(field.getValue());
            }
        } else if (node instanceof NumericNode) {
            size += 4;
        } else {
            size += node.asText().length();
        }
        return size;
    }

    public static int size(JsonDoc doc) {
        return size(doc.getRoot());
    }

    public static int jsonDocListSize(List<JsonDoc> list) {
        int size = 0;
        for (JsonDoc doc : list) {
            size += size(doc);
        }
        return size;
    }

    public static int size(TranslatedDoc doc) {
        return size(doc.doc.getRoot());
    }

    public static int translatedDocListSize(List<TranslatedDoc> list) {
        int size = 0;
        for (TranslatedDoc doc : list) {
            size += size(doc);
        }
        return size;
    }

    private static JsonNode rawValueToJson(Object value) {
        if(value instanceof Number) {
            if(value instanceof Float || value instanceof Double) {
                return DoubleType.TYPE.toJson(JsonNodeFactory.instance,value);
            } else {
                return IntegerType.TYPE.toJson(JsonNodeFactory.instance,value);
            }
        } else if(value instanceof Date) {
            return DateType.TYPE.toJson(JsonNodeFactory.instance,value);
        } else if(value instanceof Boolean) {
            return BooleanType.TYPE.toJson(JsonNodeFactory.instance,value);
        } else if(value==null) {
            return JsonNodeFactory.instance.nullNode();
        } else {
            return JsonNodeFactory.instance.textNode(value.toString());
        }
    }

    private static ArrayNode rawArrayToJson(List list) {
        ArrayNode node=JsonNodeFactory.instance.arrayNode();
        for(Object value:list) {
            if(value instanceof DBObject) {
                node.add(rawObjectToJson((DBObject)value));
            } else if(value instanceof List) {
                node.add(rawArrayToJson((List)value));
            } else {
                node.add(rawValueToJson(value));
            }
        }
        return node;
    }

    public static ObjectNode rawObjectToJson(DBObject obj) {
        ObjectNode ret=JsonNodeFactory.instance.objectNode();
        for(String key:obj.keySet()) {
            Object value=obj.get(key);
            if(value instanceof DBObject) {
                ret.set(key,rawObjectToJson( (DBObject)value ));
            } else if(value instanceof List) {
                ret.set(key,rawArrayToJson( (List)value ) );
            } else {
                ret.set(key,rawValueToJson(value));
            }
        }
        return ret;
    }

    public static Stream<IndexSortKey> getCaseInsensitiveIndexes(List<Index> indexes) {
        return indexes.stream()
                .map(Index::getFields)
                .flatMap(Collection::stream)
                .filter(IndexSortKey::isCaseInsensitive);
    }
}
