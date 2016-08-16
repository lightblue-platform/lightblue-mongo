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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Date;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.redhat.lightblue.crud.MetadataResolver;
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
import com.redhat.lightblue.query.ArrayContainsExpression;
import com.redhat.lightblue.query.ArrayMatchExpression;
import com.redhat.lightblue.query.ArrayUpdateExpression;
import com.redhat.lightblue.query.BinaryComparisonOperator;
import com.redhat.lightblue.query.CompositeSortKey;
import com.redhat.lightblue.query.FieldAndRValue;
import com.redhat.lightblue.query.FieldComparisonExpression;
import com.redhat.lightblue.query.NaryFieldRelationalExpression;
import com.redhat.lightblue.query.NaryLogicalExpression;
import com.redhat.lightblue.query.NaryLogicalOperator;
import com.redhat.lightblue.query.NaryRelationalOperator;
import com.redhat.lightblue.query.NaryValueRelationalExpression;
import com.redhat.lightblue.query.PartialUpdateExpression;
import com.redhat.lightblue.query.PrimitiveUpdateExpression;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.RValueExpression;
import com.redhat.lightblue.query.RegexMatchExpression;
import com.redhat.lightblue.query.SetExpression;
import com.redhat.lightblue.query.Sort;
import com.redhat.lightblue.query.SortKey;
import com.redhat.lightblue.query.UnaryLogicalExpression;
import com.redhat.lightblue.query.UnaryLogicalOperator;
import com.redhat.lightblue.query.UnsetExpression;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.query.UpdateExpressionList;
import com.redhat.lightblue.query.Value;
import com.redhat.lightblue.query.ValueComparisonExpression;
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
public class Translator {

    public static final String OBJECT_TYPE_STR = "objectType";
    public static final Path OBJECT_TYPE = new Path(OBJECT_TYPE_STR);

    public static final Path ID_PATH = new Path("_id");
    public static final Path HIDDEN_SUB_PATH = new Path("@mongoHidden");

    public static final String ERR_NO_OBJECT_TYPE = "mongo-translation:no-object-type";
    public static final String ERR_INVALID_OBJECTTYPE = "mongo-translation:invalid-object-type";
    public static final String ERR_INVALID_FIELD = "mongo-translation:invalid-field";
    public static final String ERR_INVALID_COMPARISON = "mongo-translation:invalid-comparison";
    public static final String ERR_CANNOT_TRANSLATE_REFERENCE = "mongo-translation:cannot-translate-reference";

    private static final Logger LOGGER = LoggerFactory.getLogger(Translator.class);

    private final MetadataResolver mdResolver;
    private final JsonNodeFactory factory;

    private static final Map<BinaryComparisonOperator, String> BINARY_COMPARISON_OPERATOR_JS_MAP;
    private static final Map<BinaryComparisonOperator, String> BINARY_COMPARISON_OPERATOR_MAP;
    private static final Map<NaryLogicalOperator, String> NARY_LOGICAL_OPERATOR_MAP;
    private static final Map<UnaryLogicalOperator, String> UNARY_LOGICAL_OPERATOR_MAP;
    private static final Map<NaryRelationalOperator, String> NARY_RELATIONAL_OPERATOR_MAP;

    private static final String LITERAL_THIS_DOT = "this.";

    static {
        BINARY_COMPARISON_OPERATOR_JS_MAP = new HashMap<>();
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._eq, "==");
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._neq, "!=");
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._lt, "<");
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._gt, ">");
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._lte, "<=");
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._gte, ">=");

        BINARY_COMPARISON_OPERATOR_MAP = new HashMap<>();
        BINARY_COMPARISON_OPERATOR_MAP.put(BinaryComparisonOperator._eq, "$eq");
        BINARY_COMPARISON_OPERATOR_MAP.put(BinaryComparisonOperator._neq, "$ne");
        BINARY_COMPARISON_OPERATOR_MAP.put(BinaryComparisonOperator._lt, "$lt");
        BINARY_COMPARISON_OPERATOR_MAP.put(BinaryComparisonOperator._gt, "$gt");
        BINARY_COMPARISON_OPERATOR_MAP.put(BinaryComparisonOperator._lte, "$lte");
        BINARY_COMPARISON_OPERATOR_MAP.put(BinaryComparisonOperator._gte, "$gte");

        NARY_LOGICAL_OPERATOR_MAP = new HashMap<>();
        NARY_LOGICAL_OPERATOR_MAP.put(NaryLogicalOperator._and, "$and");
        NARY_LOGICAL_OPERATOR_MAP.put(NaryLogicalOperator._or, "$or");

        UNARY_LOGICAL_OPERATOR_MAP = new HashMap<>();
        UNARY_LOGICAL_OPERATOR_MAP.put(UnaryLogicalOperator._not, "$nor"); // Note: _not maps to $nor, not $not. $not applies to operator expression

        NARY_RELATIONAL_OPERATOR_MAP = new HashMap<>();
        NARY_RELATIONAL_OPERATOR_MAP.put(NaryRelationalOperator._in, "$in");
        NARY_RELATIONAL_OPERATOR_MAP.put(NaryRelationalOperator._not_in, "$nin");
    }

    /**
     * Constructs a translator using the given metadata resolver and factory
     */
    public Translator(MetadataResolver mdResolver,
                      JsonNodeFactory factory) {
        this.mdResolver = mdResolver;
        this.factory = factory;
    }

    /**
     * Translate a path to a mongo path
     *
     * Any * in the path is removed. Array indexes remain intact.
     */
    public static String translatePath(Path p) {
        StringBuilder str = new StringBuilder();
        int n = p.numSegments();
        for (int i = 0; i < n; i++) {
            String s = p.head(i);
            if (!s.equals(Path.ANY)) {
                if (i > 0) {
                    str.append('.');
                }
                str.append(s);
            }
        }
        return str.toString();
    }

    /**
     * Translate a path to a javascript path
     *
     * Path cannot have *. Indexes are put into brackets
     */
    public static String translateJsPath(Path p) {
        StringBuilder str = new StringBuilder();
        int n = p.numSegments();
        for (int i = 0; i < n; i++) {
            String s = p.head(i);
            if (s.equals(Path.ANY)) {
                throw Error.get(MongoCrudConstants.ERR_TRANSLATION_ERROR, p.toString());
            } else if (p.isIndex(i)) {
                str.append('[').append(s).append(']');
            } else {
                if (i > 0) {
                    str.append('.');
                }
                str.append(s);
            }
        }
        return str.toString();
    }

    /**
     * Appends objectType:X to the query
     */
    public static QueryExpression appendObjectType(QueryExpression q,String entityName) {
        QueryExpression ot=new ValueComparisonExpression(OBJECT_TYPE,BinaryComparisonOperator._eq,new Value(entityName));
        if(q==null) {
            return ot;
        } if(q instanceof NaryLogicalExpression &&
             ((NaryLogicalExpression)q).getOp()==NaryLogicalOperator._and) {
            List<QueryExpression> l=new ArrayList<>(((NaryLogicalExpression)q).getQueries());
            l.add(ot);
            return new NaryLogicalExpression(NaryLogicalOperator._and,l);
        } else {
            return new NaryLogicalExpression(NaryLogicalOperator._and,q,ot);
        }
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
    public JsonDoc toJson(DBObject object) {
        LOGGER.debug("toJson() enter");
        Object type = object.get(OBJECT_TYPE_STR);
        if (type == null) {
            throw Error.get(ERR_NO_OBJECT_TYPE);
        }
        EntityMetadata md = mdResolver.getEntityMetadata(type.toString());
        if (md == null) {
            throw Error.get(ERR_INVALID_OBJECTTYPE, type.toString());
        }
        JsonDoc doc = toJson(object, md);
        LOGGER.debug("toJson() return");
        return doc;
    }

    /**
     * Translates DBObjects into Json documents
     */
    public List<JsonDoc> toJson(List<DBObject> objects) {
        List<JsonDoc> list = new ArrayList<>(objects.size());
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
     * Translates a sort expression to Mongo sort expression
     */
    public DBObject translate(Sort sort) {
        LOGGER.debug("translate {}", sort);
        Error.push("translateSort");
        DBObject ret;
        try {
            if (sort instanceof CompositeSortKey) {
                ret = translateCompositeSortKey((CompositeSortKey) sort);
            } else {
                ret = translateSortKey((SortKey) sort);
            }
        } catch (Error e) {
            // rethrow lightblue error
            throw e;
        } catch (Exception e) {
            // throw new Error (preserves current error context)
            LOGGER.error(e.getMessage(), e);
            throw Error.get(MongoCrudConstants.ERR_INVALID_OBJECT, e.getMessage());
        } finally {
            Error.pop();
        }
        return ret;
    }

    /**
     * Translates a query to Mongo query
     *
     * @param md Entity metadata
     * @param query The query expression
     */
    public DBObject translate(EntityMetadata md, QueryExpression query) {
        Error.push("translateQuery");
        FieldTreeNode mdRoot = md.getFieldTreeRoot();
        try {
            return translate(mdRoot, query, md, new MutablePath());
        } catch (Error e) {
            // rethrow lightblue error
            throw e;
        } catch (Exception e) {
            // throw new Error (preserves current error context)
            LOGGER.error(e.getMessage(), e);
            throw Error.get(MongoCrudConstants.ERR_INVALID_OBJECT, e.getMessage());
        } finally {
            Error.pop();
        }
    }

    /**
     * Tranlates an update expression to Mongo query
     *
     * @param md Entity metedata
     * @param expr Update expression
     *
     * If the update expresssion is something that can be translated into a
     * mongo update expression, translation is performed. Otherwise,
     * CannotTranslateException is thrown, and the update operation must be
     * performed using the Updaters.
     */
    public DBObject translate(EntityMetadata md, UpdateExpression expr)
            throws CannotTranslateException {
        Error.push("translateUpdate");
        try {
            BasicDBObject ret = new BasicDBObject();
            translateUpdate(md.getFieldTreeRoot(), expr, ret);
            return ret;
        } catch (Error | CannotTranslateException e) {
            // rethrow lightblue error
            throw e;
        } catch (Exception e) {
            // throw new Error (preserves current error context)
            LOGGER.error(e.getMessage(), e);
            throw Error.get(MongoCrudConstants.ERR_INVALID_OBJECT, e.getMessage());
        } finally {
            Error.pop();
        }
    }

    /**
     * Returns all the fields required to evaluate the given projection, query,
     * and sort
     *
     * @param md Entity metadata
     * @param p Projection
     * @param q Query
     * @param s Sort
     *
     * All arguments are optional. The returned set contains the fields required
     * to evaluate all the non-null expressions
     */
    public static Set<Path> getRequiredFields(EntityMetadata md,
                                              Projection p,
                                              QueryExpression q,
                                              Sort s) {
        Set<Path> fields = new HashSet<>();
        FieldCursor cursor = md.getFieldCursor();
        // skipPrefix will be set to the root of a subtree that needs to be skipped.
        // If it is non-null, all fields with a prefix 'skipPrefix' will be skipped.
        Path skipPrefix = null;
        if (cursor.next()) {
            boolean done = false;
            do {
                Path field = cursor.getCurrentPath();
                if (skipPrefix != null) {
                    if (!field.matchingDescendant(skipPrefix)) {
                        skipPrefix = null;
                    }
                }
                if (skipPrefix == null) {
                    FieldTreeNode node = cursor.getCurrentNode();
                    LOGGER.debug("Checking if {} is included ({})", field, node);
                    if (node instanceof ResolvedReferenceField
                            || node instanceof ReferenceField) {
                        skipPrefix = field;
                    } else {
                        if ((node instanceof ObjectField)
                                || (node instanceof ArrayField && ((ArrayField) node).getElement() instanceof ObjectArrayElement)
                                || (node instanceof ArrayElement)) {
                            // include its member fields
                        } else if ((p != null && p.isFieldRequiredToEvaluateProjection(field))
                                || (q != null && q.isRequired(field))
                                || (s != null && s.isRequired(field))) {
                            LOGGER.debug("{}: required", field);
                            fields.add(field);
                        } else {
                            LOGGER.debug("{}: not required", field);
                        }
                        done = !cursor.next();
                    }
                } else {
                    done = !cursor.next();
                }
            } while (!done);
        }
        return fields;
    }

    /**
     * Writes a MongoDB projection containing fields to evaluate the projection,
     * sort, and query
     */
    public DBObject translateProjection(EntityMetadata md,
                                        Projection p,
                                        QueryExpression q,
                                        Sort s) {
        Set<Path> fields = getRequiredFields(md, p, q, s);
        BasicDBObject ret = new BasicDBObject();
        for (Path f : fields) {
            ret.append(translatePath(f), 1);
        }
        LOGGER.debug("Resulting projection:{}", ret);
        return ret;
    }

    public static Stream<IndexSortKey> getCaseInsensitiveIndexes(List<Index> indexes) {
        return indexes.stream()
                .map(Index::getFields)
                .flatMap(Collection::stream)
                .filter(IndexSortKey::isCaseInsensitive);
    }

    /**
     * Translate update expression list and primitive updates. Anything else
     * causes an exception.
     */
    private void translateUpdate(FieldTreeNode root, UpdateExpression expr, BasicDBObject dest)
            throws CannotTranslateException {
        if (expr instanceof ArrayUpdateExpression) {
            throw new CannotTranslateException(expr);
        } else if (expr instanceof PrimitiveUpdateExpression) {
            translatePrimitiveUpdate(root, (PrimitiveUpdateExpression) expr, dest);
        } else if (expr instanceof UpdateExpressionList) {
            for (PartialUpdateExpression x : ((UpdateExpressionList) expr).getList()) {
                translateUpdate(root, x, dest);
            }
        }
    }

    /**
     * Attempt to translate a primitive update expression. If the epxression
     * touches any arrays or array elements, translation fails.
     */
    private void translatePrimitiveUpdate(FieldTreeNode root,
                                          PrimitiveUpdateExpression expr,
                                          BasicDBObject dest)
            throws CannotTranslateException {
        if (expr instanceof SetExpression) {
            translateSet(root, (SetExpression) expr, dest);
        } else if (expr instanceof UnsetExpression) {
            translateUnset(root, (UnsetExpression) expr, dest);
        } else {
            throw new CannotTranslateException(expr);
        }
    }

    private void translateSet(FieldTreeNode root,
                              SetExpression expr,
                              BasicDBObject dest)
            throws CannotTranslateException {
        String op;
        switch (expr.getOp()) {
            case _set:
                op = "$set";
                break;
            case _add:
                op = "$inc";
                break;
            default:
                throw new CannotTranslateException(expr);
        }
        BasicDBObject obj = (BasicDBObject) dest.get(op);
        if (obj == null) {
            obj = new BasicDBObject();
            dest.put(op, obj);
        }
        for (FieldAndRValue frv : expr.getFields()) {
            Path field = frv.getField();
            if (hasArray(root, field)) {
                throw new CannotTranslateException(expr);
            }
            RValueExpression rvalue = frv.getRValue();
            if (rvalue.getType() == RValueExpression.RValueType._value) {
                Value value = rvalue.getValue();
                FieldTreeNode ftn = root.resolve(field);
                if (ftn == null) {
                    throw new CannotTranslateException(expr);
                }
                if (!(ftn instanceof SimpleField)) {
                    throw new CannotTranslateException(expr);
                }
                Object valueObject = ftn.getType().cast(value.getValue());
                if (field.equals(ID_PATH)) {
                    valueObject = createIdFrom(valueObject);
                }
                obj.put(translatePath(field), filterBigNumbers(valueObject));
            } else {
                throw new CannotTranslateException(expr);
            }
        }
    }

    private void translateUnset(FieldTreeNode root,
                                UnsetExpression expr,
                                BasicDBObject dest)
            throws CannotTranslateException {
        BasicDBObject obj = (BasicDBObject) dest.get("$unset");
        if (obj == null) {
            obj = new BasicDBObject();
            dest.put("$unset", obj);
        }
        for (Path field : expr.getFields()) {
            if (hasArray(root, field)) {
                throw new CannotTranslateException(expr);
            }
            obj.put(translatePath(field), "");
        }
    }

    /**
     * Returns true if the field is an array, or points to a field within an
     * array
     */
    private boolean hasArray(FieldTreeNode root, Path field)
            throws CannotTranslateException {
        FieldTreeNode node = root.resolve(field);
        if (node == null) {
            throw new CannotTranslateException(field);
        }
        do {
            if (node instanceof ArrayField
                    || node instanceof ArrayElement) {
                return true;
            } else {
                node = node.getParent();
            }
        } while (node != null);
        return false;
    }

    private DBObject translateSortKey(SortKey sort) {
        return new BasicDBObject(translatePath(sort.getField()), sort.isDesc() ? -1 : 1);
    }

    private DBObject translateCompositeSortKey(CompositeSortKey sort) {
        DBObject ret = null;
        for (SortKey key : sort.getKeys()) {
            if (ret == null) {
                ret = translateSortKey(key);
            } else {
                ret.put(translatePath(key.getField()), key.isDesc() ? -1 : 1);
            }
        }
        return ret;
    }

    private DBObject translate(FieldTreeNode context, QueryExpression query, EntityMetadata emd, MutablePath fullPath) {
        DBObject ret;
        if (query instanceof ArrayContainsExpression) {
            ret = translateArrayContains(context, (ArrayContainsExpression) query);
        } else if (query instanceof ArrayMatchExpression) {
            ret = translateArrayElemMatch(context, (ArrayMatchExpression) query, emd, fullPath);
        } else if (query instanceof FieldComparisonExpression) {
            ret = translateFieldComparison(context, (FieldComparisonExpression) query);
        } else if (query instanceof NaryLogicalExpression) {
            ret = translateNaryLogicalExpression(context, (NaryLogicalExpression) query, emd, fullPath);
        } else if (query instanceof NaryValueRelationalExpression) {
            ret = translateNaryValueRelationalExpression(context, (NaryValueRelationalExpression) query);
        } else if (query instanceof NaryFieldRelationalExpression) {
            ret = translateNaryFieldRelationalExpression(context, (NaryFieldRelationalExpression) query);
        } else if (query instanceof RegexMatchExpression) {
            ret = translateRegexMatchExpression(context, (RegexMatchExpression) query, emd, fullPath);
        } else if (query instanceof UnaryLogicalExpression) {
            ret = translateUnaryLogicalExpression(context, (UnaryLogicalExpression) query, emd, fullPath);
        } else {
            ret = translateValueComparisonExpression(context, (ValueComparisonExpression) query);
        }
        return ret;
    }

    private FieldTreeNode resolve(FieldTreeNode context, Path field) {
        FieldTreeNode node = context.resolve(field);
        if (node == null) {
            throw Error.get(ERR_INVALID_FIELD, field.toString());
        }
        return node;
    }

    /**
     * Converts a value list to a list of values with the proper type
     *
     * @param idList If true, the list contains _ids
     */
    private List<Object> translateValueList(Type t, List<Value> values,boolean idList) {
       List<Object> ret = new ArrayList<>(values==null?0:values.size());
        if(values!=null) {
            for (Value v : values) {
                Object value = v == null ? null : v.getValue();
                if (value != null) {
                    value = filterBigNumbers(t.cast(value));
                    if(idList)
                        value=createIdFrom(value);
                }
                ret.add(value);
            }
        }
        return ret;
    }

    private DBObject translateValueComparisonExpression(FieldTreeNode context, ValueComparisonExpression expr) {
        Type t = resolve(context, expr.getField()).getType();

        Object value = expr.getRvalue().getValue();
        if (expr.getOp() == BinaryComparisonOperator._eq
                || expr.getOp() == BinaryComparisonOperator._neq) {
            if (!t.supportsEq() && value != null) {
                throw Error.get(ERR_INVALID_COMPARISON, expr.toString());
            }
        } else if (!t.supportsOrdering()) {
            throw Error.get(ERR_INVALID_COMPARISON, expr.toString());
        }
        Object valueObject = filterBigNumbers(t.cast(value));
        if (expr.getField().equals(ID_PATH)) {
            valueObject = createIdFrom(valueObject);
        }
        if (expr.getOp() == BinaryComparisonOperator._eq) {
            return new BasicDBObject(translatePath(expr.getField()), valueObject);
        } else {
            return new BasicDBObject(translatePath(expr.getField()),
                    new BasicDBObject(BINARY_COMPARISON_OPERATOR_MAP.get(expr.getOp()), valueObject));
        }
    }

    private DBObject translateRegexMatchExpression(FieldTreeNode context, RegexMatchExpression expr, EntityMetadata emd, MutablePath fullPath) {
        fullPath.push(expr.getField());
        StringBuilder options = new StringBuilder();
        BasicDBObject regex = new BasicDBObject("$regex", expr.getRegex());
        Path field = expr.getField();

        if (expr.isCaseInsensitive()) {
            options.append('i');
            for (Index index : emd.getEntityInfo().getIndexes().getIndexes()) {
                if (index.isCaseInsensitiveKey(fullPath)) {
                    field = getHiddenForField(expr.getField());
                    regex.replace("$regex", expr.getRegex().toUpperCase());
                    options.deleteCharAt(options.length() - 1);
                    break;
                }
            }
        }
        if (expr.isMultiline()) {
            options.append('m');
        }
        if (expr.isExtended()) {
            options.append('x');
        }
        if (expr.isDotAll()) {
            options.append('s');
        }
        String opStr = options.toString();
        if (opStr.length() > 0) {
            regex.append("$options", opStr);
        }
        fullPath.pop();
        return new BasicDBObject(translatePath(field), regex);
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

    private DBObject translateNaryValueRelationalExpression(FieldTreeNode context, NaryValueRelationalExpression expr) {
        Type t = resolve(context, expr.getField()).getType();
        if (t.supportsEq()) {
            List<Object> values = translateValueList(t, expr.getValues(),expr.getField().equals(ID_PATH));
            return new BasicDBObject(translatePath(expr.getField()),
                    new BasicDBObject(NARY_RELATIONAL_OPERATOR_MAP.get(expr.getOp()),
                            values));
        } else {
            throw Error.get(ERR_INVALID_FIELD, expr.toString());
        }
    }

    /**
     *
     * @param doc
     * @param md
     * @throws IOException
     */
    public static void populateDocHiddenFields(DBObject doc, EntityMetadata md) throws IOException {
        Stream<IndexSortKey> ciIndexes = Translator.getCaseInsensitiveIndexes(md.getEntityInfo().getIndexes().getIndexes());
        Map<String, String> fieldMap = new HashMap<>();
        ciIndexes.forEach(i -> {
            String hidden = Translator.getHiddenForField(i.getField()).toString();
            fieldMap.put(i.getField().toString(), hidden);
        });
        populateDocHiddenFields(doc, fieldMap);
    }

    /**
     *
     * @param doc
     * @param fieldMap <field, hiddenCounterpart>
     * @throws IOException
     */
    public static void populateDocHiddenFields(DBObject doc, Map<String, String> fieldMap) throws IOException {
        for (String index : fieldMap.keySet()) {
            int arrIndex = index.indexOf("*");
            if (arrIndex > -1) {
                // recurse if we have more arrays in the path
                populateHiddenArrayField(doc, index, fieldMap.get(index));
            } else {
                DBObject currentDbo = doc;
                Path path = new Path(index);
                for (int i = 0; i < path.numSegments() - 1; i++) {
                    // recurse down the obj tree and
                    currentDbo = (DBObject) currentDbo.get(path.head(i));
                }
                // given the last basic object, populate its hidden field
                String val = (String) currentDbo.get(path.getLast());
                DBObject hidden = (DBObject) currentDbo.get(HIDDEN_SUB_PATH.toString());
                if (val != null) {
                    if (hidden == null) {
                        currentDbo.put(HIDDEN_SUB_PATH.toString(), new BasicDBObject(path.getLast(), val.toUpperCase()));
                    } else {
                        hidden.put(path.getLast(), val.toUpperCase());
                    }
                } else if (val == null && hidden != null) {
                    currentDbo.removeField(HIDDEN_SUB_PATH.toString());
                }
            }
        }
    }

    /**
     * Given an index and it's hidden counterpart, populate the document with
     * the correct value for the hidden field
     *
     * @param doc
     * @param index
     * @param hidden
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    private static void populateHiddenArrayField(DBObject doc, String index, String hidden) throws IOException {
        String[] indexSplit = splitArrayPath(index);
        String fieldPre = indexSplit[0];
        String fieldPost = indexSplit[1];

        String[] hiddenSplit = splitArrayPath(hidden);
        String hiddenPre = hiddenSplit[0];
        String hiddenPost = hiddenSplit[1];

        List docArr = null;
        try {
            docArr = (List) getDBObject(doc, new Path(fieldPre));
        } catch (Exception e) {
            LOGGER.debug("Error when populating hidden field {} with value from canonical field {}\n"
                    + "Document being populated: \n{}", hidden, index, doc);
            throw e;
        }

        if (docArr != null) {
            ObjectNode arrNode = JsonNodeFactory.instance.objectNode();
            for (int i = 0; i < docArr.size(); i++) {
                // check if there's an array in the index
                int indx = fieldPost.indexOf("*");
                String fullIdxPath = fieldPre + "." + i + fieldPost;
                String fullHiddenPath = hiddenPre + "." + i + hiddenPost;
                if (indx > -1) {
                    // if we have another array, descend
                    populateHiddenArrayField(doc, fullIdxPath, fullHiddenPath);
                } else {
                    // if no more arrays, set the field and continue
                    String node = null;
                    Object object = docArr.get(i);
                    if (object instanceof BasicDBObject) {
                        Object obj = null;
                        try {
                            obj = getDBObject((BasicDBObject) object, new Path(fieldPost.substring(1)));
                        } catch (Exception e) {
                            LOGGER.debug("Error when populating hidden field {} with value from canonical field {}\n"
                                    + "Document being populated: \n{}", fullHiddenPath, fullIdxPath, doc);
                            throw e;
                        }

                        if (obj != null) {
                            node = obj.toString().toUpperCase();
                        }
                    } else {
                        node = object.toString().toUpperCase();
                    }
                    if (node != null) {
                        LOGGER.debug("Adding field '" + fullHiddenPath + "' to document with value " + node);
                        JsonDoc.modify(arrNode, new Path(fullHiddenPath), JsonNodeFactory.instance.textNode(node), true);
                    }
                }
                ObjectMapper mapper = new ObjectMapper();
                JsonNode merged = merge(mapper.readTree(doc.toString()), mapper.readTree(arrNode.toString()));
                DBObject obj = (DBObject) JSON.parse(merged.toString());
                ((BasicDBObject) doc).clear();
                ((BasicDBObject) doc).putAll(obj);
            }
        }
    }

    /**
     * Splits a path with a * based on its first occurrence
     *
     * @param index
     * @return A tuple where the first index is the path before the array and
     * the second index is the path after the array
     */
    private static String[] splitArrayPath(String index) {
        String[] indexSplit = index.split("\\*");
        String fieldPre = indexSplit[0];
        fieldPre = fieldPre.substring(0, fieldPre.length() - 1);
        String fieldPost = StringUtils.join(Arrays.copyOfRange(indexSplit, 1, indexSplit.length), "*");
        if (!fieldPost.isEmpty()) {
            if (index.lastIndexOf("*") == index.length() - 1) {
                // re-append the * from the split
                fieldPost += "*";
            } else if (index.lastIndexOf(".") == index.length() - 1) {
                fieldPost = fieldPost.substring(0, fieldPost.length() - 1);
            }
        }
        return new String[]{fieldPre, fieldPost};
    }

    private static JsonNode merge(JsonNode mainNode, JsonNode updateNode) {
        updateNode.fieldNames().forEachRemaining(f -> {
            JsonNode valueToBeUpdated = mainNode.get(f);
            JsonNode updatedValue = updateNode.get(f);
            // if node is an array
            if (valueToBeUpdated != null && updatedValue.isArray()) {
                for (int i = 0; i < updatedValue.size(); i++) {
                    JsonNode updatedChildNode = updatedValue.get(i);
                    if (valueToBeUpdated.size() <= i) {
                        ((ArrayNode) valueToBeUpdated).add(updatedChildNode);
                    }
                    JsonNode childNodeToBeUpdated = valueToBeUpdated.get(i);
                    merge(childNodeToBeUpdated, updatedChildNode);
                }
                // if node is an object
            } else if (valueToBeUpdated != null && valueToBeUpdated.isObject()) {
                merge(valueToBeUpdated, updatedValue);
            } else if (mainNode instanceof ObjectNode) {
                ((ObjectNode) mainNode).replace(f, updatedValue);
            }
        });
        return mainNode;
    }

    private DBObject translateNaryFieldRelationalExpression(FieldTreeNode context, NaryFieldRelationalExpression expr) {
        Type t = resolve(context, expr.getField()).getType();
        if (t.supportsEq()) {
            // Call resolve, which will verify the field exists.  Don't need the response.
            resolve(context, expr.getRfield());
            boolean in = expr.getOp() == NaryRelationalOperator._in;
            return new BasicDBObject("$where",
                    String.format("function() for(var nfr=0;nfr<this.%s.length;nfr++) {if ( %s == %s[nfr] ) return %s;} return %s;}",
                            translateJsPath(expr.getRfield()),
                            translateJsPath(expr.getField()),
                            translateJsPath(expr.getRfield()),
                            in ? "true" : "false",
                            in ? "false" : "true"));
        } else {
            throw Error.get(ERR_INVALID_FIELD, expr.toString());
        }
    }

    private DBObject translateUnaryLogicalExpression(FieldTreeNode context, UnaryLogicalExpression expr, EntityMetadata emd, MutablePath fullPath) {
        List<DBObject> l = new ArrayList<>(1);
        l.add(translate(context, expr.getQuery(), emd, fullPath));
        return new BasicDBObject(UNARY_LOGICAL_OPERATOR_MAP.get(expr.getOp()), l);
    }

    private DBObject translateNaryLogicalExpression(FieldTreeNode context, NaryLogicalExpression expr, EntityMetadata emd, MutablePath fullPath) {
        List<QueryExpression> queries = expr.getQueries();
        List<DBObject> list = new ArrayList<>(queries.size());
        for (QueryExpression query : queries) {
            list.add(translate(context, query, emd, fullPath));
        }
        return new BasicDBObject(NARY_LOGICAL_OPERATOR_MAP.get(expr.getOp()), list);
    }

    private String writeJSForLoop(StringBuilder bld, Path p, String varPrefix) {
        StringBuilder arr = new StringBuilder();
        int n = p.numSegments();
        int j = 0;
        for (int i = 0; i < n; i++) {
            String seg = p.head(i);
            if (Path.ANY.equals(seg)) {
                bld.append(String.format("for(var %s%d=0;%s%d<this.%s.length;%s%d++) {", varPrefix, j, varPrefix, j, arr.toString(), varPrefix, j));
                arr.append('[').append(varPrefix).append(j).append(']');
                j++;
            } else if (p.isIndex(i)) {
                arr.append('[').append(seg).append(']');
            } else {
                if (i > 0) {
                    arr.append('.');
                }
                arr.append(seg);
            }
        }
        return arr.toString();
    }

    private static final String ARR_ARR_EQ = "if(this.f1.length==this.f2.length) { "
            + "  var allEq=true;"
            + "  for(var i=0;i<this.f1.length;i++) { "
            + "     if(this.f1[i] != this.f2[i]) { allEq=false; break; } "
            + "  } "
            + "  if(allEq) return true;"
            + "}";

    private static final String ARR_ARR_NEQ = "if(this.f1.length==this.f2.length) { "
            + "  var allEq=true;"
            + "  for(var i=0;i<this.f1.length;i++) { "
            + "     if(this.f1[i] != this.f2[i]) { allEq=false; break; } "
            + "  } "
            + "  if(!allEq) return true;"
            + "} else { return true; }";

    private static final String ARR_ARR_CMP = "if(this.f1.length==this.f2.length) {"
            + "  var allOk=true;"
            + "  for(var i=0;i<this.f1.length;i++) {"
            + "    if(!(this.f1[i] op this.f2[i])) {allOk=false; break;} "
            + "  }"
            + " if(allOk) return true;}";

    private String writeArrayArrayComparisonJS(String field1, String field2, BinaryComparisonOperator op) {
        switch (op) {
            case _eq:
                return ARR_ARR_EQ.replaceAll("f1", field1).replaceAll("f2", field2);
            case _neq:
                return ARR_ARR_NEQ.replaceAll("f1", field1).replaceAll("f2", field2);
            default:
                return ARR_ARR_CMP.replaceAll("f1", field1).replaceAll("f2", field2).replace("op", BINARY_COMPARISON_OPERATOR_JS_MAP.get(op));
        }
    }

    private String writeArrayFieldComparisonJS(String field, String array, String op) {
        return String.format("for(var i=0;i<this.%s.length;i++) { if(!(this.%s %s this.%s[i])) return false; } return true;", array, field, op, array);
    }

    private String writeComparisonJS(Path field1, boolean field1IsArray,
                                     Path field2, boolean field2IsArray,
                                     BinaryComparisonOperator op) {
        return writeComparisonJS(translateJsPath(field1), field1IsArray, translateJsPath(field2), field2IsArray, op);
    }

    private String writeComparisonJS(String field1, boolean field1IsArray,
                                     String field2, boolean field2IsArray,
                                     BinaryComparisonOperator op) {
        if (field1IsArray) {
            if (field2IsArray) {
                return writeArrayArrayComparisonJS(field1, field2, op);
            } else {
                return writeArrayFieldComparisonJS(field2, field1, BINARY_COMPARISON_OPERATOR_JS_MAP.get(op.invert()));
            }
        } else if (field2IsArray) {
            return writeArrayFieldComparisonJS(field1, field2, BINARY_COMPARISON_OPERATOR_JS_MAP.get(op));
        } else {
            return String.format("if(this.%s %s this.%s) { return true;}", field1, BINARY_COMPARISON_OPERATOR_JS_MAP.get(op), field2);
        }
    }

    private DBObject translateFieldComparison(FieldTreeNode context, FieldComparisonExpression expr) {
        StringBuilder str = new StringBuilder(256);
        // We have to deal with array references here
        Path rField = expr.getRfield();
        boolean rIsArray = context.resolve(rField) instanceof ArrayField;
        Path lField = expr.getField();
        boolean lIsArray = context.resolve(lField) instanceof ArrayField;
        int rn = rField.nAnys();
        int ln = lField.nAnys();
        str.append("function() {");
        if (rn > 0 && ln > 0) {
            // Write a function with nested for loops
            // function() {
            //   for(var x1=0;x1<a.b.length;x1++) {
            //     for(var x2=0;x2<a.b[x1].c.d.length;x2++) {
            //        for(var y1=y1<m.n.length;y1++) {
            //        ...
            //       if(this.a.b[x1].x.d[x2] = this.m.n[y1]) return true;
            //      }
            //     }
            //    }
            // return false; }
            String rJSField = writeJSForLoop(str, rField, "r");
            String lJSField = writeJSForLoop(str, lField, "l");
            str.append(writeComparisonJS(lJSField, lIsArray, rJSField, rIsArray, expr.getOp()));
            for (int i = 0; i < rn + ln; i++) {
                str.append('}');
            }
            str.append("return false;}");
        } else if (rn > 0 || ln > 0) {
            // Only one of them has ANY, write a single for loop
            // function() {
            //   for(var x1=0;x1<a.b.length;x1++) {
            //     for(var x2=0;x2<a.b[x1].c.d.length;x2++) {
            //      if(this.a.b[x1].c.d[x2]==this.rfield) return true;
            //     }
            //   }
            //  return false; }
            String jsField = writeJSForLoop(str, rn > 0 ? rField : lField, "i");
            str.append(writeComparisonJS(ln > 0 ? jsField : translateJsPath(lField), lIsArray,
                    rn > 0 ? jsField : translateJsPath(rField), rIsArray,
                    expr.getOp()));
            for (int i = 0; i < rn + ln; i++) {
                str.append('}');
            }
            str.append("return false;}");
        } else {
            // No ANYs, direct comparison
            //  function() {return this.lfield = this.rfield}
            str.append(writeComparisonJS(lField, lIsArray, rField, rIsArray, expr.getOp()));
            str.append("return false;}");
        }

        return new BasicDBObject("$where", str.toString());
    }

    private DBObject translateArrayElemMatch(FieldTreeNode context, ArrayMatchExpression expr, EntityMetadata emd, MutablePath fullPath) {
        FieldTreeNode arrayNode = resolve(context, expr.getArray());
        if (arrayNode instanceof ArrayField) {
            ArrayElement el = ((ArrayField) arrayNode).getElement();
            if (el instanceof ObjectArrayElement) {
                fullPath.push(expr.getArray()).push(Path.ANYPATH);
                BasicDBObject obj = new BasicDBObject(translatePath(expr.getArray()), new BasicDBObject("$elemMatch", translate(el, expr.getElemMatch(), emd, fullPath)));
                fullPath.pop().pop();
                return obj;
            }
        }
        throw Error.get(ERR_INVALID_FIELD, expr.toString());
    }

    private DBObject translateArrayContains(FieldTreeNode context, ArrayContainsExpression expr) {
        DBObject ret = null;
        FieldTreeNode arrayNode = resolve(context, expr.getArray());
        if (arrayNode instanceof ArrayField) {
            Type t = ((ArrayField) arrayNode).getElement().getType();
            switch (expr.getOp()) {
                case _all:
                    ret = translateArrayContainsAll(t, expr.getArray(), expr.getValues());
                    break;
                case _any:
                    ret = translateArrayContainsAny(t, expr.getArray(), expr.getValues());
                    break;
                case _none:
                    ret = translateArrayContainsNone(t, expr.getArray(), expr.getValues());
                    break;
            }
        } else {
            throw Error.get(ERR_INVALID_FIELD, expr.toString());
        }
        return ret;
    }

    /**
     * <pre>
     *   { field : { $all:[values] } }
     * </pre>
     */
    private DBObject translateArrayContainsAll(Type t, Path array, List<Value> values) {
        return new BasicDBObject(translatePath(array),
                new BasicDBObject("$all",
                                  translateValueList(t, values,false)));
    }

    /**
     * <pre>
     *     { $or : [ {field:value1},{field:value2},...] }
     * </pre>
     */
    private DBObject translateArrayContainsAny(Type t, Path array, List<Value> values) {
        List<BasicDBObject> l = new ArrayList<>(values.size());
        for (Value x : values) {
            l.add(new BasicDBObject(translatePath(array), x == null ? null
                    : x.getValue() == null ? null : t.cast(x.getValue())));
        }
        return new BasicDBObject("$or", l);
    }

    /**
     * <pre>
     * { $not : { $or : [ {field:value1},{field:value2},...]}}
     * </pre>
     */
    private DBObject translateArrayContainsNone(Type t, Path array, List<Value> values) {
        return new BasicDBObject("$not", translateArrayContainsAny(t, array, values));
    }

    private JsonDoc toJson(DBObject object, EntityMetadata md) {
        // Translation is metadata driven. We don't know how to
        // translate something that's not defined in metadata.
        FieldCursor cursor = md.getFieldCursor();
        if (cursor.firstChild()) {
            return new JsonDoc(objectToJson(object, md, cursor));
        } else {
            return null;
        }
    }

    /**
     * Called after firstChild is called on cursor
     */
    private ObjectNode objectToJson(DBObject object, EntityMetadata md, FieldCursor mdCursor) {
        ObjectNode node = factory.objectNode();
        do {
            Path p = mdCursor.getCurrentPath();
            FieldTreeNode field = mdCursor.getCurrentNode();
            String fieldName = field.getName();
            LOGGER.debug("{}", p);
            // Retrieve field value
            Object value = object.get(fieldName);
            if (value != null) {
                if (field instanceof SimpleField) {
                    convertSimpleFieldToJson(node, field, value, fieldName);
                } else if (field instanceof ObjectField) {
                    convertObjectFieldToJson(node, fieldName, md, mdCursor, value, p);
                } else if (field instanceof ResolvedReferenceField) {
                    // This should not happen
                } else if (field instanceof ArrayField && value instanceof List && mdCursor.firstChild()) {
                    convertArrayFieldToJson(node, fieldName, md, mdCursor, value);
                } else if (field instanceof ReferenceField) {
                    convertReferenceFieldToJson(value);
                }
            } // Don't add any null values to the document
        } while (mdCursor.nextSibling());
        return node;
    }

    private void convertSimpleFieldToJson(ObjectNode node, FieldTreeNode field, Object value, String fieldName) {
        JsonNode valueNode = field.getType().toJson(factory, value);
        if (valueNode != null && !(valueNode instanceof NullNode)) {
            node.set(fieldName, valueNode);
        }
    }

    private void convertObjectFieldToJson(ObjectNode node, String fieldName, EntityMetadata md, FieldCursor mdCursor, Object value, Path p) {
        if (value instanceof DBObject) {
            if (mdCursor.firstChild()) {
                JsonNode valueNode = objectToJson((DBObject) value, md, mdCursor);
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
    private void convertArrayFieldToJson(ObjectNode node, String fieldName, EntityMetadata md, FieldCursor mdCursor, Object value) {
        ArrayNode valueNode = factory.arrayNode();
        node.set(fieldName, valueNode);
        // We must have an array element here
        FieldTreeNode x = mdCursor.getCurrentNode();
        if (x instanceof ArrayElement) {
            for (Object item : (List) value) {
                valueNode.add(arrayElementToJson(item, (ArrayElement) x, md, mdCursor));
            }
        }
        mdCursor.parent();
    }

    private void convertReferenceFieldToJson(Object value) {
        //TODO
        LOGGER.debug("Converting reference field: ");
    }

    private JsonNode arrayElementToJson(Object value,
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
                    ret = objectToJson((DBObject) value, md, mdCursor);
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

    private Object filterBigNumbers(Object value) {
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
            if (fieldMdNode instanceof SimpleField) {
                toBson(ret, (SimpleField) fieldMdNode, path, node, md);
            } else if (fieldMdNode instanceof ObjectField) {
                convertObjectFieldToBson(node, cursor, ret, path, md);
            } else if (fieldMdNode instanceof ArrayField) {
                convertArrayFieldToBson(node, cursor, ret, fieldMdNode, path, md);
            } else if (fieldMdNode instanceof ReferenceField) {
                convertReferenceFieldToBson(node, path);
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

    public static int size(List<JsonDoc> list) {
        int size = 0;
        for (JsonDoc doc : list) {
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
}
