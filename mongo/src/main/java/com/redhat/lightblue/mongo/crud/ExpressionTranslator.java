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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Date;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
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
import com.redhat.lightblue.util.JsonNodeCursor;
import com.redhat.lightblue.util.MutablePath;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.Util;

import com.redhat.lightblue.mongo.crud.js.JSQueryTranslator;

/**
 * Translations between BSON and JSON. This class is thread-safe, and can be
 * shared between threads
 */
public class ExpressionTranslator {

    public static final Path ID_PATH = new Path("_id");

    public static final String ERR_NO_OBJECT_TYPE = "mongo-translation:no-object-type";
    public static final String ERR_INVALID_OBJECTTYPE = "mongo-translation:invalid-object-type";
    public static final String ERR_INVALID_FIELD = "mongo-translation:invalid-field";
    public static final String ERR_INVALID_COMPARISON = "mongo-translation:invalid-comparison";

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionTranslator.class);

    private final MetadataResolver mdResolver;
    private final JsonNodeFactory factory;

    private static final Map<BinaryComparisonOperator, String> BINARY_COMPARISON_OPERATOR_JS_MAP;
    private static final Map<BinaryComparisonOperator, String> BINARY_COMPARISON_OPERATOR_MAP;
    private static final Map<NaryLogicalOperator, String> NARY_LOGICAL_OPERATOR_MAP;
    private static final Map<UnaryLogicalOperator, String> UNARY_LOGICAL_OPERATOR_MAP;
    private static final Map<NaryRelationalOperator, String> NARY_RELATIONAL_OPERATOR_MAP;

    /**
     * This is used as flow control. If query translation fails
     * because the query cannot be translated and a Javascript query
     * is required, this is thrown. It is caught and handled when it is 
     * possible to write a JS query
     */
    private static final class NeedsJS extends RuntimeException {}

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
    public ExpressionTranslator(MetadataResolver mdResolver,
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
     * Appends objectType:X to the query
     */
    public static QueryExpression appendObjectType(QueryExpression q,String entityName) {
        QueryExpression ot=new ValueComparisonExpression(DocTranslator.OBJECT_TYPE,BinaryComparisonOperator._eq,new Value(entityName));
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
        ret.append(translatePath(DocTranslator.HIDDEN_SUB_PATH),1);
        LOGGER.debug("Resulting projection:{}", ret);
        return ret;
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
                    valueObject = DocTranslator.createIdFrom(valueObject);
                }
                obj.put(translatePath(field), DocTranslator.filterBigNumbers(valueObject));
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

    /**
     * @param context The field tree node for the latest enclosing context (array of elemMatch, or top level node)
     * @param query The query to translate
     * @param emd The metadata
     * @param fullPath Current full path
     */
    private DBObject translate(FieldTreeNode context, QueryExpression query, EntityMetadata emd, MutablePath fullPath) {
        DBObject ret;
        try {
            if (query instanceof ArrayContainsExpression) {
                ret = translateArrayContains(context, emd, (ArrayContainsExpression) query,fullPath);
            } else if (query instanceof ArrayMatchExpression) {
                ret = translateArrayElemMatch(context, emd, (ArrayMatchExpression) query, emd, fullPath);
            } else if (query instanceof FieldComparisonExpression) {
                ret = translateFieldComparison(context, emd, (FieldComparisonExpression) query, fullPath);
            } else if (query instanceof NaryLogicalExpression) {
                ret = translateNaryLogicalExpression(context, emd, (NaryLogicalExpression) query, emd, fullPath);
            } else if (query instanceof NaryValueRelationalExpression) {
                ret = translateNaryValueRelationalExpression(context, emd, (NaryValueRelationalExpression) query,fullPath);
            } else if (query instanceof NaryFieldRelationalExpression) {
                ret = translateNaryFieldRelationalExpression(context, emd, (NaryFieldRelationalExpression) query,fullPath);
            } else if (query instanceof RegexMatchExpression) {
                ret = translateRegexMatchExpression(context, emd, (RegexMatchExpression) query, emd, fullPath);
            } else if (query instanceof UnaryLogicalExpression) {
                ret = translateUnaryLogicalExpression(context, emd, (UnaryLogicalExpression) query, emd, fullPath);
            } else {
                ret = translateValueComparisonExpression(context, emd, (ValueComparisonExpression) query,fullPath);
            }
        } catch (NeedsJS x) {
            // We can't translate the query, we have to write a $where query
            // But we can't do that if we're in an arrayElemMatch
            if(fullPath.isEmpty()) {
                JSQueryTranslator tx=new JSQueryTranslator(emd);
                ret=new BasicDBObject("$where",tx.translateQuery(query).toString());
            } else
                throw x; // Continue unroll stack
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

    private static class FieldInfo {
        final Path field;
        final FieldTreeNode fieldMd;

        FieldInfo(Path field,FieldTreeNode fieldMd) {
            this.field=field;
            this.fieldMd=fieldMd;
        }
    }
    
    private FieldInfo resolveFieldForQuery(FieldTreeNode context,Path contextPath,Path field) {
        int n=field.numSegments();
        // Two paths: p: the full thing, s: only the relative field
        // If the field reference moves to a parent of relative field, we use p
        // otherwise, we use s
        MutablePath p=new MutablePath(contextPath);
        MutablePath s=new MutablePath();
        FieldTreeNode fieldNode=context;
        for(int i=0;i<n;i++) {
            String seg=field.head(i);
            if(field.isIndex(i)) {
                p.push(seg);
                if(s!=null)
                    s.push(seg);
                if(fieldNode instanceof ArrayField) {
                    fieldNode=((ArrayField)fieldNode).getElement();
                } else {
                    throw Error.get(ERR_INVALID_FIELD,field.toString());
                }
            } else {
                if(Path.THIS.equals(seg)) {
                    // Don't push anything
                    ;
                } else if(Path.PARENT.equals(seg)) {
                    p.pop();
                    if(s!=null)
                        if(s.isEmpty())
                            s=null;
                        else
                            s.pop();
                    fieldNode=fieldNode.getParent();
                } else {
                    p.push(seg);
                    if(s!=null)
                        s.push(seg);
                    fieldNode=fieldNode.resolve(new Path(seg));
                }
            }
        }
        if(!contextPath.isEmpty()) {
            // If we are in a nonempty context (i.e. nested query under elemMatch), and our variable
            // refers to a field above that context, then mongo language can't handle it, and we need
            // to convert to JS
            FieldTreeNode trc=fieldNode.getParent();
            boolean fieldIsUnderContext=false;
            while(trc!=null) {
                if(trc==context) {
                    fieldIsUnderContext=true;
                    break;
                } else {
                    trc=trc.getParent();
                }
            }
            if(!fieldIsUnderContext)
                throw new NeedsJS();
        }
        return new FieldInfo(s==null?p.immutableCopy():s.immutableCopy(),fieldNode);
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
                    value = DocTranslator.filterBigNumbers(t.cast(value));
                    if(idList)
                        value=DocTranslator.createIdFrom(value);
                }
                ret.add(value);
            }
        }
        return ret;
    }

    private DBObject translateValueComparisonExpression(FieldTreeNode context, EntityMetadata md, ValueComparisonExpression expr,MutablePath fullPath) {
        FieldInfo finfo=resolveFieldForQuery(context,fullPath.immutableCopy(),expr.getField());       
        Type t = finfo.fieldMd.getType();

        Object value = expr.getRvalue().getValue();
        if (expr.getOp() == BinaryComparisonOperator._eq
                || expr.getOp() == BinaryComparisonOperator._neq) {
            if (!t.supportsEq() && value != null) {
                throw Error.get(ERR_INVALID_COMPARISON, expr.toString());
            }
        } else if (!t.supportsOrdering()) {
            throw Error.get(ERR_INVALID_COMPARISON, expr.toString());
        }
        Object valueObject = DocTranslator.filterBigNumbers(t.cast(value));
        if (finfo.field.equals(ID_PATH)) {
            valueObject = DocTranslator.createIdFrom(valueObject);
        }
        if (expr.getOp() == BinaryComparisonOperator._eq) {
            return new BasicDBObject(translatePath(finfo.field), valueObject);
        } else {
            return new BasicDBObject(translatePath(finfo.field),
                    new BasicDBObject(BINARY_COMPARISON_OPERATOR_MAP.get(expr.getOp()), valueObject));
        }
    }

    private DBObject translateRegexMatchExpression(FieldTreeNode context, EntityMetadata md,  RegexMatchExpression expr, EntityMetadata emd, MutablePath fullPath) {
        FieldInfo finfo=resolveFieldForQuery(context,fullPath.immutableCopy(),expr.getField());
        
        fullPath.push(finfo.field);
        StringBuilder options = new StringBuilder();
        BasicDBObject regex = new BasicDBObject("$regex", expr.getRegex());
        Path field = finfo.field;

        if (expr.isCaseInsensitive()) {
            options.append('i');
            for (Index index : emd.getEntityInfo().getIndexes().getIndexes()) {
                if (index.isCaseInsensitiveKey(fullPath)) {
                    field = DocTranslator.getHiddenForField(finfo.field);
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

    private DBObject translateNaryValueRelationalExpression(FieldTreeNode context,  EntityMetadata md, NaryValueRelationalExpression expr,MutablePath fullPath) {
        FieldInfo finfo=resolveFieldForQuery(context,fullPath.immutableCopy(),expr.getField());
        Type t = finfo.fieldMd.getType();
        if (t.supportsEq()) {
            List<Object> values = translateValueList(t, expr.getValues(),finfo.field.equals(ID_PATH));
            return new BasicDBObject(translatePath(finfo.field),
                    new BasicDBObject(NARY_RELATIONAL_OPERATOR_MAP.get(expr.getOp()),
                                      values));
        } else {
            throw Error.get(ERR_INVALID_FIELD, expr.toString());
        }
    }

    private DBObject translateNaryFieldRelationalExpression(FieldTreeNode context, EntityMetadata md,  NaryFieldRelationalExpression expr,MutablePath fullPath) {        
        FieldInfo finfo=resolveFieldForQuery(context,fullPath.immutableCopy(),expr.getField());
        Type t = finfo.fieldMd.getType();
        if (t.supportsEq()) {
            if(!fullPath.isEmpty()) {
                // We are in array elemMatch query, and can't use a $where
                throw new NeedsJS();
            }
            JSQueryTranslator jstx=new JSQueryTranslator(md);
            return new BasicDBObject("$where",jstx.translateQuery(expr).toString());
        } else {
            throw Error.get(ERR_INVALID_FIELD, expr.toString());
        }
    }

    private DBObject translateUnaryLogicalExpression(FieldTreeNode context,  EntityMetadata md, UnaryLogicalExpression expr, EntityMetadata emd, MutablePath fullPath) {
        List<DBObject> l = new ArrayList<>(1);
        l.add(translate(context, expr.getQuery(), emd, fullPath));
        return new BasicDBObject(UNARY_LOGICAL_OPERATOR_MAP.get(expr.getOp()), l);
    }

    private DBObject translateNaryLogicalExpression(FieldTreeNode context, EntityMetadata md,  NaryLogicalExpression expr, EntityMetadata emd, MutablePath fullPath) {
        List<QueryExpression> queries = expr.getQueries();
        List<DBObject> list = new ArrayList<>(queries.size());
        for (QueryExpression query : queries) {
            list.add(translate(context, query, emd, fullPath));
        }
        return new BasicDBObject(NARY_LOGICAL_OPERATOR_MAP.get(expr.getOp()), list);
    }

    private DBObject translateFieldComparison(FieldTreeNode context,  EntityMetadata md, FieldComparisonExpression expr,MutablePath fullPath) {
        if(!fullPath.isEmpty()) {
            // We are in arrayElemMatch query, and we can't use a $where here
            throw new NeedsJS();
        }
        JSQueryTranslator jstx=new JSQueryTranslator(md);
        return new BasicDBObject("$where",jstx.translateQuery(expr).toString());
    }
    
    private DBObject translateArrayElemMatch(FieldTreeNode context,  EntityMetadata md, ArrayMatchExpression expr, EntityMetadata emd, MutablePath fullPath) {
        FieldInfo finfo=resolveFieldForQuery(context,fullPath.immutableCopy(),expr.getArray());
        if (finfo.fieldMd instanceof ArrayField) {
            ArrayElement el = ((ArrayField) finfo.fieldMd).getElement();
            if (el instanceof ObjectArrayElement) {
                fullPath.push(finfo.field).push(Path.ANYPATH);
                BasicDBObject obj = new BasicDBObject(translatePath(finfo.field), new BasicDBObject("$elemMatch", translate(el, expr.getElemMatch(), emd, fullPath)));
                fullPath.pop().pop();
                return obj;
            } else {
            	throw new NeedsJS();
            }
        }
        throw Error.get(ERR_INVALID_FIELD, expr.toString());
    }

    private DBObject translateArrayContains(FieldTreeNode context, EntityMetadata md,  ArrayContainsExpression expr,MutablePath fullPath) {
        DBObject ret = null;
        FieldInfo finfo=resolveFieldForQuery(context,fullPath.immutableCopy(),expr.getArray());
        if (finfo.fieldMd instanceof ArrayField) {
            Type t = ((ArrayField) finfo.fieldMd).getElement().getType();
            switch (expr.getOp()) {
                case _all:
                    ret = translateArrayContainsAll(t, finfo.field, expr.getValues());
                    break;
                case _any:
                    ret = translateArrayContainsAny(t, finfo.field, expr.getValues());
                    break;
                case _none:
                    ret = translateArrayContainsNone(t, finfo.field, expr.getValues());
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
}
