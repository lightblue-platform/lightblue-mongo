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

import java.math.BigInteger;
import java.math.BigDecimal;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;

import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.FieldTreeNode;
import com.redhat.lightblue.metadata.Type;
import com.redhat.lightblue.metadata.types.*;

import com.redhat.lightblue.query.*;

import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.Path;

/**
 * This class translates a query to javascript. It is used to
 * translate nontrivial query expressions to be used under a $where
 * construct
 */
public class JSQueryTranslator {

    public static final String ERR_INVALID_COMPARISON = "mongo-translation:invalid-comparison";

    private final EntityMetadata md;
    private int nameIndex=0;

    private static final SimpleDateFormat ISODATE_FORMAT=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    
    private static final Map<BinaryComparisonOperator,String> BINARY_COMPARISON_OPERATOR_JS_MAP;
    
    static{
        BINARY_COMPARISON_OPERATOR_JS_MAP = new HashMap<>();
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._eq, "==");
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._neq, "!=");
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._lt, "<");
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._gt, ">");
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._lte, "<=");
        BINARY_COMPARISON_OPERATOR_JS_MAP.put(BinaryComparisonOperator._gte, ">=");
    }
    
    private static class Context {
        FieldTreeNode contextNode;
        Block contextBlock;
        Function topLevel;

        public Context(FieldTreeNode contextNode,Block contextBlock) {
            this.contextNode=contextNode;
            this.contextBlock=contextBlock;
        }

        public Context enter(FieldTreeNode node,Block parent) {
            Context ctx=new Context(node,parent);
            ctx.topLevel=topLevel;
            return ctx;
        }
        
        public Name varName(Name localName) {
            Name p=new Name();
            if(contextBlock!=null)
                p.add(contextBlock.getDocumentLoopVarAsPrefix());
            int n=localName.length();
            for(int i=0;i<n;i++) {
                Part seg=localName.getPart(i);
                if(Path.PARENT.equals(seg.name)) {
                    p.removeLast();
                } else if(Path.THIS.equals(seg.name)) {
                    ; // Stay here
                } else {
                    p.add(seg);
                }
            }
            return p;
        }
    }

    public JSQueryTranslator(EntityMetadata md) {
        this.md=md;
    }
    
    public Expression translateQuery(QueryExpression query) {
        Context ctx=new Context(md.getFieldTreeRoot(),null);
        ctx.topLevel=new Function();
        ctx.topLevel.block=translateQuery(ctx,query);
        return ctx.topLevel;
    }

    private Block translateQuery(Context context,QueryExpression query) {
        Block ret=null;
        if (query instanceof ArrayContainsExpression) {
            //            ret = translateArrayContains(context, (ArrayContainsExpression) query);
        } else if (query instanceof ArrayMatchExpression) {
            ret = translateArrayElemMatch(context, (ArrayMatchExpression) query);
        } else if (query instanceof FieldComparisonExpression) {
            //            ret = translateFieldComparison(context, (FieldComparisonExpression) query);
        } else if (query instanceof NaryLogicalExpression) {
            ret = translateNaryLogicalExpression(context, (NaryLogicalExpression) query);
        } else if (query instanceof NaryValueRelationalExpression) {
            ret = translateNaryValueRelationalExpression(context, (NaryValueRelationalExpression) query);
        } else if (query instanceof NaryFieldRelationalExpression) {
            //            ret = translateNaryFieldRelationalExpression(context, (NaryFieldRelationalExpression) query);
        } else if (query instanceof RegexMatchExpression) {
            ret = translateRegexMatchExpression(context, (RegexMatchExpression) query);
        } else if (query instanceof UnaryLogicalExpression) {
            ret = translateUnaryLogicalExpression(context, (UnaryLogicalExpression) query);
        } else {
            ret = translateValueComparisonExpression(context, (ValueComparisonExpression) query);
        }
        return ret;
    }

    private Block translateNaryValueRelationalExpression(Context ctx,NaryValueRelationalExpression query) {
        FieldTreeNode fieldMd=ctx.contextNode.resolve(query.getField());
        List<Value> values=query.getValues();
        StringBuilder arr=new StringBuilder();
        arr.append('[');
        boolean first=true;
        for(Value v:values) {
            Object value=filterBigNumbers(fieldMd.getType().cast(v.getValue()));
            if(first) {
                first=false;
            } else {
                arr.append(',');
            }
            if(value==null)
                arr.append("null");
            else {
                if(fieldMd.getType() instanceof DateType) {
                    arr.append(String.format("ISODate('%1$s')",toISODate((Date)value)));
                } else {
                    arr.append(quote(fieldMd.getType(),value.toString()));
                }
            }
        }
        arr.append(']');
        String globalArr=ctx.topLevel.newGlobal(arr.toString());
        Block block=new Block(ctx.topLevel.newGlobal(query.getOp()==NaryRelationalOperator._in?"false":"true"));
        ForLoop forLoop=new ForLoop(null);
        block.add(forLoop);
        String loopVar=newName("i");
        forLoop.init=new SimpleExpression("var %1$s=0",loopVar);
        forLoop.test=new SimpleExpression("%1$s<%2$s.length",loopVar,globalArr);
        forLoop.term=new SimpleExpression("%1$s++",loopVar);
        forLoop.block=new Block(null);
        String tmpCmp=ctx.topLevel.newGlobal(null);
        if(fieldMd.getType() instanceof DateType) {
            forLoop.add(new SimpleExpression("%1$s=this.%2$s-%3$s[%4$s]",tmpCmp,ctx.varName(new Name(query.getField())),globalArr,loopVar));
            if(query.getOp()==NaryRelationalOperator._in) {
                forLoop.block.add(new IfStatement(null,new SimpleExpression("%1$s==0",tmpCmp),
                                                   new Block(null,new SimpleStatement("%1$s=true",block.resultVar),
                                                                       SimpleStatement.S_BREAK)));
            } else {
                forLoop.block.add(new IfStatement(null,new SimpleExpression("%1$s==0",tmpCmp),
                                                   new Block(null,new SimpleStatement("%1$s=false",block.resultVar),
                                                                       SimpleStatement.S_BREAK)));
            }
        } else {
            if(query.getOp()==NaryRelationalOperator._in) {
                forLoop.block.add(new IfStatement(null,new SimpleExpression("this.%1$s==%2$s[%3$s]",ctx.varName(new Name(query.getField())),globalArr,loopVar),
                                                   new Block(null,new SimpleStatement("%1$s=true",block.resultVar),
                                                                       SimpleStatement.S_BREAK)));
            } else {
                forLoop.block.add(new IfStatement(null,new SimpleExpression("this.%1$s==%2$s[%3$s]",ctx.varName(new Name(query.getField())),globalArr,loopVar),
                                                   new Block(null,new SimpleStatement("%1$s=false",block.resultVar),
                                                                       SimpleStatement.S_BREAK)));
            }
        }        
        
        return block;
    }

    private Block translateRegexMatchExpression(Context ctx,RegexMatchExpression query) {
        FieldTreeNode fieldMd=ctx.contextNode.resolve(query.getField());
        if(fieldMd.getType() instanceof StringType) {
            Name fieldName=ctx.varName(new Name(query.getField()));
            String regexVar=ctx.topLevel.newGlobal(String.format("new RegExp(\"%1$s\",\"%2$s\")",
                                                                 query.getRegex().replaceAll("\"","\\\""),
                                                                 regexFlags(query)));
            
            Block block=new Block(ctx.topLevel.newGlobalBoolean());
            block.add(new SimpleStatement("%1$s=%2$s.test(this.%3$s)",
                                           block.resultVar,
                                           regexVar,
                                           fieldName.toString()));
            return block;
        } else
            throw Error.get(ERR_INVALID_COMPARISON,query.toString());
    }

    private String regexFlags(RegexMatchExpression query) {
        StringBuilder bld=new StringBuilder();
        if(query.isCaseInsensitive())
            bld.append('i');
        if(query.isMultiline())
            bld.append('m');
        return bld.toString();
    }

    private Block translateValueComparisonExpression(Context ctx,ValueComparisonExpression query) {
        FieldTreeNode fieldMd=ctx.contextNode.resolve(query.getField());
        Object value = query.getRvalue().getValue();
        if (query.getOp() == BinaryComparisonOperator._eq
                || query.getOp() == BinaryComparisonOperator._neq) {
            if (!fieldMd.getType().supportsEq() && value != null) {
                throw Error.get(ERR_INVALID_COMPARISON, query.toString());
            }
        } else if (!fieldMd.getType().supportsOrdering()) {
            throw Error.get(ERR_INVALID_COMPARISON, query.toString());
        }
        Object valueObject = filterBigNumbers(fieldMd.getType().cast(value));
        Name fieldName=ctx.varName(new Name(query.getField()));
        
        Block block=new Block(ctx.topLevel.newGlobalBoolean());
        if(valueObject!=null&&fieldMd.getType() instanceof DateType) {
            block.add(new SimpleStatement("%1$s=this.%2$s-ISODate('%3$s')%4$s 0",block.resultVar,fieldName.toString(),
                                           toISODate((Date)valueObject),BINARY_COMPARISON_OPERATOR_JS_MAP.get(query.getOp())));
        } else {
            block.add(new SimpleStatement("%1$s=this.%2$s %3$s %4$s",block.resultVar,
                                           fieldName.toString(),
                                           BINARY_COMPARISON_OPERATOR_JS_MAP.get(query.getOp()),
                                           valueObject==null?"null":quote(fieldMd.getType(),valueObject.toString())));
        }
        return block;
    }
    
    private Block translateArrayElemMatch(Context ctx,ArrayMatchExpression query) {
        // An elemMatch expression is a for-loop
        Block block=new Block(ctx.topLevel.newGlobalBoolean());
        ArrForLoop loop=new ArrForLoop(null,newName("i"),ctx.varName(new Name(query.getArray())));
        Context newCtx=ctx.enter(ctx.contextNode.resolve(new Path(query.getArray(),Path.ANYPATH)),loop);
        loop.block=translateQuery(newCtx,query.getElemMatch());
        loop.block.add(new IfStatement(null,
                                        new SimpleExpression(loop.block.resultVar),
                                        new Block(null,
                                                            new SimpleStatement("%1$s=true",block.resultVar),
                                                            new SimpleStatement("break"))));
        block.add(loop);
        return block;
    }

    private Block translateNaryLogicalExpression(Context ctx,NaryLogicalExpression query) {
        Block block=new Block(ctx.topLevel.newGlobalBoolean());
        List<String> vars=new ArrayList();
        for(QueryExpression x:query.getQueries()) {
            Block nested=translateQuery(ctx,x);
            vars.add(nested.resultVar);
            block.add(nested);            
        }
        String op=query.getOp()==NaryLogicalOperator._and?"&&":"||";
        block.add(new SimpleStatement("%1$s=%2$s",block.resultVar,String.join(op,vars)));
        return block;
    }    
    
    private Block translateUnaryLogicalExpression(Context ctx,UnaryLogicalExpression query) {
        // Only NOT is a unary operator
        Block block=new Block(ctx.topLevel.newGlobalBoolean());
        Block nested=translateQuery(ctx,query.getQuery());
        block.add(nested);
        block.add(new SimpleStatement("%1$s=!%2$s",block.resultVar,nested.resultVar));
        return block;
    }

    private String quote(Type t,String value) {
        if(t instanceof StringType||t instanceof BigDecimalType|| t instanceof BigIntegerType)
            return String.format("\"%1$s\"",value);
        else
            return value;
    }
    
    private String newName(String prefix) {
        return prefix+Integer.toString(nameIndex++);
    }

    private Object filterBigNumbers(Object value) {
        // Store big values as string. Mongo does not support big values
        if (value instanceof BigDecimal || value instanceof BigInteger) {
            return value.toString();
        } else {
            return value;
        }
    }

    private String toISODate(Date d) {
        return ((SimpleDateFormat)ISODATE_FORMAT.clone()).format(d);
    }
}
