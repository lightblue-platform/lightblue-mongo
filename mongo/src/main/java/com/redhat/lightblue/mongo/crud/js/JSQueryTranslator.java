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

import org.bson.FieldNameValidator;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;

import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.FieldTreeNode;
import com.redhat.lightblue.metadata.ArrayField;
import com.redhat.lightblue.metadata.Type;
import com.redhat.lightblue.metadata.types.*;

import com.redhat.lightblue.query.*;

import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.MutablePath;
import com.redhat.lightblue.util.Path;

/**
 * This class translates a query to javascript. It is used to
 * translate nontrivial query expressions to be used under a $where
 * construct
 */
public class JSQueryTranslator {

    public static final String ERR_INVALID_COMPARISON = "mongo-translation:invalid-comparison";

    private final EntityMetadata md;
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
            ret = translateArrayContainsExpression(context, (ArrayContainsExpression) query);
        } else if (query instanceof ArrayMatchExpression) {
            ret = translateArrayElemMatch(context, (ArrayMatchExpression) query);
        } else if (query instanceof FieldComparisonExpression) {
            ret = translateFieldComparison(context, (FieldComparisonExpression) query);
        } else if (query instanceof NaryLogicalExpression) {
            ret = translateNaryLogicalExpression(context, (NaryLogicalExpression) query);
        } else if (query instanceof NaryValueRelationalExpression) {
            ret = translateNaryValueRelationalExpression(context, (NaryValueRelationalExpression) query);
        } else if (query instanceof NaryFieldRelationalExpression) {
            ret = translateNaryFieldRelationalExpression(context, (NaryFieldRelationalExpression) query);
        } else if (query instanceof RegexMatchExpression) {
            ret = translateRegexMatchExpression(context, (RegexMatchExpression) query);
        } else if (query instanceof UnaryLogicalExpression) {
            ret = translateUnaryLogicalExpression(context, (UnaryLogicalExpression) query);
        } else {
            ret = translateValueComparisonExpression(context, (ValueComparisonExpression) query);
        }
        return ret;
    }

    /**
     * If field1 and field2 are both non-arrays:
     * <pre>
     *    result=field1 op field2
     * </pre>
     *
     * If field1 is an array and field2 is not:
     * <pre>
     *    result=false;
     *    for(i=0;i<this.field1.length;i++) {
     *      if(this.field1[i]==field2) {
     *        result=true;
     *        break;
     *      }
     *   }
     * </pre>
     *
     * If both field1 and field2 are arrays:
     * <pre>
     *    op=cmp:
     *  result=false;
     *  if(this.field1.length==this.field2.length) {
     *     result=true;
     *     for(int i=0;i<this.field1.length;i++) {
     *        if(!(this.field1[i] cmp this.field2[i])) {
     *           result=false;
     *           break;
     *        }
     *     }
     *  } 
     *
     *  op=ne
     *  result=true;
     *  if(this.field1.length==this.field2.length) {
     *     result=false;
     *     for(int i=0;i<this.field1.length;i++) {
     *        if(this.field1[i] != this.field2[i]) {
     *           result=true;
     *           break;
     *        }
     *     }
     *  } 
     * </pre>
     *
     * If field1 has n ANYs and field2 has none:
     * <pre>
     *    for(var i1=0;i1<this.field1.blah.length;i1++) {
     *       for(var i2=0;i2<this.field1.blah[i1].yada.length;i2++) {
     *          CMP
     *          if(CMP) return true
     *       }
     *    }
     * </pre>
     *
     * If field1 has n ANYs and field2 has m anys:
     * <pre>
     *    for(var i1=0;i1<this.field1.blah.length;i1++) {
     *       for(var i2=0;i2<this.field1.blah[i1].yada.length;i2++) {
     *         for(var i3=0;i3<this.field2.blah.length;i3++) {
     *           for(var i4=0;i4<this.field2.blah[i3].yada.length;i4++) {
     *             CMP
     *             if(CMP) return true
     *           }
     *         }
     *       }
     *    }
     * </pre>
     */
    private Block translateFieldComparison(Context ctx,FieldComparisonExpression query) {
        Path rField = query.getRfield();
        FieldTreeNode rFieldMd = ctx.contextNode.resolve(rField);
        Path lField = query.getField();
        FieldTreeNode lFieldMd = ctx.contextNode.resolve(lField);

        Block comparisonBlock=new Block(ctx.topLevel.newGlobalBoolean(ctx));
        Block parentBlock=comparisonBlock;

        Name lfieldLocalName=new Name();
        // First deal with the nested arrays of lField
        parentBlock=processNestedArrays(ctx,lField,parentBlock,lfieldLocalName);
        
        // Then deal with the nested arrays of rField
        Name rfieldLocalName=new Name();
        parentBlock=processNestedArrays(ctx,rField,parentBlock,rfieldLocalName);

        if(rFieldMd instanceof ArrayField && lFieldMd instanceof ArrayField) {
            String loopVar=ctx.newName("i");
            // Both fields are arrays
            if(query.getOp()==BinaryComparisonOperator._neq) {
                parentBlock.add(new SimpleStatement("%s=true",comparisonBlock.resultVar));
                parentBlock.add(new IfStatement(new SimpleExpression("this.%s.length==this.%s.length",ctx.varName(lfieldLocalName),ctx.varName(rfieldLocalName)),
                                                new SimpleStatement("%s=false",comparisonBlock.resultVar),
                                                new ForLoop(loopVar,true,ctx.varName(lfieldLocalName).toString(),
                                                            new Block(new IfStatement(new SimpleExpression("this.%s[%s]!=this.%s[%s]",ctx.varName(lfieldLocalName),loopVar,
                                                                                                           ctx.varName(rfieldLocalName),loopVar),
                                                                                      new SimpleStatement("%s=true",comparisonBlock.resultVar),
                                                                                      SimpleStatement.S_BREAK)))));
            } else {
                parentBlock.add(new IfStatement(new SimpleExpression("this.%s.length==this.%s.length",ctx.varName(lfieldLocalName),ctx.varName(rfieldLocalName)),
                                                new SimpleStatement("%s=true",comparisonBlock.resultVar),
                                                new ForLoop(loopVar,true,ctx.varName(lfieldLocalName).toString(),
                                                            new Block(new IfStatement(new SimpleExpression("!(this.%s[%s] %s this.%s[%s])",ctx.varName(lfieldLocalName),loopVar,
                                                                                                           BINARY_COMPARISON_OPERATOR_JS_MAP.get(query.getOp()),
                                                                                                           ctx.varName(rfieldLocalName),loopVar),
                                                                                      new SimpleStatement("%s=false",comparisonBlock.resultVar),
                                                                                      SimpleStatement.S_BREAK)))));
            }                                        
        } else if(rFieldMd instanceof ArrayField || lFieldMd instanceof ArrayField) {
            // Only one field is an array. If comparison is true for one element, then it is true
            Name arrayFieldLocalName;
            Name simpleFieldLocalName;
            BinaryComparisonOperator op;
            if(rFieldMd instanceof ArrayField) {
                arrayFieldLocalName=rfieldLocalName;
                simpleFieldLocalName=lfieldLocalName;
                op=query.getOp().invert();
            } else {
                arrayFieldLocalName=lfieldLocalName;
                simpleFieldLocalName=rfieldLocalName;
                op=query.getOp();
            }
            String loopVar=ctx.newName("i");
            parentBlock.add(new ForLoop(loopVar,true,ctx.varName(arrayFieldLocalName).toString(),
                                        new Block(new IfStatement(new SimpleExpression("this.%s[%s] %s %s",ctx.varName(arrayFieldLocalName),loopVar,
                                                                                       BINARY_COMPARISON_OPERATOR_JS_MAP.get(query.getOp()),
                                                                                       ctx.varName(simpleFieldLocalName)),
                                                                  new SimpleStatement("%s=true",comparisonBlock.resultVar),
                                                                  SimpleStatement.S_BREAK))));
        } else {
            // Simple comparison
            parentBlock.add(new SimpleStatement("%s=%s %s %s",comparisonBlock.resultVar,ctx.varName(lfieldLocalName),BINARY_COMPARISON_OPERATOR_JS_MAP.get(query.getOp()),ctx.varName(rfieldLocalName)));
        }
        // Add breaks to the end of for loops
        // Trace ctx back to ctx, add breaks to for loops
        IfStatement breakIfNecessary=new IfStatement(new SimpleExpression(comparisonBlock.resultVar),
                                                     SimpleStatement.S_BREAK);
        Statement parentStmt=parentBlock;
        while(parentStmt!=comparisonBlock) {            
            if(parentStmt instanceof ForLoop)
                ((ForLoop)parentStmt).add(breakIfNecessary);
            parentStmt=((Statement)parentStmt).parent;
        }
        
        return comparisonBlock;
        
    }

    private Block processNestedArrays(Context ctx,Path field,Block parent,Name arrayFieldName) {
        MutablePath pathSegment=new MutablePath();
        for(int i=0;i<field.numSegments();i++) {
            String seg=field.head(i);
            if(Path.ANY.equals(seg)) {
                ArrForLoop loop=new ArrForLoop(ctx.newName("ri"),ctx.varName(arrayFieldName));
                parent.add(loop);
                parent=loop;
                pathSegment.push(Path.ANY);
                arrayFieldName.add(loop.loopVar,true);
            } else {
                arrayFieldName.add(seg,field.isIndex(i));
                pathSegment.push(seg);
            }
        }
        return parent;
    }
    

    /**
     * <pre>
     * in:
     * var r0=false;
     * nin:
     * var r=true;
     * for(var i=0;i<this.arr.length;i++) {
     *    if(this.arr[i]==this.field) {
     * in:
     *       r0=true;
     * nin:  
     *       r0=false;
     *       break;
     *    }
     * }
     * return r0
     * </pre>
     */
    private Block translateNaryFieldRelationalExpression(Context ctx,NaryFieldRelationalExpression query) {
        FieldTreeNode fieldMd=ctx.contextNode.resolve(query.getField());
        if(!fieldMd.getType().supportsEq()) {
            throw Error.get(ERR_INVALID_COMPARISON, query.toString());            
        }
        Block block=new Block(ctx.topLevel.newGlobal(ctx,query.getOp()==NaryRelationalOperator._in?"false":"true"));
        ArrForLoop loop=new ArrForLoop(ctx.newName("i"),ctx.varName(new Name(query.getRfield())));
        block.add(loop);
        loop.add(new IfStatement(new SimpleExpression("this.%s[%s]==this.%s",ctx.varName(new Name(query.getRfield())),
                                                      loop.loopVar,
                                                      ctx.varName(new Name(query.getField()))),
                                 new SimpleStatement("%s=%s",block.resultVar,query.getOp()==NaryRelationalOperator._in?"true":"false"),
                                 SimpleStatement.S_BREAK));
        return block;
    }
    
    /**
     * <pre>
     * var r0=[values];
     * any:
     * var r1=false;
     * all: none:
     * var r1=true;
     * var r3=false;
     * for(var i=0;i<r0.length;i++) {
     *    r3=false;
     *    for(var j=0;j<this.arr.length;j++) {
     *         if(this.arr[j]==r0[i]) {
     *            r3=true;break;
     *         }
     *    }
     *    any:
     *    if(r3) {
     *       r1=true;
     *       break;
     *    }
     *    all:
     *    if(!r3) {
     *       r1=false;
     *       break;
     *    }
     *    none:
     *    if(r3) {
     *       r1=false;
     *       break;
     *    }
     * }
     * </pre>
     */
    private Block translateArrayContainsExpression(Context ctx,ArrayContainsExpression query) {
        FieldTreeNode fieldMd=ctx.contextNode.resolve(query.getArray());
        String valueArr=declareValueArray(ctx,((ArrayField)fieldMd).getElement(),query.getValues());
        Block arrayContainsBlock=new Block(ctx.topLevel.newGlobal(ctx,query.getOp()==ContainsOperator._any?"false":"true"));


        // for(var i=0;i<r0.length;i++) 
        String arrayLoopIndex=ctx.newName("i");
        ForLoop arrayLoop=new ForLoop(arrayLoopIndex,valueArr+".length");
        arrayLoop.resultVar=ctx.topLevel.newGlobalBoolean(ctx);
        arrayContainsBlock.add(arrayLoop);

        ArrForLoop innerLoop=new ArrForLoop(ctx.newName("j"),ctx.varName(new Name(query.getArray())));
        arrayLoop.add(new SimpleStatement("%s=false",arrayLoop.resultVar));
        arrayLoop.add(innerLoop);
        innerLoop.add(new IfStatement(new SimpleExpression("this.%s[%s]==%s[%s]",
                                                           ctx.varName(new Name(query.getArray())),
                                                           innerLoop.loopVar,
                                                           valueArr,
                                                           arrayLoopIndex),
                                      new SimpleStatement("%s=true",arrayLoop.resultVar),
                                      SimpleStatement.S_BREAK));

        switch(query.getOp()) {
        case _any:
            arrayLoop.add(new IfStatement(new SimpleExpression(arrayLoop.resultVar),
                                          new SimpleStatement("%s=true",arrayContainsBlock.resultVar),
                                          SimpleStatement.S_BREAK));
            break;
        case _all:
            arrayLoop.add(new IfStatement(new SimpleExpression("!%s",arrayLoop.resultVar),
                                          new SimpleStatement("%s=false",arrayContainsBlock.resultVar),
                                          SimpleStatement.S_BREAK));
            break; 
        case _none:
            arrayLoop.add(new IfStatement(new SimpleExpression(arrayLoop.resultVar),
                                          new SimpleStatement("%s=false",arrayContainsBlock.resultVar),
                                          SimpleStatement.S_BREAK));
            break; 
        }
        return arrayContainsBlock;
    }

    /**
     * <pre>
     *   var arr=[values];
     * </pre>
     */
    private String declareValueArray(Context ctx,FieldTreeNode fieldMd,List<Value> values) {
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
                    arr.append(String.format("ISODate('%s')",toISODate((Date)value)));
                } else {
                    arr.append(quote(fieldMd.getType(),value.toString()));
                }
            }
        }
        arr.append(']');
        return ctx.topLevel.newGlobal(ctx,arr.toString());
    }

    /**
     * <pre>
     * var r0=[values];
     * var r1=false;
     * for(var i=0;i<r0.length;i++) {
     *   if(field==r0[i]) {
     *      r1=true;break;
     *   }
     * }
     * return r1;
     * </pre>
     */
    private Block translateNaryValueRelationalExpression(Context ctx,NaryValueRelationalExpression query) {
        FieldTreeNode fieldMd=ctx.contextNode.resolve(query.getField());
        String globalArr=declareValueArray(ctx,fieldMd,query.getValues());
        Block block=new Block(ctx.topLevel.newGlobal(ctx,query.getOp()==NaryRelationalOperator._in?"false":"true"));
        String loopVar=ctx.newName("i");
        ForLoop forLoop=new ForLoop(loopVar,globalArr+".length");
        block.add(forLoop);
        String tmpCmp=ctx.topLevel.newGlobal(ctx,null);
        if(fieldMd.getType() instanceof DateType) {
            forLoop.add(new SimpleStatement("%s=this.%s-%s[%s]",tmpCmp,ctx.varName(new Name(query.getField())),globalArr,loopVar));
            if(query.getOp()==NaryRelationalOperator._in) {
                forLoop.add(new IfStatement(new SimpleExpression("%s==0",tmpCmp),
                                            new SimpleStatement("%s=true",block.resultVar),
                                            SimpleStatement.S_BREAK));
            } else {
                forLoop.add(new IfStatement(new SimpleExpression("%s==0",tmpCmp),
                                            new SimpleStatement("%s=false",block.resultVar),
                                            SimpleStatement.S_BREAK));
            }
        } else {
            if(query.getOp()==NaryRelationalOperator._in) {
                forLoop.add(new IfStatement(new SimpleExpression("this.%s==%s[%s]",ctx.varName(new Name(query.getField())),globalArr,loopVar),
                                            new SimpleStatement("%s=true",block.resultVar),
                                            SimpleStatement.S_BREAK));
            } else {
                forLoop.add(new IfStatement(new SimpleExpression("this.%s==%s[%s]",ctx.varName(new Name(query.getField())),globalArr,loopVar),
                                            new SimpleStatement("%s=false",block.resultVar),
                                            SimpleStatement.S_BREAK));
            }
        }        
        
        return block;
    }

    /**
     *  <pre>
     *   var r0=new RegExp("pattern","options");
     *   var r1=false;
     *   r1=r0.test(field);
     *   return r1;
     * </pre>
     */
    private Block translateRegexMatchExpression(Context ctx,RegexMatchExpression query) {
        FieldTreeNode fieldMd=ctx.contextNode.resolve(query.getField());
        if(fieldMd.getType() instanceof StringType) {
            Name fieldName=ctx.varName(new Name(query.getField()));
            String regexVar=ctx.topLevel.newGlobal(ctx,String.format("new RegExp(\"%s\",\"%s\")",
                                                                     query.getRegex().replaceAll("\"","\\\""),
                                                                     regexFlags(query)));
            
            Block block=new Block(ctx.topLevel.newGlobalBoolean(ctx));
            block.add(new SimpleStatement("%s=%s.test(this.%s)",
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

    /**
     * <pre>
     *   var r0=false;
     *   r0=this.field == value
     *   return r0
     * </pre>
     */
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
        
        Block block=new Block(ctx.topLevel.newGlobalBoolean(ctx));
        if(valueObject!=null&&fieldMd.getType() instanceof DateType) {
            block.add(new SimpleStatement("%s=this.%s-ISODate('%s')%s 0",block.resultVar,fieldName.toString(),
                                           toISODate((Date)valueObject),BINARY_COMPARISON_OPERATOR_JS_MAP.get(query.getOp())));
        } else {
            block.add(new SimpleStatement("%s=this.%s %s %s",block.resultVar,
                                           fieldName.toString(),
                                           BINARY_COMPARISON_OPERATOR_JS_MAP.get(query.getOp()),
                                           valueObject==null?"null":quote(fieldMd.getType(),valueObject.toString())));
        }
        return block;
    }

    /**
     * <pre>
     *   var r0=false;
     *   for(var i=0;i<this.arr.length;i++) {
     *      elemMatchQuery
     *      if(resultOfElemMatch) {
     *         r0=true;break;
     *      }
     *   }
     *  return r0;
     * </pre>
     */
    private Block translateArrayElemMatch(Context ctx,ArrayMatchExpression query) {
        // An elemMatch expression is a for-loop
        Block block=new Block(ctx.topLevel.newGlobalBoolean(ctx));

        // for (elem:array) 
        ArrForLoop loop=new ArrForLoop(ctx.newName("i"),ctx.varName(new Name(query.getArray())));
        Context newCtx=ctx.enter(ctx.contextNode.resolve(new Path(query.getArray(),Path.ANYPATH)),loop);
        Block queryBlock=translateQuery(newCtx,query.getElemMatch());
        loop.add(queryBlock);
        loop.add(new IfStatement(new SimpleExpression(queryBlock.resultVar),
                                 new SimpleStatement("%s=true",block.resultVar),
                                 SimpleStatement.S_BREAK));
        block.add(loop);
        return block;
    }

    /**
     * <pre>
     *    var r0=false;
     *    {
     *       q1;
     *       q2;...
     *    }
     *    r0=r1&&r2&&...
     *    return r0;
     * </pre>
     */
    private Block translateNaryLogicalExpression(Context ctx,NaryLogicalExpression query) {
        Block block=new Block(ctx.topLevel.newGlobalBoolean(ctx));
        List<String> vars=new ArrayList();
        for(QueryExpression x:query.getQueries()) {
            Block nested=translateQuery(ctx,x);
            vars.add(nested.resultVar);
            block.add(nested);            
        }
        String op=query.getOp()==NaryLogicalOperator._and?"&&":"||";
        block.add(new SimpleStatement("%s=%s",block.resultVar,String.join(op,vars)));
        return block;
    }    

    /**
     * <pre>
     *   var r0=false;
     *   q;
     *   r0=!q
     *   return r0;
     * </pre>
     */
    private Block translateUnaryLogicalExpression(Context ctx,UnaryLogicalExpression query) {
        // Only NOT is a unary operator
        Block block=new Block(ctx.topLevel.newGlobalBoolean(ctx));
        Block nested=translateQuery(ctx,query.getQuery());
        block.add(nested);
        block.add(new SimpleStatement("%s=!%s",block.resultVar,nested.resultVar));
        return block;
    }

    private String quote(Type t,String value) {
        if(t instanceof StringType||t instanceof BigDecimalType|| t instanceof BigIntegerType)
            return String.format("\"%s\"",value);
        else
            return value;
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
