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

public class IfStatement extends Block {
    
    protected Expression test;
    
    public IfStatement(Expression test,Statement...s) {
        super(s);
        this.test=test;
    }

    public static IfStatement ifDefined(Name var,Statement...s) {
        StringBuffer b=new StringBuffer(64);
        for(int i=0;i<var.length();i++) {
        }
        return new IfStatement(new SimpleExpression(buildNotUndefined(var)),s);
    }
    
    public static IfStatement ifDefined(Name var1, Name var2 ,Statement...s) {
        return new IfStatement(new SimpleExpression(buildNotUndefined(var1)+"&&"+buildNotUndefined(var2)),s);
    }
    
    private static String buildNotUndefined(Name var) {
        StringBuilder b=new StringBuilder(64);
        for(int i=0;i<var.length();i++) {
            if(b.length()>0) {
                b.append("&&");
            }
            b.append("typeof this.");
            b.append(var.getPrefix(i+1));
            b.append("!='undefined'");
        }
        return b.toString();
    }
    
    @Override
    public StringBuilder appendToStr(StringBuilder bld) {
        bld.append("if(");
        test.appendToStr(bld);
        bld.append(")");
        return super.appendToStr(bld);
    }
}

