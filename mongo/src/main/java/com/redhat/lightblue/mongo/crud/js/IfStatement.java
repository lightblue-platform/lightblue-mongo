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

public class IfStatement extends Statement {
    
    protected Expression test;
    protected Block trueBlock;
    protected Block elseBlock;
    
    public IfStatement(Expression test,Block trueBlock,Block elseBlock) {
        this.test=test;
        this.trueBlock=trueBlock;
        this.elseBlock=elseBlock;
        if(trueBlock!=null)
            trueBlock.parent=this;
        if(elseBlock!=null)
            elseBlock.parent=this;
    }
    
    public IfStatement(Expression test,Block trueBlock) {
        this(test,trueBlock,null);
    }
    
    @Override
    public StringBuilder appendToStr(StringBuilder bld) {
        if(elseBlock==null) {
            return bld.append(String.format("if(%1$s) %2$s ",test.toString(),trueBlock.toString()));
        } else {
            return bld.append(String.format("if(%1$s) %2$s else %3$s ",test.toString(),trueBlock.toString(),elseBlock.toString()));
        }
    }
}

