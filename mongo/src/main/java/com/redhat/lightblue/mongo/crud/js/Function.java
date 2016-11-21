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

import java.util.ArrayList;

public class Function extends Statement {
    
    protected Block block;
    protected final ArrayList<GlobalVar> globals=new ArrayList<>();
    
    public Function(Block block) {
        this.block=block;
    }
    
    public Function() {
    }
    
    public void global(String name,String initExpr) {
        globals.add(new GlobalVar(name,initExpr));
    }
    
    public String newGlobal(Context ctx,String initExpr) {
        String name=ctx.newName("r");
        global(name,initExpr);
        return name;
    }
    
    public String newGlobalBoolean(Context ctx) {
        String name=ctx.newName("r");
        global(name,"false");
        return name;
    }
    
    public String newGlobalInt(Context ctx) {
        String name=ctx.newName("r");
        global(name,"0");
        return name;
    }
    
    @Override
    public StringBuilder appendToStr(StringBuilder bld) {
        bld.append("function() {");
        for(GlobalVar g:globals) {
            bld.append(g.toString());
        }
        bld.append(block.toString());
        bld.append("return ").append(block.resultVar).append(';');
        bld.append("}");
        return bld;
    }
}

