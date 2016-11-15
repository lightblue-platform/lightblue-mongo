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

public class Block extends Statement {
    protected ArrayList<Statement> statements=new ArrayList<>();
    protected String resultVar;

    public Block(String resultVar,Statement...x) {
        this.resultVar=resultVar;
        for(Statement e:x)
            add(e);
    }
    
    public Block(String resultVar) {
        this.resultVar=resultVar;
    }
    
    public Block(Statement...x) {
        this(null,x);
    }
    
    public Block() {
        this((String)null);
    }

    public void add(Statement x) {
        statements.add(x);
        x.parent=this;
    }
    
    @Override
    public StringBuilder appendToStr(StringBuilder bld) {
        bld.append("{");
        for(Statement x:statements) {
            x.appendToStr(bld);
        }
        return bld.append("}");
    }
}
