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

import com.redhat.lightblue.metadata.FieldTreeNode;

import com.redhat.lightblue.util.Path;

class Context {
    FieldTreeNode contextNode;
    Block contextBlock;
    Function topLevel;
    int nameIndex=0;
    Context parentCtx;
    
    public Context(FieldTreeNode contextNode,Block contextBlock) {
        this.contextNode=contextNode;
        this.contextBlock=contextBlock;
    }
    
    public Context enter(FieldTreeNode node,Block parent) {
        Context ctx=new Context(node,parent);
        ctx.topLevel=topLevel;
        ctx.parentCtx=this;
        return ctx;
    }

    public Context copy() {
        Context ctx=new Context(contextNode,contextBlock);
        ctx.topLevel=topLevel;
        ctx.parentCtx=parentCtx;
        return ctx;
    }
    
    public Name varName(Name localName) {
        Name p=new Name();
        if(contextBlock!=null)
            p.add(contextBlock.getDocumentLoopVarAsPrefix());
        int n=localName.length();
        for(int i=0;i<n;i++) {
            Name.Part seg=localName.getPart(i);
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

    public String newName(String prefix) {
        if(parentCtx!=null)
            return parentCtx.newName(prefix);
        else
            return prefix+Integer.toString(nameIndex++);
    }
}
