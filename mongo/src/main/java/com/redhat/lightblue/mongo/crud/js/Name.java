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

import com.redhat.lightblue.util.Path;

public class Name {

    public static class Part {
        boolean index;
        String name;
        
        public Part(String name,boolean index) {
            this.name=name;
            this.index=index;
        }        
    }
    
    
    ArrayList<Part> parts=new ArrayList<>();
    
    public Name() {}
    
    public Name(Name n) {
        parts.addAll(n.parts);
    }
    
    public Name(Path p) {
        int n=p.numSegments();
        for(int i=0;i<n;i++) {
            String s=p.head(i);
            parts.add(new Part(s,p.isIndex(i)));
        }
    }
    
    public Name add(String name,boolean index) {
        parts.add(new Part(name,index));
        return this;
    }
    
    public Name add(Name n) {
        parts.addAll(n.parts);
        return this;
    }
    
    public Name add(Part n) {
        parts.add(n);
        return this;
    }
    
    public int length() {
        return parts.size();
    }
    
    public String get(int i) {
        return getPart(i).name;
    }
    
    public Part getPart(int i) {
        return parts.get(i);
    }
    
    public Name removeLast() {
        parts.remove(parts.size()-1);
        return this;
    }
    
    @Override
    public String toString() {
        StringBuilder bld=new StringBuilder();
        boolean first=true;
        for(Part p:parts) {
            if(p.index)
                bld.append('[').append(p.name).append(']');
            else {
                if(bld.length()>0) {
                    bld.append('.');
                }
                bld.append(p.name);
            }
        }
        return bld.toString();
    }
}
