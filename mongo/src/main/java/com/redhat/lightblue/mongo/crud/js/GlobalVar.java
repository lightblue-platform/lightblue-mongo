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

/**
 * A global variable. Defines the variable name and initialization
 *
 * <pre>
 *    var name=init;
 * </pre>
 */
public class GlobalVar implements Str {
    protected final String name;
    protected final String init;
    
    public GlobalVar(String name,String init) {
        this.name=name;
        this.init=init;
    }
    
    @Override
    public StringBuilder appendToStr(StringBuilder bld) {
        if(init==null)
            return bld.append(String.format("var %1$s;",name));
        else
            return bld.append(String.format("var %1$s=%2$s;",name,init));
    }

    @Override
    public String toString() {
        return appendToStr(new StringBuilder()).toString();
    }
}
