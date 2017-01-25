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

import java.util.List;
import java.util.Objects;

import org.bson.types.ObjectId;

import com.mongodb.DBObject;

import com.redhat.lightblue.metadata.Type;

/**
 * A representation of document version. Contains id:version as a pair
 */
public final class DocIdVersion {
    public final Object id;
    public final ObjectId version;
    
    public DocIdVersion(Object id,ObjectId version) {
        this.id=id;
        this.version=version;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id)+Objects.hashCode(version);
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof DocIdVersion) {
            return Objects.equals( ((DocIdVersion)o).id,id)&&
                Objects.equals( ((DocIdVersion)o).version,version);
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("%s:%s",id.toString(),version);
    }

    static public DocIdVersion valueOf(String s,Type idType) {
        int index=s.indexOf(":");
        if(index!=-1)
            return new DocIdVersion(DocTranslator.createIdFrom(idType.cast(s.substring(0,index))),
                                    new ObjectId(s.substring(index+1)));
        else
            throw new IllegalArgumentException(s);
    }

    static public DocIdVersion getDocumentVersion(DBObject document) {
        List<ObjectId> list=DocVerUtil.getVersionList(document);
        if(list!=null&&!list.isEmpty())
            return new DocIdVersion(DocTranslator.createIdFrom(document.get("_id")),list.get(0));
        else
            return null;
    }
}
