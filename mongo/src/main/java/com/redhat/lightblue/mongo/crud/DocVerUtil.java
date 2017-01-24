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
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;

import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

import org.bson.types.ObjectId;

public class DocVerUtil {

    public static final String DOCVER="docver";

    public static final long TOO_OLD_MS=1l*60l*1000l; // Any docver older than 1 minute is to old

    /**
     * Returns the @mongoHidden at the root level of the document. Adds one if necessary.
     */
    public static DBObject getHidden(DBObject doc,boolean addIfNotFound) {
        DBObject hidden=(DBObject)doc.get(DocTranslator.HIDDEN_SUB_PATH.toString());
        if(hidden==null&&addIfNotFound) {
            doc.put(DocTranslator.HIDDEN_SUB_PATH.toString(),hidden=new BasicDBObject());            
        }
        return hidden;
    }

    /**
     * Returns the version list, if one present
     */
    public static List<ObjectId> getVersionList(DBObject doc) {
        DBObject hidden=(DBObject)doc.get(DocTranslator.HIDDEN_SUB_PATH.toString());
        if(hidden!=null) {
            return (List<ObjectId>)hidden.get(DOCVER);
        }
        return null;
    }

    /**
     * Clears doc version, and rewrites it to contain only a single docver
     */
    public static void overwriteDocVer(DBObject doc,ObjectId docver) {
        getHidden(doc,true).removeField(DOCVER);
        setDocVer(doc,docver);
    }

    /**
     * Adds the given version to the top of the docver list
     */
    public static void setDocVer(DBObject doc,ObjectId docver) {
        DBObject hidden=getHidden(doc,true);
        List<ObjectId> list=(List<ObjectId>)hidden.get(DOCVER);
        if(list==null) {
            list=new ArrayList<ObjectId>();
        }
        list.add(0,docver);
        hidden.put(DOCVER,list);
    }

    /**
     * Copies the hidden field from the source doc to the dest doc
     */
    public static void copyDocVer(DBObject destDoc,DBObject sourceDoc) {
        DBObject hidden=getHidden(sourceDoc,false);
        if(hidden!=null) {
            destDoc.put(DocTranslator.HIDDEN_SUB_PATH.toString(),hidden);
        }
    }

    /**
     * Removes old docvers from a document
     */
    public static void cleanupOldDocVer(DBObject doc,ObjectId docVer) {
        DBObject hidden=getHidden(doc,false);
        if(hidden!=null) {
            List<ObjectId> list=(List<ObjectId>)hidden.get(DOCVER);
            if(list!=null) {
                List<ObjectId> copy=new ArrayList<>(list.size());
                long now=docVer.getDate().getTime();
                for(ObjectId id:list) {
                    if(!id.equals(docVer)) {
                        Date d=id.getDate();
                        if(now-d.getTime()<TOO_OLD_MS) {
                            copy.add(id);
                        }
                    } else {
                        copy.add(id);
                    }
                }
                if(copy.size()!=list.size()) {
                    hidden.put(DOCVER,copy);
                }
            }
        }
    }

}
