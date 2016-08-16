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

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.ArrayList;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.BasicDBObject;

public final class Utils {

    /**
     * Returns the _ids of documents that are failed to be updated
     *
     * @param collection The DB collection
     * @param currentDocVer The docver of the current operation. The
     * docversion field of all docs are set to this value for the
     * current operation
     * @param idToDocVerMap A mapping of doc _id to the old transaction
     * id of the document
     *
     * This algorithm will catch all modifications to documents
     * between they are first read and the update attempt. However,
     * since there is a time window where someone else may update the
     * documents until this algorithm is complete, there is a chance
     * that a successfully updated document may appear as a concurrent
     * update even though the actual update took place after our
     * update. So, 
     *
     * @return A set of _ids whose `document version is not equal to currentDocVer
     */
    public static Set<Object> checkFailedUpdates(DBCollection collection,
                                                 String currentDocVer,
                                                 Map<Object,String> idToDocVerMap) {
        // We would like to find all _id:currentDocVer documents. Those
        // that are missing, i.e. not in the resultset, are the docs
        // that are modified during or after our update operation
        DBCursor cursor=null;
        try {
            Set<Object> keys=idToDocVerMap.keySet();
            cursor=collection.find(new BasicDBObject(Translator.DOC_VERSION_FULLPATH_STR,currentDocVer).
                                   append(Translator.ID_STR,new BasicDBObject("$in",new ArrayList<>(keys))),
                                   new BasicDBObject(Translator.ID_STR,1)); 
            Set<Object> failedIds=new HashSet<>(keys);
            while(cursor.hasNext()) {
                failedIds.remove(cursor.next().get(Translator.ID_STR));
            }
            return failedIds;
        } finally {
            if(cursor!=null) {
                try {
                    cursor.close();
                } catch (Exception e) {}
            }
        }
    }
    
    private Utils() {}
}
