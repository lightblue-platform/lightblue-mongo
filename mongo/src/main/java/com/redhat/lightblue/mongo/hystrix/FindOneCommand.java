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
package com.redhat.lightblue.mongo.hystrix;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import java.util.concurrent.TimeUnit;

/**
 * Hystrix command for executing findOne on a MongoDB collection.
 *
 * @author nmalik
 */
public class FindOneCommand extends AbstractMongoCommand<DBObject> {
    private final DBObject query;
    private final DBObject projection;
    private long maxQueryTimeMS;

    /**
     *
     * @param clientKey used to set thread pool key
     * @param query
     * @deprecated Use other constructor
     */
    @Deprecated
    public FindOneCommand(DBCollection collection, DBObject query) {
        this(collection,query,null,-1);
    }

    /**
     * 
     * @param collection the collection
     * @param query the query
     * @param projection the projection (optional, null means no projection)
     * @param maxQueryTimeMS the max time query should run on database in milliseconds, <=0 means no limit
     */
    public FindOneCommand(DBCollection collection, DBObject query, DBObject projection, long maxQueryTimeMS) {
        super(FindOneCommand.class.getSimpleName(), collection);
        this.query = query;
        this.projection = projection;
        this.maxQueryTimeMS = maxQueryTimeMS;
    }


    @Override
    protected DBObject runMongoCommand() {
        DBObject q=query==null?new BasicDBObject():query;
        DBCursor cursor;
        if(projection == null) {
            cursor = getDBCollection().find(q);
        } else {
            cursor = getDBCollection().find(q, projection);
        }
        if (maxQueryTimeMS > 0) {
            cursor.maxTime(maxQueryTimeMS, TimeUnit.MILLISECONDS);
        }
        return cursor.one();
    }
}
