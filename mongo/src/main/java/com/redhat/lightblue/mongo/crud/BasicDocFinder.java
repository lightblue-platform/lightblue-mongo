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

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.crud.ListDocumentStream;
import com.redhat.lightblue.util.Error;

/**
 * Basic doc search operation
 */
public class BasicDocFinder implements DocFinder {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicDocFinder.class);
    private static final Logger RESULTSET_LOGGER = LoggerFactory.getLogger("com.redhat.lightblue.crud.mongo.slowresults");

    private final DocTranslator translator;
    private ReadPreference readPreference;
    private int maxResultSetSize = 0;
    private long maxQueryTimeMS = 0;

    public BasicDocFinder(DocTranslator translator, ReadPreference readPreference) {
        this.translator = translator;
        this.readPreference = readPreference;
    }

    @Override
    public void setMaxResultSetSize(int size) {
        maxResultSetSize = size;
    }

    @Override
    public void setMaxQueryTimeMS(long maxQueryTimeMS) {
        this.maxQueryTimeMS = maxQueryTimeMS;
    }

    @Override
    public long find(CRUDOperationContext ctx,
                     DBCollection coll,
                     DBObject mongoQuery,
                     DBObject mongoProjection,
                     DBObject mongoSort,
                     Long from,
                     Long to) {
        LOGGER.debug("Submitting query {}", mongoQuery);

        long executionTime = System.currentTimeMillis();
        DBCursor cursor = null;
        boolean cursorInUse=false;
        try {
            cursor = coll.find(mongoQuery, mongoProjection);
            if (readPreference != null) {
                cursor.setReadPreference(readPreference);
            }

            if (ctx.isLimitQueryTime()&&maxQueryTimeMS > 0) {
                cursor.maxTime(maxQueryTimeMS, TimeUnit.MILLISECONDS);
            }

            executionTime = System.currentTimeMillis() - executionTime;

            LOGGER.debug("Query evaluated");
            if (mongoSort != null) {
                cursor = cursor.sort(mongoSort);
                LOGGER.debug("Result set sorted");
            }
            
            LOGGER.debug("Applying limits: {} - {}", from, to);
            boolean retrieve=true;
            int nRetrieve=0;
            int numMatched=0;
            // f and t are from and to indexes, both inclusive
            int f=from==null?0:from.intValue();
            if(f<0) {
                f=0;
            }
            cursor.skip(f);
            if(ctx.isComputeCounts()) {
                numMatched=cursor.count();
            }
            int t;
            if(to!=null) {
                t=to.intValue();
                if(t<f) {
                    retrieve=false;
                } else {
                    cursor.limit(nRetrieve=t-f+1);
                }
            } else {                
                if(ctx.isComputeCounts()) {
                    t=numMatched-1;
                    nRetrieve=numMatched-f;
                } else {
                    t=Integer.MAX_VALUE;
                }
            }
            if(retrieve) {
                LOGGER.debug("Retrieving results");
                CursorStream stream=new CursorStream(cursor,translator,mongoQuery,executionTime,f,t);
                ctx.setDocumentStream(stream);
                cursorInUse=true;
            } else {
            	ctx.setDocumentStream(new ListDocumentStream<DocCtx>(new ArrayList<>()));
            }
            if (RESULTSET_LOGGER.isDebugEnabled() && (executionTime > 100 ) ) {
                RESULTSET_LOGGER.debug("execution_time={}, query={}, from={}, to={}",
                                       executionTime, 
                                       mongoQuery,
                                       f, t);
            }            
            return numMatched;
        } finally {
            if(cursor!=null&&!cursorInUse) {
                cursor.close();
            }
        }
    }

}
