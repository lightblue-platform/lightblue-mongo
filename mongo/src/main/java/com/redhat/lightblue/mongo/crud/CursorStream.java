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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;
import com.mongodb.DBCursor;

import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.DocumentStream;

public class CursorStream implements DocumentStream<DocCtx> {
    private static final Logger RESULTSET_LOGGER = LoggerFactory.getLogger("com.redhat.lightblue.crud.mongo.slowresults");

    private final DBCursor cursor;
    private final DocTranslator translator;
    private long retrievalStart=0;
    private int dataSize=0;
    private final DBObject mongoQuery;
    private final long executionTime;
    private final long from;
    private final long to;

    public CursorStream(DBCursor cursor,DocTranslator translator,DBObject mongoQuery,long executionTime,long from,long to) {
        this.cursor=cursor;
        this.translator=translator;
        this.mongoQuery=mongoQuery;
        this.executionTime=executionTime;
        this.from=from;
        this.to=to;
    }

    @Override
    public boolean hasNext() {
        boolean next=cursor.hasNext();
        return next;
    }

    @Override
    public DocCtx next() {
        if(retrievalStart==0)
            retrievalStart=System.currentTimeMillis();
        DBObject obj=cursor.next();
        DocTranslator.TranslatedDoc d=translator.toJson(obj);
        dataSize+=DocTranslator.size(d);
        if(!hasNext()) {
            long retrievalTime=System.currentTimeMillis()-retrievalStart;
            if (RESULTSET_LOGGER.isDebugEnabled() && (retrievalTime > 100 ) ) {
                RESULTSET_LOGGER.debug("execution_time={}, retrieval_time={}, resultset_size={}, data_size={}, query={}, from={}, to={}",
                                       executionTime, retrievalTime, cursor.numSeen(),dataSize,
                                       mongoQuery, from, to);
            }
        }
        
        DocCtx ctx=new DocCtx(d.doc,d.rmd);
        ctx.setCRUDOperationPerformed(CRUDOperation.FIND);
        return ctx;
    }

    @Override
    public void close() {
        try{
            cursor.close();
        } catch (Exception e) {}
    }
        
}
