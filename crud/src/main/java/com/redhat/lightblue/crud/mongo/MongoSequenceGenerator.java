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
package com.redhat.lightblue.crud.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.WriteConcern;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoSequenceGenerator {

    public static final String NAME="name";
    public static final String INIT="initialValue";
    public static final String INC="increment";
    public static final String VALUE="value";

    private static final Logger LOGGER=LoggerFactory.getLogger(MongoSequenceGenerator.class);
    
    private DBCollection coll;

    public MongoSequenceGenerator(DBCollection coll) {
        this.coll=coll;
    }

    private void initIndex() {
        // Make sure we have a unique index on name
        BasicDBObject keys=new BasicDBObject(NAME,1);
        BasicDBObject options=new BasicDBObject("unique",1);
        coll.ensureIndex(keys,options);
    }
   
    public long getNextSequenceValue(String name,long init,long inc) {
        LOGGER.debug("getNextSequenceValue({})",name);
        BasicDBObject q=new BasicDBObject(NAME,name);
        DBObject doc=coll.findOne(q);
        if(doc==null) {
            LOGGER.debug("inserting sequence record name={}, init={}, inc={}",name,init,inc);
            if(inc==0)
                inc=1;
            // Here, we also make sure we have the indexes setup properly
            initIndex();
            BasicDBObject u=new BasicDBObject().
                append(NAME,name).
                append(INIT,init).
                append(INC,inc).
                append(VALUE,init);
            try {
                coll.insert(u,WriteConcern.SAFE);
            } catch (Exception e) {
                LOGGER.debug("Insertion failed with {}, trying to read",e);
            }
            doc=coll.findOne(q);
            if(doc==null)
                throw new RuntimeException("Cannot generate value for "+name);
        }
        LOGGER.debug("Sequence doc={}",doc);
        Long increment=(Long)doc.get(INC);
        BasicDBObject u=new BasicDBObject().
            append("$inc",new BasicDBObject(VALUE,increment));
        doc=coll.findAndModify(q,u);
        Long l=(Long)doc.get(VALUE);
        LOGGER.debug("{} -> {}",name,l);
        return l;
    }
}
