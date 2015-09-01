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

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;

import com.redhat.lightblue.metadata.ValueGenerator;
import com.redhat.lightblue.metadata.EntityMetadata;

import com.redhat.lightblue.util.Error;

import com.redhat.lightblue.common.mongo.MongoDataStore;

import com.redhat.lightblue.extensions.valuegenerator.ValueGeneratorSupport;


public class MongoSequenceSupport implements ValueGeneratorSupport {

    private static final Logger LOGGER=LoggerFactory.getLogger(MongoSequenceSupport.class);

    private final MongoCRUDController controller;

    public static final String DEFAULT_COLLECTION_NAME="sequences";

    public static final String PROP_NAME="name";
    public static final String PROP_COLLECTION="collection";
    public static final String PROP_INITIAL_VALUE="initialValue";
    public static final String PROP_INCREMENT="increment";

    private static final ValueGenerator.ValueGeneratorType[] TYPES={ValueGenerator.ValueGeneratorType.IntSequence};
    
    public MongoSequenceSupport(MongoCRUDController controller) {
        this.controller=controller;
    }

    @Override
    public ValueGenerator.ValueGeneratorType[] getSupportedGeneratorTypes() {
        return TYPES;
    }

    @Override
    public Object generateValue(EntityMetadata md,ValueGenerator generator) {
        Properties p=generator.getProperties();
        // We expect to see at least a name for the generator
        String name=p.getProperty(PROP_NAME);
        if(name==null)
            throw Error.get(MongoCrudConstants.ERR_NO_SEQUENCE_NAME);
        String collection=p.getProperty(PROP_COLLECTION);
        if(collection==null)
            collection=DEFAULT_COLLECTION_NAME;
        String initialValueStr=p.getProperty(PROP_INITIAL_VALUE);
        long initialValue;
        if(initialValueStr==null)
            initialValue=1;
        else
            initialValue=Long.valueOf(initialValueStr).longValue();
        String incrementStr=p.getProperty(PROP_INCREMENT);
        long increment;
        if(incrementStr==null)
            increment=1;
        else
            increment=Long.valueOf(incrementStr).longValue();
        DB db=controller.getDbResolver().get((MongoDataStore)md.getDataStore());
        DBCollection coll=db.getCollection(collection);
        MongoSequenceGenerator gen=new MongoSequenceGenerator(coll);
        return gen.getNextSequenceValue(name,initialValue,increment);
    }
}
