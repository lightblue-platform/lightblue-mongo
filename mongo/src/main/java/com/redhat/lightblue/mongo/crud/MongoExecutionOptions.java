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

import com.mongodb.ReadPreference;

import com.redhat.lightblue.ExecutionOptions;

import com.redhat.lightblue.mongo.config.MongoReadPreference;

/**
 * This class deals with mongodb specific execution options
 */
public class MongoExecutionOptions {

    public static final String OPT_READ_PREFERENCE="mongo:ReadPreference";

    public static final String OPT_READ_PREFERENCE_NEAREST=MongoReadPreference.READ_PREFERENCE_NEAREST;
    public static final String OPT_READ_PREFERENCE_PRIMARY=MongoReadPreference.READ_PREFERENCE_PRIMARY;
    public static final String OPT_READ_PREFERENCE_PRIMARY_PREFERRED=MongoReadPreference.READ_PREFERENCE_PRIMARY_PREFERRED;
    public static final String OPT_READ_PREFERENCE_SECONDARY=MongoReadPreference.READ_PREFERENCE_SECONDARY;
    public static final String OPT_READ_PREFERENCE_SECONDARY_PREFERRED=MongoReadPreference.READ_PREFERENCE_SECONDARY_PREFERRED;
    
    /**
     * Returns a read preference based on the execution options. If the execution options don't specify a
     * read preference, returns null
     *
     * The read preferences can be nearest, primary, primaryPreferred, secondary, and secondaryPreferred, with
     * optional tags. The tags are specified as:
     * <pre>
     *    readPreference ( tags1, tags2,... )
     * <pre>
     * where each 'tags' is a JSON string containing name/value pairs:
     * <pre>
     *    { name:value, name:value, ... }
     * </pre>
     */
    public static ReadPreference getReadPreference(ExecutionOptions options) {
        if(options!=null) {
            String value=options.getOptions().get(OPT_READ_PREFERENCE);
            if(value!=null) {
                value=value.trim();
                if(value.length()>0) {
                    return MongoReadPreference.parse(value);
                }
            }
        } 
        return null;
    }
    
}
