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
package com.redhat.lightblue.mongo.config;

import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import com.mongodb.ReadPreference;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.TaggableReadPreference;
import com.mongodb.TagSet;
import com.mongodb.Tag;

import com.redhat.lightblue.util.JsonUtils;

/**
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
public class MongoReadPreference {

    public static class InvalidReadPreference extends RuntimeException {
        public InvalidReadPreference(String s) {
            super(s);
        }
    }

    public static class InvalidReadPreferenceArgs extends RuntimeException {
        public InvalidReadPreferenceArgs(String s) {
            super(s);
        }
    }
    
    public static final String READ_PREFERENCE_NEAREST="nearest";
    public static final String READ_PREFERENCE_PRIMARY="primary";
    public static final String READ_PREFERENCE_PRIMARY_PREFERRED="primaryPreferred";
    public static final String READ_PREFERENCE_SECONDARY="secondary";
    public static final String READ_PREFERENCE_SECONDARY_PREFERRED="secondaryPreferred";

    public static ReadPreference parse(String value) {
        value=value.trim();
        int paren=value.indexOf('(');
        String pref;
        List<TagSet> tags;
        if(paren!=-1) {
            pref=value.substring(0,paren).trim();
            String argsStr=value.substring(paren+1).trim();
            if(!argsStr.endsWith(")"))
                throw new InvalidReadPreference(value);
            tags=parseArgs(argsStr.substring(0,argsStr.length()-1));
        } else {
            pref=value;
            tags=null;
        }
        switch(pref) {
        case READ_PREFERENCE_NEAREST:
            if(tags==null)
                return ReadPreference.nearest();
            else
                return ReadPreference.nearest(tags);
        case READ_PREFERENCE_PRIMARY:
            return ReadPreference.primary();
        case READ_PREFERENCE_PRIMARY_PREFERRED:
            if(tags==null)
                return ReadPreference.primaryPreferred();
            else
                return ReadPreference.primaryPreferred(tags);
        case READ_PREFERENCE_SECONDARY:
            if(tags==null)
                return ReadPreference.secondary();
            else
                return ReadPreference.secondary(tags);
        case READ_PREFERENCE_SECONDARY_PREFERRED:
            if(tags==null)
                return ReadPreference.secondaryPreferred();
            else
                return ReadPreference.secondaryPreferred(tags);
        default:
            throw new InvalidReadPreference(value);
        }
    }

    
    private static List<TagSet> parseArgs(String args) {
        args=args.trim();
        if(args.length()==0) {
            return null;
        } else {
            try {
                return parseArgs(JsonUtils.json(args));
            } catch(InvalidReadPreferenceArgs x) {
                throw x;                    
            } catch(Exception e) {
                throw new InvalidReadPreferenceArgs(args);
            }
        }
    }

    private static List<TagSet> parseArgs(JsonNode args) {
        List<TagSet> list=new ArrayList<>();
        if(args instanceof ObjectNode) {
            list.add(parseArg((ObjectNode)args));
        } else if(args instanceof ArrayNode) {
            ArrayNode array=(ArrayNode)args;
            for(Iterator<JsonNode> itr=array.elements();itr.hasNext();) {
                list.add(parseArg( (ObjectNode)itr.next() ));
            }
        } else
            throw new InvalidReadPreferenceArgs(args.toString());
        return list;
    }
    
    private static TagSet parseArg(ObjectNode arg) {
        List<Tag> tags=new ArrayList<>();
        for(Iterator<Map.Entry<String,JsonNode>> itr=arg.fields();itr.hasNext();) {
            Map.Entry<String,JsonNode> entry=itr.next();
            tags.add(new Tag(entry.getKey(),entry.getValue().asText()));
        }
        return new TagSet(tags);
    }
}
