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

import com.redhat.lightblue.util.JsonUtils;

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

    private static final class Tags {
        private final DBObject first;
        private final DBObject[] remaining;

        public Tags(DBObject x) {
            first=x;
            remaining=null;
        }
        
        public Tags(List<DBObject> list) {
            first=list.get(0);
            if(list.size()==1) {
                remaining=null;
            } else {
                remaining=list.subList(1,list.size()).toArray(new DBObject[list.size()-1]);;
            }
        }

        public TaggableReadPreference nearest() {
            if(remaining==null)
                return ReadPreference.nearest(first);
            else
                return ReadPreference.nearest(first,remaining);
        }

        public TaggableReadPreference primaryPreferred() {
            if(remaining==null)
                return ReadPreference.primaryPreferred(first);
            else
                return ReadPreference.primaryPreferred(first,remaining);
        }

        public TaggableReadPreference secondaryPreferred() {
            if(remaining==null)
                return ReadPreference.secondaryPreferred(first);
            else
                return ReadPreference.secondaryPreferred(first,remaining);
        }

        public TaggableReadPreference secondary() {
            if(remaining==null)
                return ReadPreference.secondary(first);
            else
                return ReadPreference.secondary(first,remaining);
        }
    }

    public static ReadPreference parse(String value) {
        value=value.trim();
        int paren=value.indexOf('(');
        String pref;
        Tags tags;
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
                return tags.nearest();
        case READ_PREFERENCE_PRIMARY:
            return ReadPreference.primary();
        case READ_PREFERENCE_PRIMARY_PREFERRED:
            if(tags==null)
                return ReadPreference.primaryPreferred();
            else
                return tags.primaryPreferred();
        case READ_PREFERENCE_SECONDARY:
            if(tags==null)
                return ReadPreference.secondary();
            else
                return tags.secondary();
        case READ_PREFERENCE_SECONDARY_PREFERRED:
            if(tags==null)
                return ReadPreference.secondaryPreferred();
            else
                return tags.secondaryPreferred();
        default:
            throw new InvalidReadPreference(value);
        }
    }

    
    private static Tags parseArgs(String args) {
        args=args.trim();
        if(args.length()==0) {
            return null;
        } else {
            try {
                return parseArgs(JsonUtils.json(args));
            } catch(Exception e) {
                throw new InvalidReadPreferenceArgs(args);
            }
        }
    }

    private static Tags parseArgs(JsonNode args) {
        if(args instanceof ObjectNode) {
            return new Tags(parseArg((ObjectNode)args));
        } else if(args instanceof ArrayNode) {
            ArrayNode array=(ArrayNode)args;
            List<DBObject> list=new ArrayList<>(array.size());
            for(Iterator<JsonNode> itr=array.elements();itr.hasNext();) {
                list.add(parseArg( (ObjectNode)itr.next() ));
            }
            return new Tags(list);
        } else
            throw new InvalidReadPreferenceArgs(args.toString());
    }
    
    private static DBObject parseArg(ObjectNode arg) {
        BasicDBObject ret=new BasicDBObject();
        for(Iterator<Map.Entry<String,JsonNode>> itr=arg.fields();itr.hasNext();) {
            Map.Entry<String,JsonNode> entry=itr.next();
            ret.append(entry.getKey(),entry.getValue().asText());
        }
        return ret;
    }
}
