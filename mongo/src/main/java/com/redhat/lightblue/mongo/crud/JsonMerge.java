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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import com.redhat.lightblue.metadata.EntityMetadata;

import com.redhat.lightblue.util.JsonCompare;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.MutablePath;
import com.redhat.lightblue.util.JsonDoc;

/**
 * During a save operation, the document provided by the client replaces the
 * copy in the database. If the client has a more limited view of data than is
 * already present (i.e. client using an earlier version of metadata), then
 * there may be some invisible fields, fields that are in the document, but not
 * in the metadata used by the client. To prevent overwriting those fields, we
 * perform a merge operation: all invisible fields are preserved in the updated
 * document.
 */
public final class JsonMerge {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonMerge.class);

    private final EntityMetadata md;

    public JsonMerge(EntityMetadata md) {
        this.md=md;
    }

    public void merge(JsonNode oldDoc,JsonNode newDoc) {
        JsonCompare cmp=md.getEntitySchema().getDocComparator();
        try {
            LOGGER.debug("Merge start");
            JsonCompare.Difference diff=cmp.compareNodes(oldDoc,newDoc);
            LOGGER.debug("Diff:{}"+diff);
            // We look for things removed in the new document
            for(JsonCompare.Delta delta:diff.getDelta()) {
                if(delta instanceof JsonCompare.Removal) {
                    JsonCompare.Removal removal=(JsonCompare.Removal)delta;
                    Path removedField=removal.getField1();
                    
                    boolean hidden=true;
                    
                    // If this is a removed array element, we don't add it back
                    int numSegments=removedField.numSegments();
                    if(numSegments>1&&removedField.isIndex(numSegments-1)) {
                        hidden=false;
                    }
                    if(hidden) {
                        // Is the removed field in metadata?
                        try {
                            md.resolve(removedField);
                            hidden=false;
                        } catch (Exception e) {
                            hidden=true;
                        }
                        if(hidden) {
                            LOGGER.debug("Field {} is hidden but removed, adding it back",removedField);
                            addField(newDoc,diff.getDelta(),removal);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void addField(JsonNode doc,
                          List<JsonCompare.Delta> delta,
                          JsonCompare.Removal removedField) {
        Path field=removedField.getField1();
        // 'field' is the name of the removed field in the old document.
        // We have to find the field name it has in the new document, because array elements might have moved around
        MutablePath newFieldName=new MutablePath();
        int n=field.numSegments();
        for(int i=0;i<n;i++) {
            if(field.isIndex(i)) {
                // At this point, newFieldName points to an array,
                // field.prefix(i) points to an array element. If there
                // is a Move delta for the move of field.prefix(i+1)
                // -> someField, then we get the new index from
                // someField
                Path arrayElement=field.prefix(i);
                // arrayElement.tail(0) is an index
                Path movedTo=null;
                for(JsonCompare.Delta d:delta) {
                    if(d instanceof JsonCompare.Move) {
                        JsonCompare.Move move=(JsonCompare.Move)d;
                        if(move.getField1().equals(arrayElement)) {
                            movedTo=move.getField2();
                            break;
                        }
                    }
                }
                if(movedTo!=null) {
                    // arrayElement moved to somewhere else, get the new index, push it to the newFieldName
                    newFieldName.push(movedTo.tail(0));
                } else {
                    // ArrayElement did not move
                    newFieldName.push(field.head(i));
                }
            } else {
                newFieldName.push(field.head(i));
            }
        }
        // At this point, newFieldName contains the field in the new document
        LOGGER.debug("Adding contents of {} to {}",field,newFieldName);
        JsonDoc.modify(doc,newFieldName,removedField.getRemovedNode(),true);
    }
}
