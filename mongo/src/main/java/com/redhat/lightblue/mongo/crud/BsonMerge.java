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
import java.util.Map;
import java.util.Iterator;
import java.util.Collection;
import java.util.HashSet;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

import com.redhat.lightblue.metadata.EntityMetadata;

import com.redhat.lightblue.util.DocComparator;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.util.MutablePath;

/**
 * During a save operation, the document provided by the client replaces the
 * copy in the database. If the client has a more limited view of data than is
 * already present (i.e. client using an earlier version of metadata), then
 * there may be some invisible fields, fields that are in the document, but not
 * in the metadata used by the client. To prevent overwriting those fields, we
 * perform a merge operation: all invisible fields are preserved in the updated
 * document.
 */
public final class BsonMerge extends DocComparator<Object, Object, DBObject, List> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BsonMerge.class);

    private final EntityMetadata md;
    private final HashSet<Path> ignoredIdentities=new HashSet<>();

    public BsonMerge(EntityMetadata md) {
        this.md = md;
        Map<Path, List<Path>> idMap = md.getEntitySchema().getArrayIdentities();
        for (Map.Entry<Path, List<Path>> entry : idMap.entrySet()) {
            addArrayIdentity(entry.getKey(), entry.getValue().toArray(new Path[entry.getValue().size()]));
        }
    }

    public class BsonIdentityExtractor implements IdentityExtractor<Object> {
        private final Path[] fields;

        public BsonIdentityExtractor(ArrayIdentityFields fields) {
            this.fields = fields.getFields();
        }

        @Override
        public Object getIdentity(Object element) {
            Object[] nodes = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                nodes[i] = DocTranslator.getDBObject((DBObject) element, fields[i]);
            }            
            return new DefaultIdentity(nodes);
        }
    }

    @Override
    protected boolean isValue(Object value) {
        return !(value instanceof Collection)
                && !(value instanceof DBObject);
    }

    @Override
    protected boolean isArray(Object value) {
        return value instanceof Collection;
    }

    @Override
    protected boolean isObject(Object value) {
        return value instanceof DBObject;
    }

    @Override
    protected boolean isNull(Object value) {
        return value == null;
    }

    @Override
    protected Object asValue(Object value) {
        return value;
    }

    @Override
    protected List asArray(Object value) {
        return (List) value;
    }

    @Override
    protected DBObject asObject(Object value) {
        return (DBObject) value;
    }

    @Override
    protected boolean equals(Object value1, Object value2) {
        return (value1 == null && value2 == null)
                || (value1 != null && value1.equals(value2));
    }

    @Override
    protected Iterator<Map.Entry<String, Object>> getFields(DBObject node) {
        return node.toMap().entrySet().iterator();
    }

    @Override
    protected boolean hasField(DBObject value, String field) {
        return value.containsField(field);
    }

    @Override
    protected Object getField(DBObject value, String field) {
        return value.get(field);
    }

    @Override
    protected IdentityExtractor getArrayIdentityExtractorImpl(ArrayIdentityFields fields) {
        return new BsonIdentityExtractor(fields);
    }

    @Override
    protected Object getElement(List value, int index) {
        return value.get(index);
    }

    @Override
    protected int size(List value) {
        return value.size();
    }

    @Override
    public IdentityExtractor getArrayIdentityExtractor(Path arrayField) {
        MutablePath p = new MutablePath();
        int n = arrayField.numSegments();
        for (int i = 0; i < n; i++) {
            if (arrayField.isIndex(i)) {
                p.push(Path.ANY);
            } else {
                p.push(arrayField.head(i));
            }
        }
        if(ignoredIdentities.contains(p))
            return null;
        else {
            ArrayIdentityFields fields = getArrayIdentities().get(p);
            if (fields != null) {
                return getArrayIdentityExtractorImpl(fields);
            }
        }
        return null;
    }
    
        /**
     * Input is a path to an array element
     * Output is the array containing that element, and all indexes replaced with *
     */
    private Path getArray(Path p) {
        int lastIndex=p.numSegments()-1;
        while(lastIndex>0) {
            if(p.isIndex(lastIndex))
                break;
            lastIndex--;
        }
        MutablePath mp=new MutablePath();
        for(int i=0;i<lastIndex;i++) {
            if(p.isIndex(i))
                mp.push(Path.ANY);
            else
                mp.push(p.head(i));
        }
        return mp.immutableCopy();
    }

    public DocComparator.Difference<Object> compareNodesWithFallback(DBObject oldDoc,DBObject newDoc) throws Exception {
        // Compare two docs. If we get a duplicate array identity exception, remove that array identity from
        // the identity map, and compare again. This is to deal with docs that contain duplicate array identities either
        // because the identity constraint is added after data is screwed up, or becase we forgot to validate it

        int n=getArrayIdentities().size()+1;
        // This can loop at most the number of array identities times + 1 (if there are none, it should do it once)
        for(int i=0;i<n;i++) {
            try {
                return compareNodes(oldDoc,newDoc);
            } catch(DocComparator.DuplicateArrayIdentity dai) {
                LOGGER.warn("Duplicate array element id for doc._id:{}, field:{}",oldDoc.get("_id"),dai.getPath());
                Path p=dai.getPath();
                ignoredIdentities.add(getArray(p));
                LOGGER.debug("Ignored paths:{}",ignoredIdentities);
            } catch(DocComparator.InvalidArrayIdentity iai) {
                LOGGER.warn("Invalid array element id for doc._id:{}, field:{}",oldDoc.get("_id"),iai.getPath());
                Path p=iai.getPath();
                ignoredIdentities.add(getArray(p));
                LOGGER.debug("Ignored paths:{}",ignoredIdentities);
            }
        }
        throw new RuntimeException("Cannot compute document diffs even with ignored identities:"+ignoredIdentities);
    }

    /**
     * Copies all fields in oldDoc that are not in metadata into newDoc
     *
     * @return true if some fields are copied
     */
    public boolean merge(DBObject oldDoc, DBObject newDoc) {
        boolean ret = false;
        try {
            LOGGER.debug("Merge start");
            DocComparator.Difference<Object> diff = compareNodesWithFallback(oldDoc, newDoc);
            LOGGER.debug("Diff:{}" + diff);
            // We look for things removed in the new document
            for (DocComparator.Delta<Object> delta : diff.getDelta()) {
                if (delta instanceof DocComparator.Removal) {
                    DocComparator.Removal<Object> removal = (DocComparator.Removal<Object>) delta;
                    Path removedField = removal.getField1();

                    boolean hidden = true;

                    // If this is a removed array element, we don't add it back
                    int numSegments = removedField.numSegments();
                    if (numSegments > 1 && removedField.isIndex(numSegments - 1)) {
                        hidden = false;
                    }
                    if (hidden) {
                        // Is the removed field in metadata?
                        try {
                            // don't try to resolve a hidden field
                            if (!hasHiddenField(removedField)) {
                                md.resolve(removedField);
                                hidden = false;
                            } else {
                                hidden = true;
                            }
                        } catch (Exception e) {
                            hidden = true;
                        }
                        if (hidden) {
                            LOGGER.debug("Field {} is hidden but removed, adding it back", removedField);
                            addField(newDoc, diff.getDelta(), removal);
                            ret = true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error in merge:{}",e,e);
            throw new RuntimeException(e);
        }
        return ret;
    }

    private boolean hasHiddenField(Path path) {
        for (int i = 0; i < path.numSegments(); i++) {
            if (path.head(i).equals(DocTranslator.HIDDEN_SUB_PATH.toString())) {
                return true;
            }
        }
        return false;
    }

    private void addField(DBObject doc,
                          List<DocComparator.Delta<Object>> delta,
                          DocComparator.Removal<Object> removedField) {
        Path field = removedField.getField1();
        // 'field' is the name of the removed field in the old document.
        // We have to find the field name it has in the new document, because array elements might have moved around
        MutablePath newFieldName = new MutablePath();
        int n = field.numSegments();
        for (int i = 0; i < n; i++) {
            if (field.isIndex(i)) {
                // At this point, newFieldName points to an array,
                // field.prefix(i) points to an array element. If there
                // is a Move delta for the move of field.prefix(i+1)
                // -> someField, then we get the new index from
                // someField
                Path arrayElement = field.prefix(i);
                // arrayElement.tail(0) is an index
                Path movedTo = null;
                for (DocComparator.Delta<Object> d : delta) {
                    if (d instanceof DocComparator.Move) {
                        DocComparator.Move<Object> move = (DocComparator.Move<Object>) d;
                        if (move.getField1().equals(arrayElement)) {
                            movedTo = move.getField2();
                            break;
                        }
                    }
                }
                if (movedTo != null) {
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
        LOGGER.debug("Adding contents of {} to {}", field, newFieldName);
        DBObject parent;
        if (newFieldName.numSegments() > 1) {
            parent = (DBObject) DocTranslator.getDBObject(doc, newFieldName.prefix(-1));
        } else {
            parent = doc;
        }
        parent.put(newFieldName.tail(0), copy(removedField.getRemovedNode()));
    }

    private static Object copy(Object fld) {
        Object ret;
        if(fld instanceof List) {
            ArrayList<Object> l=new ArrayList<>();
            for(Object s:(List)fld) {
                l.add(copy(s));
            }
            ret=l;
        } else if (fld instanceof DBObject) {
            DBObject r=new BasicDBObject();
            for(String s:((DBObject)fld).keySet()) {
                r.put(s,copy( ((DBObject)fld).get(s)));
            }
            ret=r;
        } else {
            ret=fld;
        }
        return ret;
    }
}
