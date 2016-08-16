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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteError;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.CrudConstants;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.eval.FieldAccessRoleEvaluator;
import com.redhat.lightblue.interceptor.InterceptPoint;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.Field;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.Path;

/**
 * Basic doc saver with no transaction support
 */
public class BasicDocSaver implements DocSaver {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicDocSaver.class);

    private final int batchSize;

    private final FieldAccessRoleEvaluator roleEval;
    private final Translator translator;
    private final EntityMetadata md;
    private final WriteConcern writeConcern;

    private final Field[] idFields;
    private final Path[] idPaths;
    private final String[] mongoIdFields;
    private final String newDocver=ObjectId.get().toString();

    /**
     * Creates a doc saver with the given translator and role evaluator
     */
    public BasicDocSaver(Translator translator,
                         FieldAccessRoleEvaluator roleEval,
                         EntityMetadata md,
                         WriteConcern writeConcern, int batchSize) {
        this.translator = translator;
        this.roleEval = roleEval;
        this.md = md;
        this.writeConcern = writeConcern;
        this.batchSize = batchSize;

        Field[] idf = md.getEntitySchema().getIdentityFields();
        if (idf == null || idf.length == 0) {
            // Assume _id is the id
            idFields = new Field[]{(Field) md.resolve(Translator.ID_PATH)};
        } else {
            idFields = idf;
        }
        idPaths = new Path[idFields.length];
        mongoIdFields = new String[idFields.length];
        for (int i = 0; i < mongoIdFields.length; i++) {
            idPaths[i] = md.getEntitySchema().getEntityRelativeFieldName(idFields[i]);
            mongoIdFields[i] = Translator.translatePath(idPaths[i]);
        }
    }

    private final class DocInfo {
        final DBObject newDoc; // translated input doc to be written
        final DocCtx inputDoc; // The doc coming from client
        DBObject oldDoc; // The doc in the db
        Object[] id;

        public DocInfo(DBObject newDoc, DocCtx inputDoc) {
            this.newDoc = newDoc;
            this.inputDoc = inputDoc;
        }

        public BasicDBObject getIdQuery() {
            BasicDBObject q = new BasicDBObject();
            for (int i = 0; i < id.length; i++) {
                q.append(mongoIdFields[i], id[i]);
            }
            return q;
        }
    }

    @Override
    public void saveDocs(CRUDOperationContext ctx,
                         Op op,
                         boolean upsert,
                         DBCollection collection,
                         DBObject[] dbObjects,
                         DocCtx[] inputDocs) {
        // Operate in batches
        List<DocInfo> batch = new ArrayList<>(batchSize);
        for (int i = 0; i < dbObjects.length; i++) {
            DocInfo item = new DocInfo(dbObjects[i], inputDocs[i]);
            batch.add(item);
            if (batch.size() >= batchSize) {
                saveDocs(ctx, op, upsert, collection, batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            saveDocs(ctx, op, upsert, collection, batch);
        }
    }

    private void saveDocs(CRUDOperationContext ctx,
                          Op op,
                          boolean upsert,
                          DBCollection collection,
                          List<DocInfo> batch) {
        // If this is a save operation, we have to load the existing DB objects
        if (op == DocSaver.Op.save) {
            LOGGER.debug("Retrieving existing {} documents for save operation", batch.size());
            List<BasicDBObject> idQueries = new ArrayList<>(batch.size());
            for (DocInfo doc : batch) {
                doc.id = getFieldValues(doc.newDoc, idPaths);
                if (!isNull(doc.id)) {
                    idQueries.add(doc.getIdQuery());
                }
            }
            if (!idQueries.isEmpty()) {
                BasicDBObject retrievalq = new BasicDBObject("$or", idQueries);
                LOGGER.debug("Existing document retrieval query={}", retrievalq);
                try (DBCursor cursor = collection.find(retrievalq, null)) {
                    List<DBObject> results = cursor.toArray();
                    LOGGER.debug("Retrieved {} docs", results.size());

                    // Now associate the docs in the retrieved results with the docs in the batch
                    for (DBObject dbDoc : results) {
                        // Get the id from the doc
                        Object[] id = getFieldValues(dbDoc, idPaths);
                        // Find this doc in the batch
                        DocInfo doc = findDoc(batch, id);
                        if (doc != null) {
                            doc.oldDoc = dbDoc;
                        } else {
                            LOGGER.warn("Cannot find doc with id={}", id);
                        }
                    }
                }
            }
        }
        // Some docs in the batch will be inserted, some saved, based on the operation. Lets decide that now
        List<DocInfo> saveList;
        List<DocInfo> insertList;
        if (op == DocSaver.Op.insert) {
            saveList = new ArrayList<>();
            insertList = batch;
        } else {
            // This is a save operation
            saveList = new ArrayList<>();
            insertList = new ArrayList<>();
            for (DocInfo doc : batch) {
                if (doc.oldDoc == null) {
                    // there is no doc in the db
                    if (upsert) {
                        // This is an insertion
                        insertList.add(doc);
                    } else {
                        // This is an invalid  request
                        LOGGER.warn("Invalid request, cannot update or insert");
                        doc.inputDoc.addError(Error.get(op.toString(), MongoCrudConstants.ERR_SAVE_ERROR_INS_WITH_NO_UPSERT,
                                                        "New document, but upsert=false"));
                    }
                } else {
                    // There is a doc in the db
                    saveList.add(doc);
                }
            }
        }
        LOGGER.debug("Save docs={}, insert docs={}, error docs={}", saveList.size(), insertList.size(), batch.size() - saveList.size() - insertList.size());
        insertDocs(ctx, collection, insertList);
        updateDocs(ctx, collection, saveList);
    }

    private void insertDocs(CRUDOperationContext ctx,
                            DBCollection collection,
                            List<DocInfo> list) {
        if (!list.isEmpty()) {
            LOGGER.debug("Inserting {} docs", list.size());
            if (!md.getAccess().getInsert().hasAccess(ctx.getCallerRoles())) {
                for (DocInfo doc : list) {
                    doc.inputDoc.addError(Error.get("insert",
                            MongoCrudConstants.ERR_NO_ACCESS,
                            "insert:" + md.getName()));
                }
            } else {
                List<DocInfo> insertionAttemptList = new ArrayList<>(list.size());
                for (DocInfo doc : list) {
                    Set<Path> paths = roleEval.getInaccessibleFields_Insert(doc.inputDoc);
                    LOGGER.debug("Inaccessible fields:{}", paths);
                    if (paths == null || paths.isEmpty()) {
                        try {
                            Translator.populateCaseInsensitiveHiddenFields(doc.newDoc, md);
                            Translator.setDocVersion(doc.newDoc,newDocver);
                            insertionAttemptList.add(doc);
                        } catch (IOException e) {
                            doc.inputDoc.addError(Error.get("insert", MongoCrudConstants.ERR_TRANSLATION_ERROR, e));
                        }
                    } else {
                        for (Path path : paths) {
                            doc.inputDoc.addError(Error.get("insert", CrudConstants.ERR_NO_FIELD_INSERT_ACCESS, path.toString()));
                        }
                    }
                }
                LOGGER.debug("After access checks, inserting {} docs", insertionAttemptList.size());
                if (!insertionAttemptList.isEmpty()) {
                    BulkWriteOperation bw = collection.initializeUnorderedBulkOperation();
                    for (DocInfo doc : insertionAttemptList) {
                        ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_INSERT_DOC, ctx, doc.inputDoc);
                        bw.insert(doc.newDoc);
                        doc.inputDoc.setCRUDOperationPerformed(CRUDOperation.INSERT);
                    }
                    try {
                        if (writeConcern == null) {
                            LOGGER.debug("Bulk inserting docs");
                            bw.execute();
                        } else {
                            LOGGER.debug("Bulk inserting docs with writeConcern={} from execution", writeConcern);
                            bw.execute(writeConcern);
                        }
                    } catch (BulkWriteException bwe) {
                        LOGGER.error("Bulk write exception", bwe);
                        BulkUpdateCtx.
                            handleBulkWriteError(bwe.getWriteErrors(), "insert", (int i)->insertionAttemptList.get(i).inputDoc);
                    } catch (RuntimeException e) {
                        LOGGER.error("Exception", e);
                        throw e;
                    } finally {
                        for (DocInfo doc : insertionAttemptList) {
                            if (!doc.inputDoc.hasErrors()) {
                                ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_INSERT_DOC, ctx, doc.inputDoc);
                            }
                        }
                    }
                }
            }
        }
    }

    private void updateDocs(CRUDOperationContext ctx,
                            DBCollection collection,
                            List<DocInfo> list) {
        if (!list.isEmpty()) {
            LOGGER.debug("Updating {} docs", list.size());
            if (!md.getAccess().getUpdate().hasAccess(ctx.getCallerRoles())) {
                for (DocInfo doc : list) {
                    doc.inputDoc.addError(Error.get("update",
                            CrudConstants.ERR_NO_ACCESS, "update:" + md.getName()));
                }
            } else {
                BsonMerge merge = new BsonMerge(md);
                List<DocInfo> updateAttemptList=new ArrayList<>(list.size());
                for (DocInfo doc : list) {
                    JsonDoc oldDoc = translator.toJson(doc.oldDoc);
                    doc.inputDoc.setOriginalDocument(oldDoc);
                    Set<Path> paths = roleEval.getInaccessibleFields_Update(doc.inputDoc, oldDoc);
                    if (paths == null || paths.isEmpty()) {
                        try {
                            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE_DOC, ctx, doc.inputDoc);
                            merge.merge(doc.oldDoc, doc.newDoc);
                            Translator.populateCaseInsensitiveHiddenFields(doc.newDoc, md);
                            updateAttemptList.add(doc);
                        } catch (Exception e) {
                            doc.inputDoc.addError(Error.get("update", MongoCrudConstants.ERR_TRANSLATION_ERROR, e));
                        }
                    } else {
                        doc.inputDoc.addError(Error.get("update",
                                                        CrudConstants.ERR_NO_FIELD_UPDATE_ACCESS, paths.toString()));
                    }
                }
                LOGGER.debug("After checks and merge, updating {} docs", updateAttemptList.size());
                if (!updateAttemptList.isEmpty()) {
                    BulkUpdateCtx updateCtx=new BulkUpdateCtx(collection);
                    for (DocInfo doc : updateAttemptList) {
                        BulkUpdateCtx.UpdateDoc updateDoc=new BulkUpdateCtx.UpdateDoc(doc.oldDoc,doc.inputDoc);
                        updateCtx.addDoc(updateDoc,doc.newDoc);
                    }
                    updateCtx.execute(writeConcern);
                    for (DocInfo doc : updateAttemptList) {
                        if (!doc.inputDoc.hasErrors()) {
                            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_UPDATE_DOC, ctx, doc.inputDoc);
                        }
                    }
                }
            }
        }
    }

    private DocInfo findDoc(List<DocInfo> list, Object[] id) {
        for (DocInfo doc : list) {
            if (idEquals(doc.id, id)) {
                return doc;
            }
        }
        return null;
    }

    private boolean valueEquals(Object v1, Object v2) {
        LOGGER.debug("v1={}, v2={}", v1, v2);
        if (!v1.equals(v2)) {
            if (v1.toString().equals(v2.toString())) {
                return true;
            }
        } else {
            return true;
        }
        return false;
    }

    private boolean idEquals(Object[] id1, Object[] id2) {
        if (id1 != null && id2 != null) {
            for (int i = 0; i < id1.length; i++) {
                if ((id1[i] == null && id2[i] == null)
                        || (id1[i] != null && id2[i] != null && valueEquals(id1[i], id2[i]))) {
                    ;
                } else {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Return the values for the fields
     */
    private Object[] getFieldValues(DBObject object, Path[] fields) {
        Object[] ret = new Object[fields.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = Translator.getDBObject(object, fields[i]);
        }
        return ret;
    }

    /**
     * Return if all values are null
     */
    private boolean isNull(Object[] values) {
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    return false;
                }
            }
        }
        return true;
    }
}
