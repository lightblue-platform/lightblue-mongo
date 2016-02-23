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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.*;
import com.redhat.lightblue.config.ControllerConfiguration;
import com.redhat.lightblue.crud.*;
import com.redhat.lightblue.eval.FieldAccessRoleEvaluator;
import com.redhat.lightblue.eval.Projector;
import com.redhat.lightblue.eval.Updater;
import com.redhat.lightblue.interceptor.InterceptPoint;
import com.redhat.lightblue.metadata.*;
import com.redhat.lightblue.metadata.types.StringType;
import com.redhat.lightblue.mongo.metadata.MongoMetadataConstants;
import com.redhat.lightblue.mongo.common.DBResolver;
import com.redhat.lightblue.mongo.common.MongoDataStore;
import com.redhat.lightblue.mongo.config.MongoConfiguration;
import com.redhat.lightblue.query.*;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.MutablePath;
import com.redhat.lightblue.util.Path;
import com.redhat.lightblue.extensions.ExtensionSupport;
import com.redhat.lightblue.extensions.Extension;
import com.redhat.lightblue.extensions.synch.LockingSupport;
import com.redhat.lightblue.extensions.valuegenerator.ValueGeneratorSupport;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class MongoCRUDController implements CRUDController, MetadataListener, ExtensionSupport {

    public static final String ID_STR = "_id";

    /**
     * Name of the property for the operation context that keeps the last saver
     * class instance used
     */
    public static final String PROP_SAVER = "MongoCRUDController:saver";

    /**
     * Name of the property for the operation context that keeps the last
     * updater class instance used
     */
    public static final String PROP_UPDATER = "MongoCRUDController:updater";

    /**
     * Name of the property for the operation context that keeps the last
     * deleter class instance used
     */
    public static final String PROP_DELETER = "MongoCRUDController:deleter";

    /**
     * Name of the property for the operation context that keeps the last finder
     * class instance used
     */
    public static final String PROP_FINDER = "MongoCRUDController:finder";

    public static final String OP_INSERT = "insert";
    public static final String OP_SAVE = "save";
    public static final String OP_FIND = "find";
    public static final String OP_UPDATE = "update";
    public static final String OP_DELETE = "delete";

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoCRUDController.class);

    private static final Projection ID_PROJECTION = new FieldProjection(new Path(ID_STR), true, false);
    private static final Projection EMPTY_PROJECTION = new FieldProjection(new Path("*"), false,false);

    private final DBResolver dbResolver;
    private final ControllerConfiguration controllerCfg;

    public MongoCRUDController(ControllerConfiguration controllerCfg,DBResolver dbResolver) {
        this.dbResolver = dbResolver;
        this.controllerCfg=controllerCfg;
    }

    public DBResolver getDbResolver() {
        return dbResolver;
    }

    public ControllerConfiguration getControllerConfiguration() {
        return controllerCfg;
    }

    /**
     * Insertion operation for mongo
     */
    @Override
    public CRUDInsertionResponse insert(CRUDOperationContext ctx,
                                        Projection projection) {
        LOGGER.debug("insert() start");
        CRUDInsertionResponse response = new CRUDInsertionResponse();
        ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_INSERT, ctx);
        int n = saveOrInsert(ctx, false, projection, OP_INSERT);
        response.setNumInserted(n);
        ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_INSERT, ctx);
        return response;
    }

    @Override
    public CRUDSaveResponse save(CRUDOperationContext ctx,
                                 boolean upsert,
                                 Projection projection) {
        LOGGER.debug("save() start");
        CRUDSaveResponse response = new CRUDSaveResponse();
        ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_SAVE, ctx);
        int n = saveOrInsert(ctx, upsert, projection, OP_SAVE);
        response.setNumSaved(n);
        ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_SAVE, ctx);
        return response;
    }

    private int saveOrInsert(CRUDOperationContext ctx,
                             boolean upsert,
                             Projection projection,
                             String operation) {
        int ret = 0;
        List<DocCtx> documents = ctx.getDocumentsWithoutErrors();
        if (documents == null || documents.isEmpty()) {
            return ret;
        }
        for (DocCtx doc : documents) {
            doc.setOriginalDocument(doc);
        }
        LOGGER.debug("saveOrInsert() start");
        Error.push(operation);
        Translator translator = new Translator(ctx, ctx.getFactory().getNodeFactory());
        try {
            FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(ctx.getEntityMetadata(ctx.getEntityName()),
                    ctx.getCallerRoles());
            LOGGER.debug("saveOrInsert: Translating docs");
            EntityMetadata md = ctx.getEntityMetadata(ctx.getEntityName());
            DBObject[] dbObjects = translator.toBson(documents);
            // dbObjects[i] is the translation of documents.get(i)
            if (dbObjects != null) {
                LOGGER.debug("saveOrInsert: {} docs translated to bson", dbObjects.length);

                MongoDataStore store = (MongoDataStore) md.getDataStore();
                DB db = dbResolver.get(store);
                DBCollection collection = db.getCollection(store.getCollectionName());

                Projection combinedProjection = Projection.add(projection, roleEval.getExcludedFields(FieldAccessRoleEvaluator.Operation.find));

                Projector projector;
                if (combinedProjection != null) {
                    projector = Projector.getInstance(combinedProjection, md);
                } else {
                    projector = null;
                }
                DocSaver saver = new BasicDocSaver(translator, roleEval);
                ctx.setProperty(PROP_SAVER, saver);
                for (int docIndex = 0; docIndex < dbObjects.length; docIndex++) {
                    DBObject dbObject = dbObjects[docIndex];
                    DocCtx inputDoc = documents.get(docIndex);
                    try {
                        saver.saveDoc(ctx, operation.equals(OP_INSERT) ? DocSaver.Op.insert : DocSaver.Op.save,
                                upsert, collection, md, dbObject, inputDoc);
                    } catch (Exception e) {
                        LOGGER.error("saveOrInsert failed: {}", e);
                        inputDoc.addError(analyzeException(e, operation, MongoCrudConstants.ERR_SAVE_ERROR, true));
                    }
                    if (projector != null) {
                        JsonDoc jsonDoc = translator.toJson(dbObject);
                        LOGGER.debug("Translated doc: {}", jsonDoc);
                        inputDoc.setOutputDocument(projector.project(jsonDoc, ctx.getFactory().getNodeFactory()));
                    } else {
                        inputDoc.setOutputDocument(new JsonDoc(new ObjectNode(ctx.getFactory().getNodeFactory())));
                    }
                    LOGGER.debug("projected doc: {}", inputDoc.getOutputDocument());
                    if (!inputDoc.hasErrors()) {
                        ret++;
                    }
                }
                ctx.getHookManager().queueHooks(ctx);
            }
        } catch (Error e) {
            ctx.addError(e);
        } catch (Exception e) {
            ctx.addError(analyzeException(e, CrudConstants.ERR_CRUD));
        } finally {
            Error.pop();
        }
        LOGGER.debug("saveOrInsert() end: {} docs requested, {} saved", documents.size(), ret);
        return ret;
    }

    @Override
    public CRUDUpdateResponse update(CRUDOperationContext ctx,
                                     QueryExpression query,
                                     UpdateExpression update,
                                     Projection projection) {
        LOGGER.debug("update start: q:{} u:{} p:{}", query, update, projection);
        Error.push(OP_UPDATE);
        CRUDUpdateResponse response = new CRUDUpdateResponse();
        Translator translator = new Translator(ctx, ctx.getFactory().getNodeFactory());
        try {
            if (query == null) {
                throw Error.get("update",MongoCrudConstants.ERR_NULL_QUERY,"");
            }
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE, ctx);
            EntityMetadata md = ctx.getEntityMetadata(ctx.getEntityName());
            if (md.getAccess().getUpdate().hasAccess(ctx.getCallerRoles())) {
                ConstraintValidator validator = ctx.getFactory().getConstraintValidator(md);
                LOGGER.debug("Translating query {}", query);
                DBObject mongoQuery = translator.translate(md, query);
                LOGGER.debug("Translated query {}", mongoQuery);
                FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(md, ctx.getCallerRoles());

                Projector projector;
                if (projection != null) {
                    Projection x = Projection.add(projection, roleEval.getExcludedFields(FieldAccessRoleEvaluator.Operation.find));
                    LOGGER.debug("Projection={}", x);
                    projector = Projector.getInstance(x, md);
                } else {
                    projector = null;
                }
                DB db = dbResolver.get((MongoDataStore) md.getDataStore());
                DBCollection coll = db.getCollection(((MongoDataStore) md.getDataStore()).getCollectionName());
                Projector errorProjector;
                if (projector == null) {
                    errorProjector = Projector.getInstance(ID_PROJECTION, md);
                } else {
                    errorProjector = projector;
                }

                // If there are any constraints for updated fields, or if we're updating arrays, we have to use iterate-update
                Updater updater = Updater.getInstance(ctx.getFactory().getNodeFactory(), md, update);

                DocUpdater docUpdater = new IterateAndUpdate(ctx.getFactory().getNodeFactory(), validator, roleEval, translator, updater,
                        projector, errorProjector);
                ctx.setProperty(PROP_UPDATER, docUpdater);
                docUpdater.update(ctx, coll, md, response, mongoQuery);
                ctx.getHookManager().queueHooks(ctx);
            } else {
                ctx.addError(Error.get(MongoCrudConstants.ERR_NO_ACCESS, "update:" + ctx.getEntityName()));
            }
        } catch (Error e) {
            ctx.addError(e);
        } catch (Exception e) {
            ctx.addError(analyzeException(e, CrudConstants.ERR_CRUD));
        } finally {
            Error.pop();
        }
        ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_UPDATE, ctx);
        LOGGER.debug("update end: updated: {}, failed: {}", response.getNumUpdated(), response.getNumFailed());
        return response;
    }

    @Override
    public CRUDDeleteResponse delete(CRUDOperationContext ctx,
                                     QueryExpression query) {
        LOGGER.debug("delete start: q:{}", query);
        Error.push(OP_DELETE);
        CRUDDeleteResponse response = new CRUDDeleteResponse();
        Translator translator = new Translator(ctx, ctx.getFactory().getNodeFactory());
        try {
            if (query == null) {
                throw Error.get("delete",MongoCrudConstants.ERR_NULL_QUERY,"");
            }
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_DELETE, ctx);
            EntityMetadata md = ctx.getEntityMetadata(ctx.getEntityName());
            if (md.getAccess().getDelete().hasAccess(ctx.getCallerRoles())) {
                LOGGER.debug("Translating query {}", query);
                DBObject mongoQuery = translator.translate(md, query);
                LOGGER.debug("Translated query {}", mongoQuery);
                DB db = dbResolver.get((MongoDataStore) md.getDataStore());
                DBCollection coll = db.getCollection(((MongoDataStore) md.getDataStore()).getCollectionName());
                DocDeleter deleter = new IterateDeleter(translator);
                ctx.setProperty(PROP_DELETER, deleter);
                deleter.delete(ctx, coll, mongoQuery, response);
                ctx.getHookManager().queueHooks(ctx);
            } else {
                ctx.addError(Error.get(MongoCrudConstants.ERR_NO_ACCESS, "delete:" + ctx.getEntityName()));
            }
        } catch (Error e) {
            ctx.addError(e);
        } catch (Exception e) {
            ctx.addError(analyzeException(e, CrudConstants.ERR_CRUD));
        } finally {
            Error.pop();
        }
        ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_DELETE, ctx);
        LOGGER.debug("delete end: deleted: {}}", response.getNumDeleted());
        return response;
    }

    /**
     * Search implementation for mongo
     */
    @Override
    public CRUDFindResponse find(CRUDOperationContext ctx,
                                 QueryExpression query,
                                 Projection projection,
                                 Sort sort,
                                 Long from,
                                 Long to) {
        LOGGER.debug("find start: q:{} p:{} sort:{} from:{} to:{}", query, projection, sort, from, to);
        Error.push(OP_FIND);
        CRUDFindResponse response = new CRUDFindResponse();
        Translator translator = new Translator(ctx, ctx.getFactory().getNodeFactory());
        try {
            ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_FIND, ctx);
            EntityMetadata md = ctx.getEntityMetadata(ctx.getEntityName());
            if (md.getAccess().getFind().hasAccess(ctx.getCallerRoles())) {
                FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(md, ctx.getCallerRoles());
                LOGGER.debug("Translating query {}", query);
                DBObject mongoQuery = query==null?null:translator.translate(md, query);
                LOGGER.debug("Translated query {}", mongoQuery);
                DBObject mongoSort;
                if (sort != null) {
                    LOGGER.debug("Translating sort {}", sort);
                    mongoSort = translator.translate(sort);
                    LOGGER.debug("Translated sort {}", mongoSort);
                } else {
                    mongoSort = null;
                }
                DBObject mongoProjection = translator.translateProjection(md, getProjectionFields(projection, md), query, sort);
                LOGGER.debug("Translated projection {}", mongoProjection);
                DB db = dbResolver.get((MongoDataStore) md.getDataStore());
                DBCollection coll = db.getCollection(((MongoDataStore) md.getDataStore()).getCollectionName());
                LOGGER.debug("Retrieve db collection:" + coll);
                DocFinder finder = new BasicDocFinder(translator);
                MongoConfiguration cfg=dbResolver.getConfiguration( (MongoDataStore)md.getDataStore());
                if(cfg!=null)
                    finder.setMaxResultSetSize(cfg.getMaxResultSetSize());
                ctx.setProperty(PROP_FINDER, finder);
                response.setSize(finder.find(ctx, coll, mongoQuery, mongoProjection, mongoSort, from, to));
                // Project results
                Projector projector = Projector.getInstance(projection==null?EMPTY_PROJECTION:
                                                            Projection.add(projection, roleEval.getExcludedFields(FieldAccessRoleEvaluator.Operation.find)), md);
                for (DocCtx document : ctx.getDocuments()) {
                    document.setOutputDocument(projector.project(document, ctx.getFactory().getNodeFactory()));
                }
                ctx.getHookManager().queueHooks(ctx);
            } else {
                ctx.addError(Error.get(MongoCrudConstants.ERR_NO_ACCESS, "find:" + ctx.getEntityName()));
            }
        } catch (Error e) {
            ctx.addError(e);
        } catch (Exception e) {
            LOGGER.error("Error during find:",e);
            ctx.addError(analyzeException(e, CrudConstants.ERR_CRUD));
        } finally {
            Error.pop();
        }
        ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_FIND, ctx);
        LOGGER.debug("find end: query: {} results: {}", response.getSize());
        return response;
    }

    @Override
    public void updatePredefinedFields(CRUDOperationContext ctx, JsonDoc doc) {
        // If it is a save operation, we rely on _id being passed by client, so we don't auto-generate that
        // If not, it needs to be auto-generated
        if(ctx.getCRUDOperation()!=CRUDOperation.SAVE) {
            JsonNode idNode = doc.get(Translator.ID_PATH);
            if (idNode == null || idNode instanceof NullNode) {
                doc.modify(Translator.ID_PATH,
                           ctx.getFactory().getNodeFactory().textNode(ObjectId.get().toString()),
                           false);
            }
        }
    }

    @Override
    public <E extends Extension> E getExtensionInstance(Class<? extends Extension> extensionClass) {
        if(extensionClass.equals(LockingSupport.class))
            return (E)new MongoLockingSupport(this);
        else if(extensionClass.equals(ValueGeneratorSupport.class))
            return (E)new MongoSequenceSupport(this);
        return null;
    }

    @Override
    public MetadataListener getMetadataListener() {
        return this;
    }

    @Override
    public void afterUpdateEntityInfo(Metadata md, EntityInfo ei, boolean newEntity) {
        createUpdateEntityInfoIndexes(ei, md);
    }

    @Override
    public void beforeUpdateEntityInfo(Metadata md, EntityInfo ei, boolean newEntity) {
        validateIndexFields(ei);
        ensureIdIndex(ei);
        validateSaneIndexSet(ei.getIndexes().getIndexes());
    }

    @Override
    public void afterCreateNewSchema(Metadata md, EntityMetadata emd) {
    }

    @Override
    public void beforeCreateNewSchema(Metadata md, EntityMetadata emd) {
        validateNoHiddenInMetaData(emd);
        validateIndexFields(emd.getEntityInfo());
        ensureIdField(emd);
    }

    private Path translateIndexPath(Path p) {
        MutablePath newPath = new MutablePath();
        int n = p.numSegments();
        for (int i = 0; i < n; i++) {
            String x = p.head(i);
            if (!x.equals(Path.ANY)) {
                if (p.isIndex(i)) {
                    throw Error.get(MongoCrudConstants.ERR_INVALID_INDEX_FIELD, p.toString());
                }
                newPath.push(x);
            }
        }
        return newPath.immutableCopy();
    }

    private void validateNoHiddenInMetaData(EntityMetadata emd) {
        FieldCursor cursor = emd.getFieldCursor();
        while (cursor.next()) {
            if(cursor.getCurrentPath().getLast().equals(Translator.HIDDEN_SUB_PATH.getLast())) {
                throw Error.get(MongoCrudConstants.ERR_RESERVED_FIELD);
            }
        }
    }

    /**
     * No two index should have the same field signature
     */
    private void validateSaneIndexSet(List<Index> indexes) {
        int n=indexes.size();
        for(int i=0;i<n;i++) {
            List<IndexSortKey> keys1=indexes.get(i).getFields();
            for(int j=i+1;j<n;j++) {
                List<IndexSortKey> keys2=indexes.get(j).getFields();
                if(sameSortKeys(keys1,keys2)) {
                    throw Error.get(MongoCrudConstants.ERR_DUPLICATE_INDEX);
                }
            }
        }
    }

    private  boolean sameSortKeys(List<IndexSortKey> keys1,List<IndexSortKey> keys2) {
        if(keys1.size()==keys2.size()) {
            for(int i=0;i<keys1.size();i++) {
                SortKey k1=keys1.get(i);
                SortKey k2=keys2.get(i);
                if(!k1.getField().equals(k2.getField())||
                   k1.isDesc()!=k2.isDesc())
                    return false;
            }
            return true;
        }
        return false;
    }

    private void validateIndexFields(EntityInfo ei) {
        for (Index ix : ei.getIndexes().getIndexes()) {
            List<IndexSortKey> fields = ix.getFields();
            List<IndexSortKey> newFields = null;
            boolean copied = false;
            int i = 0;
            for (SortKey key : fields) {
                Path p = key.getField();
                Path newPath = translateIndexPath(p);
                if (!p.equals(newPath)) {
                    IndexSortKey newKey = new IndexSortKey(newPath, key.isDesc());
                    if (!copied) {
                        newFields = new ArrayList<>();
                        newFields.addAll(fields);
                        copied = true;
                    }
                    newFields.set(i, newKey);
                }
            }
            if (copied) {
                ix.setFields(newFields);
                LOGGER.debug("Index rewritten as {}", ix);
            }
        }
    }

    private void ensureIdField(EntityMetadata md) {
        ensureIdField(md.getEntitySchema());
    }

    private void ensureIdField(EntitySchema schema) {
        LOGGER.debug("ensureIdField: begin");

        SimpleField idField;

        FieldTreeNode field;
        try {
            field = schema.resolve(Translator.ID_PATH);
        } catch (Error e) {
            field = null;
        }
        if (field == null) {
            LOGGER.debug("Adding _id field");
            idField = new SimpleField(ID_STR, StringType.TYPE);
            schema.getFields().addNew(idField);
        } else {
            if (field instanceof SimpleField) {
                idField = (SimpleField) field;
            } else {
                throw Error.get(MongoMetadataConstants.ERR_INVALID_ID);
            }
        }

        LOGGER.debug("ensureIdField: end");
    }

    private void ensureIdIndex(EntityInfo ei) {
        LOGGER.debug("ensureIdIndex: begin");

        Indexes indexes = ei.getIndexes();
        // We are looking for a unique index on _id
        boolean found = false;
        for (Index ix : indexes.getIndexes()) {
            List<IndexSortKey> fields = ix.getFields();
            if (fields.size() == 1 && fields.get(0).getField().equals(Translator.ID_PATH)
                    && ix.isUnique()) {
                found = true;
                break;
            }
        }
        if (!found) {
            LOGGER.debug("Adding _id index");
            Index idIndex = new Index();
            idIndex.setUnique(true);
            List<IndexSortKey> fields = new ArrayList<>();
            fields.add(new IndexSortKey(Translator.ID_PATH, false));
            idIndex.setFields(fields);
            List<Index> ix = indexes.getIndexes();
            ix.add(idIndex);
            indexes.setIndexes(ix);
        } else {
            LOGGER.debug("_id index exists");
        }
        LOGGER.debug("ensureIdIndex: end");
    }

    private void createUpdateEntityInfoIndexes(EntityInfo ei, Metadata md) {
        LOGGER.debug("createUpdateEntityInfoIndexes: begin");

        Indexes indexes = ei.getIndexes();

        MongoDataStore ds = (MongoDataStore) ei.getDataStore();
        DB entityDB = dbResolver.get(ds);
        DBCollection entityCollection = entityDB.getCollection(ds.getCollectionName());
        Error.push("createUpdateIndex");
        try {
            List<DBObject> existingIndexes = entityCollection.getIndexInfo();
            LOGGER.debug("Existing indexes: {}", existingIndexes);

            // This is how index creation/modification works:
            //  - The _id index will remain untouched.
            //  - If there is an index with name X in metadata, find the same named index, and compare
            //    its fields/flags. If different, drop and recreate. Drop all indexes with the same field signature.
            //
            //  - If there is an index with null name in metadata, see if there is an index with same
            //    fields and flags. If so, no change. Otherwise, create index. Drop all indexes with the same field signature.

            List<Index> createIndexes=new ArrayList<>();
            List<DBObject> dropIndexes=new ArrayList<>();
            List<DBObject> foundIndexes=new ArrayList<>();
            for(Index index:indexes.getIndexes()) {
                if(!isIdIndex(index)) {
                    if(index.getName()!=null&&index.getName().trim().length()>0) {
                        LOGGER.debug("Processing index {}",index.getName());
                        DBObject found=null;
                        for(DBObject existingIndex:existingIndexes) {
                            if(index.getName().equals(existingIndex.get("name"))) {
                                found=existingIndex;
                                break;
                            }
                        }
                        if(found!=null) {
                            foundIndexes.add(found);
                            // indexFieldsMatch will handle checking for hidden versions of the index
                            if(indexFieldsMatch(index,found) &&
                               indexOptionsMatch(index,found) ) {
                                LOGGER.debug("{} already exists",index.getName());
                            } else {
                                LOGGER.debug("{} modified, dropping and recreating index",index.getName());
                                existingIndexes.remove(found);
                                dropIndexes.add(found);
                                createIndexes.add(index);
                            }
                        } else {
                            LOGGER.debug("{} not found, checking if there is an index with same field signature",index.getName());
                            found=findIndexWithSignature(existingIndexes,index);
                            if(found==null) {
                                LOGGER.debug("{} not found, creating",index.getName());
                                createIndexes.add(index);
                            } else {
                                LOGGER.debug("There is an index with same field signature as {}, drop and recreate",index.getName());
                                foundIndexes.add(found);
                                dropIndexes.add(found);
                                createIndexes.add(index);
                            }
                        }
                    } else {
                        LOGGER.debug("Processing index with fields {}",index.getFields());
                        DBObject found=findIndexWithSignature(existingIndexes,index);
                        if(found!=null) {
                            foundIndexes.add(found);
                            LOGGER.debug("An index with same keys found: {}",found);
                            if(indexOptionsMatch(index,found)) {
                                LOGGER.debug("Same options as well, not changing");
                            } else {
                                LOGGER.debug("Index with different options, drop/recreate");
                                dropIndexes.add(found);
                                createIndexes.add(index);
                            }
                        } else {
                            LOGGER.debug("Creating index with fields {}",index.getFields());
                            createIndexes.add(index);
                        }
                    }
                }
            }
            // Any index in existingIndexes but not in foundIndexes should be deleted as well
            for(DBObject index:existingIndexes) {
                boolean found=false;
                for(DBObject x:foundIndexes)
                    if(x==index) {
                        found=true;
                        break;
                    }
                if(!found) {
                    for(DBObject x:dropIndexes)
                        if(x==index) {
                            found=true;
                            break;
                        }
                    if(!found&&!isIdIndex(index)) {
                        LOGGER.info("Dropping {}",index.get("name"));
                        entityCollection.dropIndex(index.get("name").toString());
                    }
                }
            }
            for(DBObject index:dropIndexes) {
                LOGGER.info("Dropping {}",index.get("name"));
                entityCollection.dropIndex(index.get("name").toString());
            }
            boolean background = true;
            // fieldMap is <canonicalPath, hiddenPath>
            Map<String, String> fieldMap = new HashMap<>();
            for(Index index:createIndexes) {
                LOGGER.info("Creating index {} with {}",index.getName(),index.getFields());
                DBObject newIndex = new BasicDBObject();
                for (IndexSortKey p : index.getFields()) {
                    String field = Translator.translatePath(p.getField());
                    if (p.isCaseInsensitive()) {
                        // build a map of the index's field to it's actual @mongoHidden pathd
                        fieldMap.put(p.getField().toString(), Translator.getHiddenForField(p.getField()).toString());

                        // if we have a case insensitive index, we want this operation to be blocking because we need to generate fields afterwards
                        background = false;
                        LOGGER.info("Index creation will be blocking.");
                    }
                    newIndex.put(field, p.isDesc() ? -1 : 1);
                }
                BasicDBObject options = new BasicDBObject("unique", index.isUnique());
                // if index is unique also make it a sparse index, so we can have non-required unique fields
                options.append("sparse", index.isUnique());
                if (index.getName() != null && index.getName().trim().length() > 0) {
                    options.append("name", index.getName().trim());
                }
                options.append("background", background);
                LOGGER.debug("Creating index {} with options {}", newIndex, options);
                entityCollection.createIndex(newIndex, options);
            }
            if (!background) {
                LOGGER.info("Executing post-index creation updates...");
                // if we're not running in the background, we need to execute the items in this block afterwards
                // caseInsensitive indexes have been updated or created, run the server-side updater on mongo to recalculate all hidden fields
                String js = new String(Files.readAllBytes(Paths.get("populate-hidden-fields.js")));
                entityDB.doEval(js, fieldMap);

                // TODO: remove hidden fields on index deletions?
            }
        } catch (MongoException me) {
            throw Error.get(MongoCrudConstants.ERR_ENTITY_INDEX_NOT_CREATED, me.getMessage());
        } catch (Error e) {
            // rethrow lightblue error
            throw e;
        } catch (Exception e) {
            throw analyzeException(e, MetadataConstants.ERR_ILL_FORMED_METADATA);
        } finally {
            Error.pop();
        }

        LOGGER.debug("createUpdateEntityInfoIndexes: end");
    }

    private DBObject findIndexWithSignature(List<DBObject> existingIndexes,Index index) {
        for(DBObject existingIndex:existingIndexes) {
            if(indexFieldsMatch(index,existingIndex))
                return existingIndex;
        }
        return null;
    }

    private boolean isIdIndex(Index index) {
        List<IndexSortKey> fields = index.getFields();
        return fields.size() == 1
                && fields.get(0).getField().equals(Translator.ID_PATH);
    }


      private boolean isIdIndex(DBObject index) {
        BasicDBObject keys=(BasicDBObject)index.get("key");
        if(keys!=null&&keys.size()==1&&keys.containsKey("_id"))
            return true;
        return false;
    }

    private boolean compareSortKeys(IndexSortKey sortKey, String fieldName, Object dir) {
        String field = sortKey.getField().toString();
        if (sortKey.isCaseInsensitive()) {
            // if this is a case insensitive key, we need to change the field to how mongo actually stores the index
            field = Translator.translatePath(Translator.getHiddenForField(sortKey.getField()));
        }

        if (field.equals(fieldName)) {
            int direction = ((Number) dir).intValue();
            return sortKey.isDesc() == (direction < 0);
        }
        return false;
    }

    protected boolean indexFieldsMatch(Index index, DBObject existingIndex) {
        BasicDBObject keys = (BasicDBObject) existingIndex.get("key");
        if (keys != null) {
            List<IndexSortKey> fields = index.getFields();
            if (keys.size() == fields.size()) {
                Iterator<IndexSortKey> sortKeyItr = fields.iterator();
                for (Map.Entry<String, Object> dbKeyEntry : keys.entrySet()) {
                    IndexSortKey sortKey = sortKeyItr.next();
                    if (!compareSortKeys(sortKey, dbKeyEntry.getKey(), dbKeyEntry.getValue())) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    private boolean indexOptionsMatch(Index index, DBObject existingIndex) {
        Boolean unique = (Boolean) existingIndex.get("unique");
        if (unique != null) {
            if ((unique && index.isUnique())
                    || (!unique && !index.isUnique())) {
                return true;
            }
        } else {
            if (!index.isUnique()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a projection containing the requested projection, all identity
     * fields, and the objectType field
     */
    private Projection getProjectionFields(Projection requestedProjection,
                                           EntityMetadata md) {
        Field[] identityFields = md.getEntitySchema().getIdentityFields();
        List<Projection> projectFields = new ArrayList<>(identityFields == null ? 1 : identityFields.length + 1);
        if (requestedProjection instanceof ProjectionList) {
            projectFields.addAll(((ProjectionList) requestedProjection).getItems());
        } else if (requestedProjection != null) {
            projectFields.add(requestedProjection);
        }
        if (identityFields != null) {
            for (Field x : identityFields) {
                projectFields.add(new FieldProjection(x.getFullPath(), true, false));
            }
        }
        projectFields.add(new FieldProjection(Translator.OBJECT_TYPE, true, false));

        return new ProjectionList(projectFields);
    }

    private Error analyzeException(Exception e, final String otherwise) {
        return analyzeException(e, otherwise, null, false);
    }

    private Error analyzeException(Exception e, final String otherwise, final String msg, boolean specialHandling) {
        if(e instanceof Error)
            return (Error)e;

        if(e instanceof MongoException) {
            MongoException me=(MongoException)e;
            if(me.getCode()==18) {
                return Error.get(CrudConstants.ERR_AUTH_FAILED, e.getMessage());
            } else {
                if(me instanceof MongoTimeoutException||
                   me instanceof MongoExecutionTimeoutException) {
                    return Error.get(CrudConstants.ERR_DATASOURCE_TIMEOUT, e.getMessage());
                } else if(me instanceof DuplicateKeyException) {
                    return Error.get(MongoCrudConstants.ERR_DUPLICATE,e.getMessage());
                } else if(me instanceof MongoSocketException) {
                    return Error.get(MongoCrudConstants.ERR_CONNECTION_ERROR,e.getMessage());
                } else {
                    return Error.get(MongoCrudConstants.ERR_MONGO_ERROR,e.getMessage());
                }
            }
        } else {
            if(msg==null) {
                return Error.get(otherwise,e.getMessage());
            } else {
                if(specialHandling) {
                    return Error.get(otherwise,msg,e);
                } else {
                    return Error.get(otherwise,msg);
                }
            }
        }
    }
}
