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

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.CrudConstants;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.eval.FieldAccessRoleEvaluator;
import com.redhat.lightblue.interceptor.InterceptPoint;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.Field;
import com.redhat.lightblue.mongo.hystrix.FindOneCommand;
import com.redhat.lightblue.mongo.hystrix.InsertCommand;
import com.redhat.lightblue.mongo.hystrix.UpdateCommand;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.Path;

/**
 * Basic doc saver with no transaction support
 */
public class BasicDocSaver implements DocSaver {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicDocSaver.class);

    private final FieldAccessRoleEvaluator roleEval;
    private final Translator translator;

    /**
     * Creates a doc saver with the given translator and role evaluator
     */
    public BasicDocSaver(Translator translator,
            FieldAccessRoleEvaluator roleEval) {
        this.translator = translator;
        this.roleEval = roleEval;
    }

    @Override
    public void saveDoc(CRUDOperationContext ctx,
                        Op op,
                        boolean upsert,
                        DBCollection collection,
                        EntityMetadata md,
                        DBObject dbObject,
                        DocCtx inputDoc) {

        WriteResult result = null;
        String error = null;

        DBObject oldDBObject=null;

        Object id=dbObject.get(MongoCRUDController.ID_STR);
        if(id==null) {
            LOGGER.debug("Null _id, looking up the doc using identity fields");
            Field[] identityFields=md.getEntitySchema().getIdentityFields();
            Object[] identityFieldValues=fill(dbObject,identityFields);
            if(!isNull(identityFieldValues)) {
                DBObject lookupq=getLookupQ(identityFields,identityFieldValues);
                LOGGER.debug("Lookup query: {}",lookupq);
                oldDBObject=new FindOneCommand(collection,lookupq).executeAndUnwrap();
                LOGGER.debug("Retrieved:{}",oldDBObject);
                if(oldDBObject!=null)
                    id=oldDBObject.get(MongoCRUDController.ID_STR);
                LOGGER.debug("Retrieved id:{}",id);
            }
        }

        if (op == DocSaver.Op.insert
            || (id==null && upsert)) {
            // Inserting
            result = insertDoc(ctx, collection, md, dbObject, inputDoc);
        } else if (op == DocSaver.Op.save && id!=null) {
            BsonMerge merge=new BsonMerge(md);
            // Updating
            LOGGER.debug("Updating doc {}" + id);
            BasicDBObject q = new BasicDBObject(MongoCRUDController.ID_STR, Translator.createIdFrom(id));
            if(oldDBObject==null) {
                oldDBObject = new FindOneCommand(collection, q).executeAndUnwrap();
            }
            if (oldDBObject != null) {
                if (md.getAccess().getUpdate().hasAccess(ctx.getCallerRoles())) {
                    JsonDoc oldDoc = translator.toJson(oldDBObject);
                    inputDoc.setOriginalDocument(oldDoc);
                    Set<Path> paths = roleEval.getInaccessibleFields_Update(inputDoc, oldDoc);
                    if (paths == null || paths.isEmpty()) {
                        ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE_DOC, ctx, inputDoc);
                        merge.merge(oldDBObject,dbObject);
                        result = new UpdateCommand(collection, q, dbObject, upsert, false).executeAndUnwrap();
                        inputDoc.setCRUDOperationPerformed(CRUDOperation.UPDATE);
                        ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_UPDATE_DOC, ctx, inputDoc);
                    } else {
                        inputDoc.addError(Error.get("update",
                                CrudConstants.ERR_NO_FIELD_UPDATE_ACCESS, paths.toString()));
                    }
                } else {
                    inputDoc.addError(Error.get("update",
                            CrudConstants.ERR_NO_ACCESS, "update:" + md.getName()));
                }
            } else {
                // Cannot update, doc does not exist, insert
                result = insertDoc(ctx, collection, md, dbObject, inputDoc);
            }
        } else {
            // Error, invalid request
            LOGGER.warn("Invalid request, cannot update or insert");
            inputDoc.addError(Error.get(op.toString(), MongoCrudConstants.ERR_SAVE_ERROR, "Invalid request"));
        }

        LOGGER.debug("Write result {}", result);
        if (result != null) {
            if (error == null) {
                error = result.getError();
            }
            if (error != null) {
                inputDoc.addError(Error.get(op.toString(), MongoCrudConstants.ERR_SAVE_ERROR, error));
            }
        }
    }

    private DBObject getLookupQ(Field[] fields,Object[] values) {
        BasicDBObject dbObject=new BasicDBObject();
        for(int i=0;i<fields.length;i++) {
            String path=Translator.translatePath(fields[i].getFullPath());
            if(!path.equals(MongoCRUDController.ID_STR))
                dbObject.append(path,values[i]);
        }
        return dbObject;
    }

    /**
     * Return the values for the fields
     */
    private Object[] fill(DBObject object,Field[] fields) {
        Object[] ret=new Object[fields.length];
        for(int i=0;i<ret.length;i++)
            ret[i]=Translator.getDBObject(object,fields[i].getFullPath());
        return ret;
    }

    /**
     * Return if all values are null
     */
    private boolean isNull(Object[] values) {
        if(values!=null) {
            for(int i=0;i<values.length;i++) {
                if(values[i]!=null) {
                    return false;
                }
            }
        }
        return true;
    }


    private WriteResult insertDoc(CRUDOperationContext ctx,
            DBCollection collection,
            EntityMetadata md,
            DBObject dbObject,
            DocCtx inputDoc) {
        LOGGER.debug("Inserting doc");
        if (!md.getAccess().getInsert().hasAccess(ctx.getCallerRoles())) {
            inputDoc.addError(Error.get("insert",
                    MongoCrudConstants.ERR_NO_ACCESS,
                    "insert:" + md.getName()));
        } else {
            Set<Path> paths = roleEval.getInaccessibleFields_Insert(inputDoc);
            LOGGER.debug("Inaccessible fields:{}", paths);
            if (paths == null || paths.isEmpty()) {
                try {
                    ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_INSERT_DOC, ctx, inputDoc);
                    WriteResult r = new InsertCommand(collection, dbObject).executeAndUnwrap();
                    inputDoc.setCRUDOperationPerformed(CRUDOperation.INSERT);
                    ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.POST_CRUD_INSERT_DOC, ctx, inputDoc);
                    return r;
                } catch (MongoException.DuplicateKey dke) {
                    LOGGER.error("saveOrInsert failed: {}", dke);
                    inputDoc.addError(Error.get("insert", MongoCrudConstants.ERR_DUPLICATE, dke));
                }
            } else {
                for(Path path : paths){
                    inputDoc.addError(Error.get("insert", CrudConstants.ERR_NO_FIELD_INSERT_ACCESS, path.toString()));
                }
            }
        }
        return null;
    }
}
