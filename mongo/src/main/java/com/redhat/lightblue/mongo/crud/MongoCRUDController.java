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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.util.JSON;
import com.redhat.lightblue.config.ControllerConfiguration;
import com.redhat.lightblue.crud.CRUDController;
import com.redhat.lightblue.crud.CRUDDeleteResponse;
import com.redhat.lightblue.crud.CRUDFindResponse;
import com.redhat.lightblue.crud.LightblueHealth;
import com.redhat.lightblue.crud.CRUDInsertionResponse;
import com.redhat.lightblue.crud.CRUDOperation;
import com.redhat.lightblue.crud.CRUDOperationContext;
import com.redhat.lightblue.crud.CRUDSaveResponse;
import com.redhat.lightblue.crud.CRUDUpdateResponse;
import com.redhat.lightblue.crud.ConstraintValidator;
import com.redhat.lightblue.crud.CrudConstants;
import com.redhat.lightblue.crud.DocCtx;
import com.redhat.lightblue.crud.ExplainQuerySupport;
import com.redhat.lightblue.crud.DocumentStream;
import com.redhat.lightblue.crud.MetadataResolver;
import com.redhat.lightblue.eval.FieldAccessRoleEvaluator;
import com.redhat.lightblue.eval.Projector;
import com.redhat.lightblue.eval.Updater;
import com.redhat.lightblue.extensions.Extension;
import com.redhat.lightblue.extensions.ExtensionSupport;
import com.redhat.lightblue.extensions.synch.LockingSupport;
import com.redhat.lightblue.extensions.valuegenerator.ValueGeneratorSupport;
import com.redhat.lightblue.interceptor.InterceptPoint;
import com.redhat.lightblue.metadata.EntityInfo;
import com.redhat.lightblue.metadata.EntityMetadata;
import com.redhat.lightblue.metadata.EntitySchema;
import com.redhat.lightblue.metadata.Field;
import com.redhat.lightblue.metadata.FieldCursor;
import com.redhat.lightblue.metadata.FieldTreeNode;
import com.redhat.lightblue.metadata.Index;
import com.redhat.lightblue.metadata.IndexSortKey;
import com.redhat.lightblue.metadata.Indexes;
import com.redhat.lightblue.metadata.Metadata;
import com.redhat.lightblue.metadata.MetadataConstants;
import com.redhat.lightblue.metadata.MetadataListener;
import com.redhat.lightblue.metadata.SimpleField;
import com.redhat.lightblue.metadata.types.StringType;
import com.redhat.lightblue.mongo.common.DBResolver;
import com.redhat.lightblue.mongo.common.MongoDataStore;
import com.redhat.lightblue.mongo.config.MongoConfiguration;
import com.redhat.lightblue.mongo.metadata.MongoMetadataConstants;
import com.redhat.lightblue.query.FieldProjection;
import com.redhat.lightblue.query.Projection;
import com.redhat.lightblue.query.ProjectionList;
import com.redhat.lightblue.query.QueryExpression;
import com.redhat.lightblue.query.Sort;
import com.redhat.lightblue.query.UpdateExpression;
import com.redhat.lightblue.util.Error;
import com.redhat.lightblue.util.JsonDoc;
import com.redhat.lightblue.util.Path;

public class MongoCRUDController implements CRUDController, MetadataListener, ExtensionSupport, ExplainQuerySupport {

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
	private static final Projection EMPTY_PROJECTION = new FieldProjection(new Path("*"), false, false);

	private final DBResolver dbResolver;
	private final ControllerConfiguration controllerCfg;

	private final int batchSize;
	private final ConcurrentModificationDetectionCfg concurrentModificationDetection;

	public MongoCRUDController(ControllerConfiguration controllerCfg, DBResolver dbResolver) {
		this.dbResolver = dbResolver;
		this.controllerCfg = controllerCfg;
		this.batchSize = getIntOption("updateBatchSize", 64);
		this.concurrentModificationDetection = new ConcurrentModificationDetectionCfg(controllerCfg);
	}

	private String getOption(String optionName, String defaultValue) {
		if (controllerCfg != null) {
			ObjectNode node = controllerCfg.getOptions();
			if (node != null) {
				JsonNode value = node.get(optionName);
				if (value != null && !value.isNull())
					return value.asText();
			}
		}
		return defaultValue;
	}

	private int getIntOption(String optionName, int defaultValue) {
		String v = getOption(optionName, null);
		if (v != null) {
			return Integer.valueOf(v);
		} else {
			return defaultValue;
		}
	}

	private boolean getBooleanOption(String optionName, boolean defaultValue) {
		String v = getOption(optionName, null);
		if (v != null) {
			return Boolean.valueOf(v);
		} else {
			return defaultValue;
		}
	}

	public DBResolver getDbResolver() {
		return dbResolver;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public ControllerConfiguration getControllerConfiguration() {
		return controllerCfg;
	}

	/**
	 * Insertion operation for mongo
	 */
	@Override
	public CRUDInsertionResponse insert(CRUDOperationContext ctx, Projection projection) {
		LOGGER.debug("insert() start");
		CRUDInsertionResponse response = new CRUDInsertionResponse();
		ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_INSERT, ctx);
		int n = saveOrInsert(ctx, false, projection, OP_INSERT);
		response.setNumInserted(n);
		return response;
	}

	@Override
	public CRUDSaveResponse save(CRUDOperationContext ctx, boolean upsert, Projection projection) {
		LOGGER.debug("save() start");
		CRUDSaveResponse response = new CRUDSaveResponse();
		ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_SAVE, ctx);
		int n = saveOrInsert(ctx, upsert, projection, OP_SAVE);
		response.setNumSaved(n);
		return response;
	}

	private int saveOrInsert(CRUDOperationContext ctx, boolean upsert, Projection projection, String operation) {
		int ret = 0;
		List<DocCtx> documents = ctx.getInputDocumentsWithoutErrors();
		if (documents == null || documents.isEmpty()) {
			return ret;
		}
		for (DocCtx doc : documents) {
			doc.setOriginalDocument(doc);
		}
		LOGGER.debug("saveOrInsert() start");
		Error.push("mongo:" + operation);
		DocTranslator translator = new DocTranslator(ctx, ctx.getFactory().getNodeFactory());
		try {
			FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(ctx.getEntityMetadata(ctx.getEntityName()),
					ctx.getCallerRoles());
			LOGGER.debug("saveOrInsert: Translating docs");
			EntityMetadata md = ctx.getEntityMetadata(ctx.getEntityName());
			DocTranslator.TranslatedBsonDoc[] dbObjects = translator.toBson(documents);
			// dbObjects[i] is the translation of documents.get(i)
			if (dbObjects != null) {
				LOGGER.debug("saveOrInsert: {} docs translated to bson", dbObjects.length);

				MongoDataStore store = (MongoDataStore) md.getDataStore();
				DB db = dbResolver.get(store);
				MongoConfiguration cfg = dbResolver.getConfiguration(store);
				DBCollection collection = db.getCollection(store.getCollectionName());

				Projection combinedProjection = Projection.add(projection,
						roleEval.getExcludedFields(FieldAccessRoleEvaluator.Operation.find));

				Projector projector;
				if (combinedProjection != null) {
					projector = Projector.getInstance(combinedProjection, md);
				} else {
					projector = null;
				}
				DocSaver saver = new BasicDocSaver(translator, roleEval, md,
						MongoExecutionOptions.getWriteConcern(ctx.getExecutionOptions()), batchSize,
						concurrentModificationDetection);
				ctx.setProperty(PROP_SAVER, saver);

				saver.saveDocs(ctx, operation.equals(OP_INSERT) ? DocSaver.Op.insert : DocSaver.Op.save, upsert,
						collection, dbObjects, documents.toArray(new DocCtx[documents.size()]));

				for (int docIndex = 0; docIndex < dbObjects.length; docIndex++) {
					DocCtx inputDoc = documents.get(docIndex);
					DocTranslator.TranslatedDoc jsonDoc = translator.toJson(dbObjects[docIndex].doc);
					LOGGER.debug("Translated doc: {}", jsonDoc.doc);
					inputDoc.setUpdatedDocument(jsonDoc.doc);
					inputDoc.setResultMetadata(jsonDoc.rmd);
					if (projector != null) {
						inputDoc.setOutputDocument(projector.project(jsonDoc.doc, ctx.getFactory().getNodeFactory()));
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
			LOGGER.error("Error in saveOrInsert", e);
			ctx.addError(e);
		} catch (Exception e) {
			LOGGER.error("Exception in saveOrInsert", e);
			ctx.addError(analyzeException(e, CrudConstants.ERR_CRUD));
		} finally {
			Error.pop();
		}
		LOGGER.debug("saveOrInsert() end: {} docs requested, {} saved", documents.size(), ret);
		return ret;
	}

	@Override
	public CRUDUpdateResponse update(CRUDOperationContext ctx, QueryExpression query, UpdateExpression update,
			Projection projection) {
		LOGGER.debug("update start: q:{} u:{} p:{}", query, update, projection);
		Error.push("mongo:" + OP_UPDATE);
		CRUDUpdateResponse response = new CRUDUpdateResponse();
		DocTranslator translator = new DocTranslator(ctx, ctx.getFactory().getNodeFactory());
		ExpressionTranslator xtranslator = new ExpressionTranslator(ctx, ctx.getFactory().getNodeFactory());
		try {
			if (query == null) {
				throw Error.get("update", MongoCrudConstants.ERR_NULL_QUERY, "");
			}
			ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_UPDATE, ctx);
			EntityMetadata md = ctx.getEntityMetadata(ctx.getEntityName());
			if (md.getAccess().getUpdate().hasAccess(ctx.getCallerRoles())) {
				ConstraintValidator validator = ctx.getFactory().getConstraintValidator(md);
				LOGGER.debug("Translating query {}", query);
				DBObject mongoQuery = xtranslator.translate(md,
						ExpressionTranslator.appendObjectType(query, ctx.getEntityName()));
				LOGGER.debug("Translated query {}", mongoQuery);
				FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(md, ctx.getCallerRoles());

				Projector projector;
				if (projection != null) {
					Projection x = Projection.add(projection,
							roleEval.getExcludedFields(FieldAccessRoleEvaluator.Operation.find));
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

				// If there are any constraints for updated fields, or if we're
				// updating arrays, we have to use iterate-update
				Updater updater = Updater.getInstance(ctx.getFactory().getNodeFactory(), md, update);

				DocUpdater docUpdater = new IterateAndUpdate(ctx.getFactory().getNodeFactory(), validator, roleEval,
						translator, updater, projector, errorProjector,
						MongoExecutionOptions.getWriteConcern(ctx.getExecutionOptions()), batchSize,
						concurrentModificationDetection);
				ctx.setProperty(PROP_UPDATER, docUpdater);
				docUpdater.update(ctx, coll, md, response, mongoQuery);
				ctx.getHookManager().queueHooks(ctx);
			} else {
				ctx.addError(Error.get(MongoCrudConstants.ERR_NO_ACCESS, "update:" + ctx.getEntityName()));
			}
		} catch (Error e) {
			LOGGER.error("Error in update", e);
			ctx.addError(e);
		} catch (Exception e) {
			LOGGER.error("Exception in update", e);
			ctx.addError(analyzeException(e, CrudConstants.ERR_CRUD));
		} finally {
			Error.pop();
		}
		LOGGER.debug("update end: updated: {}, failed: {}", response.getNumUpdated(), response.getNumFailed());
		return response;
	}

	@Override
	public CRUDDeleteResponse delete(CRUDOperationContext ctx, QueryExpression query) {
		LOGGER.debug("delete start: q:{}", query);
		Error.push("mongo:" + OP_DELETE);
		CRUDDeleteResponse response = new CRUDDeleteResponse();
		DocTranslator translator = new DocTranslator(ctx, ctx.getFactory().getNodeFactory());
		ExpressionTranslator xtranslator = new ExpressionTranslator(ctx, ctx.getFactory().getNodeFactory());
		try {
			if (query == null) {
				throw Error.get("delete", MongoCrudConstants.ERR_NULL_QUERY, "");
			}
			ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_DELETE, ctx);
			EntityMetadata md = ctx.getEntityMetadata(ctx.getEntityName());
			if (md.getAccess().getDelete().hasAccess(ctx.getCallerRoles())) {
				LOGGER.debug("Translating query {}", query);
				DBObject mongoQuery = xtranslator.translate(md,
						ExpressionTranslator.appendObjectType(query, ctx.getEntityName()));
				LOGGER.debug("Translated query {}", mongoQuery);
				DB db = dbResolver.get((MongoDataStore) md.getDataStore());
				DBCollection coll = db.getCollection(((MongoDataStore) md.getDataStore()).getCollectionName());
				DocDeleter deleter = new BasicDocDeleter(translator,
						MongoExecutionOptions.getWriteConcern(ctx.getExecutionOptions()), batchSize);
				ctx.setProperty(PROP_DELETER, deleter);
				deleter.delete(ctx, coll, mongoQuery, response);
				ctx.getHookManager().queueHooks(ctx);
			} else {
				ctx.addError(Error.get(MongoCrudConstants.ERR_NO_ACCESS, "delete:" + ctx.getEntityName()));
			}
		} catch (Error e) {
			LOGGER.error("Error in delete", e);
			ctx.addError(e);
		} catch (Exception e) {
			LOGGER.error("Exception in delete", e);
			ctx.addError(analyzeException(e, CrudConstants.ERR_CRUD));
		} finally {
			Error.pop();
		}
		LOGGER.debug("delete end: deleted: {}}", response.getNumDeleted());
		return response;
	}

	protected long getMaxQueryTimeMS(MongoConfiguration cfg, CRUDOperationContext ctx) {
		// pick the default, even if we don't have a configuration coming in
		long output = MongoConfiguration.DEFAULT_MAX_QUERY_TIME_MS;

		// if we have a config, get that value
		if (cfg != null) {
			output = cfg.getMaxQueryTimeMS();
		}

		// if context has execution option for maxQueryTimeMS use that instead
		// of global default
		if (ctx != null && ctx.getExecutionOptions() != null && ctx.getExecutionOptions()
				.getOptionValueFor(MongoConfiguration.PROPERTY_NAME_MAX_QUERY_TIME_MS) != null) {
			try {
				output = Long.parseLong(ctx.getExecutionOptions()
						.getOptionValueFor(MongoConfiguration.PROPERTY_NAME_MAX_QUERY_TIME_MS));

			} catch (NumberFormatException nfe) {
				// oh well, do nothing. couldn't parse
				LOGGER.debug("Unable to parse execution option: maxQueryTimeMS=" + ctx.getExecutionOptions()
						.getOptionValueFor(MongoConfiguration.PROPERTY_NAME_MAX_QUERY_TIME_MS));
			}
		}

		return output;
	}

	/**
	 * Search implementation for mongo
	 */
	@Override
	public CRUDFindResponse find(CRUDOperationContext ctx, QueryExpression query, Projection projection, Sort sort,
			Long from, Long to) {
		LOGGER.debug("find start: q:{} p:{} sort:{} from:{} to:{}", query, projection, sort, from, to);
		Error.push("mongo:" + OP_FIND);
		CRUDFindResponse response = new CRUDFindResponse();
		DocTranslator translator = new DocTranslator(ctx, ctx.getFactory().getNodeFactory());
		ExpressionTranslator xtranslator = new ExpressionTranslator(ctx, ctx.getFactory().getNodeFactory());
		try {
			ctx.getFactory().getInterceptors().callInterceptors(InterceptPoint.PRE_CRUD_FIND, ctx);
			EntityMetadata md = ctx.getEntityMetadata(ctx.getEntityName());
			if (md.getAccess().getFind().hasAccess(ctx.getCallerRoles())) {
				FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(md, ctx.getCallerRoles());
				LOGGER.debug("Translating query {}", query);
				DBObject mongoQuery = xtranslator.translate(md,
						ExpressionTranslator.appendObjectType(query, ctx.getEntityName()));
				LOGGER.debug("Translated query {}", mongoQuery);
				DBObject mongoSort;
				if (sort != null) {
					LOGGER.debug("Translating sort {}", sort);
					mongoSort = xtranslator.translate(sort);
					LOGGER.debug("Translated sort {}", mongoSort);
				} else {
					mongoSort = null;
				}
				DBObject mongoProjection = xtranslator.translateProjection(md, getProjectionFields(projection, md),
						query, sort);
				LOGGER.debug("Translated projection {}", mongoProjection);
				DB db = dbResolver.get((MongoDataStore) md.getDataStore());
				DBCollection coll = db.getCollection(((MongoDataStore) md.getDataStore()).getCollectionName());
				LOGGER.debug("Retrieve db collection:" + coll);
				DocFinder finder = new BasicDocFinder(translator,
						MongoExecutionOptions.getReadPreference(ctx.getExecutionOptions()));
				MongoConfiguration cfg = dbResolver.getConfiguration((MongoDataStore) md.getDataStore());
				if (cfg != null) {
					finder.setMaxResultSetSize(cfg.getMaxResultSetSize());
				}
				finder.setMaxQueryTimeMS(getMaxQueryTimeMS(cfg, ctx));
				ctx.setProperty(PROP_FINDER, finder);
				response.setSize(finder.find(ctx, coll, mongoQuery, mongoProjection, mongoSort, from, to));
				// Project results
				Projector projector = Projector
						.getInstance(
								projection == null ? EMPTY_PROJECTION
										: Projection.add(projection,
												roleEval.getExcludedFields(FieldAccessRoleEvaluator.Operation.find)),
								md);
				ctx.setDocumentStream(DocumentStream.map(ctx.getDocumentStream(), d -> {
					ctx.measure.begin("projectFound");
					d.setOutputDocument(projector.project(d, JsonNodeFactory.instance));
					ctx.measure.end("projectFound");
					return d;
				}));
				ctx.getHookManager().queueHooks(ctx);
			} else {
				ctx.addError(Error.get(MongoCrudConstants.ERR_NO_ACCESS, "find:" + ctx.getEntityName()));
			}
		} catch (Error e) {
			LOGGER.error("Error in find", e);
			ctx.addError(e);
		} catch (Exception e) {
			LOGGER.error("Error during find:", e);
			ctx.addError(analyzeException(e, CrudConstants.ERR_CRUD));
		} finally {
			Error.pop();
		}
		LOGGER.debug("find end: query: {} results: {}", response.getSize());
		return response;
	}

	@Override
	public void explain(CRUDOperationContext ctx, QueryExpression query, Projection projection, Sort sort, Long from,
			Long to, JsonDoc destDoc) {

		LOGGER.debug("explain start: q:{} p:{} sort:{} from:{} to:{}", query, projection, sort, from, to);
		Error.push("explain");
		ExpressionTranslator xtranslator = new ExpressionTranslator(ctx, ctx.getFactory().getNodeFactory());
		try {
			EntityMetadata md = ctx.getEntityMetadata(ctx.getEntityName());
			FieldAccessRoleEvaluator roleEval = new FieldAccessRoleEvaluator(md, ctx.getCallerRoles());
			LOGGER.debug("Translating query {}", query);
			DBObject mongoQuery = xtranslator.translate(md,
					ExpressionTranslator.appendObjectType(query, ctx.getEntityName()));
			LOGGER.debug("Translated query {}", mongoQuery);
			DBObject mongoProjection = xtranslator.translateProjection(md, getProjectionFields(projection, md), query,
					sort);
			LOGGER.debug("Translated projection {}", mongoProjection);
			DB db = dbResolver.get((MongoDataStore) md.getDataStore());
			DBCollection coll = db.getCollection(((MongoDataStore) md.getDataStore()).getCollectionName());
			LOGGER.debug("Retrieve db collection:" + coll);

			try (DBCursor cursor = coll.find(mongoQuery, mongoProjection)) {
				DBObject plan = cursor.explain();
				JsonNode jsonPlan = DocTranslator.rawObjectToJson(plan);
				if (mongoQuery != null)
					destDoc.modify(new Path("mongo.query"), DocTranslator.rawObjectToJson(mongoQuery), true);
				if (mongoProjection != null)
					destDoc.modify(new Path("mongo.projection"), DocTranslator.rawObjectToJson(mongoProjection), true);
				destDoc.modify(new Path("mongo.plan"), jsonPlan, true);
			}

		} catch (Error e) {
			ctx.addError(e);
		} catch (Exception e) {
			LOGGER.error("Error during explain:", e);
			ctx.addError(analyzeException(e, CrudConstants.ERR_CRUD));
		} finally {
			Error.pop();
		}
		LOGGER.debug("explain end: query: {} ", query);
	}

	@Override
	public void updatePredefinedFields(CRUDOperationContext ctx, JsonDoc doc) {
		// If it is a save operation, we rely on _id being passed by client, so
		// we don't auto-generate that
		// If not, it needs to be auto-generated
		if (ctx.getCRUDOperation() != CRUDOperation.SAVE) {
			JsonNode idNode = doc.get(DocTranslator.ID_PATH);
			if (idNode == null || idNode instanceof NullNode) {
				doc.modify(DocTranslator.ID_PATH, ctx.getFactory().getNodeFactory().textNode(ObjectId.get().toString()),
						false);
			}
		}
	}

	@Override
	public LightblueHealth checkHealth() {
		boolean isHealthy = true;
		List<MongoConfiguration> configs = dbResolver.getConfigurations();
		List<String> details = new ArrayList<>(configs.size());
		DBObject ping = new BasicDBObject("ping", 1);

		for (MongoConfiguration config : configs) {
			try {
				CommandResult result = config.getDB().command(ping);
				if (!result.get("ok").equals(1.0)) {
					isHealthy = false;
					details.add(config + ":ping_ok!=1.0");
				} else {
					details.add(config + ":ping_ok=1.0");
				}
			} catch (Exception e) {
				isHealthy = false;
				details.add(config + ":ping_exception=" + e);
			}
		}

		return new LightblueHealth(isHealthy, details.toString());
	}

	@Override
	public <E extends Extension> E getExtensionInstance(Class<? extends Extension> extensionClass) {
		if (extensionClass.equals(LockingSupport.class)) {
			return (E) new MongoLockingSupport(this);
		} else if (extensionClass.equals(ValueGeneratorSupport.class)) {
			return (E) new MongoSequenceSupport(this);
		}
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
		ensureIdIndex(ei);
		validateArrayIndexes(ei.getIndexes().getIndexes());
		validateSaneIndexSet(ei.getIndexes().getIndexes());
	}

	/**
	 * Array indexes must not end with `*`
	 * 
	 * @param indexes
	 */
	private void validateArrayIndexes(List<Index> indexes) {
		boolean endsWithAny = indexes.stream().flatMap(i -> i.getFields().stream()).map(f -> f.getField())
				.anyMatch(p -> p.getLast().equals(Path.ANY));
		if (endsWithAny) {
			throw Error.get(MongoCrudConstants.ERR_INVALID_INDEX_FIELD);
		}
	}

	@Override
	public void afterCreateNewSchema(Metadata md, EntityMetadata emd) {
	}

	@Override
	public void beforeCreateNewSchema(Metadata md, EntityMetadata emd) {
		validateNoHiddenInMetaData(emd);
		ensureIdField(emd);
	}

	private void validateNoHiddenInMetaData(EntityMetadata emd) {
		FieldCursor cursor = emd.getFieldCursor();
		while (cursor.next()) {
			if (cursor.getCurrentPath().getLast().equals(DocTranslator.HIDDEN_SUB_PATH.getLast())) {
				throw Error.get(MongoCrudConstants.ERR_RESERVED_FIELD);
			}
		}
	}

	/**
	 * No two index should have the same field signature
	 */
	private void validateSaneIndexSet(List<Index> indexes) {
		int n = indexes.size();
		for (int i = 0; i < n; i++) {
			List<IndexSortKey> keys1 = indexes.get(i).getFields();
			for (int j = i + 1; j < n; j++) {
				List<IndexSortKey> keys2 = indexes.get(j).getFields();
				if (sameSortKeys(keys1, keys2)) {
					throw Error.get(MongoCrudConstants.ERR_DUPLICATE_INDEX);
				}
			}
		}
	}

	private boolean sameSortKeys(List<IndexSortKey> keys1, List<IndexSortKey> keys2) {
		if (keys1.size() == keys2.size()) {
			for (int i = 0; i < keys1.size(); i++) {
				IndexSortKey k1 = keys1.get(i);
				IndexSortKey k2 = keys2.get(i);
				if (!k1.getField().equals(k2.getField()) || k1.isDesc() != k2.isDesc()
						|| k1.isCaseInsensitive() != k2.isCaseInsensitive()) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	private void ensureIdField(EntityMetadata md) {
		ensureIdField(md.getEntitySchema());
	}

	private void ensureIdField(EntitySchema schema) {
		LOGGER.debug("ensureIdField: begin");

		SimpleField idField;

		FieldTreeNode field;
		try {
			field = schema.resolve(DocTranslator.ID_PATH);
		} catch (Error e) {
			field = null;
		}
		if (field == null) {
			LOGGER.debug("Adding _id field");
			idField = new SimpleField(ID_STR, StringType.TYPE);
			schema.getFields().addNew(idField);
		} else if (!(field instanceof SimpleField)) {
			throw Error.get(MongoMetadataConstants.ERR_INVALID_ID);
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
			if (fields.size() == 1 && fields.get(0).getField().equals(DocTranslator.ID_PATH) && ix.isUnique()) {
				found = true;
				break;
			}
		}
		if (!found) {
			LOGGER.debug("Adding _id index");
			Index idIndex = new Index();
			idIndex.setUnique(true);
			List<IndexSortKey> fields = new ArrayList<>();
			fields.add(new IndexSortKey(DocTranslator.ID_PATH, false));
			idIndex.setFields(fields);
			List<Index> ix = indexes.getIndexes();
			ix.add(idIndex);
			indexes.setIndexes(ix);
		} else {
			LOGGER.debug("_id index exists");
		}
		LOGGER.debug("ensureIdIndex: end");
	}

	public static final String PARTIAL_FILTER_EXPRESSION_OPTION_NAME = "partialFilterExpression";

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
			// - The _id index will remain untouched.
			// - If there is an index with name X in metadata, find the same
			// named index, and compare
			// its fields/flags. If different, drop and recreate. Drop all
			// indexes with the same field signature.
			//
			// - If there is an index with null name in metadata, see if there
			// is an index with same
			// fields and flags. If so, no change. Otherwise, create index. Drop
			// all indexes with the same field signature.
			List<Index> createIndexes = new ArrayList<>();
			List<DBObject> dropIndexes = new ArrayList<>();
			List<DBObject> foundIndexes = new ArrayList<>();
			for (Index index : indexes.getIndexes()) {
				if (!isIdIndex(index)) {
					if (index.getName() != null && index.getName().trim().length() > 0) {
						LOGGER.debug("Processing index {}", index.getName());
						DBObject found = null;
						for (DBObject existingIndex : existingIndexes) {
							if (index.getName().equals(existingIndex.get("name"))) {
								found = existingIndex;
								break;
							}
						}
						if (found != null) {
							foundIndexes.add(found);
							// indexFieldsMatch will handle checking for hidden
							// versions of the index
							if (indexFieldsMatch(index, found) && indexOptionsMatch(index, found)) {
								LOGGER.debug("{} already exists", index.getName());
							} else {
								LOGGER.debug("{} modified, dropping and recreating index", index.getName());
								existingIndexes.remove(found);
								dropIndexes.add(found);
								createIndexes.add(index);
							}
						} else {
							LOGGER.debug("{} not found, checking if there is an index with same field signature",
									index.getName());
							found = findIndexWithSignature(existingIndexes, index);
							if (found == null) {
								LOGGER.debug("{} not found, creating", index.getName());
								createIndexes.add(index);
							} else {
								LOGGER.debug("There is an index with same field signature as {}, drop and recreate",
										index.getName());
								foundIndexes.add(found);
								dropIndexes.add(found);
								createIndexes.add(index);
							}
						}
					} else {
						LOGGER.debug("Processing index with fields {}", index.getFields());
						DBObject found = findIndexWithSignature(existingIndexes, index);
						if (found != null) {
							foundIndexes.add(found);
							LOGGER.debug("An index with same keys found: {}", found);
							if (indexOptionsMatch(index, found)) {
								LOGGER.debug("Same options as well, not changing");
							} else {
								LOGGER.debug("Index with different options, drop/recreate");
								dropIndexes.add(found);
								createIndexes.add(index);
							}
						} else {
							LOGGER.debug("Creating index with fields {}", index.getFields());
							createIndexes.add(index);
						}
					}
				}
			}
			// Any index in existingIndexes but not in foundIndexes should be
			// deleted as well
			for (DBObject index : existingIndexes) {
				boolean found = false;
				for (DBObject x : foundIndexes) {
					if (x == index) {
						found = true;
						break;
					}
				}
				if (!found) {
					for (DBObject x : dropIndexes) {
						if (x == index) {
							found = true;
							break;
						}
					}
					if (!found && !isIdIndex(index)) {
						LOGGER.warn("Dropping index {}", index.get("name"));
						entityCollection.dropIndex(index.get("name").toString());
					}
				}
			}
			for (DBObject index : dropIndexes) {
				LOGGER.warn("Dropping index {}", index.get("name"));
				entityCollection.dropIndex(index.get("name").toString());
			}
			// we want to run in the background if we're only creating indexes
			// (no field generation)
			boolean hidden = false;
			// fieldMap is <canonicalPath, hiddenPath>
			List<Path> fields = new ArrayList<>();
			for (Index index : createIndexes) {
				DBObject newIndex = new BasicDBObject();
				for (IndexSortKey p : index.getFields()) {
					Path field = p.getField();
					if (p.isCaseInsensitive()) {
						fields.add(p.getField());
						field = DocTranslator.getHiddenForField(field);
						// if we have a case insensitive index, we want the
						// index creation operation to be blocking
						hidden = true;
					}
					newIndex.put(ExpressionTranslator.translatePath(field), p.isDesc() ? -1 : 1);
				}
				BasicDBObject options = new BasicDBObject("unique", index.isUnique());
				// if index is unique and non-partial, also make it a sparse
				// index, so we can have non-required unique fields
				options.append("sparse",
						index.isUnique() && !index.getProperties().containsKey(PARTIAL_FILTER_EXPRESSION_OPTION_NAME));
				if (index.getName() != null && index.getName().trim().length() > 0) {
					options.append("name", index.getName().trim());
				}
				options.append("background", true);
				// partial index
				if (index.getProperties().containsKey(PARTIAL_FILTER_EXPRESSION_OPTION_NAME)) {
					try {
						@SuppressWarnings("unchecked")
						DBObject filter = new BasicDBObject(
								(Map<String, Object>) index.getProperties().get(PARTIAL_FILTER_EXPRESSION_OPTION_NAME));
						options.append(PARTIAL_FILTER_EXPRESSION_OPTION_NAME, filter);
					} catch (ClassCastException e) {
						throw new RuntimeException("Index property " + PARTIAL_FILTER_EXPRESSION_OPTION_NAME
								+ " needs to be a mongo query in json format", e);
					}
				}
				LOGGER.debug("Creating index {} with options {}", newIndex, options);
				LOGGER.warn("Creating index {} with fields={}, options={}", index.getName(), index.getFields(),
						options);
				entityCollection.createIndex(newIndex, options);
			}
			if (hidden) {
				LOGGER.info("Executing post-index creation updates...");
				// case insensitive indexes have been updated or created.
				// recalculate all hidden fields
				Thread pop = new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							populateHiddenFields(ei, md, fields);
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}
				});
				pop.start();

				// TODO: remove hidden fields on index deletions? Worth it?
			}
		} catch (MongoException me) {
			throw Error.get(MongoCrudConstants.ERR_ENTITY_INDEX_NOT_CREATED, me.getMessage());
		} catch (Exception e) {
			throw analyzeException(e, MetadataConstants.ERR_ILL_FORMED_METADATA);
		} finally {
			Error.pop();
		}
		LOGGER.debug("createUpdateEntityInfoIndexes: end");
	}

	/**
	 * Reindexes all lightblue managed, hidden, indexes.
	 *
	 * This operation is blocking and is therefore suggested to be ran in a
	 * separate thread
	 *
	 * @param md
	 * @throws IOException
	 */
	public void reindex(EntityInfo ei, Metadata md) throws IOException {
		reindex(ei, md, null, null);
	}

	public void reindex(EntityInfo ei, Metadata md, String version, QueryExpression query) throws IOException {
		Map<String, String> fieldMap = DocTranslator.getCaseInsensitiveIndexes(ei.getIndexes().getIndexes())
				.collect(Collectors.toMap(i -> i.getField().toString(),
						i -> DocTranslator.getHiddenForField(i.getField()).toString()));
		List<Path> fields = DocTranslator.getCaseInsensitiveIndexes(ei.getIndexes().getIndexes()).map(i -> i.getField())
				.collect(Collectors.toList());
		if (!fieldMap.keySet().isEmpty()) {
			populateHiddenFields(ei, md, version, fields, query);
		}
		// This is not a common command, I think INFO level is safe and
		// appropriate
		LOGGER.info("Starting reindex of {} for fields:  {}", ei.getName(), fieldMap.keySet());
	}

	/**
	 *
	 * Populates all hidden fields from their initial index values in the
	 * collection in this context
	 *
	 * This method has the potential to be extremely heavy and nonperformant.
	 * Recommended to run in a background thread.
	 *
	 * @param ei
	 * @param fieldMap
	 *            <index, hiddenPath>
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	protected void populateHiddenFields(EntityInfo ei, Metadata md, List<Path> fields) throws IOException {
		populateHiddenFields(ei, md, null, fields, null);
	}

	protected void populateHiddenFields(EntityInfo ei, Metadata md, String version, List<Path> fields,
			QueryExpression query) throws IOException {
		LOGGER.info("Starting population of hidden fields due to new or modified indexes.");
		MongoDataStore ds = (MongoDataStore) ei.getDataStore();
		DB entityDB = dbResolver.get(ds);
		DBCollection coll = entityDB.getCollection(ds.getCollectionName());
		DBCursor cursor = null;
		try {
			if (query != null) {
				MetadataResolver mdResolver = new MetadataResolver() {
					@Override
					public EntityMetadata getEntityMetadata(String entityName) {
						String v = version == null ? ei.getDefaultVersion() : version;
						return md.getEntityMetadata(entityName, v);
					}
				};
				ExpressionTranslator trans = new ExpressionTranslator(mdResolver, JsonNodeFactory.instance);
				DBObject mongoQuery = trans.translate(mdResolver.getEntityMetadata(ei.getName()), query);
				cursor = coll.find(mongoQuery);
			} else {
				cursor = coll.find();
			}
			while (cursor.hasNext()) {
				DBObject doc = cursor.next();
				DBObject original = (DBObject) ((BasicDBObject) doc).copy();
				try {
					DocTranslator.populateDocHiddenFields(doc, fields);
					LOGGER.debug("Original doc:{}, new doc:{}", original, doc);
					if (!doc.equals(original)) {
						coll.save(doc);
					}
				} catch (Exception e) {
					// skip the doc if there's a problem, don't outright fail
					LOGGER.error(e.getMessage());
					LOGGER.debug("Original doc:\n{}", original);
					LOGGER.debug("Error saving doc:\n{}", doc);
				}
			}
		} catch (Exception e) {
			LOGGER.error("Error during reindexing");
			LOGGER.error(e.getMessage());
			throw new RuntimeException(e);
		} finally {
			cursor.close();
		}
		LOGGER.info("Finished population of hidden fields.");
	}

	private DBObject findIndexWithSignature(List<DBObject> existingIndexes, Index index) {
		for (DBObject existingIndex : existingIndexes) {
			if (indexFieldsMatch(index, existingIndex)) {
				return existingIndex;
			}
		}
		return null;
	}

	private boolean isIdIndex(Index index) {
		List<IndexSortKey> fields = index.getFields();
		return fields.size() == 1 && fields.get(0).getField().equals(DocTranslator.ID_PATH);
	}

	private boolean isIdIndex(DBObject index) {
		BasicDBObject keys = (BasicDBObject) index.get("key");
		return (keys != null && keys.size() == 1 && keys.containsField("_id"));
	}

	private boolean compareSortKeys(IndexSortKey sortKey, String fieldName, Object dir) {
		String field;
		if (sortKey.isCaseInsensitive()) {
			// if this is a case insensitive key, we need to change the field to
			// how mongo actually stores the index
			field = ExpressionTranslator.translatePath(DocTranslator.getHiddenForField(sortKey.getField()));
		} else {
			// strip out wild card.
			// this happens because we forget mongo fields != lightblue path
			// especially given case insensitive index requires lightblue path
			field = ExpressionTranslator.translatePath(sortKey.getField());
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

	/**
	 * Compare index options in metadata with existing indexes in mongo. The
	 * only 2 options that matter are 'unique' and 'partialIndexExpression'.
	 *
	 * @param index
	 * @param existingIndex
	 * @return true if all index options recognized by Lightblue match, false
	 *         otherwise
	 */
	public static boolean indexOptionsMatch(Index index, DBObject existingIndex) {
		Boolean unique = (Boolean) existingIndex.get("unique");

		if (unique == null) {
			// existing unique can be null, that's the same as false
			unique = false;
		}

		if (index.isUnique() != unique) {
			LOGGER.debug("Index unique flag changed to {}", index.isUnique());
			return false;
		}

		if (index.getProperties().containsKey(PARTIAL_FILTER_EXPRESSION_OPTION_NAME)) {
			LOGGER.debug("Index partialFilterExpression option changed from {} to {}",
					existingIndex.get(PARTIAL_FILTER_EXPRESSION_OPTION_NAME),
					index.getProperties().get(PARTIAL_FILTER_EXPRESSION_OPTION_NAME));
			return index.getProperties().get(PARTIAL_FILTER_EXPRESSION_OPTION_NAME)
					.equals(existingIndex.get(PARTIAL_FILTER_EXPRESSION_OPTION_NAME));
		}

		return true;
	}

	/**
	 * Returns a projection containing the requested projection, all identity
	 * fields, and the objectType field
	 */
	private Projection getProjectionFields(Projection requestedProjection, EntityMetadata md) {
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
		projectFields.add(new FieldProjection(DocTranslator.OBJECT_TYPE, true, false));

		return new ProjectionList(projectFields);
	}

	private Error analyzeException(Exception e, final String otherwise) {
		return analyzeException(e, otherwise, null, false);
	}

	private Error analyzeException(Exception e, final String otherwise, final String msg, boolean specialHandling) {
		if (e instanceof Error) {
			return (Error) e;
		}

		if (e instanceof MongoException) {
			MongoException me = (MongoException) e;
			if (me.getCode() == 18) {
				return Error.get(CrudConstants.ERR_AUTH_FAILED, e.getMessage());
			} else if (me instanceof MongoTimeoutException || me instanceof MongoExecutionTimeoutException) {
				LOGGER.error(CrudConstants.ERR_DATASOURCE_TIMEOUT, e);
				return Error.get(CrudConstants.ERR_DATASOURCE_TIMEOUT, e.getMessage());
			} else if (me instanceof DuplicateKeyException) {
				return Error.get(MongoCrudConstants.ERR_DUPLICATE, e.getMessage());
			} else if (me instanceof MongoSocketException) {
				LOGGER.error(MongoCrudConstants.ERR_CONNECTION_ERROR, e);
				return Error.get(MongoCrudConstants.ERR_CONNECTION_ERROR, e.getMessage());
			} else {
				LOGGER.error(MongoCrudConstants.ERR_MONGO_ERROR, e);
				return Error.get(MongoCrudConstants.ERR_MONGO_ERROR, e.getMessage());
			}
		} else if (msg == null) {
			return Error.get(otherwise, e.getMessage());
		} else if (specialHandling) {
			return Error.get(otherwise, msg, e);
		} else {
			return Error.get(otherwise, msg);
		}
	}
}
