package com.redhat.lightblue.mongo.crud;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.redhat.lightblue.config.ControllerConfiguration;
import com.redhat.lightblue.metadata.EntityInfo;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;

public class IndexManagementCfgJsonTest {
  private static final JsonNodeFactory jnf = JsonNodeFactory.withExactBigDecimals(true);

  ControllerConfiguration controllerCfg = new ControllerConfiguration();
  ObjectNode options = jnf.objectNode();
  ObjectNode indexManagement = jnf.objectNode();
  ArrayNode managedJson = jnf.arrayNode();
  ArrayNode unmanagedJson = jnf.arrayNode();

  @Before
  public void addIndexManagementToControllerConfig() {
    options.set("indexManagement", indexManagement);
    controllerCfg.setOptions(options);
  }

  @Test
  public void ignoresNullEntities() {
    managedJson.add(jnf.nullNode());
    managedJson.add("test");
    unmanagedJson.add(jnf.nullNode());
    unmanagedJson.add("foo");
    indexManagement.set("managedEntities", managedJson);
    indexManagement.set("unmanagedEntities", unmanagedJson);

    IndexManagementCfg cfg = new IndexManagementCfg(controllerCfg);

    assertTrue(cfg.isManaged(new EntityInfo("test")));
    assertFalse(cfg.isManaged(new EntityInfo("foo")));
  }

  @Test
  public void convertsNotTextEntriesToText() {
    managedJson.add(jnf.nullNode());
    managedJson.add(1);
    unmanagedJson.add(jnf.nullNode());
    unmanagedJson.add(false);
    indexManagement.set("managedEntities", managedJson);
    indexManagement.set("unmanagedEntities", unmanagedJson);

    IndexManagementCfg cfg = new IndexManagementCfg(controllerCfg);

    assertTrue(cfg.isManaged(new EntityInfo("1")));
    assertFalse(cfg.isManaged(new EntityInfo("false")));
  }

  @Test
  public void ignoresNonValueEntries() {
    managedJson.add(jnf.objectNode().put("foo", "bar"));
    managedJson.add("test1");
    unmanagedJson.add(jnf.arrayNode().add("what"));
    unmanagedJson.add("test2");
    indexManagement.set("managedEntities", managedJson);
    indexManagement.set("unmanagedEntities", unmanagedJson);

    IndexManagementCfg cfg = new IndexManagementCfg(controllerCfg);

    assertTrue(cfg.isManaged(new EntityInfo("test1")));
    assertFalse(cfg.isManaged(new EntityInfo("test2")));
  }
}
