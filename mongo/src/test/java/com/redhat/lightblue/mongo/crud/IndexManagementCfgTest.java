package com.redhat.lightblue.mongo.crud;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.redhat.lightblue.config.ControllerConfiguration;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Set;

@RunWith(Parameterized.class)
public class IndexManagementCfgTest {
  private static final JsonNodeFactory jnf = JsonNodeFactory.withExactBigDecimals(true);

  interface CfgFactory {
    IndexManagementCfg create(Set<String> managed, Set<String> unmanaged);

  }

  @Parameterized.Parameter
  public CfgFactory factory;

  @Parameterized.Parameters
  public static Object[] constructors() {
    return new CfgFactory[]{
        IndexManagementCfg::new,
        (managed, unmanaged) -> {
          ControllerConfiguration cfg = new ControllerConfiguration();
          ObjectNode options = jnf.objectNode();
          ObjectNode indexManagement = jnf.objectNode();
          ArrayNode managedJson = jnf.arrayNode();
          ArrayNode unmanagedJson = jnf.arrayNode();

          if (managed != null) {
            managed.forEach(managedJson::add);
            indexManagement.set("managedEntities", managedJson);
          }

          if (unmanaged != null) {
            unmanaged.forEach(unmanagedJson::add);
            indexManagement.set("unmanagedEntities", unmanagedJson);
          }

          options.set("indexManagement", indexManagement);
          cfg.setOptions(options);

          return new IndexManagementCfg(cfg);
        }
    };
  }

  @Test
  public void managedEntitiesAreManaged() {
    IndexManagementCfg cfg = factory.create(Sets.newHashSet("managed"), null);
    assertTrue(cfg.isManaged("managed"));
  }

  @Test
  public void managementIsCaseSensitive() {
    IndexManagementCfg cfg = factory.create(Sets.newHashSet("managed"), null);
    assertFalse(cfg.isManaged("Managed"));
  }

  @Test
  public void entitiesNotUnmanagedAreManaged() {
    IndexManagementCfg cfg = factory.create(null, Sets.newHashSet("unmanaged"));
    assertTrue(cfg.isManaged("managed"));
  }

  @Test
  public void entitiesUnmanagedAreNotManaged() {
    IndexManagementCfg cfg = factory.create(null, Sets.newHashSet("unmanaged"));
    assertFalse(cfg.isManaged("unmanaged"));
  }

  @Test
  public void unmanagedEntitiesAreCaseSensitive() {
    IndexManagementCfg cfg = factory.create(null, Sets.newHashSet("unmanaged"));
    assertTrue(cfg.isManaged("unManaged"));
  }

  @Test
  public void unmanagedTakesPrecedenceOverManaged() {
    IndexManagementCfg cfg = factory.create(Sets.newHashSet("entity"), Sets.newHashSet("entity"));
    assertFalse(cfg.isManaged("entity"));
  }

  @Test
  public void entityIsManagedIfInManagedSetAndNotInUnmanagedSet() {
    IndexManagementCfg cfg = factory.create(Sets.newHashSet("managed"), Sets.newHashSet("unmanaged"));
    assertTrue(cfg.isManaged("managed"));
  }

  @Test
  public void entityIsNotManagedIfNotInManagedSetAndInUnmanagedSet() {
    IndexManagementCfg cfg = factory.create(Sets.newHashSet("managed"), Sets.newHashSet("unmanaged"));
    assertFalse(cfg.isManaged("unmanaged"));
  }

  @Test
  public void entityIsNotManagedIfNotInManagedSetAndNotInUnmanagedSet() {
    IndexManagementCfg cfg = factory.create(Sets.newHashSet("managed"), Sets.newHashSet("unmanaged"));
    assertFalse(cfg.isManaged("entity"));
  }

  @Test
  public void noEntitiesManagedIfEmptyManagedSet() {
    IndexManagementCfg cfg = factory.create(Sets.newHashSet(), null);
    assertFalse(cfg.isManaged("entity"));
  }

  @Test
  public void noEntitiesManagedIfEmptyManagedSetAndEmptyUnmanagedSet() {
    IndexManagementCfg cfg = factory.create(Sets.newHashSet(), Sets.newHashSet());
    assertFalse(cfg.isManaged("entity"));
  }

  @Test
  public void allEntitiesManagedIfEmptyUnmanagedSet() {
    IndexManagementCfg cfg = factory.create(null, Sets.newHashSet());
    assertTrue(cfg.isManaged("entity"));
  }

  @Test
  public void allEntitiesManagedIfBothSetsNull() {
    IndexManagementCfg cfg = factory.create(null, null);
    assertTrue(cfg.isManaged("entity"));
  }
}
