package com.redhat.lightblue.mongo.crud;

import com.redhat.lightblue.config.ControllerConfiguration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Defines whether or not an entity's indexes should be managed automatically by
 * {@link MongoCRUDController} or if indexes are managed externally.
 *
 * <p>Management is determined by two optional sets: a inclusive managed set, and an exclusive
 * unmanaged set. An entity is managed if it is in the managed set (or the managed set is null) and
 * it is not in the unmanaged set (or the unmanaged set is null).
 *
 * <p>Entity names are matched exactly (case sensitive).</p>
 */
public class IndexManagementCfg {
  private final Set<String> managed;
  private final Set<String> unmanaged;

  /**
   * Managed entity sets are pulled from the options field under "indexManagement.managedEntities"
   * and "indexManagement.unmanagedEntities" as in
   *
   * <pre>
   * "options": {
   *   "indexManagement": {
   *     "managedEntities": ["myEntity", "otherEntity"],
   *     "unmanagedEntities": ["otherEntity"]
   *   }
   * }
   * </pre>
   *
   * @param controllerCfg Controller configuration that contains options object.
   */
  public IndexManagementCfg(ControllerConfiguration controllerCfg) {
    ObjectNode options = controllerCfg != null
        ? controllerCfg.getOptions()
        : null;

    if (options != null) {
      JsonNode maybeIndexManagement = options.get("indexManagement");

      if (maybeIndexManagement != null && maybeIndexManagement.isObject()) {
        ObjectNode indexManagement = (ObjectNode) maybeIndexManagement;

        this.managed = getStringSetOption(indexManagement, "managedEntities");
        this.unmanaged = getStringSetOption(indexManagement, "unmanagedEntities");
      } else {
        this.managed = null;
        this.unmanaged = null;
      }
    } else {
      this.managed = null;
      this.unmanaged = null;
    }
  }

  /**
   * @param managedEntities May be null.
   * @param unmanagedEntities May be null.
   */
  public IndexManagementCfg(Set<String> managedEntities, Set<String> unmanagedEntities) {
    this.managed = managedEntities;
    this.unmanaged = unmanagedEntities;
  }

  public boolean isManaged(String entityName) {
    boolean answer = true;

    if (managed != null) {
      answer = managed.contains(entityName);
    }

    if (unmanaged != null) {
      answer &= !unmanaged.contains(entityName);
    }

    return answer;
  }

  private static Set<String> getStringSetOption(ObjectNode node, String optionName) {
    if (node == null) {
      return null;
    }

    JsonNode value = node.get(optionName);

    if (!value.isArray()) {
      return null;
    }

    ArrayNode asArray = (ArrayNode) value;
    Iterator<JsonNode> elements = asArray.elements();

    Set<String> answer = new LinkedHashSet<>(asArray.size());

    while (elements.hasNext()) {
      JsonNode element = elements.next();

      if (element != null && !element.isNull()) {
        answer.add(element.asText());
      }
    }

    return answer;

  }
}
