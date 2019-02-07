/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.store;

import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;
import co.cask.cdap.spi.data.table.field.Fields;

import java.io.IOException;

/**
 * A class which contains all the store definition, the table name the store will use, the schema of the table should
 * all be specified here.
 * TODO: CDAP-14674 Make sure all the store definition goes here.
 */
public final class StoreDefinition {
  private StoreDefinition() {
    // prevent instantiation
  }

  /**
   * Create all system tables.
   *
   * @param tableAdmin the table admin to create the table
   */
  public static void createAllTables(StructuredTableAdmin tableAdmin, StructuredTableRegistry registry,
                                     boolean overWrite) throws IOException, TableAlreadyExistsException {
    registry.initialize();
    if (overWrite || tableAdmin.getSpecification(ArtifactStore.ARTIFACT_DATA_TABLE) == null) {
      ArtifactStore.createTables(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(NamespaceStore.NAMESPACES) == null) {
      NamespaceStore.createTable(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(SecretStore.SECRET_STORE_TABLE) == null) {
      SecretStore.createTable(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(WorkflowStore.WORKFLOW_STATISTICS) == null) {
      WorkflowStore.createTables(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(DatasetInstanceStore.DATASET_INSTANCES) == null) {
      DatasetInstanceStore.createTables(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(DatasetTypeStore.DATASET_TYPES) == null) {
      DatasetTypeStore.createTables(tableAdmin);
    }
  }

  public static void createAllTables(StructuredTableAdmin tableAdmin, StructuredTableRegistry registry)
    throws IOException, TableAlreadyExistsException {
    createAllTables(tableAdmin, registry, false);
  }

  /**
   * Namespace store schema
   */
  public static final class NamespaceStore {
    public static final StructuredTableId NAMESPACES = new StructuredTableId("namespaces");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String NAMESPACE_METADATA_FIELD = "namespace_metadata";

    public static final StructuredTableSpecification NAMESPACE_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(NAMESPACES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(NAMESPACE_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD)
        .build();

    public static void createTable(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(NAMESPACE_TABLE_SPEC);
    }
  }

  /**
   * Schema for workflow table
   */
  public static final class WorkflowStore {
    public static final StructuredTableId WORKFLOW_STATISTICS = new StructuredTableId("workflow_statistics");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String PROGRAM_FIELD = "program";
    public static final String START_TIME_FIELD = "start_time";
    public static final String RUN_ID_FIELD = "run_id";
    public static final String TIME_TAKEN_FIELD = "time_taken";
    public static final String PROGRAM_RUN_DATA = "program_run_data";

    public static final StructuredTableSpecification WORKFLOW_TABLE_SPEC = new StructuredTableSpecification.Builder()
      .withId(WORKFLOW_STATISTICS)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(APPLICATION_FIELD),
                  Fields.stringType(VERSION_FIELD),
                  Fields.stringType(PROGRAM_FIELD),
                  Fields.longType(START_TIME_FIELD),
                  Fields.stringType(RUN_ID_FIELD),
                  Fields.longType(TIME_TAKEN_FIELD),
                  Fields.stringType(PROGRAM_RUN_DATA))
      .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_FIELD, START_TIME_FIELD)
      .build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(WORKFLOW_TABLE_SPEC);
    }
  }

  /**
   *
   */
  public static final class ArtifactStore {
    public static final StructuredTableId ARTIFACT_DATA_TABLE = new StructuredTableId("artifact_data");
    public static final StructuredTableId APP_DATA_TABLE = new StructuredTableId("app_data");
    public static final StructuredTableId PLUGIN_DATA_TABLE = new StructuredTableId("plugin_data");
    public static final StructuredTableId UNIV_PLUGIN_DATA_TABLE = new StructuredTableId("universal_plugin_data");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String ARTIFACT_NAMESPACE_FIELD = "artifact_namespace";
    public static final String ARTIFACT_NAME_FIELD = "artifact_name";
    public static final String ARTIFACT_VER_FIELD = "artifiact_version";
    public static final String ARTIFACT_DATA_FIELD = "artifact_data";
    public static final String CLASS_NAME_FIELD = "class_name";
    public static final String APP_DATA_FIELD = "app_data";
    public static final String PARENT_NAMESPACE_FIELD = "parent_namespace";
    public static final String PARENT_NAME_FIELD = "parent_name";
    public static final String PLUGIN_TYPE_FIELD = "plugin_type";
    public static final String PLUGIN_NAME_FIELD = "plugin_name";
    public static final String PLUGIN_DATA_FIELD = "plugin_data";

    // Artifact Data table
    public static final StructuredTableSpecification ARTIFACT_DATA_SPEC = new StructuredTableSpecification.Builder()
      .withId(ARTIFACT_DATA_TABLE)
      .withFields(Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                  Fields.stringType(ARTIFACT_NAME_FIELD),
                  Fields.stringType(ARTIFACT_VER_FIELD),
                  Fields.stringType(ARTIFACT_DATA_FIELD))
      .withPrimaryKeys(ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
      .build();

    // App Data table
    public static final StructuredTableSpecification APP_DATA_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(APP_DATA_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(CLASS_NAME_FIELD),
                    Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                    Fields.stringType(ARTIFACT_NAME_FIELD),
                    Fields.stringType(ARTIFACT_VER_FIELD),
                    Fields.stringType(APP_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, CLASS_NAME_FIELD, ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD,
                         ARTIFACT_VER_FIELD)
        .build();

    // Plugin Data table
    public static final StructuredTableSpecification PLUGIN_DATA_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PLUGIN_DATA_TABLE)
        .withFields(Fields.stringType(PARENT_NAMESPACE_FIELD),
                    Fields.stringType(PARENT_NAME_FIELD),
                    Fields.stringType(PLUGIN_TYPE_FIELD),
                    Fields.stringType(PLUGIN_NAME_FIELD),
                    Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                    Fields.stringType(ARTIFACT_NAME_FIELD),
                    Fields.stringType(ARTIFACT_VER_FIELD),
                    Fields.stringType(PLUGIN_DATA_FIELD))
        .withPrimaryKeys(PARENT_NAMESPACE_FIELD, PARENT_NAME_FIELD, PLUGIN_TYPE_FIELD, PLUGIN_NAME_FIELD,
                         ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
        .build();
    

    // Universal Plugin Data table
    public static final StructuredTableSpecification UNIV_PLUGIN_DATA_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(UNIV_PLUGIN_DATA_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(PLUGIN_TYPE_FIELD),
                    Fields.stringType(PLUGIN_NAME_FIELD),
                    Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                    Fields.stringType(ARTIFACT_NAME_FIELD),
                    Fields.stringType(ARTIFACT_VER_FIELD),
                    Fields.stringType(PLUGIN_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, PLUGIN_TYPE_FIELD, PLUGIN_NAME_FIELD,
                         ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(ARTIFACT_DATA_SPEC);
      tableAdmin.create(APP_DATA_SPEC);
      tableAdmin.create(PLUGIN_DATA_SPEC);
      tableAdmin.create(UNIV_PLUGIN_DATA_SPEC);
    }
  }

  /**
   * Schema for {@link SecretStore}.
   */
  public static final class SecretStore {

    public static final StructuredTableId SECRET_STORE_TABLE = new StructuredTableId("secret_store");
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String SECRET_NAME_FIELD = "secret_name";
    public static final String SECRET_DATA_FIELD = "secret_data";

    public static final StructuredTableSpecification SECRET_STORE_SPEC = new StructuredTableSpecification.Builder()
      .withId(SECRET_STORE_TABLE)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(SECRET_NAME_FIELD),
                  Fields.bytesType(SECRET_DATA_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, SECRET_NAME_FIELD)
      .build();

    public static void createTable(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(SECRET_STORE_SPEC);
    }
  }

  /**
   * Dataset instance store schema
   */
  public static final class DatasetInstanceStore {

    public static final StructuredTableId DATASET_INSTANCES =
      new StructuredTableId("dataset_instances");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String DATASET_FIELD = "dataset";
    public static final String DATASET_METADATA_FIELD = "dataset_metadata";

    public static final StructuredTableSpecification DATASET_INSTANCES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(DATASET_INSTANCES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(DATASET_FIELD),
                    Fields.stringType(DATASET_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, DATASET_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(DATASET_INSTANCES_SPEC);
    }
  }

  /**
   * Dataset type store schema
   */
  public static final class DatasetTypeStore {

    public static final StructuredTableId DATASET_TYPES = new StructuredTableId("dataset_types");
    public static final StructuredTableId MODULE_TYPES = new StructuredTableId("module_types");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String MODULE_NAME_FIELD = "module_name";
    public static final String TYPE_NAME_FIELD = "type_name";
    public static final String DATASET_METADATA_FIELD = "dataset_metadata";

    public static final StructuredTableSpecification DATASET_TYPES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(DATASET_TYPES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(TYPE_NAME_FIELD),
                    Fields.stringType(DATASET_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, TYPE_NAME_FIELD)
        .build();
    public static final StructuredTableSpecification MODULE_TYPES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(MODULE_TYPES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(MODULE_NAME_FIELD),
                    Fields.stringType(DATASET_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, MODULE_NAME_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(DATASET_TYPES_SPEC);
      tableAdmin.create(MODULE_TYPES_SPEC);
    }
  }
}
