/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Helper methods to create keys for {@link co.cask.cdap.proto.id.EntityId}.
 * This should be used in place of {@link EntityId#toString()} while persisting {@link EntityId} to avoid
 * incompatibility issues across CDAP upgrades.
 */
// Note: these methods were refactored from MetadataDataset class. Once CDAP-3657 is fixed, these methods will need
// to be cleaned up CDAP-4291
public final class EntityIdKeyHelper {
  private static final Map<Class<? extends NamespacedEntityId>, String> TYPE_MAP =
    ImmutableMap.<Class<? extends NamespacedEntityId>, String>builder()
      .put(NamespaceId.class, EntityTypeSimpleName.NAMESPACE.getSerializedForm())
      .put(ArtifactId.class, EntityTypeSimpleName.ARTIFACT.getSerializedForm())
      .put(ApplicationId.class, EntityTypeSimpleName.APP.getSerializedForm())
      .put(ProgramId.class, EntityTypeSimpleName.PROGRAM.getSerializedForm())
      .put(WorkflowId.class, EntityTypeSimpleName.PROGRAM.getSerializedForm())
      .put(ServiceId.class, EntityTypeSimpleName.PROGRAM.getSerializedForm())
      .put(DatasetId.class, EntityTypeSimpleName.DATASET.getSerializedForm())
      .put(ScheduleId.class, EntityTypeSimpleName.SCHEDULE.getSerializedForm())
      .build();

  public static void addTargetIdToKey(MDSKey.Builder builder, NamespacedEntityId namespacedEntityId) {
    String type = getTargetType(namespacedEntityId);
    if (type.equals(TYPE_MAP.get(NamespaceId.class))) {
      NamespaceId namespaceId = (NamespaceId) namespacedEntityId;
      builder.add(namespaceId.getNamespace());
    } else if (type.equals(TYPE_MAP.get(ProgramId.class))) {
      ProgramId program = (ProgramId) namespacedEntityId;
      String namespaceId = program.getNamespace();
      String appId = program.getApplication();
      String programType = program.getType().name();
      String programId = program.getProgram();
      builder.add(namespaceId);
      builder.add(appId);
      builder.add(programType);
      builder.add(programId);
    } else if (type.equals(TYPE_MAP.get(ApplicationId.class))) {
      ApplicationId application = (ApplicationId) namespacedEntityId;
      String namespaceId = application.getNamespace();
      String appId = application.getApplication();
      builder.add(namespaceId);
      builder.add(appId);
    } else if (type.equals(TYPE_MAP.get(DatasetId.class))) {
      DatasetId datasetInstance = (DatasetId) namespacedEntityId;
      String namespaceId = datasetInstance.getNamespace();
      String datasetId = datasetInstance.getDataset();
      builder.add(namespaceId);
      builder.add(datasetId);
    } else if (type.equals(TYPE_MAP.get(ArtifactId.class))) {
      ArtifactId artifactId = (ArtifactId) namespacedEntityId;
      String namespaceId = artifactId.getNamespace();
      String name = artifactId.getArtifact();
      String version = artifactId.getVersion();
      builder.add(namespaceId);
      builder.add(name);
      builder.add(version);
    } else if (type.equals(TYPE_MAP.get(ScheduleId.class))) {
      ScheduleId scheduleId = (ScheduleId) namespacedEntityId;
      String namespaceId = scheduleId.getNamespace();
      String appId = scheduleId.getApplication();
      String version = scheduleId.getVersion();
      String scheduleName = scheduleId.getSchedule();
      builder.add(namespaceId);
      builder.add(appId);
      builder.add(version);
      builder.add(scheduleName);
    } else {
      throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
    }
  }

  private static String getTargetType(NamespacedEntityId namespacedEntityId) {
    return TYPE_MAP.get(namespacedEntityId.getClass());
  }

  /**
   * To get entity type in v1 format. We need this method because in 5.0 we are renaming Dataset
   * serialization from DatasetInstance to Dataset.
   *
   * @param namespacedEntityId entity for which type is needed
   * @return v1 type of the entity
   */
  public static String getV1TargetType(NamespacedEntityId namespacedEntityId) {
    String v1Type = TYPE_MAP.get(namespacedEntityId.getClass());
    switch (v1Type) {
      case MetadataEntity.DATASET:
        v1Type = "datasetinstance";
        break;
    }
    return v1Type;
  }

  private EntityIdKeyHelper() {
  }
}
