/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.meta;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link CheckpointManager} that uses {@link Table} dataset to persist data.
 */
public final class DefaultCheckpointManager implements CheckpointManager<KafkaOffset> {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultCheckpointManager.class);

  private static final byte [] OFFSET_COL_NAME = Bytes.toBytes("nextOffset");
  private static final byte [] NEXT_TIME_COL_NAME = Bytes.toBytes("nextEventTime");
  private static final byte [] MAX_TIME_COL_NAME = Bytes.toBytes("maxEventTime");

  private final byte [] rowKeyPrefix;
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  private Map<Integer, Checkpoint<KafkaOffset>> lastCheckpoint;

  @Inject
  DefaultCheckpointManager(DatasetFramework datasetFramework, TransactionSystemClient txClient,
                           String topic, byte[] prefix) {
    this.rowKeyPrefix = Bytes.add(prefix, Bytes.toBytes(topic));
    this.lastCheckpoint = new HashMap<>();
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  private Table getCheckpointTable(DatasetContext context) throws IOException, DatasetManagementException {
    return LoggingStoreTableUtil.getMetadataTable(datasetFramework, context);
  }

  @Override
  public void saveCheckpoints(final Map<Integer, ? extends Checkpoint<KafkaOffset>> checkpoints) throws Exception {
    // if the checkpoints have not changed, we skip writing to table and return.
    if (lastCheckpoint.equals(checkpoints)) {
      return;
    }

    lastCheckpoint = Transactionals.execute(transactional, context -> {
      Map<Integer, Checkpoint<KafkaOffset>> result = new HashMap<>();

      Table table = getCheckpointTable(context);
      for (Map.Entry<Integer, ? extends Checkpoint<KafkaOffset>> entry : checkpoints.entrySet()) {
        byte[] key = Bytes.add(rowKeyPrefix, Bytes.toBytes(entry.getKey()));
        Checkpoint<KafkaOffset> checkpoint = entry.getValue();
        table.put(key, OFFSET_COL_NAME, Bytes.toBytes(checkpoint.getOffset().getNextOffset()));
        table.put(key, NEXT_TIME_COL_NAME, Bytes.toBytes(checkpoint.getOffset().getNextEventTime()));
        table.put(key, MAX_TIME_COL_NAME, Bytes.toBytes(checkpoint.getMaxEventTime()));
        result.put(entry.getKey(), new Checkpoint<>(checkpoint.getOffset(),
                                                  checkpoint.getMaxEventTime()));
      }
      return result;
    }, ServiceUnavailableException.class);

    LOG.trace("Saved checkpoints for partitions {}", checkpoints);
  }

  @Override
  public Map<Integer, Checkpoint<KafkaOffset>> getCheckpoint(final Set<Integer> partitions) throws Exception {
    return Transactionals.execute(transactional, context -> {
      Table table = getCheckpointTable(context);
      Map<Integer, Checkpoint<KafkaOffset>> checkpoints = new HashMap<>();
      for (final int partition : partitions) {
        Row result = table.get(Bytes.add(rowKeyPrefix, Bytes.toBytes(partition)));
        checkpoints.put(partition, createFromRow(result));
      }
      return checkpoints;
    }, ServiceUnavailableException.class);
  }

  @Override
  public Checkpoint<KafkaOffset> getCheckpoint(final int partition) throws Exception {
    Checkpoint<KafkaOffset> checkpoint = Transactionals.execute(transactional, context -> {
      Row result = getCheckpointTable(context).get(Bytes.add(rowKeyPrefix, Bytes.toBytes(partition)));
      return createFromRow(result);
    }, ServiceUnavailableException.class);
    LOG.trace("Read checkpoint {} for partition {}", checkpoint, partition);
    return checkpoint;
  }

  /**
   * Create a {@link Checkpoint} from a {@link Row} with backward compatibility
   */
  private Checkpoint<KafkaOffset> createFromRow(Row row) {
    long maxEventTime = row.getLong(MAX_TIME_COL_NAME, -1);
    // If CDAP is upgraded from an older version with no NEXT_TIME_COL_NAME, use maxEventTime as default, since
    // in both older and current CDAP versions, OFFSET_COL_NAME is the offset to restart consuming from.
    // In older CDAP version MAX_TIME_COL_NAME used to be the log event time of the message fetched by the previous
    // offset of OFFSET_COL_NAME, which is the same as NEXT_TIME_COL_NAME in current version.
    return new Checkpoint<>(new KafkaOffset(row.getLong(OFFSET_COL_NAME, -1),
                                            row.getLong(NEXT_TIME_COL_NAME, maxEventTime)), maxEventTime);
  }
}
