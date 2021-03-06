/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test AppMetadataStore.
 */
public class AppMetadataStoreTest {
  private static DatasetFramework datasetFramework;
  private static CConfiguration cConf;
  private static TransactionExecutorFactory txExecutorFactory;
  private static final List<ProgramRunStatus> STOP_STATUSES =
    ImmutableList.of(ProgramRunStatus.COMPLETED, ProgramRunStatus.FAILED, ProgramRunStatus.KILLED);
  private static final ArtifactId ARTIFACT_ID = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
  private static final Map<String, String> SINGLETON_PROFILE_MAP =
    Collections.singletonMap(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());

  private final AtomicInteger sourceId = new AtomicInteger();
  private final AtomicLong runIdTime = new AtomicLong();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    AppFabricTestHelper.ensureNamespaceExists(NamespaceId.DEFAULT);
    datasetFramework = injector.getInstance(DatasetFramework.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    cConf = injector.getInstance(CConfiguration.class);
  }

  private void recordProvisionAndStart(ProgramRunId programRunId, AppMetadataStore metadataStoreDataset) {
    metadataStoreDataset.recordProgramProvisioning(programRunId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                   ARTIFACT_ID);
    metadataStoreDataset.recordProgramProvisioned(programRunId, 0,
                                                  AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
    metadataStoreDataset.recordProgramStart(programRunId, null, ImmutableMap.of(),
                                            AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
  }

  private AppMetadataStore getMetadataStore(String tableName) throws Exception {
    DatasetId storeTable = NamespaceId.DEFAULT.dataset(tableName);
    datasetFramework.addInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);

    Table table = datasetFramework.getDataset(storeTable, ImmutableMap.of(), null);
    Assert.assertNotNull(table);
    return new AppMetadataStore(table, cConf);
  }

  private TransactionExecutor getTxExecutor(AppMetadataStore metadataStoreDataset) {
    return txExecutorFactory.createExecutor(Collections.singleton(metadataStoreDataset));
  }

  @Test
  public void testSmallerSourceIdRecords() throws Exception {
    AppMetadataStore metadataStoreDataset = getMetadataStore("testSmallerSourceIdRecords");
    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);
    // STARTING status is persisted with the largest sourceId
    assertPersistedStatus(metadataStoreDataset, txnl, 100L, 10L, 1L, ProgramRunStatus.STARTING);
    assertPersistedStatus(metadataStoreDataset, txnl, 100L, 1L, 10L, ProgramRunStatus.STARTING);
    // RUNNING status is persisted with the largest sourceId
    assertPersistedStatus(metadataStoreDataset, txnl, 1L, 100L, 10L, ProgramRunStatus.RUNNING);
    // KILLED status is persisted with the largest sourceId
    assertPersistedStatus(metadataStoreDataset, txnl, 1L, 10L, 100L, ProgramRunStatus.KILLED);
  }

  @Test
  public void testPendingToCompletedIsIgnored() throws Exception {
    AppMetadataStore metadataStoreDataset = getMetadataStore("testPendingToCompletedIgnored");

    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    RunId runId1 = RunIds.generate();
    ProgramRunId programRunId = program.run(runId1);

    txnl.execute(() -> {
      metadataStoreDataset.recordProgramProvisioning(programRunId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                                     AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                     ARTIFACT_ID);
      metadataStoreDataset.recordProgramStop(programRunId, 0, ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId);
      Assert.assertEquals(ProgramRunStatus.PENDING, runRecordMeta.getStatus());
    });
  }

  @Test
  public void testInvalidStatusPersistence() throws Exception {
    final AppMetadataStore metadataStoreDataset = getMetadataStore("testInvalidStatusPersistence");
    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final RunId runId1 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId1 = program.run(runId1);
    final AtomicLong sourceId = new AtomicLong();
    // No status can be persisted if STARTING is not present
    txnl.execute(() -> {
      metadataStoreDataset.recordProgramRunning(programRunId1, RunIds.getTime(runId1, TimeUnit.SECONDS),
                                                null,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId1, RunIds.getTime(runId1, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId1);
      // no run record is expected to be persisted without STARTING persisted
      Assert.assertNull(runRecordMeta);
    });
    final RunId runId2 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId2 = program.run(runId2);
    txnl.execute(() -> {
      metadataStoreDataset.recordProgramSuspend(programRunId2,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), -1);
      metadataStoreDataset.recordProgramResumed(programRunId2,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), -1);
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId2);
      // no run record is expected to be persisted without STARTING persisted
      Assert.assertNull(runRecordMeta);
    });
    final RunId runId3 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId3 = program.run(runId3);
    txnl.execute(() -> {
      metadataStoreDataset.recordProgramStop(programRunId3, RunIds.getTime(runId3, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId3, RunIds.getTime(runId3, TimeUnit.SECONDS),
                                             ProgramRunStatus.KILLED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId3, RunIds.getTime(runId3, TimeUnit.SECONDS),
                                             ProgramRunStatus.FAILED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId3);
      // no run record is expected to be persisted without STARTING persisted
      Assert.assertNull(runRecordMeta);
    });
    final RunId runId4 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId4 = program.run(runId4);
    // Once a stop status is reached, any incoming status will be ignored
    txnl.execute(() -> {
      recordProvisionAndStart(programRunId4, metadataStoreDataset);
      metadataStoreDataset.recordProgramStop(programRunId4, RunIds.getTime(runId4, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId4, RunIds.getTime(runId4, TimeUnit.SECONDS),
                                             ProgramRunStatus.KILLED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId4);
      // KILLED after COMPLETED is ignored
      Assert.assertEquals(ProgramRunStatus.COMPLETED, runRecordMeta.getStatus());
    });
    final RunId runId5 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId5 = program.run(runId5);
    txnl.execute(() -> {
      recordProvisionAndStart(programRunId5, metadataStoreDataset);
      metadataStoreDataset.recordProgramStop(programRunId5, RunIds.getTime(runId5, TimeUnit.SECONDS),
                                             ProgramRunStatus.FAILED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStop(programRunId5, RunIds.getTime(runId5, TimeUnit.SECONDS),
                                             ProgramRunStatus.COMPLETED, null,
                                             AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId5);
      // COMPLETED after FAILED is ignored
      Assert.assertEquals(ProgramRunStatus.FAILED, runRecordMeta.getStatus());
    });
    final RunId runId6 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId6 = program.run(runId6);
    Long currentTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // STARTING status will be ignored if there's any existing record
    txnl.execute(() -> {
      recordProvisionAndStart(programRunId6, metadataStoreDataset);
      // CDAP-13551 - seems like the program should not be allowed to suspend when in starting state
      metadataStoreDataset.recordProgramSuspend(programRunId6,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                                currentTime);
      metadataStoreDataset.recordProgramStart(programRunId6, null, Collections.emptyMap(),
                                              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId6);
      // STARTING status is ignored since there's an existing SUSPENDED record
      Assert.assertEquals(ProgramRunStatus.SUSPENDED, runRecordMeta.getStatus());
      Assert.assertEquals(currentTime, runRecordMeta.getSuspendTs());
    });
    final RunId runId7 = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId7 = program.run(runId7);
    txnl.execute(() -> {
      long startTime = RunIds.getTime(runId7, TimeUnit.SECONDS);
      recordProvisionAndStart(programRunId7, metadataStoreDataset);
      metadataStoreDataset.recordProgramRunning(programRunId7, startTime, null,
                                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      metadataStoreDataset.recordProgramStart(programRunId7, null, Collections.emptyMap(),
                                              AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      RunRecordMeta runRecordMeta = metadataStoreDataset.getRun(programRunId7);
      // STARTING status is ignored since there's an existing RUNNING record
      Assert.assertEquals(ProgramRunStatus.RUNNING, runRecordMeta.getStatus());
    });
  }

  private void assertPersistedStatus(final AppMetadataStore metadataStoreDataset, TransactionExecutor txnl,
                                     final long startSourceId, final long runningSourceId,
                                     final long killedSourceId, final ProgramRunStatus expectedRunStatus)
    throws Exception {
    // Add some run records
    ApplicationId application = NamespaceId.DEFAULT.app("app");
    final ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    final AtomicReference<RunRecordMeta> resultRecord = new AtomicReference<>();
    final RunId runId = RunIds.generate(runIdTime.incrementAndGet());
    final ProgramRunId programRunId = program.run(runId);
    txnl.execute(() -> {
      metadataStoreDataset.recordProgramProvisioning(programRunId, null, SINGLETON_PROFILE_MAP,
                                                     AppFabricTestHelper.createSourceId(startSourceId), ARTIFACT_ID);
      metadataStoreDataset.recordProgramProvisioned(programRunId, 0,
                                                    AppFabricTestHelper.createSourceId(startSourceId + 1));
      metadataStoreDataset.recordProgramStart(programRunId, null, ImmutableMap.of(),
                                              AppFabricTestHelper.createSourceId(startSourceId + 2));
      metadataStoreDataset.recordProgramRunning(programRunId, RunIds.getTime(runId, TimeUnit.SECONDS),
                                                null, AppFabricTestHelper.createSourceId(runningSourceId));
      metadataStoreDataset.recordProgramStop(programRunId, RunIds.getTime(runId, TimeUnit.SECONDS),
                                             ProgramRunStatus.KILLED,
                                             null, AppFabricTestHelper.createSourceId(killedSourceId));
      resultRecord.set(metadataStoreDataset.getRun(programRunId));
    });
    Assert.assertEquals(expectedRunStatus, resultRecord.get().getStatus());
  }

  @Test
  public void testScanRunningInRangeWithBatch() throws Exception {
    final AppMetadataStore metadataStoreDataset = getMetadataStore("testScanRunningInRange");
    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);

    // Add some run records
    TreeSet<Long> expected = new TreeSet<>();
    for (int i = 0; i < 100; ++i) {
      ApplicationId application = NamespaceId.DEFAULT.app("app" + i);
      final ProgramId program = application.program(ProgramType.values()[i % ProgramType.values().length],
                                                    "program" + i);
      final RunId runId = RunIds.generate(runIdTime.incrementAndGet());
      final ProgramRunId programRunId = program.run(runId);
      expected.add(RunIds.getTime(runId, TimeUnit.MILLISECONDS));
      // Start the program and stop it
      final int j = i;
      // A sourceId to keep incrementing for each call of app meta data store persisting
      txnl.execute(() -> {
        recordProvisionAndStart(programRunId, metadataStoreDataset);
        metadataStoreDataset.recordProgramRunning(
          programRunId, RunIds.getTime(runId, TimeUnit.SECONDS) + 1, null,
          AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        metadataStoreDataset.recordProgramStop(
          programRunId, RunIds.getTime(runId, TimeUnit.SECONDS),
          STOP_STATUSES.get(j % STOP_STATUSES.size()),
          null, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      });
    }

    // Run full scan
    runScan(txnl, metadataStoreDataset, expected, 0, Long.MAX_VALUE);

    // In all below assertions, TreeSet and metadataStore both have start time inclusive and end time exclusive.
    // Run the scan with time limit
    runScan(txnl, metadataStoreDataset, expected.subSet(30 * 10000L, 90 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(30 * 10000), TimeUnit.MILLISECONDS.toSeconds(90 * 10000));

    runScan(txnl, metadataStoreDataset, expected.subSet(90 * 10000L, 101 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(90 * 10000), TimeUnit.MILLISECONDS.toSeconds(101 * 10000));

    // After range
    runScan(txnl, metadataStoreDataset, expected.subSet(101 * 10000L, 200 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(101 * 10000), TimeUnit.MILLISECONDS.toSeconds(200 * 10000));

    // Identical start and end time
    runScan(txnl, metadataStoreDataset, expected.subSet(31 * 10000L, 31 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(31 * 10000), TimeUnit.MILLISECONDS.toSeconds(31 * 10000));

    // One unit difference between start and end time
    runScan(txnl, metadataStoreDataset, expected.subSet(30 * 10000L, 31 * 10000L),
            TimeUnit.MILLISECONDS.toSeconds(30 * 10000), TimeUnit.MILLISECONDS.toSeconds(31 * 10000));

    // Before range
    runScan(txnl, metadataStoreDataset, expected.subSet(1000L, 10000L),
            TimeUnit.MILLISECONDS.toSeconds(1000), TimeUnit.MILLISECONDS.toSeconds(10000));
  }

  private void runScan(TransactionExecutor txnl, final AppMetadataStore metadataStoreDataset,
                       final Set<Long> expected, final long startTime, final long stopTime)
    throws InterruptedException, TransactionFailureException {
    txnl.execute(() -> {
      // Run the scan
      Set<Long> actual = new TreeSet<>();
      int maxScanTimeMillis = 25;
      // Create a ticker that counts one millisecond per element, so that we can test batching
      // Hence number of elements per batch = maxScanTimeMillis
      CountingTicker countingTicker = new CountingTicker(1);
      List<Iterable<RunId>> batches =
        metadataStoreDataset.getRunningInRangeForStatus("runRecordCompleted", startTime, stopTime, maxScanTimeMillis,
                                                        countingTicker);
      Iterable<RunId> runIds = Iterables.concat(batches);
      Iterables.addAll(actual, Iterables.transform(runIds, input -> RunIds.getTime(input, TimeUnit.MILLISECONDS)));

      Assert.assertEquals(expected, actual);
      int numBatches = Iterables.size(batches);
      // Each batch needs 2 extra calls to Ticker.read, once during init and once for final condition check
      // Hence the number of batches should be --
      // (num calls to Ticker.read - (2 * numBatches)) / number of elements per batch
      Assert.assertEquals((countingTicker.getNumProcessed() - (2 * numBatches)) / maxScanTimeMillis, numBatches);
    });
  }

  @Test
  public void testgetRuns() throws Exception {
    final AppMetadataStore metadataStoreDataset = getMetadataStore("testgetRuns");
    TransactionExecutor txnl = getTxExecutor(metadataStoreDataset);

    // Add some run records
    final Set<String> expected = new TreeSet<>();
    final Set<String> expectedHalf = new TreeSet<>();
    final Set<ProgramRunId> programRunIdSet = new HashSet<>();
    final Set<ProgramRunId> programRunIdSetHalf = new HashSet<>();
    for (int i = 0; i < 100; ++i) {
      ApplicationId application = NamespaceId.DEFAULT.app("app");
      final ProgramId program = application.program(ProgramType.SERVICE, "program");
      final RunId runId = RunIds.generate(runIdTime.incrementAndGet());
      expected.add(runId.toString());
      final int index = i;

      // Add every other runId
      if ((i % 2) == 0) {
        expectedHalf.add(runId.toString());
      }

      ProgramRunId programRunId = program.run(runId);
      programRunIdSet.add(programRunId);

      //Add every other programRunId
      if ((i % 2) == 0) {
        programRunIdSetHalf.add(programRunId);
      }

      // A sourceId to keep incrementing for each call of app meta data store persisting
      txnl.execute(() -> {
        // Start the program and stop it
        recordProvisionAndStart(programRunId, metadataStoreDataset);
        metadataStoreDataset.recordProgramRunning(
          programRunId, RunIds.getTime(runId, TimeUnit.SECONDS), null,
          AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        metadataStoreDataset.recordProgramStop(programRunId, RunIds.getTime(runId, TimeUnit.SECONDS),
                                               ProgramRunStatus.values()[index % ProgramRunStatus.values().length],
                                               null,
                                               AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      });
    }

    txnl.execute(() -> {
      Map<ProgramRunId, RunRecordMeta> runMap = metadataStoreDataset.getRuns(programRunIdSet);
      Set<String> actual = new TreeSet<>();
      for (Map.Entry<ProgramRunId, RunRecordMeta> entry : runMap.entrySet()) {
        actual.add(entry.getValue().getPid());
      }
      Assert.assertEquals(expected, actual);


      Map<ProgramRunId, RunRecordMeta> runMapHalf = metadataStoreDataset.getRuns(programRunIdSetHalf);
      Set<String> actualHalf = new TreeSet<>();
      for (Map.Entry<ProgramRunId, RunRecordMeta> entry : runMapHalf.entrySet()) {
        actualHalf.add(entry.getValue().getPid());
      }
      Assert.assertEquals(expectedHalf, actualHalf);
    });
  }

  @Test
  public void testGetActiveRuns() throws Exception {
    AppMetadataStore store = getMetadataStore("testGetActiveRuns");
    TransactionExecutor txnl = getTxExecutor(store);

    // write a run record for each state for two programs in two apps in two namespaces
    String app1 = "app1";
    String app2 = "app2";
    String program1 = "prog1";
    String program2 = "prog2";

    Collection<NamespaceId> namespaces = Arrays.asList(new NamespaceId("ns1"), new NamespaceId("ns2"));
    Collection<ApplicationId> apps = namespaces.stream()
      .flatMap(ns -> Stream.of(ns.app(app1), ns.app(app2)))
      .collect(Collectors.toList());
    Collection<ProgramId> programs = apps.stream()
      .flatMap(app -> Stream.of(app.mr(program1), app.mr(program2)))
      .collect(Collectors.toList());

    for (ProgramId programId : programs) {
      txnl.execute(() -> {
        // one run in pending state
        ProgramRunId runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);

        // one run in starting state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramStart(runId, UUID.randomUUID().toString(), Collections.emptyMap(),
                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

        // one run in running state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        String twillRunId = UUID.randomUUID().toString();
        store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramRunning(runId, System.currentTimeMillis(), twillRunId,
                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));

        // one in suspended state
        runId = programId.run(RunIds.generate());
        store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                        AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
        store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        twillRunId = UUID.randomUUID().toString();
        store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
                                 AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramRunning(runId, System.currentTimeMillis(), twillRunId,
                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramSuspend(runId, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()),
                                   System.currentTimeMillis());

        // one run in each stopped state
        for (ProgramRunStatus runStatus : ProgramRunStatus.values()) {
          if (!runStatus.isEndState()) {
            continue;
          }
          runId = programId.run(RunIds.generate());
          store.recordProgramProvisioning(runId, Collections.emptyMap(), SINGLETON_PROFILE_MAP,
                                          AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()), ARTIFACT_ID);
          store.recordProgramProvisioned(runId, 3, AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
          twillRunId = UUID.randomUUID().toString();
          store.recordProgramStart(runId, twillRunId, Collections.emptyMap(),
                                   AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
          store.recordProgramStop(runId, System.currentTimeMillis(), runStatus, null,
                                  AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        }
      });
    }

    Set<ProgramRunStatus> activeStates = new HashSet<>();
    activeStates.add(ProgramRunStatus.PENDING);
    activeStates.add(ProgramRunStatus.STARTING);
    activeStates.add(ProgramRunStatus.RUNNING);
    activeStates.add(ProgramRunStatus.SUSPENDED);

    // test the instance level method and namespace level method
    txnl.execute(() -> {
      Map<ProgramId, Set<ProgramRunStatus>> allExpected = new HashMap<>();
      Map<ProgramId, Set<ProgramRunStatus>> allActual = new HashMap<>();
      // check active runs per namespace
      for (NamespaceId namespace : namespaces) {
        Map<ProgramRunId, RunRecordMeta> activeRuns = store.getActiveRuns(namespace);

        // we expect 4 runs per program, with 4 programs in each namespace
        Map<ProgramId, Set<ProgramRunStatus>> expected = new HashMap<>();
        expected.put(namespace.app(app1).mr(program1), activeStates);
        expected.put(namespace.app(app1).mr(program2), activeStates);
        expected.put(namespace.app(app2).mr(program1), activeStates);
        expected.put(namespace.app(app2).mr(program2), activeStates);

        Map<ProgramId, Set<ProgramRunStatus>> actual = new HashMap<>();
        actual.put(namespace.app(app1).mr(program1), new HashSet<>());
        actual.put(namespace.app(app1).mr(program2), new HashSet<>());
        actual.put(namespace.app(app2).mr(program1), new HashSet<>());
        actual.put(namespace.app(app2).mr(program2), new HashSet<>());
        allActual.putAll(actual);
        for (Map.Entry<ProgramRunId, RunRecordMeta> activeRun : activeRuns.entrySet()) {
          ProgramId programId = activeRun.getKey().getParent();
          Assert.assertTrue("Unexpected program returned: " + programId,
                            actual.containsKey(activeRun.getKey().getParent()));
          actual.get(programId).add(activeRun.getValue().getStatus());
        }

        Assert.assertEquals(expected, actual);
        allExpected.putAll(expected);
      }

      // test the instance level method
      for (Map.Entry<ProgramRunId, RunRecordMeta> activeRun : store.getActiveRuns(x -> true).entrySet()) {
        ProgramId programId = activeRun.getKey().getParent();
        Assert.assertTrue("Unexpected program returned: " + programId,
                          allActual.containsKey(activeRun.getKey().getParent()));
        allActual.get(programId).add(activeRun.getValue().getStatus());
      }
      Assert.assertEquals(allExpected, allActual);
    });

    // check active runs per app
    for (ApplicationId app : apps) {
      txnl.execute(() -> {
        Map<ProgramRunId, RunRecordMeta> activeRuns = store.getActiveRuns(app);

        // we expect 3 runs per program, with 2 programs in each app
        Map<ProgramId, Set<ProgramRunStatus>> expected = new HashMap<>();
        expected.put(app.mr(program1), activeStates);
        expected.put(app.mr(program2), activeStates);

        Map<ProgramId, Set<ProgramRunStatus>> actual = new HashMap<>();
        actual.put(app.mr(program1), new HashSet<>());
        actual.put(app.mr(program2), new HashSet<>());
        for (Map.Entry<ProgramRunId, RunRecordMeta> activeRun : activeRuns.entrySet()) {
          ProgramId programId = activeRun.getKey().getParent();
          Assert.assertTrue("Unexpected program returned: " + programId,
                            actual.containsKey(activeRun.getKey().getParent()));
          actual.get(programId).add(activeRun.getValue().getStatus());
        }

        Assert.assertEquals(expected, actual);
      });
    }

    // check active runs per program
    for (ProgramId program : programs) {
      txnl.execute(() -> {
        Map<ProgramRunId, RunRecordMeta> activeRuns = store.getActiveRuns(program);

        Set<ProgramRunStatus> actual = new HashSet<>();
        for (Map.Entry<ProgramRunId, RunRecordMeta> activeRun : activeRuns.entrySet()) {
          Assert.assertEquals(program, activeRun.getKey().getParent());
          actual.add(activeRun.getValue().getStatus());
        }

        Assert.assertEquals(activeStates, actual);
      });
    }
  }

  @Test
  public void testDuplicateWritesIgnored() throws Exception {
    DatasetId storeTable = NamespaceId.DEFAULT.dataset("duplicateWrites");
    datasetFramework.addInstance(Table.class.getName(), storeTable, DatasetProperties.EMPTY);

    Table table = datasetFramework.getDataset(storeTable, Collections.emptyMap(), null);
    Assert.assertNotNull(table);
    AppMetadataStore store = new AppMetadataStore(table, cConf);
    TransactionExecutor txnl = txExecutorFactory.createExecutor(Collections.singleton(store));

    ApplicationId application = NamespaceId.DEFAULT.app("app");
    ProgramId program = application.program(ProgramType.values()[ProgramType.values().length - 1],
                                            "program");
    ProgramRunId runId = program.run(RunIds.generate());

    byte[] sourceId = new byte[] { 0 };
    txnl.execute(() -> {
      assertSecondCallIsNull(() -> store.recordProgramProvisioning(runId, null, SINGLETON_PROFILE_MAP,
                                                                   sourceId, ARTIFACT_ID));
      assertSecondCallIsNull(() -> store.recordProgramProvisioned(runId, 0, sourceId));
      assertSecondCallIsNull(() -> store.recordProgramStart(runId, null, Collections.emptyMap(), sourceId));
      assertSecondCallIsNull(() -> store.recordProgramRunning(runId, System.currentTimeMillis(), null, sourceId));
      assertSecondCallIsNull(() -> store.recordProgramSuspend(runId, sourceId, System.currentTimeMillis()));
      assertSecondCallIsNull(() -> store.recordProgramRunning(runId, System.currentTimeMillis(), null, sourceId));
      assertSecondCallIsNull(() -> store.recordProgramStop(runId, System.currentTimeMillis(), ProgramRunStatus.KILLED,
                                                           null, sourceId));
      assertSecondCallIsNull(() -> store.recordProgramDeprovisioning(runId, sourceId));
      assertSecondCallIsNull(() -> store.recordProgramDeprovisioned(runId, System.currentTimeMillis(), sourceId));
    });
  }

  private <T> void assertSecondCallIsNull(Callable<T> callable) throws Exception {
    T result = callable.call();
    Assert.assertNotNull(result);
    result = callable.call();
    Assert.assertNull(result);
  }

  @Test
  public void testProfileInRunRecord() throws Exception {
    AppMetadataStore store = getMetadataStore("testProfileInRunRecord");
    TransactionExecutor txnl = getTxExecutor(store);
    ProgramRunId runId = NamespaceId.DEFAULT.app("myApp").workflow("myProgram").run(RunIds.generate());
    ProfileId profileId = NamespaceId.DEFAULT.profile("MyProfile");

    txnl.execute(() -> {
      long startSourceId = 1L;
      store.recordProgramProvisioning(runId, null,
                                      Collections.singletonMap(SystemArguments.PROFILE_NAME, profileId.getScopedName()),
                                      AppFabricTestHelper.createSourceId(startSourceId), ARTIFACT_ID);
      // the profile id should be there after the provisioning stage
      RunRecordMeta run = store.getRun(runId);
      Assert.assertNotNull(run);
      Assert.assertEquals(profileId, run.getProfileId());

      store.recordProgramProvisioned(runId, 0, AppFabricTestHelper.createSourceId(startSourceId + 1));
      store.recordProgramStart(runId, null, ImmutableMap.of(),
                               AppFabricTestHelper.createSourceId(startSourceId + 2));
      store.recordProgramRunning(runId, RunIds.getTime(runId.getRun(), TimeUnit.SECONDS),
                                 null, AppFabricTestHelper.createSourceId(startSourceId + 3));
      store.recordProgramStop(runId, RunIds.getTime(runId.getRun(), TimeUnit.SECONDS),
                              ProgramRunStatus.KILLED,
                              null, AppFabricTestHelper.createSourceId(startSourceId + 4));
      run = store.getRun(runId);
      Assert.assertNotNull(run);
      Assert.assertEquals(profileId, run.getProfileId());
    });
  }

  @Test
  public void testOrderedActiveRuns() throws Exception {
    AppMetadataStore store = getMetadataStore("testOrderedActiveRuns");
    ProgramId programId = NamespaceId.DEFAULT.app("test").workflow("test");
    TransactionExecutor txnl = getTxExecutor(store);
    List<ProgramRunId> expectedRuns = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      RunId runId = RunIds.generate(i * 1000);
      ProgramRunId run = programId.run(runId);
      expectedRuns.add(run);
      txnl.execute(() -> {
        recordProvisionAndStart(run, store);
      });
    }
    txnl.execute(() -> {
      Map<ProgramRunId, RunRecordMeta> activeRuns = store.getActiveRuns(programId);
      // the result should be sorted with larger time stamp come first
      Assert.assertEquals(Lists.reverse(expectedRuns), new ArrayList<>(activeRuns.keySet()));
    });
  }

  @Test
  public void testProgramRunCount() throws Exception {
    AppMetadataStore store = getMetadataStore("testProgramRuncount");
    ProgramId programId = NamespaceId.DEFAULT.app("test").workflow("test");
    TransactionExecutor txnl = getTxExecutor(store);
    List<ProgramRunId> runIds = addProgramCount(txnl, store, programId, 5);

    // should have 5 runs
    txnl.execute(() -> {
      Assert.assertEquals(5, store.getProgramRunCount(programId));
    });

    // stop all the program runs
    for (ProgramRunId runId : runIds) {
      txnl.execute(() -> {
        store.recordProgramRunning(
          runId, RunIds.getTime(runId.getRun(), TimeUnit.SECONDS) + 10, null,
          AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
        store.recordProgramStop(runId, RunIds.getTime(runId.getRun(), TimeUnit.SECONDS) + 20,
                                ProgramRunStatus.COMPLETED, null,
                                AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()));
      });
    }

    // should still have 5 runs even we record stop of the program run
    txnl.execute(() -> {
      Assert.assertEquals(5, store.getProgramRunCount(programId));
    });

    addProgramCount(txnl, store, programId, 3);

    // should have 8 runs
    txnl.execute(() -> {
      Assert.assertEquals(8, store.getProgramRunCount(programId));
    });

    // after cleanup we should only have 0 runs
    txnl.execute(() -> {
      store.deleteProgramHistory(programId.getNamespace(), programId.getApplication(), programId.getVersion());
      Assert.assertEquals(0, store.getProgramRunCount(programId));
    });
  }

  @Test
  public void testBatchProgramRunCount() throws Exception {
    AppMetadataStore store = getMetadataStore("testBatchProgramRunCount");
    ProgramId programId1 = NamespaceId.DEFAULT.app("test").workflow("test1");
    ProgramId programId2 = NamespaceId.DEFAULT.app("test").workflow("test2");
    ProgramId programId3 = NamespaceId.DEFAULT.app("test").workflow("test3");

    TransactionExecutor txnl = getTxExecutor(store);
    // add some run records to program1 and 2
    addProgramCount(txnl, store, programId1, 5);
    addProgramCount(txnl, store, programId2, 3);

    txnl.execute(() -> {
      Map<ProgramId, Long> counts = store.getProgramRunCounts(ImmutableList.of(programId1, programId2, programId3));
      Assert.assertEquals(5, (long) counts.get(programId1));
      Assert.assertEquals(3, (long) counts.get(programId2));
      Assert.assertEquals(0, (long) counts.get(programId3));
    });

    // after cleanup we should only have 0 runs for all programs
    txnl.execute(() -> {
      store.deleteProgramHistory(programId1.getNamespace(), programId1.getApplication(), programId1.getVersion());
      store.deleteProgramHistory(programId2.getNamespace(), programId2.getApplication(), programId2.getVersion());
      store.deleteProgramHistory(programId3.getNamespace(), programId3.getApplication(), programId3.getVersion());
    });

    txnl.execute(() -> {
      Map<ProgramId, Long> counts = store.getProgramRunCounts(ImmutableList.of(programId1, programId2, programId3));
      Assert.assertEquals(0, (long) counts.get(programId1));
      Assert.assertEquals(0, (long) counts.get(programId2));
      Assert.assertEquals(0, (long) counts.get(programId3));
    });
  }

  private List<ProgramRunId> addProgramCount(TransactionExecutor txnl, AppMetadataStore store,
                                             ProgramId programId, int count) throws Exception {
    List<ProgramRunId> runIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      RunId runId = RunIds.generate(i * 1000);
      ProgramRunId run = programId.run(runId);
      runIds.add(run);
      txnl.execute(() -> {
        recordProvisionAndStart(run, store);
      });
    }
    return runIds;
  }

  private static class CountingTicker extends Ticker {
    private final long elementsPerMillis;
    private int numProcessed = 0;

    CountingTicker(long elementsPerMillis) {
      this.elementsPerMillis = elementsPerMillis;
    }

    int getNumProcessed() {
      return numProcessed;
    }

    @Override
    public long read() {
      ++numProcessed;
      return TimeUnit.MILLISECONDS.toNanos(numProcessed / elementsPerMillis);
    }
  }
}
