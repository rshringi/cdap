/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.store;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.trigger.AndTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.OrTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.table.field.Range;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TransactionRunners;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This tests the indexing of the schedule store. Adding, retrieving. listing, deleting schedules is tested
 * in {@link co.cask.cdap.scheduler.CoreSchedulerServiceTest}, which has equivalent methods that execute
 * in a transaction.
 */
public class ProgramScheduleStoreDatasetTest extends AppFabricTestBase {

  private static final NamespaceId NS1_ID = new NamespaceId("schedtest");
  private static final NamespaceId NS2_ID = new NamespaceId("schedtestNs2");
  private static final ApplicationId APP1_ID = NS1_ID.app("app1", "1");
  private static final ApplicationId APP2_ID = NS1_ID.app("app2");
  private static final ApplicationId APP3_ID = NS2_ID.app("app3", "1");
  private static final WorkflowId PROG1_ID = APP1_ID.workflow("wf1");
  private static final WorkflowId PROG2_ID = APP2_ID.workflow("wf2");
  private static final WorkflowId PROG3_ID = APP2_ID.workflow("wf3");
  private static final WorkflowId PROG4_ID = APP3_ID.workflow("wf4");
  private static final WorkflowId PROG5_ID = APP3_ID.workflow("wf5");
  private static final DatasetId DS1_ID = NS1_ID.dataset("pfs1");
  private static final DatasetId DS2_ID = NS1_ID.dataset("pfs2");

  @BeforeClass
  public static void initialize() throws Exception {
    StructuredTableAdmin tableAdmin = getInjector().getInstance(StructuredTableAdmin.class);
    StoreDefinition.ProgramScheduleStore.createTables(tableAdmin);
  }

  @Before
  public void beforeTest() {
    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);
    // Delete all data in the tables
    TransactionRunners.run(
      transactionRunner,
      context -> {
        context.getTable(StoreDefinition.ProgramScheduleStore.PROGRAM_SCHEDULE_STORE_TABLE).deleteAll(Range.all());
        context.getTable(StoreDefinition.ProgramScheduleStore.PROGRAM_TRIGGER_STORE_TABLE).deleteAll(Range.all());
      }
    );
  }

  @Test
  public void testListSchedules() {
    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);

    final ProgramSchedule sched1 = new ProgramSchedule("sched1", "one partition schedule", PROG1_ID,
      Collections.emptyMap(), new PartitionTrigger(DS1_ID, 1), Collections.emptyList());
    final ProgramSchedule sched2 = new ProgramSchedule("sched2", "time schedule", PROG2_ID,
      Collections.emptyMap(), new TimeTrigger("* * * 1 1"), Collections.emptyList());
    final ProgramSchedule sched3 = new ProgramSchedule("sched3", "two partitions schedule", PROG4_ID,
      Collections.emptyMap(), new PartitionTrigger(DS1_ID, 2), Collections.emptyList());
    final ProgramSchedule sched4 = new ProgramSchedule("sched4", "time schedule", PROG5_ID,
      Collections.emptyMap(), new TimeTrigger("* * * 2 1"), Collections.emptyList());

    // assert no schedules exists before adding schedules
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        Assert.assertTrue(store.listSchedules(NS1_ID, schedule -> true).isEmpty());
        Assert.assertTrue(store.listSchedules(NS2_ID, schedule -> true).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(APP1_ID).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(APP2_ID).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(APP3_ID).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(PROG1_ID).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(PROG2_ID).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(PROG3_ID).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(PROG4_ID).isEmpty());
      }
    );

    // add schedules to the store
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.addSchedules(ImmutableList.of(sched1, sched2, sched3, sched4));
      }
    );

    // list schedules by namespace
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        Assert.assertEquals(ImmutableSet.of(sched1, sched2),
                            new HashSet<>(store.listSchedules(NS1_ID, schedule -> true)));
        Assert.assertEquals(ImmutableSet.of(sched3, sched4),
                            new HashSet<>(store.listSchedules(NS2_ID, schedule -> true)));
        // list schedules by app
        Assert.assertEquals(ImmutableSet.of(sched1),
                            toScheduleSet(store.listScheduleRecords(APP1_ID)));
        Assert.assertEquals(ImmutableSet.of(sched2),
                            toScheduleSet(store.listScheduleRecords(APP2_ID)));
        Assert.assertEquals(ImmutableSet.of(sched3, sched4),
                            toScheduleSet(store.listScheduleRecords(APP3_ID)));
        // list schedules by program
        Assert.assertEquals(ImmutableSet.of(sched1),
                            toScheduleSet(store.listScheduleRecords(PROG1_ID)));
        Assert.assertEquals(ImmutableSet.of(sched2),
                            toScheduleSet(store.listScheduleRecords(PROG2_ID)));
        Assert.assertEquals(ImmutableSet.of(sched3),
                            toScheduleSet(store.listScheduleRecords(PROG4_ID)));
        Assert.assertEquals(ImmutableSet.of(sched4),
                            toScheduleSet(store.listScheduleRecords(PROG5_ID)));
      }
    );
  }

  @Test
  public void testFindSchedulesByEventAndUpdateSchedule() {
    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);

    final ProgramSchedule sched11 = new ProgramSchedule("sched11", "one partition schedule", PROG1_ID,
                                                        ImmutableMap.of("prop3", "abc"),
                                                        new PartitionTrigger(DS1_ID, 1),
                                                        ImmutableList.<Constraint>of());
    final ProgramSchedule sched12 = new ProgramSchedule("sched12", "two partition schedule", PROG1_ID,
                                                        ImmutableMap.of("propper", "popper"),
                                                        new PartitionTrigger(DS2_ID, 2),
                                                        ImmutableList.<Constraint>of());
    final ProgramSchedule sched22 = new ProgramSchedule("sched22", "twentytwo partition schedule", PROG2_ID,
                                                        ImmutableMap.of("nn", "4"),
                                                        new PartitionTrigger(DS2_ID, 22),
                                                        ImmutableList.<Constraint>of());
    final ProgramSchedule sched31 = new ProgramSchedule("sched31", "a program status trigger", PROG3_ID,
                                                        ImmutableMap.of("propper", "popper"),
                                                        new ProgramStatusTrigger(PROG1_ID, ProgramStatus.COMPLETED,
                                                                                 ProgramStatus.FAILED,
                                                                                 ProgramStatus.KILLED),
                                                        ImmutableList.<Constraint>of());

    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // event for DS1 or DS2 should trigger nothing. validate it returns an empty collection
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForPartition(DS1_ID)).isEmpty());
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForPartition(DS2_ID)).isEmpty());
        // event for PROG1 should trigger nothing. it should also return an empty collection
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.COMPLETED))
                            .isEmpty());
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.FAILED))
                            .isEmpty());
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.KILLED))
                            .isEmpty());
      }
    );
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.addSchedules(ImmutableList.of(sched11, sched12, sched22, sched31));
      }
    );
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // event for ProgramStatus should trigger only sched31
        Assert.assertEquals(ImmutableSet.of(sched31),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.COMPLETED))));

        Assert.assertEquals(ImmutableSet.of(sched31),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.FAILED))));

        Assert.assertEquals(ImmutableSet.of(sched31),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.KILLED))));

        // event for DS1 should trigger only sched11
        Assert.assertEquals(ImmutableSet.of(sched11),
                            toScheduleSet(store.findSchedules(Schedulers.triggerKeyForPartition(DS1_ID))));
        // event for DS2 triggers only sched12 and sched22
        Assert.assertEquals(ImmutableSet.of(sched12, sched22),
                            toScheduleSet(store.findSchedules(Schedulers.triggerKeyForPartition(DS2_ID))));
      }
    );

    final ProgramSchedule sched11New = new ProgramSchedule(sched11.getName(), "time schedule", PROG1_ID,
                                                           ImmutableMap.of("timeprop", "time"),
                                                           new TimeTrigger("* * * * *"),
                                                           ImmutableList.<Constraint>of());
    final ProgramSchedule sched12New = new ProgramSchedule(sched12.getName(), "one partition schedule", PROG1_ID,
                                                           ImmutableMap.of("pp", "p"),
                                                           new PartitionTrigger(DS1_ID, 2),
                                                           ImmutableList.<Constraint>of());
    final ProgramSchedule sched22New = new ProgramSchedule(sched22.getName(), "program3 failed schedule", PROG2_ID,
                                                           ImmutableMap.of("ss", "s"),
                                                           new ProgramStatusTrigger(PROG3_ID, ProgramStatus.FAILED),
                                                           ImmutableList.<Constraint>of());
    final ProgramSchedule sched31New = new ProgramSchedule(sched31.getName(), "program1 failed schedule", PROG3_ID,
                                                           ImmutableMap.of("abcd", "efgh"),
                                                           new ProgramStatusTrigger(PROG1_ID, ProgramStatus.FAILED),
                                                           ImmutableList.<Constraint>of());

    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.updateSchedule(sched11New);
        store.updateSchedule(sched12New);
        store.updateSchedule(sched22New);
        store.updateSchedule(sched31New);
      }
    );
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // event for DS1 should trigger only sched12New after update
        Assert.assertEquals(ImmutableSet.of(sched12New),
                            toScheduleSet(store.findSchedules(Schedulers.triggerKeyForPartition(DS1_ID))));
        // event for DS2 triggers no schedule after update
        Assert.assertEquals(ImmutableSet.<ProgramSchedule>of(),
                            toScheduleSet(store.findSchedules(Schedulers.triggerKeyForPartition(DS2_ID))));

        // event for PS triggers only for failed program statuses, not completed nor killed
        Assert.assertEquals(ImmutableSet.of(sched31New),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.FAILED))));
        Assert.assertEquals(ImmutableSet.of(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.COMPLETED))));
        Assert.assertEquals(ImmutableSet.of(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.KILLED))));
      }
    );
  }

  private Set<ProgramSchedule> toScheduleSet(Collection<ProgramScheduleRecord> records) {
    return records.stream().map(ProgramScheduleRecord::getSchedule).collect(Collectors.toSet());
  }

  @Test
  public void testDeleteScheduleByTriggeringProgram() {
    TransactionRunner transactionRunner = getInjector().getInstance(TransactionRunner.class);

    SatisfiableTrigger prog1Trigger = new ProgramStatusTrigger(PROG1_ID, ProgramStatus.COMPLETED, ProgramStatus.FAILED,
                                                                ProgramStatus.KILLED);
    SatisfiableTrigger prog2Trigger = new ProgramStatusTrigger(PROG2_ID, ProgramStatus.COMPLETED, ProgramStatus.FAILED,
                                                    ProgramStatus.KILLED);
    final ProgramSchedule sched1 = new ProgramSchedule("sched1", "a program status trigger", PROG3_ID,
                                                        ImmutableMap.of("propper", "popper"),
                                                        prog1Trigger, ImmutableList.<Constraint>of());
    final ProgramSchedule sched2 = new ProgramSchedule("sched2", "a program status trigger", PROG3_ID,
                                                        ImmutableMap.of("propper", "popper"),
                                                        prog2Trigger, ImmutableList.<Constraint>of());

    final ProgramSchedule schedOr = new ProgramSchedule("schedOr", "an OR trigger", PROG3_ID,
                                                        ImmutableMap.of("propper", "popper"),
                                                        new OrTrigger(new PartitionTrigger(DS1_ID, 1), prog1Trigger,
                                                                      new AndTrigger(new OrTrigger(prog1Trigger,
                                                                                                   prog2Trigger),
                                                                                     new PartitionTrigger(DS2_ID, 1)),
                                                                      new OrTrigger(prog2Trigger)),
                                                        ImmutableList.<Constraint>of());

    final ProgramSchedule schedAnd = new ProgramSchedule("schedAnd", "an AND trigger", PROG3_ID,
                                                        ImmutableMap.of("propper", "popper"),
                                                        new AndTrigger(new PartitionTrigger(DS1_ID, 1), prog2Trigger,
                                                                       new AndTrigger(prog1Trigger,
                                                                                      new PartitionTrigger(DS2_ID, 1))),
                                                         ImmutableList.<Constraint>of());

    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.addSchedules(ImmutableList.of(sched1, sched2, schedOr, schedAnd));
      }
    );
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // ProgramStatus event for PROG1_ID should trigger only sched1, schedOr, schedAnd
        Assert.assertEquals(ImmutableSet.of(sched1, schedOr, schedAnd),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.COMPLETED))));
        // ProgramStatus event for PROG2_ID should trigger only sched2, schedOr, schedAnd
        Assert.assertEquals(ImmutableSet.of(sched2, schedOr, schedAnd),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG2_ID, ProgramStatus.FAILED))));
      }
    );
    // update or delete all schedules triggered by PROG1_ID
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.modifySchedulesTriggeredByDeletedProgram(PROG1_ID);
      }
    );
    final ProgramSchedule schedOrNew = new ProgramSchedule("schedOr", "an OR trigger", PROG3_ID,
                                                           ImmutableMap.of("propper", "popper"),
                                                           new OrTrigger(new PartitionTrigger(DS1_ID, 1),
                                                                         new AndTrigger(prog2Trigger,
                                                                                        new PartitionTrigger(DS2_ID,
                                                                                                             1)),
                                                                         prog2Trigger),
                                                           ImmutableList.of());
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // ProgramStatus event for PROG1_ID should trigger no schedules after modifying schedules triggered by it
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.COMPLETED))));
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.FAILED))));
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_ID, ProgramStatus.KILLED))));
        // ProgramStatus event for PROG2_ID should trigger only sched2 and schedOrNew
        Assert.assertEquals(ImmutableSet.of(sched2, schedOrNew),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG2_ID, ProgramStatus.FAILED))));
      }
    );
    // update or delete all schedules triggered by PROG2_ID
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.modifySchedulesTriggeredByDeletedProgram(PROG2_ID);
      }
    );
    final ProgramSchedule schedOrNew1 = new ProgramSchedule("schedOr", "an OR trigger", PROG3_ID,
                                                           ImmutableMap.of("propper", "popper"),
                                                           new PartitionTrigger(DS1_ID, 1),
                                                           ImmutableList.of());
    final Set<ProgramSchedule> ds1Schedules = new HashSet<>();
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // ProgramStatus event for PROG2_ID should trigger no schedules after modifying schedules triggered by it
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG2_ID, ProgramStatus.COMPLETED))));
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG2_ID, ProgramStatus.FAILED))));
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG2_ID, ProgramStatus.KILLED))));
        // event for DS1 should trigger only schedOrNew1 since all other schedules are deleted
        ds1Schedules.addAll(toScheduleSet(store.findSchedules(Schedulers.triggerKeyForPartition(DS1_ID))));
      }
    );
    Assert.assertEquals(ImmutableSet.of(schedOrNew1), ds1Schedules);
  }
}
