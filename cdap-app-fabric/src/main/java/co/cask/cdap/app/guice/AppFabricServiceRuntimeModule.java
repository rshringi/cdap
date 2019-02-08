/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package co.cask.cdap.app.guice;

import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.mapreduce.DistributedMRJobInfoFetcher;
import co.cask.cdap.app.mapreduce.LocalMRJobInfoFetcher;
import co.cask.cdap.app.mapreduce.MRJobInfoFetcher;
import co.cask.cdap.app.preview.PreviewHttpModule;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.config.guice.ConfigStoreModule;
import co.cask.cdap.data.security.DefaultSecretStore;
import co.cask.cdap.gateway.handlers.AppLifecycleHttpHandler;
import co.cask.cdap.gateway.handlers.ArtifactHttpHandler;
import co.cask.cdap.gateway.handlers.AuthorizationHandler;
import co.cask.cdap.gateway.handlers.BootstrapHttpHandler;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.gateway.handlers.ConfigHandler;
import co.cask.cdap.gateway.handlers.ConsoleSettingsHttpHandler;
import co.cask.cdap.gateway.handlers.DashboardHttpHandler;
import co.cask.cdap.gateway.handlers.ImpersonationHandler;
import co.cask.cdap.gateway.handlers.NamespaceHttpHandler;
import co.cask.cdap.gateway.handlers.OperationalStatsHttpHandler;
import co.cask.cdap.gateway.handlers.OperationsDashboardHttpHandler;
import co.cask.cdap.gateway.handlers.PreferencesHttpHandler;
import co.cask.cdap.gateway.handlers.ProfileHttpHandler;
import co.cask.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import co.cask.cdap.gateway.handlers.ProvisionerHttpHandler;
import co.cask.cdap.gateway.handlers.TransactionHttpHandler;
import co.cask.cdap.gateway.handlers.UpgradeHttpHandler;
import co.cask.cdap.gateway.handlers.UsageHandler;
import co.cask.cdap.gateway.handlers.VersionHandler;
import co.cask.cdap.gateway.handlers.WorkflowHttpHandler;
import co.cask.cdap.gateway.handlers.WorkflowStatsSLAHttpHandler;
import co.cask.cdap.gateway.handlers.meta.RemotePrivilegesHandler;
import co.cask.cdap.gateway.handlers.preview.PreviewHttpHandler;
import co.cask.cdap.internal.app.AppFabricDatasetModule;
import co.cask.cdap.internal.app.deploy.LocalApplicationManager;
import co.cask.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.namespace.DistributedStorageProviderNamespaceAdmin;
import co.cask.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import co.cask.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactStore;
import co.cask.cdap.internal.app.runtime.artifact.AuthorizationArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import co.cask.cdap.internal.app.runtime.schedule.DistributedTimeSchedulerService;
import co.cask.cdap.internal.app.runtime.schedule.ExecutorThreadPool;
import co.cask.cdap.internal.app.runtime.schedule.LocalTimeSchedulerService;
import co.cask.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import co.cask.cdap.internal.app.runtime.schedule.store.DatasetBasedTimeScheduleStore;
import co.cask.cdap.internal.app.runtime.schedule.store.TriggerMisfireLogger;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowStateWriter;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import co.cask.cdap.internal.app.services.LocalRunRecordCorrectorService;
import co.cask.cdap.internal.app.services.NoopRunRecordCorrectorService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.RunRecordCorrectorService;
import co.cask.cdap.internal.app.services.ScheduledRunRecordCorrectorService;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.bootstrap.guice.BootstrapModules;
import co.cask.cdap.internal.pipeline.SynchronousPipelineFactory;
import co.cask.cdap.internal.profile.ProfileService;
import co.cask.cdap.internal.provision.ProvisionerModule;
import co.cask.cdap.metadata.MetadataServiceModule;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.scheduler.CoreSchedulerService;
import co.cask.cdap.scheduler.Scheduler;
import co.cask.cdap.securestore.spi.SecretStore;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.DefaultUGIProvider;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import co.cask.cdap.security.store.SecureStoreHandler;
import co.cask.http.HttpHandler;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.quartz.SchedulerException;
import org.quartz.core.JobRunShellFactory;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.DefaultThreadExecutor;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.StdJobRunShellFactory;
import org.quartz.impl.StdScheduler;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * AppFabric Service Runtime Module.
 */
public final class AppFabricServiceRuntimeModule extends RuntimeModule {
  public static final String NOAUTH_ARTIFACT_REPO = "noAuthArtifactRepo";

  @Override
  public Module getInMemoryModules() {
    return Modules.combine(new AppFabricServiceModule(),
                           new NamespaceAdminModule().getInMemoryModules(),
                           new ConfigStoreModule().getInMemoryModule(),
                           new EntityVerifierModule(),
                           new AuthenticationContextModules().getMasterModule(),
                           BootstrapModules.getInMemoryModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(RunRecordCorrectorService.class).to(NoopRunRecordCorrectorService.class)
                                 .in(Scopes.SINGLETON);
                               bind(TimeSchedulerService.class).to(LocalTimeSchedulerService.class)
                                 .in(Scopes.SINGLETON);
                               bind(MRJobInfoFetcher.class).to(LocalMRJobInfoFetcher.class);
                               bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
                               bind(UGIProvider.class).to(UnsupportedUGIProvider.class);

                               Multibinder<String> servicesNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.services.names"));
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);

                               // TODO: Uncomment after CDAP-7688 is resolved
                               // servicesNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);

                               Multibinder<String> handlerHookNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.handler.hooks"));
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);

                               // TODO: Uncomment after CDAP-7688 is resolved
                               // handlerHookNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);
                             }
                           });
  }

  @Override
  public Module getStandaloneModules() {

    return Modules.combine(new AppFabricServiceModule(PreviewHttpHandler.class),
                           new NamespaceAdminModule().getStandaloneModules(),
                           new ConfigStoreModule().getStandaloneModule(),
                           new EntityVerifierModule(),
                           new AuthenticationContextModules().getMasterModule(),
                           new ProvisionerModule(),
                           BootstrapModules.getFileBasedModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(RunRecordCorrectorService.class).to(LocalRunRecordCorrectorService.class)
                                 .in(Scopes.SINGLETON);
                               bind(TimeSchedulerService.class).to(LocalTimeSchedulerService.class)
                                 .in(Scopes.SINGLETON);
                               bind(MRJobInfoFetcher.class).to(LocalMRJobInfoFetcher.class);
                               bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
                               bind(UGIProvider.class).to(UnsupportedUGIProvider.class);

                               Multibinder<String> servicesNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.services.names"));
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.PREVIEW_HTTP);

                               // for PingHandler
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.METRICS_PROCESSOR);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.LOGSAVER);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.TRANSACTION_HTTP);

                               // TODO: Uncomment after CDAP-7688 is resolved
                               // servicesNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);

                               Multibinder<String> handlerHookNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.handler.hooks"));
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.PREVIEW_HTTP);

                               // for PingHandler
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.METRICS_PROCESSOR);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.LOGSAVER);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.TRANSACTION_HTTP);

                               // TODO: Uncomment after CDAP-7688 is resolved
                               // handlerHookNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);
                             }
                           });
  }

  @Override
  public Module getDistributedModules() {

    return Modules.combine(new AppFabricServiceModule(ImpersonationHandler.class, PreviewHttpHandler.class),
                           new PreviewHttpModule().getDistributedModules(),
                           new NamespaceAdminModule().getDistributedModules(),
                           new ConfigStoreModule().getDistributedModule(),
                           new EntityVerifierModule(),
                           new AuthenticationContextModules().getMasterModule(),
                           new MetadataServiceModule(),
                           new ProvisionerModule(),
                           BootstrapModules.getFileBasedModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(RunRecordCorrectorService.class).to(ScheduledRunRecordCorrectorService.class)
                                 .in(Scopes.SINGLETON);
                               bind(TimeSchedulerService.class).to(DistributedTimeSchedulerService.class)
                                 .in(Scopes.SINGLETON);
                               bind(MRJobInfoFetcher.class).to(DistributedMRJobInfoFetcher.class);
                               bind(StorageProviderNamespaceAdmin.class)
                                 .to(DistributedStorageProviderNamespaceAdmin.class);
                               bind(UGIProvider.class).to(DefaultUGIProvider.class);

                               Multibinder<String> servicesNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.services.names"));
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.PREVIEW_HTTP);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.SECURE_STORE_SERVICE);

                               Multibinder<String> handlerHookNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.handler.hooks"));
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.PREVIEW_HTTP);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.SECURE_STORE_SERVICE);
                             }
                           });
  }

  /**
   * Guice module for AppFabricServer. Requires data-fabric related bindings being available.
   */
  private static final class AppFabricServiceModule extends AbstractModule {

    private final List<Class<? extends HttpHandler>> handlerClasses;

    private AppFabricServiceModule(Class<? extends HttpHandler>... handlerClasses) {
      this.handlerClasses = ImmutableList.copyOf(handlerClasses);
    }

    @Override
    protected void configure() {
      bind(PipelineFactory.class).to(SynchronousPipelineFactory.class);

      install(
        new FactoryModuleBuilder()
          .implement(new TypeLiteral<Manager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                     },
                     new TypeLiteral<LocalApplicationManager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                     })
          .build(new TypeLiteral<ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms>>() {
          })
      );

      // Bind system datasets defined in App-fabric
      MapBinder<String, DatasetModule> datasetModuleBinder = MapBinder.newMapBinder(
        binder(), String.class, DatasetModule.class, Constants.Dataset.Manager.DefaultDatasetModules.class);
      datasetModuleBinder.addBinding("app-fabric").toInstance(new AppFabricDatasetModule());

      bind(Store.class).to(DefaultStore.class);
      bind(SecretStore.class).to(DefaultSecretStore.class).in(Scopes.SINGLETON);

      // In App-Fabric, we can write directly, hence bind to the basic implementation
      bind(WorkflowStateWriter.class).to(BasicWorkflowStateWriter.class);

      bind(ArtifactStore.class).in(Scopes.SINGLETON);
      bind(ProfileService.class).in(Scopes.SINGLETON);
      bind(ProgramLifecycleService.class).in(Scopes.SINGLETON);
      bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
      bind(CoreSchedulerService.class).in(Scopes.SINGLETON);
      bind(Scheduler.class).to(CoreSchedulerService.class);
      bind(ArtifactRepository.class)
        .annotatedWith(Names.named(NOAUTH_ARTIFACT_REPO))
        .to(DefaultArtifactRepository.class)
        .in(Scopes.SINGLETON);
      bind(ArtifactRepository.class).to(AuthorizationArtifactRepository.class).in(Scopes.SINGLETON);
      bind(ProfileService.class).in(Scopes.SINGLETON);

      Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
        binder(), HttpHandler.class, Names.named(Constants.AppFabric.HANDLERS_BINDING));

      CommonHandlers.add(handlerBinder);
      handlerBinder.addBinding().to(ConfigHandler.class);
      handlerBinder.addBinding().to(VersionHandler.class);
      handlerBinder.addBinding().to(UsageHandler.class);
      handlerBinder.addBinding().to(NamespaceHttpHandler.class);
      handlerBinder.addBinding().to(AppLifecycleHttpHandler.class);
      handlerBinder.addBinding().to(DashboardHttpHandler.class);
      handlerBinder.addBinding().to(ProgramLifecycleHttpHandler.class);
      // TODO: [CDAP-13355] Move OperationsDashboardHttpHandler into report generation app
      handlerBinder.addBinding().to(OperationsDashboardHttpHandler.class);
      handlerBinder.addBinding().to(PreferencesHttpHandler.class);
      handlerBinder.addBinding().to(ConsoleSettingsHttpHandler.class);
      handlerBinder.addBinding().to(TransactionHttpHandler.class);
      handlerBinder.addBinding().to(WorkflowHttpHandler.class);
      handlerBinder.addBinding().to(ArtifactHttpHandler.class);
      handlerBinder.addBinding().to(WorkflowStatsSLAHttpHandler.class);
      handlerBinder.addBinding().to(AuthorizationHandler.class);
      handlerBinder.addBinding().to(SecureStoreHandler.class);
      handlerBinder.addBinding().to(RemotePrivilegesHandler.class);
      handlerBinder.addBinding().to(UpgradeHttpHandler.class);
      handlerBinder.addBinding().to(OperationalStatsHttpHandler.class);
      handlerBinder.addBinding().to(ProfileHttpHandler.class);
      handlerBinder.addBinding().to(ProvisionerHttpHandler.class);
      handlerBinder.addBinding().to(BootstrapHttpHandler.class);

      for (Class<? extends HttpHandler> handlerClass : handlerClasses) {
        handlerBinder.addBinding().to(handlerClass);
      }
    }

    @Provides
    @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS)
    @SuppressWarnings("unused")
    public InetAddress providesHostname(CConfiguration cConf) {
      String address = cConf.get(Constants.Service.MASTER_SERVICES_BIND_ADDRESS);
      return Networks.resolve(address, new InetSocketAddress("localhost", 0).getAddress());
    }

    /**
     * Provides a supplier of quartz scheduler so that initialization of the scheduler can be done after guice
     * injection. It returns a singleton of Scheduler.
     */
    @Provides
    @SuppressWarnings("unused")
    public Supplier<org.quartz.Scheduler> providesSchedulerSupplier(final DatasetBasedTimeScheduleStore scheduleStore,
                                                                    final CConfiguration cConf) {
      return new Supplier<org.quartz.Scheduler>() {
        private org.quartz.Scheduler scheduler;

        @Override
        public synchronized org.quartz.Scheduler get() {
          try {
            if (scheduler == null) {
              scheduler = getScheduler(scheduleStore, cConf);
            }
            return scheduler;
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };
    }

    /**
     * Create a quartz scheduler. Quartz factory method is not used, because inflexible in allowing custom jobstore
     * and turning off check for new versions.
     * @param store JobStore.
     * @param cConf CConfiguration.
     * @return an instance of {@link org.quartz.Scheduler}
     */
    private org.quartz.Scheduler getScheduler(JobStore store,
                                              CConfiguration cConf) throws SchedulerException {

      int threadPoolSize = cConf.getInt(Constants.Scheduler.CFG_SCHEDULER_MAX_THREAD_POOL_SIZE);
      ExecutorThreadPool threadPool = new ExecutorThreadPool(threadPoolSize);
      threadPool.initialize();
      String schedulerName = DirectSchedulerFactory.DEFAULT_SCHEDULER_NAME;
      String schedulerInstanceId = DirectSchedulerFactory.DEFAULT_INSTANCE_ID;

      QuartzSchedulerResources qrs = new QuartzSchedulerResources();
      JobRunShellFactory jrsf = new StdJobRunShellFactory();

      qrs.setName(schedulerName);
      qrs.setInstanceId(schedulerInstanceId);
      qrs.setJobRunShellFactory(jrsf);
      qrs.setThreadPool(threadPool);
      qrs.setThreadExecutor(new DefaultThreadExecutor());
      qrs.setJobStore(store);
      qrs.setRunUpdateCheck(false);
      QuartzScheduler qs = new QuartzScheduler(qrs, -1, -1);

      ClassLoadHelper cch = new CascadingClassLoadHelper();
      cch.initialize();

      store.initialize(cch, qs.getSchedulerSignaler());
      org.quartz.Scheduler scheduler = new StdScheduler(qs);

      jrsf.initialize(scheduler);
      qs.initialize();

      scheduler.getListenerManager().addTriggerListener(new TriggerMisfireLogger());
      return scheduler;
    }
  }
}
