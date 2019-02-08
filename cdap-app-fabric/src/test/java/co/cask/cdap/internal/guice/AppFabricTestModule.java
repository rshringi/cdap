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

package co.cask.cdap.internal.guice;

import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.guice.MonitorHandlerModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.InMemoryDiscoveryModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.config.guice.ConfigStoreModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionExecutorModule;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.provision.MockProvisionerModule;
import co.cask.cdap.logging.guice.LocalLogAppenderModule;
import co.cask.cdap.logging.guice.LogQueryServerModule;
import co.cask.cdap.logging.guice.LogReaderRuntimeModules;
import co.cask.cdap.messaging.guice.MessagingServerRuntimeModule;
import co.cask.cdap.metadata.MetadataReaderWriterModules;
import co.cask.cdap.metadata.MetadataServiceModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreServerModule;
import com.google.inject.AbstractModule;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import javax.annotation.Nullable;

/**
 *
 */
public final class AppFabricTestModule extends AbstractModule {

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final SConfiguration sConf;

  public AppFabricTestModule(CConfiguration configuration) {
    this(configuration, null);
  }

  public AppFabricTestModule(CConfiguration cConf, @Nullable SConfiguration sConf) {
    this.cConf = cConf;

    File localDataDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));

    hConf = new Configuration();
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));

    this.sConf = sConf == null ? SConfiguration.create() : sConf;
  }

  @Override
  protected void configure() {
    install(new DataFabricModules().getInMemoryModules());
    install(new DataSetsModules().getStandaloneModules());
    install(new TransactionExecutorModule());
    install(new DataSetServiceModules().getInMemoryModules());
    install(new ConfigModule(cConf, hConf, sConf));
    install(new IOModule());
    install(new InMemoryDiscoveryModule());
    install(new AppFabricServiceRuntimeModule().getInMemoryModules());
    install(new MonitorHandlerModule(false));
    install(new ProgramRunnerRuntimeModule().getInMemoryModules());
    install(new NonCustomLocationUnitTestModule());
    install(new LocalLogAppenderModule());
    install(new LogReaderRuntimeModules().getInMemoryModules());
    install(new LogQueryServerModule());
    install(new MetricsHandlerModule());
    install(new MetricsClientRuntimeModule().getInMemoryModules());
    install(new ExploreClientModule());
    install(new ConfigStoreModule().getInMemoryModule());
    install(new MetadataServiceModule());
    install(new AuthorizationModule());
    install(new AuthorizationEnforcementModule().getStandaloneModules());
    install(new SecureStoreServerModule());
    install(new MetadataReaderWriterModules().getInMemoryModules());
    install(new MessagingServerRuntimeModule().getInMemoryModules());
    install(new MockProvisionerModule());
  }
}
