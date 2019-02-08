/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package co.cask.cdap.internal.app.preview;

import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.preview.PreviewHttpModule;
import co.cask.cdap.app.preview.PreviewManager;
import co.cask.cdap.app.preview.PreviewRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
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
import co.cask.cdap.internal.provision.ProvisionerModule;
import co.cask.cdap.logging.guice.LocalLogAppenderModule;
import co.cask.cdap.logging.guice.LogReaderRuntimeModules;
import co.cask.cdap.messaging.guice.MessagingServerRuntimeModule;
import co.cask.cdap.metadata.MetadataReaderWriterModules;
import co.cask.cdap.metadata.MetadataServiceModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreServerModule;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests for {@link DefaultPreviewManager}.
 */
public class DefaultPreviewManagerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static Injector injector;

  @BeforeClass
  public static void beforeClass() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    injector = Guice.createInjector(
      new ConfigModule(cConf, new Configuration()),
      new IOModule(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getStandaloneModules(),
      new TransactionExecutorModule(),
      new DataSetServiceModules().getInMemoryModules(),
      new InMemoryDiscoveryModule(),
      new AppFabricServiceRuntimeModule().getInMemoryModules(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new NonCustomLocationUnitTestModule(),
      new LocalLogAppenderModule(),
      new LogReaderRuntimeModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new ConfigStoreModule().getInMemoryModule(),
      new MetadataServiceModule(),
      new MetadataReaderWriterModules().getInMemoryModules(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getStandaloneModules(),
      new SecureStoreServerModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new PreviewHttpModule().getStandaloneModules(),
      new ProvisionerModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      }
    );
  }

  private Injector getInjector() {
    return injector;
  }

  @Test
  public void testInjector() throws Exception {

    PreviewManager previewManager = getInjector().getInstance(PreviewManager.class);
    DefaultPreviewManager defaultPreviewManager = (DefaultPreviewManager) previewManager;

    Injector previewInjector = defaultPreviewManager.createPreviewInjector(new ApplicationId("ns1", "app1"));

    // Make sure same PreviewManager instance is returned for a same preview
    Assert.assertEquals(previewInjector.getInstance(PreviewRunner.class),
                        previewInjector.getInstance(PreviewRunner.class));

    Injector anotherPreviewInjector
      = defaultPreviewManager.createPreviewInjector(new ApplicationId("ns2", "app2"));

    Assert.assertNotEquals(previewInjector.getInstance(PreviewRunner.class),
                           anotherPreviewInjector.getInstance(PreviewRunner.class));
  }
}
