/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.metrics.MetricsReporterHook;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.metrics.DatasetMetricsReporter;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.store.StoreDefinition;
import co.cask.http.ChannelPipelineModifier;
import co.cask.http.NettyHttpService;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpRequest;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * DatasetService implemented using the common http netty framework.
 */
public class DatasetService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetService.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final DatasetOpExecutor opExecutorClient;
  private final Set<DatasetMetricsReporter> metricReporters;
  private final DatasetTypeService typeService;
  private final CConfiguration cConf;
  private final StructuredTableAdmin structuredTableAdmin;
  private final StructuredTableRegistry structuredTableRegistry;

  private Cancellable cancelDiscovery;
  private Cancellable opExecutorServiceWatch;
  private SettableFuture<ServiceDiscovered> opExecutorDiscovered;
  private volatile boolean stopping = false;

  @Inject
  public DatasetService(CConfiguration cConf,
                        DiscoveryService discoveryService,
                        DiscoveryServiceClient discoveryServiceClient,
                        MetricsCollectionService metricsCollectionService,
                        DatasetOpExecutor opExecutorClient,
                        Set<DatasetMetricsReporter> metricReporters,
                        DatasetTypeService datasetTypeService,
                        DatasetInstanceService datasetInstanceService,
                        StructuredTableAdmin structuredTableAdmin,
                        StructuredTableRegistry structuredTableRegistry) {
    this.cConf = cConf;
    this.typeService = datasetTypeService;
    DatasetTypeHandler datasetTypeHandler = new DatasetTypeHandler(datasetTypeService);
    DatasetInstanceHandler datasetInstanceHandler = new DatasetInstanceHandler(datasetInstanceService);
    CommonNettyHttpServiceBuilder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.DATASET_MANAGER);
    if (LOG.isTraceEnabled()) {
      builder.addChannelPipelineModifier(new ChannelPipelineModifier() {
        @Override
        public void modify(ChannelPipeline channelPipeline) {
          channelPipeline.addBefore("router", "logger", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
              if (msg instanceof HttpRequest) {
                HttpRequest req = (HttpRequest) msg;
                LOG.trace("Received {} for {} on channel {}", req.method(), req.uri(), ctx.channel());
              }
              super.channelRead(ctx, msg);
            }
          });
        }
      });
    }
    this.httpService = builder
      .setHttpHandlers(datasetTypeHandler, datasetInstanceHandler)
      .setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                Constants.Service.DATASET_MANAGER)))
      .setHost(cConf.get(Constants.Service.MASTER_SERVICES_BIND_ADDRESS))
      .setPort(cConf.getInt(Constants.Dataset.Manager.PORT))
      .setConnectionBacklog(cConf.getInt(Constants.Dataset.Manager.BACKLOG_CONNECTIONS))
      .setExecThreadPoolSize(cConf.getInt(Constants.Dataset.Manager.EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constants.Dataset.Manager.BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.Dataset.Manager.WORKER_THREADS))
      .build();
    this.discoveryService = discoveryService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.opExecutorClient = opExecutorClient;
    this.metricReporters = metricReporters;
    this.structuredTableAdmin = structuredTableAdmin;
    this.structuredTableRegistry = structuredTableRegistry;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting DatasetService...");
    StoreDefinition.createAllTables(structuredTableAdmin, structuredTableRegistry);
    typeService.startAndWait();
    opExecutorClient.startAndWait();
    httpService.start();

    // setting watch for ops executor service that we need to be running to operate correctly
    ServiceDiscovered discover = discoveryServiceClient.discover(Constants.Service.DATASET_EXECUTOR);
    opExecutorDiscovered = SettableFuture.create();
    opExecutorServiceWatch = discover.watchChanges(
      serviceDiscovered -> {
        if (!Iterables.isEmpty(serviceDiscovered)) {
          LOG.info("Discovered {} service", Constants.Service.DATASET_EXECUTOR);
          opExecutorDiscovered.set(serviceDiscovered);
        }
      }, MoreExecutors.sameThreadExecutor());

    for (DatasetMetricsReporter metricsReporter : metricReporters) {
      metricsReporter.start();
    }
  }

  @Override
  protected String getServiceName() {
    return "DatasetService";
  }

  @Override
  protected void run() throws Exception {
    waitForOpExecutorToStart();

    String announceAddress = cConf.get(Constants.Service.MASTER_SERVICES_ANNOUNCE_ADDRESS,
                                       httpService.getBindAddress().getHostName());
    int announcePort = cConf.getInt(Constants.Dataset.Manager.ANNOUNCE_PORT,
                                    httpService.getBindAddress().getPort());

    final InetSocketAddress socketAddress = new InetSocketAddress(announceAddress, announcePort);
    LOG.info("Announcing DatasetService for discovery...");
    // Register the service
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(new Discoverable(Constants.Service.DATASET_MANAGER, socketAddress)));

    LOG.info("DatasetService started successfully on {}", socketAddress);
    while (isRunning()) {
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        // It's triggered by stop
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private void waitForOpExecutorToStart() throws Exception {
    LOG.info("Waiting for {} service to be discoverable", Constants.Service.DATASET_EXECUTOR);
    while (!stopping) {
      try {
        opExecutorDiscovered.get(1, TimeUnit.SECONDS);
        opExecutorServiceWatch.cancel();
        break;
      } catch (TimeoutException e) {
        // re-try
      } catch (InterruptedException e) {
        LOG.warn("Got interrupted while waiting for service {}", Constants.Service.DATASET_EXECUTOR);
        Thread.currentThread().interrupt();
        opExecutorServiceWatch.cancel();
        break;
      } catch (ExecutionException e) {
        LOG.error("Error during discovering service {}, DatasetService start failed",
                  Constants.Service.DATASET_EXECUTOR);
        opExecutorServiceWatch.cancel();
        throw e;
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    stopping = true;
    super.triggerShutdown();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping DatasetService...");

    for (DatasetMetricsReporter metricsReporter : metricReporters) {
      metricsReporter.stop();
    }

    if (opExecutorServiceWatch != null) {
      opExecutorServiceWatch.cancel();
    }

    typeService.stopAndWait();

    if (cancelDiscovery != null) {
      cancelDiscovery.cancel();
    }

    // Wait for a few seconds for requests to stop
    try {
      TimeUnit.SECONDS.sleep(3);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting...", e);
    }

    httpService.stop();
    opExecutorClient.stopAndWait();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("bindAddress", httpService.getBindAddress())
      .toString();
  }
}
