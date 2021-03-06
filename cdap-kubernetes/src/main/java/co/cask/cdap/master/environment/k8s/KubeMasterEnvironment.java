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

package co.cask.cdap.master.environment.k8s;

import co.cask.cdap.k8s.discovery.KubeDiscoveryService;
import co.cask.cdap.master.spi.environment.MasterEnvironment;
import co.cask.cdap.master.spi.environment.MasterEnvironmentContext;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Implementation of {@link MasterEnvironment} to provide the environment for running in Kubernetes.
 */
public class KubeMasterEnvironment implements MasterEnvironment {

  private static final String NAMESPACE_KEY = "master.environment.k8s.namespace";
  private static final String POD_LABELS_PATH = "master.environment.k8s.pod.labels.path";

  private static final String DEFAULT_NAMESPACE = "default";
  private static final String DEFAULT_POD_LABELS_PATH = "/etc/podinfo/pod.labels.properties";

  private KubeDiscoveryService discoveryService;

  @Override
  public void initialize(MasterEnvironmentContext context) throws IOException {
    Map<String, String> conf = context.getConfigurations();

    // Load the pod labels from the configured path. It should be setup by the CDAP operator
    String podLabelsPath = conf.getOrDefault(POD_LABELS_PATH, DEFAULT_POD_LABELS_PATH);
    Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(new File(podLabelsPath).toPath(), StandardCharsets.UTF_8)) {
      properties.load(reader);
    }
    Map<String, String> podLabels = properties.stringPropertyNames().stream()
      .collect(Collectors.toMap(k -> k, properties::getProperty));

    discoveryService = new KubeDiscoveryService(conf.getOrDefault(NAMESPACE_KEY, DEFAULT_NAMESPACE), podLabels);
  }

  @Override
  public String getName() {
    return "k8s";
  }

  @Override
  public Supplier<DiscoveryService> getDiscoveryServiceSupplier() {
    return () -> discoveryService;
  }

  @Override
  public Supplier<DiscoveryServiceClient> getDiscoveryServiceClientSupplier() {
    return () -> discoveryService;
  }
}
