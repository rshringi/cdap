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

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.client.ClientMessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.inject.Injector;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Unit test for {@link MessagingServiceMain}.
 */
public class MessagingServiceMainTest extends MasterServiceMainTestBase {

  @Test
  public void testMessagingService() throws Exception {
    // Discover the TMS endpoint
    Injector injector = getServiceMainInstance(MessagingServiceMain.class).getInjector();
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);

    // Use a separate TMS client to create topic, then publish and then poll some messages
    TopicId topicId = NamespaceId.SYSTEM.topic("test");
    MessagingService messagingService = new ClientMessagingService(discoveryServiceClient);
    messagingService.createTopic(new TopicMetadata(topicId));

    // Publish 10 messages
    List<String> messages = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String msg = "Testing Message " + i;
      messagingService.publish(StoreRequestBuilder.of(topicId).addPayload(msg).build());
      messages.add(msg);
    }

    try (CloseableIterator<RawMessage> iterator = messagingService.prepareFetch(topicId).setLimit(10).fetch()) {
      List<String> received = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
        .map(RawMessage::getPayload)
        .map(ByteBuffer::wrap)
        .map(StandardCharsets.UTF_8::decode)
        .map(CharSequence::toString)
        .collect(Collectors.toList());

      Assert.assertEquals(messages, received);
    }
  }
}
