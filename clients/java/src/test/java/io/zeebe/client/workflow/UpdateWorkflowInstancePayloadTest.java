/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.client.workflow;

import static io.zeebe.test.util.JsonUtil.fromJsonAsMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import io.zeebe.client.cmd.ClientException;
import io.zeebe.client.util.ClientTest;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateWorkflowInstancePayloadRequest;
import io.zeebe.util.StringUtil;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class UpdateWorkflowInstancePayloadTest extends ClientTest {

  @Test
  public void shouldCommandWithPayloadAsString() {
    // given
    final String updatedPayload = "{\"key\": \"val\"}";

    // when
    client.workflowClient().newUpdatePayloadCommand(123).payload(updatedPayload).send().join();

    // then
    final UpdateWorkflowInstancePayloadRequest request = gatewayService.getLastRequest();
    assertThat(request.getElementInstanceKey()).isEqualTo(123);
    assertThat(fromJsonAsMap(request.getPayload())).containsOnly(entry("key", "val"));
  }

  @Test
  public void shouldCommandWithPayloadAsStream() {
    // given
    final String updatedPayload = "{\"key\": \"val\"}";
    final InputStream payloadStream = new ByteArrayInputStream(StringUtil.getBytes(updatedPayload));

    // when
    client.workflowClient().newUpdatePayloadCommand(123).payload(payloadStream).send().join();

    // then
    final UpdateWorkflowInstancePayloadRequest request = gatewayService.getLastRequest();
    assertThat(fromJsonAsMap(request.getPayload())).containsOnly(entry("key", "val"));
  }

  @Test
  public void shouldCommandWithPayloadAsMap() {
    // given
    final Map<String, Object> payloadMap = Collections.singletonMap("key", "val");

    // when
    client.workflowClient().newUpdatePayloadCommand(123).payload(payloadMap).send().join();

    // then
    final UpdateWorkflowInstancePayloadRequest request = gatewayService.getLastRequest();
    assertThat(fromJsonAsMap(request.getPayload())).containsOnly(entry("key", "val"));
  }

  @Test
  public void shouldCommandWithPayloadAsObject() {
    // given
    final Map<String, Object> payloadMap = Collections.singletonMap("key", "val");

    // when
    client.workflowClient().newUpdatePayloadCommand(123).payload((Object) payloadMap).send().join();

    // then
    final UpdateWorkflowInstancePayloadRequest request = gatewayService.getLastRequest();
    assertThat(fromJsonAsMap(request.getPayload())).containsOnly(entry("key", "val"));
  }

  @Test
  public void shouldRaiseExceptionOnError() {
    // given
    gatewayService.errorOnRequest(
        UpdateWorkflowInstancePayloadRequest.class, () -> new ClientException("Invalid request"));

    // when
    assertThatThrownBy(
            () -> client.workflowClient().newUpdatePayloadCommand(123).payload("[]").send().join())
        .isInstanceOf(ClientException.class)
        .hasMessageContaining("Invalid request");
  }
}
