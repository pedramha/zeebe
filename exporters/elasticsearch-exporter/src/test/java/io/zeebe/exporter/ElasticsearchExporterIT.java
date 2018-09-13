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
package io.zeebe.exporter;

import static io.zeebe.test.util.record.RecordingExporter.workflowInstanceRecords;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.gateway.ZeebeClient;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.ZeebeTestRule;
import io.zeebe.test.util.TestUtil;
import io.zeebe.test.util.record.RecordingExporter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ElasticsearchExporterIT {

  @Rule public ZeebeTestRule testRule = new ZeebeTestRule();

  public static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("testProcess")
          .startEvent()
          .intermediateCatchEvent(
              "message", e -> e.message(m -> m.name("catch").zeebeCorrelationKey("$.orderId")))
          .serviceTask("task", t -> t.zeebeTaskType("work"))
          .endEvent()
          .done();

  private ZeebeClient zeebeClient;
  private TransportClient esClient;
  private IndexRequestFactory indexRequestFactory;

  @Before
  public void setUp() throws UnknownHostException {
    zeebeClient = testRule.getClient();
    esClient = createElasticsearchClient();
    indexRequestFactory = new IndexRequestFactory(null);
  }

  @Test
  public void test() {
    final String orderId = "foo-bar-123";

    zeebeClient
        .workflowClient()
        .newDeployCommand()
        .addWorkflowModel(WORKFLOW, "workflow.bpmn")
        .send();

    zeebeClient
        .workflowClient()
        .newCreateInstanceCommand()
        .bpmnProcessId("testProcess")
        .latestVersion()
        .payload(Collections.singletonMap("orderId", orderId))
        .send();

    final AtomicLong counter = new AtomicLong(3);

    zeebeClient
        .jobClient()
        .newWorker()
        .jobType("work")
        .handler(
            (client, job) -> {
              if (counter.decrementAndGet() > 0) {
                client.newFailCommand(job).retries(job.getRetries() - 1).send();
              } else {
                client.newCompleteCommand(job).send();
              }
            })
        .open();

    zeebeClient
        .workflowClient()
        .newPublishMessageCommand()
        .messageName("catch")
        .correlationKey(orderId)
        .send();

    TestUtil.waitUntil(
        () ->
            workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_COMPLETED)
                .filter(r -> r.getKey() == r.getValue().getWorkflowInstanceKey())
                .exists());

    assertRecordsExported();
  }

  private void assertRecordsExported() {
    RecordingExporter.getRecords().forEach(this::assertRecordExported);
  }

  private void assertRecordExported(Record<?> record) {
    final GetResponse response =
        esClient
            .prepareGet()
            .setIndex(indexRequestFactory.indexFor(record))
            .setType(indexRequestFactory.typeFor(record))
            .setId(indexRequestFactory.idFor(record))
            .get();

    assertThat(response.isExists()).isTrue();

    final RecordMetadata metadata = record.getMetadata();
    final Map<String, Object> source = response.getSourceAsMap();
    assertThat(source.get("position")).isEqualTo(record.getPosition());
    assertThat(source.get("raftTerm")).isEqualTo(record.getRaftTerm());
    final Object sourceRecordPosition = source.get("sourceRecordPosition");
    if (sourceRecordPosition instanceof Integer) {
      assertThat((int) sourceRecordPosition).isEqualTo(record.getSourceRecordPosition());
    } else {
      assertThat(sourceRecordPosition).isEqualTo(record.getSourceRecordPosition());
    }
    assertThat(source.get("producerId")).isEqualTo(record.getProducerId());
    final Object key = source.get("key");
    if (key instanceof Integer) {
      assertThat((int) key).isEqualTo(record.getKey());
    } else {
      assertThat(key).isEqualTo(record.getKey());
    }
    assertThat(source.get("timestamp")).isEqualTo(record.getTimestamp().toEpochMilli());
    assertThat(source.get("intent")).isEqualTo(metadata.getIntent().name());
    assertThat(source.get("partitionId")).isEqualTo(metadata.getPartitionId());
    assertThat(source.get("recordType")).isEqualTo(metadata.getRecordType().name());
    assertThat(source.get("rejectionType")).isEqualTo(metadata.getRejectionType().name());
    assertThat(source.get("rejectionReason")).isEqualTo(metadata.getRejectionReason());
    assertThat(source.get("valueType")).isEqualTo(metadata.getValueType().name());
    assertThat(source.get("value")).isEqualTo(record.getValue().toJson());
  }

  protected TransportClient createElasticsearchClient() throws UnknownHostException {
    final Settings settings = Settings.builder().put("cluster.name", "test").build();

    final TransportAddress transportAddress =
        new TransportAddress(InetAddress.getByName("localhost"), 9300);

    return new PreBuiltTransportClient(settings).addTransportAddress(transportAddress);
  }
}
