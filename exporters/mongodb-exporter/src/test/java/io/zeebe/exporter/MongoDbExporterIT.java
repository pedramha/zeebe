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
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.zeebe.exporter.record.Record;
import io.zeebe.gateway.ZeebeClient;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.ZeebeTestRule;
import io.zeebe.test.util.TestUtil;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class MongoDbExporterIT {
  private static final String COLLECTION_NAME = "zeebe";
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("testProcess")
          .startEvent()
          .intermediateCatchEvent(
              "message", e -> e.message(m -> m.name("catch").zeebeCorrelationKey("$.orderId")))
          .serviceTask("task", t -> t.zeebeTaskType("work"))
          .endEvent()
          .done();

  @Rule public final ZeebeTestRule zeebeRule = new ZeebeTestRule();
  private final MongoTestRule mongoRule = new MongoTestRule("127.0.0.1", 27107, "test");

  private MongoCollection<Document> collection;
  private ZeebeClient zeebeClient;

  @Before
  public void setup() throws Exception {
    final MongoClient client = MongoClients.create("mongodb://127.0.0.1:27017");
    collection = client.getDatabase("test").getCollection(COLLECTION_NAME);
    zeebeClient = zeebeRule.getClient();
  }

  @Test
  public void shouldExportRecords() {
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

  @Test
  public void shouldNotExportUntilBatchSizeIsReached() {}

  private void assertRecordsExported() {
    RecordingExporter.getRecords().forEach(this::assertRecordExported);
  }

  private void assertRecordExported(Record<?> record) {
    final Document filter =
        new Document(
            "_id",
            String.format("%d-%d", record.getMetadata().getPartitionId(), record.getPosition()));
    final Document document = collection.find(filter).first();

    assertThat(document).isNotNull();
    document.remove("_id");

    assertThat(document).isEqualTo(Document.parse(record.toJson()));
  }
}
