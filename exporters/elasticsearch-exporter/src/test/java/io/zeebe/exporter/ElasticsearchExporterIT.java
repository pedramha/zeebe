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

import static io.zeebe.test.util.record.RecordingExporter.jobRecords;
import static org.assertj.core.api.AssertionsForClassTypes.entry;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import io.zeebe.exporter.record.Record;
import io.zeebe.gateway.ZeebeClient;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.test.ZeebeTestRule;
import io.zeebe.test.util.TestUtil;
import io.zeebe.test.util.record.RecordingExporter;
import java.net.InetAddress;
import java.net.UnknownHostException;
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

  private ZeebeClient zeebeClient;
  private TransportClient esClient;
  private IndexRequestFactory indexRequestFactory;

  @Before
  public void setUp() throws UnknownHostException {
    zeebeClient = testRule.getClient();
    esClient = createElasticsearchClient();
    indexRequestFactory = new IndexRequestFactory();
  }

  @Test
  public void test() {
    zeebeClient.jobClient().newCreateCommand().jobType("test-job").send().join();

    TestUtil.waitUntil(() -> jobRecords(JobIntent.CREATED).exists());

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
            .setId(indexRequestFactory.idFor(record))
            .get();

    assertThat(response.isExists()).isTrue();
    assertThat(response.getSource())
        .containsOnly(
            entry("position", record.getPosition()),
            entry("raftTerm", record.getRaftTerm()),
            entry("sourceRecordPosition", record.getSourceRecordPosition()),
            entry("producerId", record.getProducerId()),
            entry("key", record.getKey()),
            entry("timestamp", record.getTimestamp()),
            entry("metadata", record.getMetadata().toJson()),
            entry("value", record.getValue().toJson()));
  }

  protected TransportClient createElasticsearchClient() throws UnknownHostException {
    final Settings settings = Settings.builder().put("cluster.name", "test").build();

    final TransportAddress transportAddress =
        new TransportAddress(InetAddress.getByName("localhost"), 9300);

    return new PreBuiltTransportClient(settings).addTransportAddress(transportAddress);
  }
}
