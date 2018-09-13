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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class IndexRequestFactory {

  private final String indexPrefix;
  private final DateTimeFormatter formatter;

  public IndexRequestFactory() {
    this("zeebe-record");
  }

  public IndexRequestFactory(String indexPrefix) {
    this.indexPrefix = indexPrefix;
    this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
  }

  public IndexRequest create(final Record<?> record) throws IOException {
    return new IndexRequest(indexFor(record), typeFor(record), idFor(record))
        .source(sourceFor(record));
  }

  public XContentBuilder sourceFor(final Record<?> record) throws IOException {
    final RecordMetadata metadata = record.getMetadata();
    return jsonBuilder()
        .startObject()
        .field("position", record.getPosition())
        .field("raftTerm", record.getRaftTerm())
        .field("sourceRecordPosition", record.getSourceRecordPosition())
        .field("producerId", record.getProducerId())
        .field("key", record.getKey())
        .field("timestamp", record.getTimestamp().toEpochMilli())
        .field("intent", metadata.getIntent())
        .field("partitionId", metadata.getPartitionId())
        .field("recordType", metadata.getRecordType())
        .field("rejectionType", metadata.getRejectionType())
        .field("rejectionReason", metadata.getRejectionReason())
        .field("valueType", metadata.getValueType())
        .field("value", record.getValue().toJson())
        .endObject();
  }

  protected String indexFor(final Record<?> record) {
    return indexPrefix + "-" + formatter.format(record.getTimestamp());
  }

  protected String idFor(final Record<?> record) {
    return record.getMetadata().getPartitionId() + "-" + record.getPosition();
  }

  public String typeFor(Record record) {
    return "_doc";
  }
}
