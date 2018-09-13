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

import io.zeebe.exporter.record.Record;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.elasticsearch.action.index.IndexRequest;

public class IndexRequestFactory {

  private final String type;
  private final DateTimeFormatter formatter;

  public IndexRequestFactory() {
    this("zeebe-records", "record");
  }

  public IndexRequestFactory(String indexPrefix, String type) {
    this.type = type;
    this.formatter =
        DateTimeFormatter.ofPattern(indexPrefix + "-yyyy-MM-dd").withZone(ZoneOffset.UTC);
  }

  public IndexRequest create(final Record<?> record) {
    return new IndexRequest(indexFor(record), type, idFor(record)).source(record.toJson());
  }

  private String indexFor(final Record<?> record) {
    return formatter.format(record.getTimestamp());
  }

  private String idFor(final Record<?> record) {
    return record.getMetadata().getPartitionId() + "-" + record.getPosition();
  }
}
