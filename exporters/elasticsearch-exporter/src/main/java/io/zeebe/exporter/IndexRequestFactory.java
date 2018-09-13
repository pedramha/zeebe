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
