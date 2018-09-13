package io.zeebe.exporter;

import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.spi.Exporter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;

public class ElasticsearchExporter implements Exporter, BulkProcessor.Listener {

  static {
    // workaround if the gateway already configured netty
    // https://discuss.elastic.co/t/elasticsearch-5-4-1-availableprocessors-is-already-set/88036
    // TODO(menski): find out what happens if the gateway is not started
    System.setProperty("es.set.netty.runtime.available.processors", "false");
  }

  private Logger log;
  private Controller controller;

  private String id;
  private ElasticsearchExporterConfiguration configuration;

  private TransportClient client;
  private BulkProcessor bulkProcessor;
  private IndexRequestFactory indexRequestFactory;

  @Override
  public void configure(Context context) {
    log = context.getLogger();
    configuration =
        context.getConfiguration().instantiate(ElasticsearchExporterConfiguration.class);
    id = context.getConfiguration().getId();
    log.debug("Exporter {} configured with {}", id, configuration);
  }

  @Override
  public void open(Controller controller) {
    this.controller = controller;
    try {
      client = createClient();
      bulkProcessor = createBulkProcessor();
      indexRequestFactory = new IndexRequestFactory();
      log.debug("Exporter {} opened", id);
    } catch (Exception e) {
      throw new RuntimeException(
          "Exporter " + id + " to connect to elasticsearch with configuration: " + configuration,
          e);
    }
  }

  @Override
  public void close() {
    client.close();
    try {
      if (bulkProcessor.awaitClose(10, TimeUnit.SECONDS)) {
        log.debug("All remaining records flushed");
      } else {
        log.warn("Failed to flush all remaining records");
      }
    } catch (final Exception e) {
      log.error("Failed to close bulk processor", e);
    }
    log.debug("Exporter {} closed", id);
  }

  @Override
  public void export(Record record) {
    bulkProcessor.add(indexRequestFactory.create(record));
  }

  private TransportClient createClient() throws UnknownHostException {
    final Settings settings =
        Settings.builder().put("cluster.name", configuration.getClusterName()).build();

    final TransportAddress transportAddress =
        new TransportAddress(
            InetAddress.getByName(configuration.getHost()), configuration.getPort());

    log.info(
        "Connecting to elasticsearch with configuration {}:{}/{}",
        configuration.getHost(),
        configuration.getPort(),
        configuration.getClusterName());
    return new PreBuiltTransportClient(settings).addTransportAddress(transportAddress);
  }

  private BulkProcessor createBulkProcessor() {
    return BulkProcessor.builder(client, this)
        .setBulkActions(10000)
        .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
        .setFlushInterval(TimeValue.timeValueSeconds(5))
        .setConcurrentRequests(1)
        .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
        .build();
  }

  @Override
  public void beforeBulk(long executionId, BulkRequest request) {
    log.debug("Exporting bulk {} with {} records", executionId, request.numberOfActions());
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
    log.debug(
        "Successfully exported bulk {} with {} records", executionId, request.numberOfActions());
  }

  @Override
  public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
    log.error(
        "Failed to export bulk {} with {} records",
        executionId,
        request.numberOfActions(),
        failure);
  }
}
