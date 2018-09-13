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

import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.util.StreamUtil;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;

public class ElasticsearchExporter implements Exporter {

  public static final String ZEEBE_RECORDS_TEMPLATE_FILENAME = "/zeebe_records_template.json";

  private Logger log;
  private Controller controller;

  private String id;
  private ElasticsearchExporterConfiguration configuration;

  private RestHighLevelClient client;
  private BulkRequest bulkRequest;
  private IndexRequestFactory indexRequestFactory;

  private long lastPosition = -1;

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
    client = createClient();
    bulkRequest = new BulkRequest();
    indexRequestFactory = new IndexRequestFactory();

    if (configuration.isCreateTemplate()) {
      createTemplate();
    }

    flushAndReschedule();

    log.debug("Exporter {} opened", id);
  }

  private void flushAndReschedule() {
    flush();
    controller.scheduleTask(
        Duration.ofSeconds(configuration.getFlushDelay()), this::flushAndReschedule);
  }

  private void createTemplate() {
    final String templateFilename = ZEEBE_RECORDS_TEMPLATE_FILENAME;

    try (InputStream resourceAsStream =
        ElasticsearchExporter.class.getResourceAsStream(templateFilename)) {
      if (resourceAsStream == null) {
        log.error("Unable to read index template from file: {}", templateFilename);
      } else {
        final String templateName = configuration.getTemplateName();

        final PutIndexTemplateRequest request =
            new PutIndexTemplateRequest(templateName)
                .source(StreamUtil.read(resourceAsStream), XContentType.JSON);
        // TODO(menski): check response
        client.indices().putTemplate(request, RequestOptions.DEFAULT);
        log.info("Updated index template '{}' from file '{}'", templateName, templateFilename);
      }
    } catch (IOException e) {
      log.error("Failed to load index template from file: {}", templateFilename, e);
    }
  }

  @Override
  public void close() {
    flush();

    try {
      client.close();
    } catch (IOException e) {
      log.warn("Failed to close elasticsearch client", e);
    }
    log.debug("Exporter {} closed", id);
  }

  private void flush() {
    if (bulkRequest.numberOfActions() > 0) {
      try {
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        bulkRequest = new BulkRequest();
        controller.updateLastExportedRecordPosition(lastPosition);
      } catch (Exception e) {
        log.warn("Failed to export bulk", e);
      }
    }
  }

  private boolean shouldFlush() {
    return bulkRequest.numberOfActions() >= configuration.getFlushSize();
  }

  @Override
  public void export(Record record) {
    try {
      bulkRequest.add(indexRequestFactory.create(record));
      lastPosition = record.getPosition();
      if (shouldFlush()) {
        flush();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to add index request to bulk", e);
    }
  }

  private RestHighLevelClient createClient() {
    log.info(
        "Connecting to elasticsearch with configuration {}:{}/{}",
        configuration.getHost(),
        configuration.getPort(),
        configuration.getClusterName());

    // TODO(menski): reduce thread pool
    return new RestHighLevelClient(
        RestClient.builder(new HttpHost(configuration.getHost(), configuration.getPort(), "http")));
  }
}
