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
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;

public class ElasticsearchExporter implements Exporter {

  public static final String ZEEBE_RECORDS_TEMPLATE_FILENAME = "/zeebe_records_template.json";

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
  private BulkRequestBuilder requestBuilder;
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
    } catch (Exception e) {
      throw new RuntimeException(
          "Exporter " + id + " to connect to elasticsearch with configuration: " + configuration,
          e);
    }

    requestBuilder = client.prepareBulk();
    indexRequestFactory = new IndexRequestFactory();

    if (configuration.isCreateTemplate()) {
      createTemplate();
    }

    log.debug("Exporter {} opened", id);
  }

  private void createTemplate() {
    final String templateFilename = ZEEBE_RECORDS_TEMPLATE_FILENAME;

    try (InputStream resourceAsStream =
        ElasticsearchExporter.class.getResourceAsStream(templateFilename)) {
      if (resourceAsStream == null) {
        log.error("Unable to read index template from file: {}", templateFilename);
      } else {
        final String templateName = configuration.getTemplateName();
        client
            .admin()
            .indices()
            .preparePutTemplate(templateName)
            .setSource(StreamUtil.read(resourceAsStream), XContentType.JSON)
            .execute()
            .actionGet();
        log.info("Updated index template '{}' from file '{}'", templateName, templateFilename);
      }
    } catch (IOException e) {
      log.error("Failed to load index template from file: {}", templateFilename, e);
    }
  }

  @Override
  public void close() {
    requestBuilder.get();
    client.close();
    log.debug("Exporter {} closed", id);
  }

  @Override
  public void export(Record record) {
    client
        .prepareIndex()
        .setIndex(indexRequestFactory.indexFor(record))
        .setType(indexRequestFactory.typeFor(record))
        .setId(indexRequestFactory.idFor(record))
        .setSource(record.toJson(), XContentType.JSON)
        .get();
    /*
    requestBuilder.add(
        client
            .prepareIndex()
            .setIndex(indexRequestFactory.indexFor(record))
            .setType(indexRequestFactory.typeFor(record))
            .setId(indexRequestFactory.idFor(record))
            .setSource(record.toJson(), XContentType.JSON));
            */
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
}
