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

import io.zeebe.exporter.ElasticsearchExporterConfiguration.IndexConfiguration;
import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import java.time.Duration;
import org.slf4j.Logger;

public class ElasticsearchExporter implements Exporter {

  public static final String ZEEBE_RECORD_TEMPLATE_JSON = "/zeebe-record-template.json";

  private Logger log;
  private Controller controller;

  private ElasticsearchExporterConfiguration configuration;

  private ElasticsearchClient client;

  private long lastPosition = -1;

  @Override
  public void configure(Context context) {
    log = context.getLogger();
    configuration =
        context.getConfiguration().instantiate(ElasticsearchExporterConfiguration.class);
    log.debug("Exporter configured with {}", configuration);
  }

  @Override
  public void open(Controller controller) {
    this.controller = controller;
    client = createClient();
    createIndexTemplates();
    flushAndReschedule();
    log.info("Exporter opened");
  }

  protected ElasticsearchClient createClient() {
    return new ElasticsearchClient(configuration, log);
  }

  @Override
  public void close() {
    flush();

    try {
      client.close();
    } catch (Exception e) {
      log.warn("Failed to close elasticsearch client", e);
    }
    log.info("Exporter closed");
  }

  @Override
  public void export(Record record) {
    if (shouldIndexRecord(record)) {
      client.index(record);
    }

    lastPosition = record.getPosition();

    if (client.shouldFlush()) {
      flush();
    }
  }

  private void flushAndReschedule() {
    if (client.shouldFlush()) {
      flush();
    }
    controller.scheduleTask(Duration.ofSeconds(configuration.bulk.delay), this::flushAndReschedule);
  }

  private void flush() {
    if (client.flush()) {
      controller.updateLastExportedRecordPosition(lastPosition);
    } else {
      log.warn("Failed to flush bulk completely");
    }
  }

  private void createIndexTemplates() {
    final IndexConfiguration index = configuration.index;

    if (index.createTemplate) {
      createRootIndexTemplate();

      if (index.deployment) {
        createValueIndexTemplate(ValueType.DEPLOYMENT);
      }
      if (index.incident) {
        createValueIndexTemplate(ValueType.INCIDENT);
      }
      if (index.job) {
        createValueIndexTemplate(ValueType.JOB);
      }
      if (index.jobBatch) {
        createValueIndexTemplate(ValueType.JOB_BATCH);
      }
      if (index.message) {
        createValueIndexTemplate(ValueType.MESSAGE);
      }
      if (index.messageSubscription) {
        createValueIndexTemplate(ValueType.MESSAGE_SUBSCRIPTION);
      }
      if (index.raft) {
        createValueIndexTemplate(ValueType.RAFT);
      }
      if (index.workflowInstance) {
        createValueIndexTemplate(ValueType.WORKFLOW_INSTANCE);
      }
      if (index.workflowInstanceSubscription) {
        createValueIndexTemplate(ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION);
      }
    }
  }

  private void createRootIndexTemplate() {
    final String templateName = configuration.index.prefix;
    final String filename = ZEEBE_RECORD_TEMPLATE_JSON;
    if (!client.putIndexTemplate(templateName, filename, "-")) {
      log.warn("Put index template {} from file {} was not acknowledged", templateName, filename);
    }
  }

  private void createValueIndexTemplate(final ValueType valueType) {
    if (!client.putIndexTemplate(valueType)) {
      log.warn("Put index template for value type {} was not acknowledged", valueType);
    }
  }

  private boolean shouldIndexRecord(Record<?> record) {
    final RecordMetadata metadata = record.getMetadata();
    return shouldIndexRecordType(metadata.getRecordType())
        && shouldIndexValueType(metadata.getValueType());
  }

  private boolean shouldIndexValueType(ValueType valueType) {
    switch (valueType) {
      case DEPLOYMENT:
        return configuration.index.deployment;
      case INCIDENT:
        return configuration.index.incident;
      case JOB:
        return configuration.index.job;
      case JOB_BATCH:
        return configuration.index.jobBatch;
      case MESSAGE:
        return configuration.index.message;
      case MESSAGE_SUBSCRIPTION:
        return configuration.index.messageSubscription;
      case RAFT:
        return configuration.index.raft;
      case WORKFLOW_INSTANCE:
        return configuration.index.workflowInstance;
      case WORKFLOW_INSTANCE_SUBSCRIPTION:
        return configuration.index.workflowInstanceSubscription;
      default:
        return false;
    }
  }

  private boolean shouldIndexRecordType(RecordType recordType) {
    switch (recordType) {
      case EVENT:
        return configuration.index.event;
      case COMMAND:
        return configuration.index.command;
      case COMMAND_REJECTION:
        return configuration.index.rejection;
      default:
        return false;
    }
  }
}
