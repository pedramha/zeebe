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

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.spi.Exporter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.Document;
import org.slf4j.Logger;

public class MongoDbExporter implements Exporter {
  private Logger logger;
  private String id;
  private Controller controller;
  private MongoDbConfiguration config;

  private MongoClient client;
  private MongoCollection<Document> collection;

  private List<Record> batch;

  @Override
  public void configure(Context context) {
    config = context.getConfiguration().instantiate(MongoDbConfiguration.class);
    logger = context.getLogger();
    id = context.getConfiguration().getId();
    batch = new ArrayList<>(config.getFlushSize());

    logger.trace("Configured!");
  }

  @Override
  public void open(Controller controller) {
    this.controller = controller;
    client = createClient();

    final MongoDatabase db = client.getDatabase(config.getDatabase());
    collection = db.getCollection(config.getCollection());
    controller.scheduleTask(config.getFlushDelay(), this::flushBatchIfPossible);

    logger.trace("Opened!");
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
      client = null;
    }

    logger.trace("Closed!");

    collection = null;
    controller = null;
    config = null;
    id = null;
    logger = null;
    batch = null;
  }

  @Override
  public void export(Record record) {
    logger.trace(
        "Exporting record {}-{}", record.getMetadata().getPartitionId(), record.getPosition());

    if (batch.size() < config.getFlushSize()) {
      batch.add(record);
    }

    if (shouldFlushBatch()) {
      flushBatch();
    }
  }

  private boolean shouldFlushBatch() {
    return batch.size() >= config.getFlushSize();
  }

  private MongoClient createClient() {
    return MongoClients.create(config.getClientSettings());
  }

  // TODO: schema validation?
  private void flushBatch() {
    if (batch.isEmpty()) {
      return;
    }

    final ClientSession session = client.startSession();
    final long lastPosition = batch.get(batch.size() - 1).getPosition();
    final ReplaceOptions modelOptions = new ReplaceOptions().upsert(true);
    final List<WriteModel<Document>> writes =
        batch.stream().map(r -> newUpdateModel(r, modelOptions)).collect(Collectors.toList());
    final BulkWriteOptions options =
        new BulkWriteOptions().ordered(false).bypassDocumentValidation(false);

    session.startTransaction();

    try {
      final BulkWriteResult result = collection.bulkWrite(writes, options);

      if (result.getUpserts().size() == writes.size()) {
        session.commitTransaction();
        controller.updateLastExportedRecordPosition(lastPosition);
        batch.clear();

        logger.trace("Flushed {} documents", writes.size());
      } else {
        throw new IncompleteBulkOperation(writes.size(), result.getUpserts().size());
      }
    } finally {
      session.close();
    }
  }

  private void flushBatchIfPossible() {
    if (shouldFlushBatch()) {
      flushBatch();
    }

    controller.scheduleTask(config.getFlushDelay(), this::flushBatchIfPossible);
  }

  private ReplaceOneModel<Document> newUpdateModel(
      final Record record, final ReplaceOptions options) {
    final Document document = newDocument(record);
    return new ReplaceOneModel<>(new Document("_id", document.get("_id")), document, options);
  }

  private Document newDocument(final Record record) {
    final Document document = Document.parse(record.toJson());
    document.append("_id", getRecordId(record));

    return document;
  }

  private String getRecordId(final Record record) {
    return String.format("%d-%d", record.getMetadata().getPartitionId(), record.getPosition());
  }
}
