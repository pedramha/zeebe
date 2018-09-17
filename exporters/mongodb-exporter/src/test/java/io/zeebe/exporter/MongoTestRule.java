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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import io.zeebe.test.util.TestUtil;
import org.bson.Document;
import org.junit.rules.ExternalResource;

public class MongoTestRule extends ExternalResource {
  private final Net bindAddress;

  private MongodExecutable mongodExe;
  private MongodProcess mongod;
  private MongoClient client;

  private final String databaseName;

  public MongoTestRule(final String host, final int port, final String databaseName) {
    this.bindAddress = new Net(host, port, false);
    this.databaseName = databaseName;
  }

  public MongoCollection<Document> getCollection(final String name) {
    MongoClient client = null;

    try {
      client =
          MongoClients.create("mongodb://" + bindAddress.getBindIp() + ":" + bindAddress.getPort());
      final MongoDatabase db = client.getDatabase(databaseName);
      db.createCollection(name);

      return db.getCollection(name);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Override
  protected void before() throws Throwable {
    final MongodStarter starter = MongodStarter.getDefaultInstance();

    final IMongodConfig config =
        new MongodConfigBuilder().version(Version.V3_6_5).net(bindAddress).build();

    mongodExe = starter.prepare(config);
    mongod = mongodExe.start();
    TestUtil.waitUntil(() -> mongod.isProcessRunning());
  }

  @Override
  protected void after() {
    if (mongod != null) {
      if (mongod.isProcessRunning()) {
        mongod.stop();
      }

      mongod = null;
    }

    if (mongodExe != null) {
      mongodExe.stop();
      mongodExe = null;
    }
  }
}
