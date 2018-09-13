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
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MongoDbExporterTest {
  private static final String DB_NAME = "test";
  private static final String COLLECTION_NAME = "zeebe";

  private MongodExecutable mongodExe;
  private MongodProcess mongod;

  @Before
  public void setup() throws Exception {
    final MongodStarter starter = MongodStarter.getDefaultInstance();

    final IMongodConfig config =
        new MongodConfigBuilder()
            .version(Version.V3_6_5)
            .net(
                new Net(
                    Network.getLocalHost().getHostAddress(),
                    Network.getFreeServerPort(),
                    Network.localhostIsIPv6()))
            .build();

    mongodExe = starter.prepare(config);
    mongod = mongodExe.start();

    final MongoClient client =
        MongoClients.create("mongodb://" + config.net().getServerAddress().toString());
    client.getDatabase(DB_NAME).createCollection(COLLECTION_NAME);
  }

  @After
  public void teardown() throws Exception {
    if (mongodExe != null) {
      this.mongodExe.stop();
    }

    if (mongod != null) {
      this.mongod.stop();
    }
  }

  @Test
  public void shouldNotExportUntilBatchSizeIsReached() {}
}
