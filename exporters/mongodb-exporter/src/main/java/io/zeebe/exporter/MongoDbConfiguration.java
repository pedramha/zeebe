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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import io.zeebe.util.DurationUtil;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MongoDbConfiguration {
  private static final List<ClusterMember> DEFAULT_HOSTS =
      Collections.singletonList(new ClusterMember("localhost", 27017));
  private static final List<String> DEFAULT_COMPRESSORS = Collections.singletonList("zlib");
  private static final Map<String, MongoCompressor> COMPRESSOR_MAP = new HashMap<>();

  static {
    COMPRESSOR_MAP.put("zlib", MongoCompressor.createZlibCompressor());
    COMPRESSOR_MAP.put("snappy", MongoCompressor.createSnappyCompressor());
  }

  private List<String> compressors = DEFAULT_COMPRESSORS;
  private List<ClusterMember> hosts = DEFAULT_HOSTS;
  private boolean useSsl = false;
  private int batchSize = 100;
  private String flushInterval = "1m";

  private String database;
  private String collection;

  private transient Duration batchFlushInterval;
  private transient MongoClientSettings clientSettings;

  public Duration getFlushInterval() {
    if (batchFlushInterval == null) {
      batchFlushInterval = DurationUtil.parse(flushInterval);
    }

    return batchFlushInterval;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public String getDatabase() {
    return database;
  }

  public String getCollection() {
    return collection;
  }

  // TODO: how to do secrets/credentials?
  // TODO: validate hosts/compressors?
  public MongoClientSettings getClientSettings() {
    if (clientSettings == null) {
      final List<MongoCompressor> compressors =
          this.compressors.stream().map(COMPRESSOR_MAP::get).collect(Collectors.toList());
      final List<ServerAddress> hosts =
          this.hosts.stream().map(ClusterMember::address).collect(Collectors.toList());
      clientSettings =
          MongoClientSettings.builder()
              .writeConcern(WriteConcern.MAJORITY)
              .applyToClusterSettings(b -> b.hosts(hosts))
              .applyToSslSettings(b -> b.enabled(useSsl))
              .compressorList(compressors)
              .build();
    }

    return clientSettings;
  }

  static class ClusterMember {
    private String host = "localhost";
    private int port = 27017;

    ClusterMember(String host, int port) {
      this.host = host;
      this.port = port;
    }

    ServerAddress address() {
      return new ServerAddress(host, port);
    }
  }
}
