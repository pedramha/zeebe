package io.zeebe.test.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.broker.exporter.record.RecordImpl;
import io.zeebe.exporter.context.Configuration;
import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.exporter.record.RecordValue;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.gateway.impl.data.ZeebeObjectMapperImpl;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExporterSimulator {
  private final Set<ExporterContainer> exporters = new HashSet<>();
  private final Counter exporterIdGenerator = new Counter(0);
  private final Counter[] partitionPositions;

  public ExporterSimulator() {
    partitionPositions = new Counter[] {new Counter()};
  }

  public ExporterSimulator(final int partitionCount) {
    partitionPositions = new Counter[partitionCount];

    for (int i = 0; i < partitionCount; i++) {
      partitionPositions[i] = new Counter();
    }
  }

  public String attach(final Exporter exporter, final Map<String, Object> arguments) {
    final String id = String.valueOf(exporterIdGenerator.increment());
    exporters.add(new ExporterContainer(exporter, id, arguments));

    return id;
  }

  public void detach(final String id) {
    exporters.removeIf(e -> e.getId().equals(id));
  }

  public void detach(final Exporter exporter) {
    exporters.removeIf(e -> e.getExporter() == exporter);
  }

  public void stream(int amount) {
    final Record[] records = generateRecords(amount);
    stream(records);
  }

  public void stream(final Record... records) {}

  private Record[] generateRecords(int amount) {
    final Record[] records = new Record[amount];

    for (int i = 0; i < amount; i++) {
      records[i] = generateRecord();
    }

    return records;
  }

  private Record generateRecord() {
    final RecordMetadata metadata = generateMetadata();
    final RecordValue value = generateValue(metadata);

    final Record record = new RecordImpl(
      new ZeebeObjectMapperImpl(),

    )
  }

  private static class Counter {
    long value = 0;

    Counter() {
      this(ThreadLocalRandom.current().nextLong());
    }

    Counter(final long initialValue) {
      value = initialValue;
    }

    long increment() {
      value += 1;
      return value;
    }
  }

  private static class ExporterContainer implements Context, Controller, Configuration {
    private static final ObjectMapper CONFIGURATOR = new ObjectMapper();

    private final Exporter exporter;
    private final Logger logger;
    private final String id;
    private final Map<String, Object> arguments;

    ExporterContainer(final Exporter exporter, final String id, final Map<String, Object> arguments) {
      this.exporter = exporter;
      this.id = id;
      this.arguments = arguments;
      this.logger = LoggerFactory.getLogger("exporter-" + id);
    }

    public Exporter getExporter() {
      return exporter;
    }

    @Override
    public Logger getLogger() {
      return logger;
    }

    @Override
    public Configuration getConfiguration() {
      return this;
    }

    @Override
    public void updateLastExportedRecordPosition(long position) {}

    @Override
    public void scheduleTask(Duration delay, Runnable task) {}

    @Override
    public String getId() {
      return id;
    }

    @Override
    public Map<String, Object> getArguments() {
      return arguments;
    }

    @Override
    public <T> T instantiate(Class<T> configClass) {
      return CONFIGURATOR.convertValue(arguments, configClass);
    }
  }

}
