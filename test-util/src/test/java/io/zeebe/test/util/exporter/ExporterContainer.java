package io.zeebe.test.util.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.exporter.context.Configuration;
import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.spi.Exporter;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExporterContainer implements Context, Controller, Configuration {
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
