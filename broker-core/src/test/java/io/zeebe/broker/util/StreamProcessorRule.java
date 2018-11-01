/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.util;

import io.zeebe.broker.logstreams.processor.KeyGenerator;
import io.zeebe.broker.logstreams.processor.TypedEventStreamProcessorBuilder;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.topic.StreamProcessorControl;
import io.zeebe.broker.transport.clientapi.BufferingServerOutput;
import io.zeebe.broker.util.TestStreams.FluentLogWriter;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.servicecontainer.testing.ServiceContainerRule;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.util.sched.clock.ControlledActorClock;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class StreamProcessorRule implements TestRule {

  public static final int PARTITION_ID = 0;
  // environment
  private final TemporaryFolder tempFolder = new TemporaryFolder();
  private final AutoCloseableRule closeables = new AutoCloseableRule();
  private final ControlledActorClock clock = new ControlledActorClock();
  private final ActorSchedulerRule actorSchedulerRule = new ActorSchedulerRule(clock);
  private final ServiceContainerRule serviceContainerRule =
      new ServiceContainerRule(actorSchedulerRule);

  // things provisioned by this rule
  public static final String STREAM_NAME = "stream";

  private BufferingServerOutput output;
  private TestStreams streams;
  private TypedStreamEnvironment streamEnvironment;

  private final SetupRule rule;

  public StreamProcessorRule() {
    this(PARTITION_ID);
  }

  public StreamProcessorRule(int partitionId) {
    rule = new SetupRule(partitionId);
    chain =
        RuleChain.outerRule(tempFolder)
            .around(actorSchedulerRule)
            .around(serviceContainerRule)
            .around(closeables)
            .around(rule);
  }

  private final RuleChain chain;

  @Override
  public Statement apply(Statement base, Description description) {
    return chain.apply(base, description);
  }

  public StreamProcessorControl runStreamProcessor(
      Function<TypedStreamEnvironment, StreamProcessor> factory) {
    final StreamProcessorControl control = initStreamProcessor(factory);
    control.start();
    return control;
  }

  public StreamProcessorControl runStreamProcessor(
      BiFunction<TypedEventStreamProcessorBuilder, ZeebeState, StreamProcessor> factory) {
    final StreamProcessorControl control = initStreamProcessor(factory);
    control.start();
    return control;
  }

  public StreamProcessorControl initStreamProcessor(
      Function<TypedStreamEnvironment, StreamProcessor> factory) {
    return streams.initStreamProcessor(STREAM_NAME, 0, () -> factory.apply(streamEnvironment));
  }

  public StreamProcessorControl initStreamProcessor(
      BiFunction<TypedEventStreamProcessorBuilder, ZeebeState, StreamProcessor> factory) {
    return streams.initStreamProcessor(
        STREAM_NAME,
        0,
        () -> {
          final ZeebeState zeebeState = new ZeebeState();
          final TypedEventStreamProcessorBuilder processorBuilder =
              streamEnvironment
                  .newStreamProcessor()
                  .keyGenerator(KeyGenerator.createKeyGenerator(0, zeebeState))
                  .withStateController(zeebeState);
          return factory.apply(processorBuilder, zeebeState);
        });
  }

  public ControlledActorClock getClock() {
    return clock;
  }

  public RecordStream events() {
    return new RecordStream(streams.events(STREAM_NAME));
  }

  public long writeEvent(long key, Intent intent, UnpackedObject value) {
    return streams
        .newRecord(STREAM_NAME)
        .recordType(RecordType.EVENT)
        .key(key)
        .intent(intent)
        .event(value)
        .write();
  }

  public long writeEvent(Intent intent, UnpackedObject value) {
    return streams
        .newRecord(STREAM_NAME)
        .recordType(RecordType.EVENT)
        .intent(intent)
        .event(value)
        .write();
  }

  public long writeCommand(long key, Intent intent, UnpackedObject value) {
    return streams
        .newRecord(STREAM_NAME)
        .recordType(RecordType.COMMAND)
        .key(key)
        .intent(intent)
        .event(value)
        .write();
  }

  public long writeCommand(Intent intent, UnpackedObject value) {
    return streams
        .newRecord(STREAM_NAME)
        .recordType(RecordType.COMMAND)
        .intent(intent)
        .event(value)
        .write();
  }

  public FluentLogWriter newRecord() {
    return streams.newRecord(STREAM_NAME);
  }

  public void truncateLog(long position) {
    streams.truncate(STREAM_NAME, position);
  }

  public BufferingServerOutput getOutput() {
    return output;
  }

  public void printAllRecords() {
    final LogStream logStream = streams.getLogStream(STREAM_NAME);
    LogStreamPrinter.printRecords(logStream);
  }

  private class SetupRule extends ExternalResource {

    private final int partitionId;

    SetupRule(int partitionId) {
      this.partitionId = partitionId;
    }

    @Override
    protected void before() {
      output = new BufferingServerOutput();

      streams =
          new TestStreams(
              tempFolder.getRoot(),
              closeables,
              serviceContainerRule.get(),
              actorSchedulerRule.get());
      streams.createLogStream(STREAM_NAME, partitionId);

      streams
          .newRecord(
              STREAM_NAME) // TODO: workaround for https://github.com/zeebe-io/zeebe/issues/478
          .event(new UnpackedObject())
          .write();

      streamEnvironment = new TypedStreamEnvironment(streams.getLogStream(STREAM_NAME), output);
    }
  }
}
