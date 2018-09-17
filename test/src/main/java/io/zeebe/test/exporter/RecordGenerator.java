package io.zeebe.test.exporter;

import io.zeebe.broker.exporter.record.RecordImpl;
import io.zeebe.broker.exporter.record.RecordMetadataImpl;
import io.zeebe.broker.exporter.record.value.DeploymentRecordValueImpl;
import io.zeebe.broker.exporter.record.value.deployment.DeploymentResourceImpl;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.exporter.record.RecordValue;
import io.zeebe.exporter.record.value.DeploymentRecordValue;
import io.zeebe.exporter.record.value.deployment.DeployedWorkflow;
import io.zeebe.exporter.record.value.deployment.DeploymentResource;
import io.zeebe.gateway.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import io.zeebe.protocol.intent.RaftIntent;
import io.zeebe.protocol.intent.SubscriberIntent;
import io.zeebe.protocol.intent.SubscriptionIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.intent.WorkflowInstanceSubscriptionIntent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RecordGenerator {
  private static final ValueType[] GENERATABLE_VALUE_TYPES =
      new ValueType[] {
        ValueType.DEPLOYMENT,
        ValueType.INCIDENT,
        ValueType.JOB,
        ValueType.MESSAGE,
        ValueType.MESSAGE_SUBSCRIPTION,
        ValueType.RAFT,
        ValueType.SUBSCRIBER,
        ValueType.SUBSCRIPTION,
        ValueType.WORKFLOW_INSTANCE,
        ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION
      };
  private static final Map<ValueType, Intent[]> VALUE_INTENT_MAP;

  static {
    VALUE_INTENT_MAP = new HashMap<>();
    VALUE_INTENT_MAP.put(ValueType.DEPLOYMENT, DeploymentIntent.values());
    VALUE_INTENT_MAP.put(ValueType.INCIDENT, IncidentIntent.values());
    VALUE_INTENT_MAP.put(ValueType.JOB, JobIntent.values());
    VALUE_INTENT_MAP.put(ValueType.MESSAGE, MessageIntent.values());
    VALUE_INTENT_MAP.put(ValueType.MESSAGE_SUBSCRIPTION, MessageSubscriptionIntent.values());
    VALUE_INTENT_MAP.put(ValueType.RAFT, RaftIntent.values());
    VALUE_INTENT_MAP.put(ValueType.SUBSCRIBER, SubscriberIntent.values());
    VALUE_INTENT_MAP.put(ValueType.SUBSCRIPTION, SubscriptionIntent.values());
    VALUE_INTENT_MAP.put(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.values());
    VALUE_INTENT_MAP.put(
        ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION, WorkflowInstanceSubscriptionIntent.values());
  }

  private final ZeebeObjectMapperImpl objectMapper;

  public RecordGenerator() {
    objectMapper = new ZeebeObjectMapperImpl();
  }

  public Record<?> generate(final int partitionId, final int term, final long latestPosition) {
    final RecordMetadata metadata = generateMetadata(partitionId);
    final RecordValue value = generateValue(metadata.getValueType());
    final long position = latestPosition + 1;

    return new RecordImpl<>(
        objectMapper,
        generateRecordKey(position),
        position,
        Instant.now(),
        term,
        generateProducerId(),
        generateSourceRecordPosition(metadata, latestPosition),
        metadata,
        value);
  }

  public RecordMetadata generateMetadata(final int partitionId) {
    final ValueType valueType = generateValueType();
    final Intent intent = generateIntent(valueType);
    final RecordType recordType = generateRecordType();
    final RejectionType rejectionType = generateRejectionType(recordType);

    return new RecordMetadataImpl(
        objectMapper,
        partitionId,
        intent,
        recordType,
        rejectionType,
        generateRejectionReason(rejectionType),
        valueType);
  }

  public RecordValue generateValue(final ValueType type) {
    switch (type) {
      case DEPLOYMENT:
        return generateDeploymentRecord();
    }
  }

  private DeploymentRecordValue generateDeploymentRecord() {
    final int resourceCount = ThreadLocalRandom.current().nextInt(0, 3);
    final List<DeploymentResource> resources = new ArrayList<>(resourceCount);
    final int workflowCount = ThreadLocalRandom.current().nextInt(0, 3);
    final List<DeployedWorkflow> workflows = new ArrayList<>(workflowCount);

    for (int i = 0; i < resourceCount; i++) {
      resources.add(generateDeploymentResource());
    }

    for (int i = 0; i < workflowCount; i++) {
      workflows.add(generateDeployedWorkflow());
    }

    return new DeploymentRecordValueImpl(objectMapper, workflows, resources);
  }

  private DeploymentResource generateDeploymentResource() {
    

    return new DeploymentResourceImpl();
  }

  private DeployedWorkflow generateDeployedWorkflow() {

  }

  private long generateRecordKey(final long position) {
    return position;
  }

  private int generateProducerId() {
    return ThreadLocalRandom.current().nextInt(-1, 100);
  }

  private long generateSourceRecordPosition(final RecordMetadata metadata, final long position) {
    if (metadata.getRecordType() == RecordType.EVENT) {
      return ThreadLocalRandom.current().nextLong(0, position - 1);
    } else {
      return -1;
    }
  }

  private ValueType generateValueType() {
    return oneOf(GENERATABLE_VALUE_TYPES);
  }

  private Intent generateIntent(final ValueType type) {
    return oneOf(VALUE_INTENT_MAP.get(type));
  }

  // TODO: should be based on intent
  private RecordType generateRecordType() {
    return oneOf(RecordType.values());
  }

  private RejectionType generateRejectionType(final RecordType type) {
    if (type == RecordType.COMMAND_REJECTION) {
      return oneOf(RejectionType.values());
    }

    return RejectionType.NULL_VAL;
  }

  private String generateRejectionReason(final RejectionType type) {
    switch (type) {
      case BAD_VALUE:
        return "bad value";
      case NOT_APPLICABLE:
        return "not applicable";
      case PROCESSING_ERROR:
        return "processing error";
      case SBE_UNKNOWN:
        return "unknown";
      default:
        return "";
    }
  }

  private <T> T oneOf(T[] values) {
    return values[ThreadLocalRandom.current().nextInt(0, values.length)];
  }
}
