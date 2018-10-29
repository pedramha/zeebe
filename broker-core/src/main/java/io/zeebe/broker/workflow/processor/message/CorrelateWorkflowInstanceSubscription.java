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
package io.zeebe.broker.workflow.processor.message;

import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.logstreams.processor.SideEffectProducer;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.subscription.message.data.WorkflowInstanceSubscriptionRecord;
import io.zeebe.broker.workflow.processor.EventOutput;
import io.zeebe.broker.workflow.state.DeployedWorkflow;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.WorkflowEngineState;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.broker.workflow.state.WorkflowSubscription;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.intent.WorkflowInstanceSubscriptionIntent;
import io.zeebe.util.sched.ActorControl;
import java.time.Duration;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;

public final class CorrelateWorkflowInstanceSubscription
    implements TypedRecordProcessor<WorkflowInstanceSubscriptionRecord> {

  public static final Duration SUBSCRIPTION_TIMEOUT = Duration.ofSeconds(10);
  public static final Duration SUBSCRIPTION_CHECK_INTERVAL = Duration.ofSeconds(30);

  private final TopologyManager topologyManager;
  private final WorkflowEngineState engineState;
  private final SubscriptionCommandSender subscriptionCommandSender;
  private final EventOutput eventOutput;

  private TypedRecord<WorkflowInstanceSubscriptionRecord> record;
  private WorkflowInstanceSubscriptionRecord subscription;
  private TypedStreamWriter streamWriter;
  private Consumer<SideEffectProducer> sideEffect;

  public CorrelateWorkflowInstanceSubscription(
      TopologyManager topologyManager,
      WorkflowEngineState engineState,
      SubscriptionCommandSender subscriptionCommandSender) {
    this.topologyManager = topologyManager;
    this.engineState = engineState;
    this.eventOutput = new EventOutput(engineState);
    this.subscriptionCommandSender = subscriptionCommandSender;
  }

  @Override
  public void onOpen(TypedStreamProcessor streamProcessor) {
    final ActorControl actor = streamProcessor.getActor();
    final LogStream logStream = streamProcessor.getEnvironment().getStream();

    subscriptionCommandSender.init(topologyManager, actor, logStream);

    final PendingWorkflowInstanceSubscriptionChecker pendingSubscriptionChecker =
        new PendingWorkflowInstanceSubscriptionChecker(
            subscriptionCommandSender,
            engineState.getWorkflowState(),
            SUBSCRIPTION_TIMEOUT.toMillis());
    actor.runAtFixedRate(SUBSCRIPTION_CHECK_INTERVAL, pendingSubscriptionChecker);
  }

  @Override
  public void onClose() {}

  @Override
  public void processRecord(
      TypedRecord<WorkflowInstanceSubscriptionRecord> record,
      TypedResponseWriter responseWriter,
      TypedStreamWriter streamWriter,
      Consumer<SideEffectProducer> sideEffect) {

    this.record = record;
    this.subscription = record.getValue();
    this.streamWriter = streamWriter;
    this.sideEffect = sideEffect;

    final ElementInstance eventInstance =
        engineState.getElementInstanceState().getInstance(subscription.getActivityInstanceKey());

    if (eventInstance == null) {
      streamWriter.writeRejection(
          record, RejectionType.NOT_APPLICABLE, "activity is not active anymore");

    } else {
      final long workflowKey = eventInstance.getValue().getWorkflowKey();
      final DeployedWorkflow workflow =
          engineState.getWorkflowState().getWorkflowByKey(workflowKey);
      if (workflow != null) {
        onWorkflowAvailable(workflow);
      } else {
        streamWriter.writeRejection(
            record, RejectionType.NOT_APPLICABLE, "workflow is not available");
      }
    }
  }

  private void onWorkflowAvailable(final DeployedWorkflow deployedWorkflow) {
    // remove subscription if pending
    final WorkflowState state = engineState.getWorkflowState();
    final WorkflowSubscription subscriptionState = state.findSubscription(subscription);
    if (subscriptionState == null) {
      streamWriter.writeRejection(
          record, RejectionType.NOT_APPLICABLE, "subscription is already correlated");

      sideEffect.accept(this::sendAcknowledgeCommand);
      return;
    }
    state.remove(subscriptionState);

    final ElementInstance eventInstance =
        engineState.getElementInstanceState().getInstance(subscription.getActivityInstanceKey());

    eventOutput.setStreamWriter(streamWriter);
    eventOutput.newBatch();
    eventOutput
        .getBatchWriter()
        .addFollowUpEvent(
            record.getKey(), WorkflowInstanceSubscriptionIntent.CORRELATED, subscription);

    final WorkflowInstanceRecord value = eventInstance.getValue();
    final DirectBuffer handlerActivityId = subscriptionState.getHandlerActivityId();

    if (handlerActivityId.capacity() == 0 || value.getActivityId().equals(handlerActivityId)) {
      value.setPayload(subscription.getPayload());
      eventOutput.writeFollowUpEvent(
          eventInstance.getKey(), WorkflowInstanceIntent.ELEMENT_COMPLETING, value);
    } else {
      final WorkflowInstanceRecord newValue = new WorkflowInstanceRecord();

      newValue.setActivityId(handlerActivityId);
      newValue.setBpmnProcessId(value.getBpmnProcessId());
      newValue.setPayload(subscription.getPayload());
      newValue.setScopeInstanceKey(value.getScopeInstanceKey());
      newValue.setVersion(value.getVersion());
      newValue.setWorkflowInstanceKey(value.getWorkflowInstanceKey());
      newValue.setWorkflowKey(value.getWorkflowKey());

      final long handlerKey =
          eventOutput.writeNewEvent(WorkflowInstanceIntent.ELEMENT_READY, newValue);

      engineState
          .getElementInstanceState()
          .getInstance(handlerKey)
          .setAttachedToKey(eventInstance.getKey());
    }

    sideEffect.accept(this::sendAcknowledgeCommand);
  }

  private boolean sendAcknowledgeCommand() {
    return subscriptionCommandSender.correlateMessageSubscription(
        subscription.getSubscriptionPartitionId(),
        subscription.getWorkflowInstanceKey(),
        subscription.getActivityInstanceKey(),
        subscription.getMessageName());
  }
}
