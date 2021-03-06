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
package io.zeebe.broker.workflow.processor;

import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.logstreams.processor.TypedEventStreamProcessorBuilder;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.workflow.processor.message.CloseWorkflowInstanceSubscription;
import io.zeebe.broker.workflow.processor.message.CorrelateWorkflowInstanceSubscription;
import io.zeebe.broker.workflow.processor.message.OpenWorkflowInstanceSubscriptionProcessor;
import io.zeebe.broker.workflow.processor.timer.CancelTimerProcessor;
import io.zeebe.broker.workflow.processor.timer.CreateTimerProcessor;
import io.zeebe.broker.workflow.processor.timer.DueDateTimerChecker;
import io.zeebe.broker.workflow.processor.timer.TriggerTimerProcessor;
import io.zeebe.broker.workflow.state.WorkflowEngineState;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.TimerIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.intent.WorkflowInstanceSubscriptionIntent;

public class WorkflowEventProcessors {

  public static void addWorkflowProcessors(
      TypedEventStreamProcessorBuilder typedProcessorBuilder,
      ZeebeState zeebeState,
      SubscriptionCommandSender subscriptionCommandSender,
      TopologyManager topologyManager,
      DueDateTimerChecker timerChecker) {
    final WorkflowState workflowState = zeebeState.getWorkflowState();

    final WorkflowEngineState workflowEngineState = new WorkflowEngineState(workflowState);
    typedProcessorBuilder.withListener(workflowEngineState);

    addWorkflowInstanceCommandProcessor(typedProcessorBuilder, workflowEngineState);

    final BpmnStepProcessor bpmnStepProcessor =
        new BpmnStepProcessor(workflowEngineState, subscriptionCommandSender);
    addBpmnStepProcessor(typedProcessorBuilder, bpmnStepProcessor);

    addMessageStreamProcessors(
        typedProcessorBuilder, workflowState, topologyManager, subscriptionCommandSender);
    addTimerStreamProcessors(typedProcessorBuilder, timerChecker, workflowState);
  }

  private static void addWorkflowInstanceCommandProcessor(
      final TypedEventStreamProcessorBuilder builder, WorkflowEngineState workflowEngineState) {

    final WorkflowInstanceCommandProcessor commandProcessor =
        new WorkflowInstanceCommandProcessor(workflowEngineState);

    builder
        .onCommand(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.CREATE, commandProcessor)
        .onCommand(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.CANCEL, commandProcessor)
        .onCommand(
            ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.UPDATE_PAYLOAD, commandProcessor);
  }

  private static void addBpmnStepProcessor(
      TypedEventStreamProcessorBuilder streamProcessorBuilder,
      BpmnStepProcessor bpmnStepProcessor) {
    streamProcessorBuilder
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN,
            bpmnStepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.ELEMENT_READY, bpmnStepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ELEMENT_ACTIVATED,
            bpmnStepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ELEMENT_COMPLETING,
            bpmnStepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.START_EVENT_OCCURRED,
            bpmnStepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.END_EVENT_OCCURRED,
            bpmnStepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.GATEWAY_ACTIVATED,
            bpmnStepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ELEMENT_COMPLETED,
            bpmnStepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ELEMENT_TERMINATING,
            bpmnStepProcessor)
        .onEvent(
            ValueType.WORKFLOW_INSTANCE,
            WorkflowInstanceIntent.ELEMENT_TERMINATED,
            bpmnStepProcessor);
  }

  private static void addMessageStreamProcessors(
      final TypedEventStreamProcessorBuilder streamProcessorBuilder,
      WorkflowState workflowState,
      TopologyManager topologyManager,
      SubscriptionCommandSender subscriptionCommandSender) {
    streamProcessorBuilder
        .onCommand(
            ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION,
            WorkflowInstanceSubscriptionIntent.OPEN,
            new OpenWorkflowInstanceSubscriptionProcessor(workflowState))
        .onCommand(
            ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION,
            WorkflowInstanceSubscriptionIntent.CORRELATE,
            new CorrelateWorkflowInstanceSubscription(
                topologyManager, workflowState, subscriptionCommandSender))
        .onCommand(
            ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION,
            WorkflowInstanceSubscriptionIntent.CLOSE,
            new CloseWorkflowInstanceSubscription(workflowState));
  }

  private static void addTimerStreamProcessors(
      final TypedEventStreamProcessorBuilder streamProcessorBuilder,
      DueDateTimerChecker timerChecker,
      WorkflowState workflowState) {
    streamProcessorBuilder
        .onCommand(
            ValueType.TIMER,
            TimerIntent.CREATE,
            new CreateTimerProcessor(timerChecker, workflowState))
        .onCommand(ValueType.TIMER, TimerIntent.TRIGGER, new TriggerTimerProcessor(workflowState))
        .onCommand(ValueType.TIMER, TimerIntent.CANCEL, new CancelTimerProcessor(workflowState))
        .withListener(timerChecker);
  }
}
