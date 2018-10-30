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
package io.zeebe.broker.workflow.model.transformation.handler;

import io.zeebe.broker.workflow.model.BpmnStep;
import io.zeebe.broker.workflow.model.element.ExecutableActivityElement;
import io.zeebe.broker.workflow.model.element.ExecutableBoundaryEventElement;
import io.zeebe.broker.workflow.model.element.ExecutableMessage;
import io.zeebe.broker.workflow.model.element.ExecutableWorkflow;
import io.zeebe.broker.workflow.model.transformation.ModelElementTransformer;
import io.zeebe.broker.workflow.model.transformation.TransformContext;
import io.zeebe.model.bpmn.instance.BoundaryEvent;
import io.zeebe.model.bpmn.instance.EventDefinition;
import io.zeebe.model.bpmn.instance.Message;
import io.zeebe.model.bpmn.instance.MessageEventDefinition;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

/**
 * A lot of logic can be shared between this and the IntermediateCatchEvent, so eventually find a
 * better way to model this. For a spike this is fine.
 */
public class BoundaryEventHandler implements ModelElementTransformer<BoundaryEvent> {
  @Override
  public Class<BoundaryEvent> getType() {
    return BoundaryEvent.class;
  }

  @Override
  public void transform(BoundaryEvent boundaryEvent, TransformContext context) {
    final ExecutableWorkflow workflow = context.getCurrentWorkflow();
    final ExecutableBoundaryEventElement element =
        workflow.getElementById(boundaryEvent.getId(), ExecutableBoundaryEventElement.class);
    final ExecutableActivityElement attachedToActivity =
        workflow.getElementById(
            boundaryEvent.getAttachedTo().getId(), ExecutableActivityElement.class);

    // transform element
    // some logic is obviously reused from IntermediateCatchEvent, so we should
    // figure out some way to share that
    setupEventDefinition(boundaryEvent, element, context);

    element.setCancelActivity(boundaryEvent.cancelActivity());
    attachedToActivity.addBoundaryEvent(element);
    bindLifecycle(context, element);
  }

  private void bindLifecycle(
      TransformContext context, final ExecutableBoundaryEventElement executableElement) {

    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_READY, BpmnStep.ACTIVATE_ELEMENT);
    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_ACTIVATED, BpmnStep.PROCESS_BOUNDARY_EVENT);
    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_COMPLETING, BpmnStep.COMPLETE_ELEMENT);
    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_COMPLETED, context.getCurrentFlowNodeOutgoingStep());
    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_TERMINATING, BpmnStep.TERMINATE_ELEMENT);
    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_TERMINATED, BpmnStep.PROPAGATE_TERMINATION);
  }

  private void setupEventDefinition(
      BoundaryEvent boundaryEvent,
      ExecutableBoundaryEventElement element,
      TransformContext context) {
    final EventDefinition eventDefinition = boundaryEvent.getEventDefinitions().iterator().next();

    if (eventDefinition instanceof MessageEventDefinition) {
      transformMessageEventDefinition(context, element, (MessageEventDefinition) eventDefinition);
    }
  }

  private void transformMessageEventDefinition(
      TransformContext context,
      final ExecutableBoundaryEventElement executableElement,
      final MessageEventDefinition messageEventDefinition) {

    final Message message = messageEventDefinition.getMessage();
    final ExecutableMessage executableMessage = context.getMessage(message.getId());
    executableElement.setMessage(executableMessage);
  }
}
