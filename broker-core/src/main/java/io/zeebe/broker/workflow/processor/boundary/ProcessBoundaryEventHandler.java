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
package io.zeebe.broker.workflow.processor.boundary;

import io.zeebe.broker.workflow.model.element.ExecutableBoundaryEventElement;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.BpmnStepHandler;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class ProcessBoundaryEventHandler
    implements BpmnStepHandler<ExecutableBoundaryEventElement> {
  private final WorkflowState workflowState;

  public ProcessBoundaryEventHandler(WorkflowState workflowState) {
    this.workflowState = workflowState;
  }

  @Override
  public void handle(BpmnStepContext<ExecutableBoundaryEventElement> context) {
    final ExecutableBoundaryEventElement boundaryEvent = context.getElement();

    if (boundaryEvent.cancelActivity()) {
      context.getOutput().newBatch();
      cancelActivity(context);
    }

    context
        .getOutput()
        .writeFollowUpEvent(
            context.getRecord().getKey(),
            WorkflowInstanceIntent.ELEMENT_COMPLETING,
            context.getValue());
  }

  private void cancelActivity(BpmnStepContext<ExecutableBoundaryEventElement> context) {
    final ElementInstance element = context.getElementInstance();
    final ElementInstance attachedTo =
        workflowState.getElementInstanceState().getInstance(element.getAttachedToKey());

    context
        .getOutput()
        .writeFollowUpEvent(
            attachedTo.getKey(), WorkflowInstanceIntent.ELEMENT_TERMINATING, attachedTo.getValue());
  }
}
