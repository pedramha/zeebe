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

import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.workflow.model.element.ExecutableActivityElement;
import io.zeebe.broker.workflow.model.element.ExecutableBoundaryEventElement;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.SideEffectQueue;
import io.zeebe.broker.workflow.processor.message.WorkflowSubscriber;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.broker.workflow.state.WorkflowSubscription;
import java.util.List;

public class BoundaryEventCleaner {
  private final WorkflowSubscriber subscriber;
  private final WorkflowState workflowState;

  public BoundaryEventCleaner(
      SubscriptionCommandSender commandSender, WorkflowState workflowState) {
    this.subscriber = new WorkflowSubscriber(commandSender);
    this.workflowState = workflowState;
  }

  public void clean(
      BpmnStepContext<ExecutableActivityElement> context, SideEffectQueue sideEffectQueue) {
    final List<ExecutableBoundaryEventElement> boundaryEvents =
        context.getElement().getBoundaryEvents();

    for (final ExecutableBoundaryEventElement boundaryEvent : boundaryEvents) {
      if (boundaryEvent.getMessage() != null) {
        unsubscribeFromMessage(context, sideEffectQueue, boundaryEvent);
      } else {
        throw new RuntimeException("Only message boundary events are supported");
      }
    }
  }

  private void unsubscribeFromMessage(
      BpmnStepContext<ExecutableActivityElement> context,
      SideEffectQueue sideEffectQueue,
      ExecutableBoundaryEventElement boundaryEvent) {
    final WorkflowSubscription subscription =
        workflowState.findSubscription(
            context.getValue().getWorkflowInstanceKey(),
            context.getRecord().getKey(),
            boundaryEvent.getId());

    if (subscription != null) {
      subscription.setClosing();
      workflowState.updateCommandSendTime(subscription);
      sideEffectQueue.add(() -> subscriber.closeSubscription(subscription));
    }
  }
}
