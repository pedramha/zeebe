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
package io.zeebe.broker.workflow.processor.activity;

import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.workflow.model.element.ExecutableActivityElement;
import io.zeebe.broker.workflow.model.element.ExecutableBoundaryEventElement;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.SideEffectQueue;
import io.zeebe.broker.workflow.processor.flownode.ActivateElementHandler;
import io.zeebe.broker.workflow.processor.message.WorkflowSubscriber;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.broker.workflow.state.WorkflowSubscription;
import java.util.ArrayList;
import java.util.List;

public class SetupActivityHandler extends ActivateElementHandler<ExecutableActivityElement> {
  private final SideEffectQueue sideEffectQueue = new SideEffectQueue();
  private final WorkflowSubscriber subscriber;
  private final WorkflowState workflowState;

  public SetupActivityHandler(
      SubscriptionCommandSender subscriptionCommandSender, WorkflowState workflowState) {
    this.subscriber = new WorkflowSubscriber(subscriptionCommandSender);
    this.workflowState = workflowState;
  }

  @Override
  protected void activateElement(BpmnStepContext<ExecutableActivityElement> context) {
    super.activateElement(context);
    subscribeToBoundaryEvents(context);
    context.getSideEffect().accept(sideEffectQueue);
  }

  private void subscribeToBoundaryEvents(BpmnStepContext<ExecutableActivityElement> context) {
    final List<ExecutableBoundaryEventElement> boundaryEvents =
        context.getElement().getBoundaryEvents();
    final List<WorkflowSubscription> messageSubscriptions = new ArrayList<>();

    for (final ExecutableBoundaryEventElement boundaryEvent : boundaryEvents) {
      if (boundaryEvent.getMessage() != null) {
        messageSubscriptions.add(
            subscriber.createSubscription(
                context.getValue(),
                context.getRecord().getKey(),
                boundaryEvent.getMessage(),
                boundaryEvent.getId()));
      } else {
        throw new RuntimeException("Only message boundary events are supported");
      }
    }

    // modify state once no exceptions might be triggered
    for (final WorkflowSubscription subscription : messageSubscriptions) {
      workflowState.put(subscription);
      sideEffectQueue.add(() -> subscriber.openSubscription(subscription));
    }
  }
}
