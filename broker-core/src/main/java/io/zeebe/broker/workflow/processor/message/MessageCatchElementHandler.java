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

import io.zeebe.broker.incident.data.ErrorType;
import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.workflow.model.element.ExecutableMessageCatchElement;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.BpmnStepHandler;
import io.zeebe.broker.workflow.processor.message.WorkflowSubscriber.SubscriptionException;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.broker.workflow.state.WorkflowSubscription;

public class MessageCatchElementHandler implements BpmnStepHandler<ExecutableMessageCatchElement> {
  private final WorkflowSubscriber subscriber;
  private final WorkflowState workflowState;

  private WorkflowSubscription subscription;

  public MessageCatchElementHandler(
      final SubscriptionCommandSender subscriptionCommandSender,
      final WorkflowState workflowState) {
    this.subscriber = new WorkflowSubscriber(subscriptionCommandSender);
    this.workflowState = workflowState;
  }

  @Override
  public void handle(final BpmnStepContext<ExecutableMessageCatchElement> context) {
    try {
      subscription =
          subscriber.createSubscription(
              context.getValue(), context.getRecord().getKey(), context.getElement().getMessage());

      context.getSideEffect().accept(this::openMessageSubscription);
      workflowState.put(subscription);
    } catch (SubscriptionException e) {
      context.raiseIncident(ErrorType.EXTRACT_VALUE_ERROR, e.getMessage());
    }
  }

  private boolean openMessageSubscription() {
    return subscriber.openSubscription(subscription);
  }
}
