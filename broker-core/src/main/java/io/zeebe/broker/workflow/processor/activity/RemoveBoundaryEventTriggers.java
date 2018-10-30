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
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.SideEffectQueue;
import io.zeebe.broker.workflow.processor.boundary.BoundaryEventCleaner;
import io.zeebe.broker.workflow.processor.flownode.CompleteElementHandler;
import io.zeebe.broker.workflow.state.WorkflowState;

public class RemoveBoundaryEventTriggers extends CompleteElementHandler<ExecutableActivityElement> {
  private final SideEffectQueue sideEffectQueue = new SideEffectQueue();
  private final BoundaryEventCleaner boundaryEventCleaner;

  public RemoveBoundaryEventTriggers(
      SubscriptionCommandSender subscriptionCommandSender, WorkflowState workflowState) {
    this.boundaryEventCleaner = new BoundaryEventCleaner(subscriptionCommandSender, workflowState);
  }

  @Override
  protected void completeActivity(BpmnStepContext<ExecutableActivityElement> context) {
    super.completeActivity(context);
    boundaryEventCleaner.clean(context, sideEffectQueue);
    context.getSideEffect().accept(sideEffectQueue);
  }
}
