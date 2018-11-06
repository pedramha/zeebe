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

import io.zeebe.broker.workflow.model.element.ExecutableActivity;
import io.zeebe.broker.workflow.model.element.ExecutableBoundaryEvent;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.flownode.ActivateFlowNodeHandler;
import io.zeebe.broker.workflow.state.ElementInstance;
import java.util.List;

public class SetupActivityHandler extends ActivateFlowNodeHandler<ExecutableActivity> {
  @Override
  protected void activate(BpmnStepContext<ExecutableActivity> context) {
    final ElementInstance element = context.getElementInstance();
    final List<ExecutableBoundaryEvent> boundaryEvents = context.getElement().getBoundaryEvents();

    for (final ExecutableBoundaryEvent boundaryEvent : boundaryEvents) {
      if (boundaryEvent.isTimerEvent()) {
        context
            .getCatchEventOutput()
            .subscribeToTimerEvent(element, boundaryEvent, context.getOutput().getBatchWriter());
      }
    }
  }
}
