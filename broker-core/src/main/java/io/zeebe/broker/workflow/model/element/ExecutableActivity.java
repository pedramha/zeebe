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
package io.zeebe.broker.workflow.model.element;

import java.util.ArrayList;
import java.util.List;

public class ExecutableActivity extends ExecutableFlowNode {
  private List<ExecutableBoundaryEvent> boundaryEvents = new ArrayList<>();

  public ExecutableActivity(String id) {
    super(id);
  }

  public List<ExecutableBoundaryEvent> getBoundaryEvents() {
    return boundaryEvents;
  }

  public void attach(ExecutableBoundaryEvent boundaryEvent) {
    boundaryEvents.add(boundaryEvent);
  }
}
