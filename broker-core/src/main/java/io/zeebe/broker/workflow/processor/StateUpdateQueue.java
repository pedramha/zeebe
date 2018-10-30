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

import io.zeebe.broker.workflow.state.WorkflowState;
import java.util.LinkedList;
import java.util.Queue;

public class StateUpdateQueue {
  private final Queue<StateUpdater> queue = new LinkedList<>();

  public void add(StateUpdater stateUpdater) {
    queue.add(stateUpdater);
  }

  public void applyUpdates(WorkflowState state) {
    StateUpdater stateUpdater;

    while ((stateUpdater = queue.poll()) != null) {
      stateUpdater.update(state);
    }
  }

  @FunctionalInterface
  public interface StateUpdater {
    void update(WorkflowState state);
  }
}
