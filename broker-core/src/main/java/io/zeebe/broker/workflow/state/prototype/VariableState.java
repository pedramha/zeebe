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
package io.zeebe.broker.workflow.state.prototype;

import org.agrona.DirectBuffer;

public interface VariableState {

  /*
   * use cases:
   *   - job creation: create a msgpack document out of all variables visible in task scope
   *   - job completion: iterate msgpack document and set individual variables with propagation behavior (or local, depending on concept)
   *   - input mapping: set a variable on local scope (with JSONpath expressions or not?)
   *   - output mapping: set a variable with propagation semantics (with JSONpath expressions or not?)
   */

  // TODO: in reality, we would return the scope here in which we set the variable, so we can write
  // a follow-up event?!
  void setVariable(long scopeKey, DirectBuffer name, DirectBuffer value);

  void setVariableLocal(long scopeKey, DirectBuffer name, DirectBuffer value);

  //  Iterator<Variable> getVariables(long scopeKey);

  DirectBuffer getVariable(long scopeKey, DirectBuffer name);

  DirectBuffer getVariableLocal(long scopeKey, DirectBuffer name);

  // we will need this in the actual implementation to write the entire local payload of a scope
  //  Iterator<Variable> getVariablesLocal(long scopeKey);

  interface Variable {
    DirectBuffer getName();

    DirectBuffer getValue();
  }
}
