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
package io.zeebe.broker.benchmarks.state.prototype;

import org.agrona.DirectBuffer;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@State(Scope.Thread)
public class GetBenchmarkPreparation extends VariableStateContext {

  @Override
  public void setUp() throws Exception {
    super.setUp();

    System.out.println("setting up get context");

    for (int scopeTree = 0; scopeTree < scopeTrees; scopeTree++) {
      final long rootScopeKey = getRootScopeKey(scopeTree);
      for (int variable = 0; variable < numVariables; variable++) {
        final DirectBuffer variableName = getVariableName(variable);
        getVariableState()
            .setVariableLocal(rootScopeKey, variableName, VariableStateContext.VARIABLE_VALUE);
      }
    }
  }
}
