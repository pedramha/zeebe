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

import io.zeebe.broker.workflow.state.prototype.VariableStateImpl;
import io.zeebe.util.FileUtil;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Thread)
public class VariableStateContext {

  // TODO: choose value properly
  public static final DirectBuffer VARIABLE_VALUE;

  static {
    final byte[] value = new byte[256];
    new Random().nextBytes(value);
    VARIABLE_VALUE = new UnsafeBuffer(value);
  }

  // longest path through scope tree
  @Param({"1", "3", "10"})
  public int scopeHierarchyDepth;

  //  // how many children per scope
  //  @Param({"1"})
  //  public int scopeHierarchyFanout;

  // how many concurrent trees
  @Param({"1", "10000", "100000"})
  public int scopeTrees;

  @Param({"1", "20"})
  public int numVariables;

  private int iteration;

  private File tempFolder;
  private VariableStateImpl variableState;
  private DirectBuffer[] variableNames;

  @Setup
  public void setUp() throws Exception {

    System.out.println("setting up context");

    createTempFolder();
    initVariableState();
    prepareScopes();
    prepareVariables();
  }

  public int countIteration() {
    return iteration++;
  }

  private void prepareVariables() {
    variableNames = new DirectBuffer[numVariables];

    for (int i = 0; i < numVariables; i++) {
      variableNames[i] = new UnsafeBuffer(("var" + i).getBytes(StandardCharsets.UTF_8));
    }
  }

  private void prepareScopes() {
    long keyGenerator = 0;

    for (int i = 0; i < scopeTrees; i++) {
      for (int j = 0; j < scopeHierarchyDepth - 1; j++) {
        variableState.addScope(keyGenerator, ++keyGenerator);
      }

      keyGenerator++;
    }
  }

  public long getLeafScopeKey(int index) {
    final int treeIndex = index % scopeTrees;
    final int leafKey = scopeHierarchyDepth * treeIndex + (scopeHierarchyDepth - 1);

    return leafKey;
  }

  public long getRootScopeKey(int index) {
    final int treeIndex = index % scopeTrees;
    return treeIndex * scopeHierarchyDepth;
  }

  public DirectBuffer getVariableName(int index) {
    return variableNames[index % variableNames.length];
  }

  private void initVariableState() {
    this.variableState = new VariableStateImpl(tempFolder, 128, VARIABLE_VALUE.capacity());
  }

  private void createTempFolder() throws IOException {
    tempFolder = File.createTempFile("benchmark", "");
    tempFolder.delete();
    tempFolder.mkdir();
  }

  @TearDown
  public void tearDown() throws Exception {
    if (variableState != null) {
      variableState.close();
    }

    if (tempFolder != null && tempFolder.exists()) {
      FileUtil.deleteFolder(tempFolder.getAbsolutePath());
    }
  }

  public VariableStateImpl getVariableState() {
    return variableState;
  }
}
