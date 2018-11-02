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

import io.zeebe.logstreams.state.StateController;
import io.zeebe.util.buffer.BufferUtil;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

/**
 * Implements the "naive" approach
 *
 * <p>ColumnFamily: (scope key, variable name) => (variable value)
 */
public class VariableStateImpl implements VariableState, ScopeState, AutoCloseable {

  // TODO: eine Idee für Optimierung:
  // parent-child-Beziehungen so gestalten, dass die Einträge einer Workflowinstanz hintereinander
  // in der RocksDB liegen

  // child scope key => parent scope key
  private static final byte[] CHILD_PARENT_FAMILY = "elementChildParent".getBytes();
  // (scope key, variable name) => (variable value)
  private static final byte[] VARIABLE_VALUE_FAMILY = "values".getBytes();

  public static final byte[][] NAMES = {
    CHILD_PARENT_FAMILY, VARIABLE_VALUE_FAMILY,
  };

  private final ColumnFamilyHandle childParentHandle;
  private final ColumnFamilyHandle valuesHandle;
  private final StateController stateController;

  private final MutableDirectBuffer keyBuffer;
  private final MutableDirectBuffer valueBuffer;
  private final MutableDirectBuffer valueView = new UnsafeBuffer(0, 0);

  /*
   * Second/third parameter is just to ease prototype implementation; is not required for actual implementation
   */
  public VariableStateImpl(
      final File dbDirectory, final int maxNameLength, final int maxValueLength) {
    final List<byte[]> columnFamilyNames =
        Stream.of(NAMES).flatMap(Stream::of).collect(Collectors.toList());

    stateController = new StateController();
    try {
      final RocksDB rocksDB = stateController.open(dbDirectory, false, columnFamilyNames);
      // TODO: what is the RocksDB object for?
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    childParentHandle = stateController.getColumnFamilyHandle(CHILD_PARENT_FAMILY);
    valuesHandle = stateController.getColumnFamilyHandle(VARIABLE_VALUE_FAMILY);

    valueBuffer = new UnsafeBuffer(new byte[maxValueLength]);
    keyBuffer = new UnsafeBuffer(new byte[BitUtil.SIZE_OF_LONG + maxNameLength]);
  }

  @Override
  public void close() throws Exception {
    stateController.close();
  }

  @Override
  public void setVariable(long scopeKey, DirectBuffer name, DirectBuffer value) {
    long currentScope = scopeKey;

    while (currentScope >= 0) {
      // TODO: this is not optimal yet => duplicate access and copying the previous value
      // unnecessarily

      if (getVariableLocal(currentScope, name) != null) {
        setVariableLocal(currentScope, name, value);
        return;
      } else {
        final long parentScope = getParent(currentScope);
        if (parentScope < 0) {
          setVariableLocal(currentScope, name, value);
          return;
        } else {
          currentScope = parentScope;
        }
      }
    }
  }

  @Override
  public void setVariableLocal(long scopeKey, DirectBuffer name, DirectBuffer value) {

    keyBuffer.putLong(0, scopeKey);
    keyBuffer.putBytes(BitUtil.SIZE_OF_LONG, name, 0, name.capacity());

    final int keyLength = BitUtil.SIZE_OF_LONG + name.capacity();

    valueBuffer.putBytes(0, value, 0, value.capacity());
    final int valueLength = value.capacity();

    stateController.put(
        valuesHandle, keyBuffer.byteArray(), 0, keyLength, valueBuffer.byteArray(), 0, valueLength);
  }

  @Override
  public DirectBuffer getVariable(long scopeKey, DirectBuffer name) {
    DirectBuffer variableValue = null;
    long currentScope = scopeKey;

    while ((variableValue = getVariableLocal(currentScope, name)) == null
        && (currentScope = getParent(currentScope)) >= 0) {}

    return variableValue;
  }

  @Override
  public DirectBuffer getVariableLocal(long scopeKey, DirectBuffer name) {

    keyBuffer.putLong(0, scopeKey);
    keyBuffer.putBytes(BitUtil.SIZE_OF_LONG, name, 0, name.capacity());

    final int keyLength = BitUtil.SIZE_OF_LONG + name.capacity();

    final int valueLength =
        stateController.get(
            valuesHandle,
            keyBuffer.byteArray(),
            0,
            keyLength,
            valueBuffer.byteArray(),
            0,
            valueBuffer.capacity());

    if (valueLength >= 0) {
      valueView.wrap(valueBuffer, 0, valueLength);
      return valueView;
    } else {
      return null;
    }
  }

  @Override
  public void addScope(long parentKey, long key) {

    valueBuffer.putLong(0, parentKey);

    stateController.put(childParentHandle, key, valueBuffer.byteArray(), 0, BitUtil.SIZE_OF_LONG);
  }

  // convenience methods for testing
  public void setVariable(long scopeKey, String name, String value) {
    setVariable(scopeKey, BufferUtil.wrapString(name), BufferUtil.wrapString(value));
  }

  public void setVariableLocal(long scopeKey, String name, String value) {
    setVariableLocal(scopeKey, BufferUtil.wrapString(name), BufferUtil.wrapString(value));
  }

  public String getVariable(long scopeKey, String name) {
    final DirectBuffer value = getVariable(scopeKey, BufferUtil.wrapString(name));
    return value != null ? BufferUtil.bufferAsString(value) : null;
  }

  public String getVariableLocal(long scopeKey, String name) {
    final DirectBuffer value = getVariableLocal(scopeKey, BufferUtil.wrapString(name));
    return value != null ? BufferUtil.bufferAsString(value) : null;
  }

  private long getParent(long key) {
    final int valueLength =
        stateController.get(
            childParentHandle, key, valueBuffer.byteArray(), 0, BitUtil.SIZE_OF_LONG);

    if (valueLength >= 0) {
      return valueBuffer.getLong(0);
    } else {
      return -1;
    }
  }
}
