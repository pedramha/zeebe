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
package io.zeebe.broker.workflow.state;

import io.zeebe.logstreams.rocksdb.ZeebeStateConstants;
import io.zeebe.logstreams.state.StateController;
import io.zeebe.msgpack.spec.MsgPackReader;
import io.zeebe.msgpack.spec.MsgPackToken;
import io.zeebe.msgpack.spec.MsgPackWriter;
import io.zeebe.util.collection.Tuple;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ObjectHashSet;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;

public class VariablesState {

  private final MsgPackReader reader = new MsgPackReader();
  private final MsgPackWriter writer = new MsgPackWriter();
  private final DirectBuffer variableNameBuffer = new UnsafeBuffer(0, 0);
  private final ExpandableArrayBuffer documentResultBuffer = new ExpandableArrayBuffer();
  private final DirectBuffer documentResultView = new UnsafeBuffer(0, 0);

  private final StateController stateController;

  // (child scope key) => (parent scope key)
  private final ColumnFamilyHandle elementChildParentHandle;
  // (scope key, variable name) => (variable value)
  private final ColumnFamilyHandle variablesHandle;

  private final ExpandableArrayBuffer stateKeyBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer stateValueBuffer = new ExpandableArrayBuffer();

  private final LocalVariableIterator localVariableIterator = new LocalVariableIterator();

  // collecting variables
  private ObjectHashSet<DirectBuffer> collectedVariables = new ObjectHashSet<>();
  private ObjectHashSet<DirectBuffer> variablesToCollect = new ObjectHashSet<>();

  public VariablesState(
      StateController stateController,
      ColumnFamilyHandle elementChildParentHandle,
      ColumnFamilyHandle variablesHandle) {
    this.stateController = stateController;
    this.elementChildParentHandle = elementChildParentHandle;
    this.variablesHandle = variablesHandle;
  }

  // TODO: maybe the document processing logic can be extracted into the msgpack utilities

  public void setVariablesLocalFromDocument(long scopeKey, DirectBuffer document) {
    reader.wrap(document, 0, document.capacity());

    final int variables = reader.readMapHeader();

    for (int i = 0; i < variables; i++) {
      final MsgPackToken variableName = reader.readToken();
      variableNameBuffer.wrap(variableName.getValueBuffer());

      final MsgPackToken variableValue = reader.readToken();
      setVariableLocal(scopeKey, variableNameBuffer, variableValue.getValueBuffer());
    }
  }

  public void setVariableLocal(long scopeKey, DirectBuffer name, DirectBuffer value) {
    int keyOffset = 0;
    stateKeyBuffer.putLong(keyOffset, scopeKey, ZeebeStateConstants.STATE_BYTE_ORDER);
    keyOffset += BitUtil.SIZE_OF_LONG;
    stateKeyBuffer.putBytes(keyOffset, name, 0, name.capacity());

    final int keyLength = keyOffset + name.capacity();

    // TODO: can we avoid the extra copy if value is already byte-array based?
    stateValueBuffer.putBytes(0, value, 0, value.capacity());

    stateController.put(
        variablesHandle,
        stateKeyBuffer.byteArray(),
        0,
        keyLength,
        stateValueBuffer.byteArray(),
        0,
        value.capacity());
  }

  private long getParent(long scopeKey) {
    // TODO: centralize offsets
    stateKeyBuffer.putLong(0, scopeKey, ZeebeStateConstants.STATE_BYTE_ORDER);
    final int bytesRead =
        stateController.get(
            elementChildParentHandle,
            stateKeyBuffer.byteArray(),
            0,
            BitUtil.SIZE_OF_LONG,
            stateValueBuffer.byteArray(),
            0,
            BitUtil.SIZE_OF_LONG);

    return bytesRead >= 0 ? stateValueBuffer.getLong(0, ZeebeStateConstants.STATE_BYTE_ORDER) : -1;
  }

  public DirectBuffer getVariablesAsDocument(long scopeKey) {

    collectedVariables.clear();
    writer.wrap(documentResultBuffer, 0);

    writer.reserveMapHeader();

    consumeVariables(
        scopeKey,
        name -> !collectedVariables.contains(name),
        (name, value) -> {
          writer.writeString(name);
          writer.writeRaw(value);

          // must copy the name, because we keep them all in the hashset at the same time
          final MutableDirectBuffer nameCopy = new UnsafeBuffer(new byte[name.capacity()]);
          nameCopy.putBytes(0, name, 0, name.capacity());
          collectedVariables.add(nameCopy);
        },
        () -> false);

    writer.writeReservedMapHeader(0, collectedVariables.size());

    documentResultView.wrap(documentResultBuffer, 0, writer.getOffset());
    return documentResultView;
  }

  private void consumeVariables(
      long scopeKey,
      Predicate<DirectBuffer> filter,
      BiConsumer<DirectBuffer, DirectBuffer> variableConsumer,
      BooleanSupplier completionCondition) {
    long currentScope = scopeKey;

    boolean completed;
    do {
      completed =
          consumeVariablesLocal(currentScope, filter, variableConsumer, completionCondition);

      currentScope = getParent(currentScope);

    } while (!completed && currentScope >= 0);
  }

  // TODO: document parameters
  private boolean consumeVariablesLocal(
      long scopeKey,
      Predicate<DirectBuffer> filter,
      BiConsumer<DirectBuffer, DirectBuffer> variableConsumer,
      BooleanSupplier completionCondition) {
    localVariableIterator.init(scopeKey);

    while (localVariableIterator.hasNext()) {
      final Tuple<DirectBuffer, DirectBuffer> currentVariable = localVariableIterator.next();

      if (filter.test(currentVariable.getLeft())) {
        variableConsumer.accept(currentVariable.getLeft(), currentVariable.getRight());

        if (completionCondition.getAsBoolean()) {
          return true;
        }
      }
    }

    localVariableIterator.close();

    return false;
  }

  // TODO: what should the second argument be? Could even be a predicate?!
  public DirectBuffer getVariablesAsDocument(
      long scopeKey, Collection<DirectBuffer> variableNames) {
    return null;
  }

  private class LocalVariableIterator implements Iterator<Tuple<DirectBuffer, DirectBuffer>> {
    private final MutableDirectBuffer scopeKeyBuffer =
        new UnsafeBuffer(new byte[BitUtil.SIZE_OF_LONG]);
    private long scopeKey;
    private RocksIterator rocksDbIterator;

    private final MutableDirectBuffer currentScopeKeyBuffer = new UnsafeBuffer(0, 0);

    private UnsafeBuffer nameView = new UnsafeBuffer(0, 0);
    private UnsafeBuffer valueView = new UnsafeBuffer(0, 0);
    private Tuple<DirectBuffer, DirectBuffer> value = new Tuple<>(nameView, valueView);

    private void init(long scopeKey) {
      this.scopeKey = scopeKey;
      scopeKeyBuffer.putLong(0, scopeKey, ZeebeStateConstants.STATE_BYTE_ORDER);

      rocksDbIterator = stateController.getDb().newIterator(variablesHandle);
      rocksDbIterator.seek(scopeKeyBuffer.byteArray());
    }

    @Override
    public boolean hasNext() {
      if (rocksDbIterator.isValid()) {
        final byte[] currentKey = rocksDbIterator.key();
        currentScopeKeyBuffer.wrap(currentKey);

        return currentScopeKeyBuffer.getLong(0, ZeebeStateConstants.STATE_BYTE_ORDER) == scopeKey;
      } else {
        return false;
      }
    }

    @Override
    public Tuple<DirectBuffer, DirectBuffer> next() {

      final byte[] currentKey = rocksDbIterator.key();
      final byte[] currentValue = rocksDbIterator.value();

      // TODO: centralize offsets

      // Must make copies because #next invalidates the current key and value;
      // This could probably be avoided if RocksIteratorInterface exposed a #hasNext method, in
      // which case
      // we wouldn't have to move to the next element already here
      final int nameLength = currentKey.length - BitUtil.SIZE_OF_LONG;
      stateKeyBuffer.putBytes(0, currentKey, BitUtil.SIZE_OF_LONG, nameLength);
      nameView.wrap(stateKeyBuffer, 0, nameLength);

      stateValueBuffer.putBytes(0, currentValue);
      valueView.wrap(stateValueBuffer, 0, currentValue.length);

      if (rocksDbIterator.isValid()) {
        rocksDbIterator.next();
      }

      return value;
    }

    private void close() {
      rocksDbIterator.close();
      rocksDbIterator = null;
    }
  }
}
