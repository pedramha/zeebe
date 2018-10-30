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

import static io.zeebe.logstreams.rocksdb.ZeebeStateConstants.STATE_BYTE_ORDER;
import static io.zeebe.util.buffer.BufferUtil.readIntoBuffer;
import static io.zeebe.util.buffer.BufferUtil.writeIntoBuffer;

import io.zeebe.broker.subscription.message.state.Subscription;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class WorkflowSubscription implements Subscription {

  private static final int STATE_OPENING = 0;
  private static final int STATE_OPENED = 1;
  private static final int STATE_CLOSING = 2;

  private final DirectBuffer messageName = new UnsafeBuffer();
  private final DirectBuffer correlationKey = new UnsafeBuffer();
  private final DirectBuffer handlerActivityId = new UnsafeBuffer();

  private long workflowInstanceKey;
  private long activityInstanceKey;
  private int subscriptionPartitionId;

  private long commandSentTime;

  private int state = STATE_OPENING;

  public WorkflowSubscription() {}

  public WorkflowSubscription(long workflowInstanceKey, long activityInstanceKey) {
    this.workflowInstanceKey = workflowInstanceKey;
    this.activityInstanceKey = activityInstanceKey;
  }

  public WorkflowSubscription(
      long workflowInstanceKey, long activityInstanceKey, DirectBuffer handlerActivityId) {
    this(workflowInstanceKey, activityInstanceKey);
    this.setHandlerActivityId(handlerActivityId);
  }

  public WorkflowSubscription(
      long workflowInstanceKey,
      long activityInstanceKey,
      DirectBuffer messageName,
      DirectBuffer correlationKey) {
    this.workflowInstanceKey = workflowInstanceKey;
    this.activityInstanceKey = activityInstanceKey;

    this.messageName.wrap(messageName);
    this.correlationKey.wrap(correlationKey);
  }

  WorkflowSubscription(
      final String messageName,
      final String correlationKey,
      final long workflowInstanceKey,
      final long activityInstanceKey,
      final long commandSentTime) {
    this(
        workflowInstanceKey,
        activityInstanceKey,
        new UnsafeBuffer(messageName.getBytes()),
        new UnsafeBuffer(correlationKey.getBytes()));

    setCommandSentTime(commandSentTime);
  }

  @Override
  public DirectBuffer getMessageName() {
    return messageName;
  }

  @Override
  public DirectBuffer getCorrelationKey() {
    return correlationKey;
  }

  public long getWorkflowInstanceKey() {
    return workflowInstanceKey;
  }

  public long getActivityInstanceKey() {
    return activityInstanceKey;
  }

  public long getCommandSentTime() {
    return commandSentTime;
  }

  public DirectBuffer getHandlerActivityId() {
    return handlerActivityId;
  }

  public void setHandlerActivityId(final DirectBuffer handlerActivityId) {
    this.handlerActivityId.wrap(handlerActivityId);
  }

  @Override
  public void setCommandSentTime(long commandSentTime) {
    this.commandSentTime = commandSentTime;
  }

  public int getSubscriptionPartitionId() {
    return subscriptionPartitionId;
  }

  public void setSubscriptionPartitionId(int subscriptionPartitionId) {
    this.subscriptionPartitionId = subscriptionPartitionId;
  }

  public boolean isOpening() {
    return state == STATE_OPENING;
  }

  public boolean isClosing() {
    return state == STATE_CLOSING;
  }

  public void setOpened() {
    state = STATE_OPENED;
  }

  public void setClosing() {
    state = STATE_CLOSING;
  }

  @Override
  public void wrap(final DirectBuffer buffer, int offset, final int length) {
    this.workflowInstanceKey = buffer.getLong(offset, STATE_BYTE_ORDER);
    offset += Long.BYTES;

    this.activityInstanceKey = buffer.getLong(offset, STATE_BYTE_ORDER);
    offset += Long.BYTES;

    this.subscriptionPartitionId = buffer.getInt(offset, STATE_BYTE_ORDER);
    offset += Integer.BYTES;

    this.commandSentTime = buffer.getLong(offset, STATE_BYTE_ORDER);
    offset += Long.BYTES;

    this.state = buffer.getInt(offset, STATE_BYTE_ORDER);
    offset += Integer.BYTES;

    offset = readIntoBuffer(buffer, offset, messageName);
    offset = readIntoBuffer(buffer, offset, correlationKey);
    readIntoBuffer(buffer, offset, handlerActivityId);
  }

  @Override
  public int getLength() {
    return Long.BYTES * 3
        + Integer.BYTES * 5
        + messageName.capacity()
        + correlationKey.capacity()
        + handlerActivityId.capacity();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, int offset) {
    buffer.putLong(offset, workflowInstanceKey, STATE_BYTE_ORDER);
    offset += Long.BYTES;

    buffer.putLong(offset, activityInstanceKey, STATE_BYTE_ORDER);
    offset += Long.BYTES;

    buffer.putInt(offset, subscriptionPartitionId, STATE_BYTE_ORDER);
    offset += Integer.BYTES;

    buffer.putLong(offset, commandSentTime, STATE_BYTE_ORDER);
    offset += Long.BYTES;

    buffer.putInt(offset, state, STATE_BYTE_ORDER);
    offset += Integer.BYTES;

    offset = writeIntoBuffer(buffer, offset, messageName);
    offset = writeIntoBuffer(buffer, offset, correlationKey);
    offset = writeIntoBuffer(buffer, offset, handlerActivityId);
    assert offset == getLength()
        : "End offset differs with getLength() (" + offset + " != " + getLength() + ")";
  }

  @Override
  public void writeCommandSentTime(MutableDirectBuffer keyBuffer, int offset) {
    keyBuffer.putLong(offset, commandSentTime, STATE_BYTE_ORDER);
  }

  @Override
  public int getKeyLength() {
    return 2 * Long.BYTES + Integer.BYTES + handlerActivityId.capacity();
  }

  @Override
  public void writeKey(MutableDirectBuffer keyBuffer, int offset) {
    final int startOffset = offset;
    keyBuffer.putLong(offset, workflowInstanceKey, STATE_BYTE_ORDER);
    offset += Long.BYTES;
    keyBuffer.putLong(offset, activityInstanceKey, STATE_BYTE_ORDER);
    offset += Long.BYTES;
    offset = writeIntoBuffer(keyBuffer, offset, handlerActivityId);

    assert (offset - startOffset) == getKeyLength()
        : "Offset problem: offset is not equal to expected key length";
  }
}
