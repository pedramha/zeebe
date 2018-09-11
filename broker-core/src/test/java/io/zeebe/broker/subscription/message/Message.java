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
package io.zeebe.broker.subscription.message;

import io.zeebe.util.buffer.BufferReader;
import io.zeebe.util.buffer.BufferWriter;
import io.zeebe.util.sched.clock.ActorClock;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class Message implements BufferWriter, BufferReader {

  private final DirectBuffer name = new UnsafeBuffer();
  private final DirectBuffer correlationKey = new UnsafeBuffer();
  private final DirectBuffer payload = new UnsafeBuffer();
  private final DirectBuffer id = new UnsafeBuffer();
  private long timeToLive;
  private long deadline;
  private long key;

  public Message() {}

  Message(
      final String name, final String correlationKey, final String payload, final long timeToLive) {
    this.name.wrap(name.getBytes());
    this.correlationKey.wrap(correlationKey.getBytes());
    this.payload.wrap(payload.getBytes());
    this.timeToLive = timeToLive;
    this.deadline = ActorClock.currentTimeMillis() + timeToLive;
  }

  Message(
      final String id,
      final String name,
      final String correlationKey,
      final String payload,
      final long timeToLive) {
    this.name.wrap(name.getBytes());
    this.correlationKey.wrap(correlationKey.getBytes());
    this.id.wrap(id.getBytes());
    this.payload.wrap(payload.getBytes());
    this.timeToLive = timeToLive;
    this.deadline = ActorClock.currentTimeMillis() + timeToLive;
  }

  public DirectBuffer getName() {
    return name;
  }

  public DirectBuffer getCorrelationKey() {
    return correlationKey;
  }

  public DirectBuffer getPayload() {
    return payload;
  }

  public DirectBuffer getId() {
    return id;
  }

  public long getTimeToLive() {
    return timeToLive;
  }

  public long getDeadline() {
    return deadline;
  }

  public long getKey() {
    return key;
  }

  @Override
  public void wrap(final DirectBuffer buffer, int offset, final int length) {
    offset = readIntoBuffer(buffer, offset, name);
    offset = readIntoBuffer(buffer, offset, correlationKey);
    offset = readIntoBuffer(buffer, offset, payload);
    offset = readIntoBuffer(buffer, offset, id);

    timeToLive = buffer.getLong(offset, ByteOrder.LITTLE_ENDIAN);
    offset += Long.BYTES;
    deadline = buffer.getLong(offset, ByteOrder.LITTLE_ENDIAN);
    offset += Long.BYTES;
    key = buffer.getLong(offset, ByteOrder.LITTLE_ENDIAN);
  }

  static int readIntoBuffer(final DirectBuffer buffer, int offset, final DirectBuffer valueBuffer) {
    final int length = buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);
    offset += Integer.BYTES;

    final byte[] bytes = new byte[length];
    valueBuffer.wrap(bytes);
    buffer.getBytes(offset, bytes, 0, length);
    offset += length;
    return offset;
  }

  static int writeIntoBuffer(
      final MutableDirectBuffer writeBuffer, int offset, final DirectBuffer valueBuffer) {
    final int valueLength = valueBuffer.capacity();
    writeBuffer.putInt(offset, valueLength, ByteOrder.LITTLE_ENDIAN);
    offset += Integer.BYTES;

    writeBuffer.putBytes(offset, valueBuffer, 0, valueLength);
    offset += valueLength;
    return offset;
  }

  @Override
  public int getLength() {
    return name.capacity()
        + correlationKey.capacity()
        + payload.capacity()
        + id.capacity()
        + Integer.BYTES * 4
        + Long.BYTES * 3;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, int offset) {
    offset = writeIntoBuffer(buffer, offset, name);
    offset = writeIntoBuffer(buffer, offset, correlationKey);
    offset = writeIntoBuffer(buffer, offset, payload);
    offset = writeIntoBuffer(buffer, offset, id);

    buffer.putLong(offset, timeToLive, ByteOrder.LITTLE_ENDIAN);
    offset += Long.BYTES;
    buffer.putLong(offset, deadline, ByteOrder.LITTLE_ENDIAN);
    offset += Long.BYTES;
    buffer.putLong(offset, key, ByteOrder.LITTLE_ENDIAN);
    offset += Long.BYTES;
    assert offset == getLength() : "End offset differs with getLength()";
  }
}
