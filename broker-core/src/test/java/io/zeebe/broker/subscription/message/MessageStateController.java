package io.zeebe.broker.subscription.message;

import io.zeebe.logstreams.state.StateController;
import java.io.File;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

public class MessageStateController extends StateController {
  private static final byte[] COLUMN_FAMILY_NAME = "timeToLive".getBytes();

  private final UnsafeBuffer longBuffer = new UnsafeBuffer(0, 0);
  private final UnsafeBuffer iterateKeyBuffer = new UnsafeBuffer(0, 0);

  private ColumnFamilyHandle timeToLiveHandle;
  private ExpandableArrayBuffer keyBuffer;
  private ExpandableArrayBuffer valueBuffer;

  @Override
  public RocksDB open(final File dbDirectory, final boolean reopen) throws Exception {
    final RocksDB rocksDB = super.open(dbDirectory, reopen);
    keyBuffer = new ExpandableArrayBuffer();
    valueBuffer = new ExpandableArrayBuffer();
    timeToLiveHandle = rocksDB.createColumnFamily(new ColumnFamilyDescriptor(COLUMN_FAMILY_NAME));
    return rocksDB;
  }

  public void put(final Message message) {
    final int offset = wrapKey(message.getName(), message.getCorrelationKey());

    message.write(valueBuffer, 0);

    put(keyBuffer.byteArray(), 0, offset, valueBuffer.byteArray(), 0, message.getLength());
    put(timeToLiveHandle, message.getDeadline(), keyBuffer.byteArray(), 0, offset);
  }

  private int wrapKey(final DirectBuffer messageName, final DirectBuffer correlationKey) {
    int offset = 0;
    final int nameLength = messageName.capacity();
    keyBuffer.putBytes(0, messageName, 0, nameLength);
    offset += nameLength;

    final int correlationKeyLength = correlationKey.capacity();
    keyBuffer.putBytes(offset, correlationKey, 0, correlationKeyLength);
    offset += correlationKeyLength;
    return offset;
  }

  public Message findMessage(final DirectBuffer name, final DirectBuffer correlationKey) {
    final int length = wrapKey(name, correlationKey);
    return getMessage(keyBuffer, 0, length);
  }

  private Message getMessage(final DirectBuffer buffer, final int offset, final int length) {
    final int valueBufferSize = valueBuffer.capacity();
    final int readBytes =
        get(buffer.byteArray(), offset, length, valueBuffer.byteArray(), 0, valueBufferSize);

    if (readBytes > valueBufferSize) {
      throw new IllegalStateException("Not enough space in value buffer");
    }

    final Message message = new Message();
    message.wrap(valueBuffer, 0, readBytes);

    return message;
  }

  public List<Message> findMessageBefore(final long timestamp) {
    final List<Message> messageList = new ArrayList<>();
    foreach(
        timeToLiveHandle,
        (key, value) -> {
          longBuffer.wrap(key);
          final long time = longBuffer.getLong(0, ByteOrder.LITTLE_ENDIAN);

          if (time < timestamp) {
            iterateKeyBuffer.wrap(value);
            messageList.add(getMessage(iterateKeyBuffer, 0, value.length));
          }
        });
    return messageList;
  }
}
