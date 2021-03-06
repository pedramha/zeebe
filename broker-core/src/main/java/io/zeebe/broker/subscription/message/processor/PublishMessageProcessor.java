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
package io.zeebe.broker.subscription.message.processor;

import static io.zeebe.util.buffer.BufferUtil.bufferAsString;

import io.zeebe.broker.logstreams.processor.SideEffectProducer;
import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.subscription.message.state.Message;
import io.zeebe.broker.subscription.message.state.MessageState;
import io.zeebe.broker.subscription.message.state.MessageSubscription;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.impl.record.value.message.MessageRecord;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.util.sched.clock.ActorClock;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PublishMessageProcessor implements TypedRecordProcessor<MessageRecord> {

  private final MessageState messageState;
  private final SubscriptionCommandSender commandSender;

  private TypedResponseWriter responseWriter;
  private MessageRecord messageRecord;
  private List<MessageSubscription> matchingSubscriptions;

  public PublishMessageProcessor(
      MessageState messageState, final SubscriptionCommandSender commandSender) {
    this.messageState = messageState;
    this.commandSender = commandSender;
  }

  @Override
  public void processRecord(
      final TypedRecord<MessageRecord> command,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {
    this.responseWriter = responseWriter;
    messageRecord = command.getValue();

    if (messageRecord.hasMessageId()
        && messageState.exist(
            messageRecord.getName(),
            messageRecord.getCorrelationKey(),
            messageRecord.getMessageId())) {
      final String rejectionReason =
          String.format(
              "message with id '%s' is already published",
              bufferAsString(messageRecord.getMessageId()));

      streamWriter.writeRejection(command, RejectionType.BAD_VALUE, rejectionReason);
      responseWriter.writeRejectionOnCommand(command, RejectionType.BAD_VALUE, rejectionReason);

    } else {
      handleNewMessage(command, responseWriter, streamWriter, sideEffect);
    }
  }

  private void handleNewMessage(
      final TypedRecord<MessageRecord> command,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {

    final TypedBatchWriter batchWriter = streamWriter.newBatch();
    final long key = batchWriter.addNewEvent(MessageIntent.PUBLISHED, command.getValue());
    responseWriter.writeEventOnCommand(key, MessageIntent.PUBLISHED, command.getValue(), command);

    // correlate the message only once per workflow instance
    // - will be improved by #1421
    matchingSubscriptions =
        messageState
            .findSubscriptions(messageRecord.getName(), messageRecord.getCorrelationKey())
            .stream()
            .collect(Collectors.groupingBy(MessageSubscription::getWorkflowInstanceKey))
            .values()
            .stream()
            .map(sub -> sub.get(0))
            .collect(Collectors.toList());

    for (final MessageSubscription sub : matchingSubscriptions) {
      sub.setMessagePayload(messageRecord.getPayload());

      messageState.updateCommandSentTime(sub);
    }

    sideEffect.accept(this::correlateMessage);

    if (messageRecord.getTimeToLive() > 0L) {
      final Message message =
          new Message(
              key,
              messageRecord.getName(),
              messageRecord.getCorrelationKey(),
              messageRecord.getPayload(),
              messageRecord.getMessageId(),
              messageRecord.getTimeToLive(),
              messageRecord.getTimeToLive() + ActorClock.currentTimeMillis());
      messageState.put(message);

      for (MessageSubscription sub : matchingSubscriptions) {
        messageState.putMessageCorrelation(message.getKey(), sub.getWorkflowInstanceKey());
      }

    } else {
      // don't add the message to the store to avoid that it can be correlated afterwards
      batchWriter.addFollowUpEvent(key, MessageIntent.DELETED, messageRecord);
    }
  }

  private boolean correlateMessage() {
    for (final MessageSubscription sub : matchingSubscriptions) {
      final boolean success =
          commandSender.correlateWorkflowInstanceSubscription(
              sub.getWorkflowInstanceKey(),
              sub.getElementInstanceKey(),
              messageRecord.getName(),
              messageRecord.getPayload());

      if (!success) {
        // try again later
        return false;
      }
    }

    return responseWriter.flush();
  }
}
