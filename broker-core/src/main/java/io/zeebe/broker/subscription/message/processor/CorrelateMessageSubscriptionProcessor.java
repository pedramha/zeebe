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

import io.zeebe.broker.logstreams.processor.SideEffectProducer;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.subscription.message.data.MessageSubscriptionRecord;
import io.zeebe.broker.subscription.message.state.MessageSubscriptionState;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import java.util.function.Consumer;

public class CorrelateMessageSubscriptionProcessor
    implements TypedRecordProcessor<MessageSubscriptionRecord> {

  private final MessageSubscriptionState subscriptionState;

  public CorrelateMessageSubscriptionProcessor(MessageSubscriptionState subscriptionState) {
    this.subscriptionState = subscriptionState;
  }

  @Override
  public void processRecord(
      TypedRecord<MessageSubscriptionRecord> record,
      TypedResponseWriter responseWriter,
      TypedStreamWriter streamWriter,
      Consumer<SideEffectProducer> sideEffect) {

    final MessageSubscriptionRecord subscriptionRecord = record.getValue();

    final boolean removed = subscriptionState.remove(subscriptionRecord.getElementInstanceKey());

    if (removed) {
      streamWriter.writeFollowUpEvent(
          record.getKey(), MessageSubscriptionIntent.CORRELATED, subscriptionRecord);
    } else {
      streamWriter.writeRejection(
          record, RejectionType.NOT_APPLICABLE, "subscription is already correlated");
    }
  }
}
