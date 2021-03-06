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
package io.zeebe.broker.workflow.processor.message;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.subscription.message.data.WorkflowInstanceSubscriptionRecord;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.broker.workflow.state.WorkflowSubscription;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.intent.WorkflowInstanceSubscriptionIntent;

public class OpenWorkflowInstanceSubscriptionProcessor
    implements TypedRecordProcessor<WorkflowInstanceSubscriptionRecord> {

  private final WorkflowState workflowState;

  public OpenWorkflowInstanceSubscriptionProcessor(WorkflowState workflowState) {
    this.workflowState = workflowState;
  }

  @Override
  public void processRecord(
      TypedRecord<WorkflowInstanceSubscriptionRecord> record,
      TypedResponseWriter responseWriter,
      TypedStreamWriter streamWriter) {

    final WorkflowInstanceSubscriptionRecord subscriptionRecord = record.getValue();

    final WorkflowSubscription subscription = workflowState.findSubscription(subscriptionRecord);
    if (subscription != null && subscription.isOpening()) {
      subscription.setOpened();
      subscription.setSubscriptionPartitionId(subscriptionRecord.getSubscriptionPartitionId());
      workflowState.put(subscription);

      streamWriter.writeFollowUpEvent(
          record.getKey(), WorkflowInstanceSubscriptionIntent.OPENED, subscriptionRecord);

    } else {
      streamWriter.writeRejection(
          record, RejectionType.NOT_APPLICABLE, "subscription is already open");
    }
  }
}
