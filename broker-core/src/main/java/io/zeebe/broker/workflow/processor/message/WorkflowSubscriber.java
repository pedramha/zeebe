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

import static io.zeebe.util.buffer.BufferUtil.cloneBuffer;

import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.workflow.model.element.ExecutableMessage;
import io.zeebe.broker.workflow.state.WorkflowSubscription;
import io.zeebe.msgpack.jsonpath.JsonPathQuery;
import io.zeebe.msgpack.query.MsgPackQueryProcessor;
import io.zeebe.msgpack.query.MsgPackQueryProcessor.QueryResult;
import io.zeebe.msgpack.query.MsgPackQueryProcessor.QueryResults;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.util.sched.clock.ActorClock;
import org.agrona.DirectBuffer;

public class WorkflowSubscriber {
  private final MsgPackQueryProcessor queryProcessor = new MsgPackQueryProcessor();
  private final SubscriptionCommandSender subscriptionCommandSender;

  public WorkflowSubscriber(SubscriptionCommandSender subscriptionCommandSender) {
    this.subscriptionCommandSender = subscriptionCommandSender;
  }

  public WorkflowSubscription createSubscription(
      WorkflowInstanceRecord workflowInstance,
      long activityInstanceKey,
      ExecutableMessage message) {
    final DirectBuffer extractedCorrelationKey =
        extractCorrelationKey(message.getCorrelationKey(), workflowInstance.getPayload());
    final WorkflowSubscription subscription =
        new WorkflowSubscription(
            workflowInstance.getWorkflowInstanceKey(),
            activityInstanceKey,
            cloneBuffer(message.getMessageName()),
            cloneBuffer(extractedCorrelationKey));
    subscription.setCommandSentTime(ActorClock.currentTimeMillis());

    return subscription;
  }

  public WorkflowSubscription createSubscription(
      WorkflowInstanceRecord workflowInstance,
      long activityInstanceKey,
      ExecutableMessage message,
      DirectBuffer handlerActivityId) {
    final WorkflowSubscription subscription =
        createSubscription(workflowInstance, activityInstanceKey, message);
    subscription.setHandlerActivityId(handlerActivityId);

    return subscription;
  }

  public boolean openSubscription(WorkflowSubscription subscription) {
    return subscriptionCommandSender.openMessageSubscription(
        subscription.getWorkflowInstanceKey(),
        subscription.getActivityInstanceKey(),
        subscription.getMessageName(),
        subscription.getCorrelationKey());
  }

  public boolean closeSubscription(WorkflowSubscription subscription) {
    return subscriptionCommandSender.closeMessageSubscription(
        subscription.getSubscriptionPartitionId(),
        subscription.getWorkflowInstanceKey(),
        subscription.getActivityInstanceKey());
  }

  private DirectBuffer extractCorrelationKey(JsonPathQuery correlationKey, DirectBuffer payload) {
    final QueryResults results = queryProcessor.process(correlationKey, payload);
    if (results.size() == 1) {
      final QueryResult result = results.getSingleResult();

      if (result.isString()) {
        return result.getString();
      } else if (result.isLong()) {
        return result.getLongAsBuffer();
      } else {
        final String failure =
            String.format(
                "Failed to extract the correlation-key by '%s': wrong type",
                correlationKey.getExpression());
        throw new SubscriptionException(failure);
      }
    } else {
      final String failure =
          String.format(
              "Failed to extract the correlation-key by '%s': no result",
              correlationKey.getExpression());
      throw new SubscriptionException(failure);
    }
  }

  static class SubscriptionException extends RuntimeException {
    private static final long serialVersionUID = -9200092668272882753L;

    SubscriptionException(String message) {
      super(message);
    }
  }
}
