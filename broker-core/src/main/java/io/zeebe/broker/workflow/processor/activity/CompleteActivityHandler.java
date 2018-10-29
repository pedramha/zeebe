package io.zeebe.broker.workflow.processor.activity;

import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.workflow.model.element.ExecutableActivityElement;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.flownode.CompleteElementHandler;
import io.zeebe.broker.workflow.processor.message.WorkflowSubscriber;

public class CompleteActivityHandler extends CompleteElementHandler<ExecutableActivityElement> {
  private final WorkflowSubscriber subscriber;

  public CompleteActivityHandler(SubscriptionCommandSender subscriptionCommandSender) {
    this.subscriber = new WorkflowSubscriber(subscriptionCommandSender);
  }

  @Override
  protected void completeActivity(BpmnStepContext<ExecutableActivityElement> context) {
    super.completeActivity(context);
    unsubscribeBoundaryEvents(context);
  }

  public void unsubscribeBoundaryEvents(BpmnStepContext<ExecutableActivityElement> context) {}
}
