package io.zeebe.broker.workflow.processor.activity;

import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.flownode.TerminateElementHandler;

public class TerminateActivityHandler extends TerminateElementHandler {

  @Override
  protected void addTerminatingRecords(
      BpmnStepContext<ExecutableFlowNode> context, TypedBatchWriter batch) {
    // cancel opened subscriptions
  }
}
