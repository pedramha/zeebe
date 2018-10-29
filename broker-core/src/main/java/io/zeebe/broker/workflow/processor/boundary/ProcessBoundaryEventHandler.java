package io.zeebe.broker.workflow.processor.boundary;

import io.zeebe.broker.workflow.model.element.ExecutableBoundaryEventElement;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.BpmnStepHandler;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class ProcessBoundaryEventHandler
    implements BpmnStepHandler<ExecutableBoundaryEventElement> {
  private final WorkflowState workflowState;

  public ProcessBoundaryEventHandler(WorkflowState workflowState) {
    this.workflowState = workflowState;
  }

  @Override
  public void handle(BpmnStepContext<ExecutableBoundaryEventElement> context) {
    final ExecutableBoundaryEventElement boundaryEvent = context.getElement();

    if (boundaryEvent.cancelActivity()) {
      context.getOutput().newBatch();
      cancelActivity(context);
    }

    context
        .getOutput()
        .writeFollowUpEvent(
            context.getRecord().getKey(),
            WorkflowInstanceIntent.ELEMENT_COMPLETING,
            context.getValue());
  }

  private void cancelActivity(BpmnStepContext<ExecutableBoundaryEventElement> context) {
    final ElementInstance element = context.getElementInstance();
    final ElementInstance attachedTo =
        workflowState.getElementInstanceState().getInstance(element.getAttachedToKey());

    context
        .getOutput()
        .writeFollowUpEvent(
            attachedTo.getKey(), WorkflowInstanceIntent.ELEMENT_TERMINATING, attachedTo.getValue());
  }
}
