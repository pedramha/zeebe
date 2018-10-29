package io.zeebe.broker.workflow.model.transformation.handler;

import io.zeebe.broker.workflow.model.BpmnStep;
import io.zeebe.broker.workflow.model.element.ExecutableActivityElement;
import io.zeebe.broker.workflow.model.element.ExecutableBoundaryEventElement;
import io.zeebe.broker.workflow.model.element.ExecutableMessage;
import io.zeebe.broker.workflow.model.element.ExecutableWorkflow;
import io.zeebe.broker.workflow.model.transformation.ModelElementTransformer;
import io.zeebe.broker.workflow.model.transformation.TransformContext;
import io.zeebe.model.bpmn.instance.BoundaryEvent;
import io.zeebe.model.bpmn.instance.EventDefinition;
import io.zeebe.model.bpmn.instance.Message;
import io.zeebe.model.bpmn.instance.MessageEventDefinition;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

/**
 * A lot of logic can be shared between this and the IntermediateCatchEvent, so eventually find a
 * better way to model this. For a spike this is fine.
 */
public class BoundaryEventHandler implements ModelElementTransformer<BoundaryEvent> {
  @Override
  public Class<BoundaryEvent> getType() {
    return BoundaryEvent.class;
  }

  @Override
  public void transform(BoundaryEvent boundaryEvent, TransformContext context) {
    final ExecutableWorkflow workflow = context.getCurrentWorkflow();
    final ExecutableBoundaryEventElement element =
        workflow.getElementById(boundaryEvent.getId(), ExecutableBoundaryEventElement.class);
    final ExecutableActivityElement attachedToActivity =
        workflow.getElementById(
            boundaryEvent.getAttachedTo().getId(), ExecutableActivityElement.class);

    // transform element
    // some logic is obviously reused from IntermediateCatchEvent, so we should
    // figure out some way to share that
    setupEventDefinition(boundaryEvent, element, context);

    element.setCancelActivity(boundaryEvent.cancelActivity());
    attachedToActivity.addBoundaryEvent(element);
    bindLifecycle(context, element);
  }

  private void bindLifecycle(
      TransformContext context, final ExecutableBoundaryEventElement executableElement) {

    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_READY, BpmnStep.ACTIVATE_ELEMENT);
    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_ACTIVATED, BpmnStep.PROCESS_BOUNDARY_EVENT);
    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_COMPLETING, BpmnStep.COMPLETE_ELEMENT);
    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_COMPLETED, context.getCurrentFlowNodeOutgoingStep());
    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_TERMINATING, BpmnStep.TERMINATE_ELEMENT);
    executableElement.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_TERMINATED, BpmnStep.PROPAGATE_TERMINATION);
  }

  private void setupEventDefinition(
      BoundaryEvent boundaryEvent,
      ExecutableBoundaryEventElement element,
      TransformContext context) {
    final EventDefinition eventDefinition = boundaryEvent.getEventDefinitions().iterator().next();

    if (eventDefinition instanceof MessageEventDefinition) {
      transformMessageEventDefinition(context, element, (MessageEventDefinition) eventDefinition);
    }
  }

  private void transformMessageEventDefinition(
      TransformContext context,
      final ExecutableBoundaryEventElement executableElement,
      final MessageEventDefinition messageEventDefinition) {

    final Message message = messageEventDefinition.getMessage();
    final ExecutableMessage executableMessage = context.getMessage(message.getId());
    executableElement.setMessage(executableMessage);
  }
}
