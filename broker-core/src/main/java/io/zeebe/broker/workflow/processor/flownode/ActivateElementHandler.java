package io.zeebe.broker.workflow.processor.flownode;

import io.zeebe.broker.incident.data.ErrorType;
import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.BpmnStepHandler;
import io.zeebe.msgpack.mapping.Mapping;
import io.zeebe.msgpack.mapping.MappingException;
import io.zeebe.msgpack.mapping.MsgPackMergeTool;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import org.agrona.DirectBuffer;

public class ActivateElementHandler<T extends ExecutableFlowNode> implements BpmnStepHandler<T> {
  @Override
  public void handle(BpmnStepContext<T> context) {
    final MappingException mappingException = applyInputMapping(context);

    try {
      activateElement(context);
    } catch (RuntimeException e) {
      context.raiseIncident(ErrorType.UNKNOWN, e.getMessage());
    }

    if (mappingException == null) {
      context
          .getOutput()
          .writeFollowUpEvent(
              context.getRecord().getKey(),
              WorkflowInstanceIntent.ELEMENT_ACTIVATED,
              context.getValue());
    } else {
      context.raiseIncident(ErrorType.IO_MAPPING_ERROR, mappingException.getMessage());
    }
  }

  protected void activateElement(BpmnStepContext<T> context) {}

  private MappingException applyInputMapping(BpmnStepContext<T> context) {
    final MsgPackMergeTool payloadMergeTool = context.getMergeTool();
    final WorkflowInstanceRecord activityEvent = context.getValue();
    final DirectBuffer sourcePayload = activityEvent.getPayload();

    final Mapping[] inputMappings = context.getElement().getInputMappings();

    MappingException mappingException = null;

    // only if we have no default mapping we have to use the mapping processor
    if (inputMappings.length > 0) {
      try {
        payloadMergeTool.reset();
        payloadMergeTool.mergeDocumentStrictly(sourcePayload, inputMappings);

        final DirectBuffer result = payloadMergeTool.writeResultToBuffer();
        activityEvent.setPayload(result);

      } catch (MappingException e) {
        mappingException = e;
      }
    }

    return mappingException;
  }
}
