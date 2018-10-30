package io.zeebe.broker.workflow.processor;

import io.zeebe.broker.workflow.model.element.ExecutableFlowElement;
import java.util.LinkedList;
import java.util.Queue;

@SuppressWarnings("unchecked")
public class BpmnStepChain<T extends ExecutableFlowElement> implements BpmnStepHandler<T> {
  private final Queue<BpmnStepHandler<? super T>> handlers = new LinkedList<>();

  @Override
  public void handle(BpmnStepContext<T> context) {
    for (final BpmnStepHandler<? super T> handler : handlers) {
      handler.handle((BpmnStepContext) context);

      if (context.wasIncidentRaised()) {
        context.getSideEffect().clear();
        break;
      }
    }
  }

  public BpmnStepChain<T> andThen(BpmnStepHandler<? super T> handler) {
    handlers.add(handler);
    return this;
  }
}
