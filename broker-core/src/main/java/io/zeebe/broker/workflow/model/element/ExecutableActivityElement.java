package io.zeebe.broker.workflow.model.element;

import java.util.ArrayList;
import java.util.List;

public class ExecutableActivityElement extends ExecutableFlowNode {
  private List<ExecutableBoundaryEventElement> boundaryEvents = new ArrayList<>();

  public ExecutableActivityElement(String id) {
    super(id);
  }

  public List<ExecutableBoundaryEventElement> getBoundaryEvents() {
    return boundaryEvents;
  }

  public void addBoundaryEvent(final ExecutableBoundaryEventElement boundaryEvent) {
    boundaryEvents.add(boundaryEvent);
  }
}
