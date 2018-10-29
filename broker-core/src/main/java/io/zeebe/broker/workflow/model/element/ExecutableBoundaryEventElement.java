package io.zeebe.broker.workflow.model.element;

public class ExecutableBoundaryEventElement extends ExecutableIntermediateCatchElement {
  private ExecutableActivityElement attachedToActivity;
  private boolean cancelActivity;

  public ExecutableBoundaryEventElement(String id) {
    super(id);
  }

  public ExecutableActivityElement getAttachedToActivity() {
    return attachedToActivity;
  }

  public void setAttachedToActivity(ExecutableActivityElement attachedToActivity) {
    this.attachedToActivity = attachedToActivity;
  }

  public boolean cancelActivity() {
    return cancelActivity;
  }

  public void setCancelActivity(boolean cancelActivity) {
    this.cancelActivity = cancelActivity;
  }
}
