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
package io.zeebe.broker.workflow.model.transformation;

import io.zeebe.broker.workflow.model.BpmnStep;
import io.zeebe.broker.workflow.model.element.ExecutableMessage;
import io.zeebe.broker.workflow.model.element.ExecutableWorkflow;
import io.zeebe.msgpack.jsonpath.JsonPathQueryCompiler;
import io.zeebe.util.buffer.BufferUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agrona.DirectBuffer;

public class TransformContext {

  private Map<DirectBuffer, ExecutableWorkflow> workflows = new HashMap<>();
  private Map<DirectBuffer, ExecutableMessage> messages = new HashMap<>();
  private JsonPathQueryCompiler jsonPathQueryCompiler;
  private MappingCompiler mappingCompiler = new MappingCompiler();

  /*
   * set whenever parsing a workflow
   */
  private ExecutableWorkflow currentWorkflow;

  /*
   * set whenever parsing a flow node
   */
  private BpmnStep currentFlowNodeOutgoingStep;

  public ExecutableWorkflow getCurrentWorkflow() {
    return currentWorkflow;
  }

  public void addWorkflow(ExecutableWorkflow workflow) {
    workflows.put(workflow.getId(), workflow);
  }

  public void setCurrentWorkflow(ExecutableWorkflow currentWorkflow) {
    this.currentWorkflow = currentWorkflow;
  }

  public ExecutableWorkflow getWorkflow(String id) {
    return workflows.get(BufferUtil.wrapString(id));
  }

  public List<ExecutableWorkflow> getWorkflows() {
    return new ArrayList<>(workflows.values());
  }

  public void addMessage(ExecutableMessage message) {
    messages.put(message.getId(), message);
  }

  public ExecutableMessage getMessage(String id) {
    return messages.get(BufferUtil.wrapString(id));
  }

  public JsonPathQueryCompiler getJsonPathQueryCompiler() {
    return jsonPathQueryCompiler;
  }

  public void setJsonPathQueryCompiler(JsonPathQueryCompiler jsonPathQueryCompiler) {
    this.jsonPathQueryCompiler = jsonPathQueryCompiler;
  }

  public BpmnStep getCurrentFlowNodeOutgoingStep() {
    return currentFlowNodeOutgoingStep;
  }

  public void setCurrentFlowNodeOutgoingStep(BpmnStep outgoingStep) {
    this.currentFlowNodeOutgoingStep = outgoingStep;
  }

  public MappingCompiler getMappingCompiler() {
    return mappingCompiler;
  }
}
