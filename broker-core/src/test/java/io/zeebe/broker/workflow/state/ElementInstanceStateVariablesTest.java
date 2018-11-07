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
package io.zeebe.broker.workflow.state;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.test.util.MsgPackUtil;
import io.zeebe.util.buffer.BufferUtil;
import org.agrona.DirectBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ElementInstanceStateVariablesTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Rule public AutoCloseableRule closeables = new AutoCloseableRule();

  private ElementInstanceState elementInstanceState;
  private VariablesState variablesState;

  @Before
  public void setUp() throws Exception {
    final WorkflowState workflowState = new WorkflowState();
    workflowState.open(folder.newFolder("rocksdb"), false);
    elementInstanceState = workflowState.getElementInstanceState();
    variablesState = elementInstanceState.getVariablesState();
    closeables.manage(workflowState);
  }

  @Test
  public void shouldGetLocalVariablesAsDocument() {
    // given
    final long scopeKey = 1;
    declareScope(scopeKey);

    final DirectBuffer var1Value = MsgPackUtil.asMsgPack("a", 1);
    variablesState.setVariableLocal(scopeKey, BufferUtil.wrapString("var1"), var1Value);

    final DirectBuffer var2Value = MsgPackUtil.asMsgPack("x", 10);
    variablesState.setVariableLocal(scopeKey, BufferUtil.wrapString("var2"), var2Value);

    // when
    final DirectBuffer variablesDocument = variablesState.getVariablesAsDocument(scopeKey);

    // then
    MsgPackUtil.assertEquality(variablesDocument, "{'var1': {'a': 1}, 'var2': {'x': 10}}");
  }

  @Test
  public void shouldGetNoVariablesAsEmptyDocument() {
    fail("implement");
  }

  private void declareScope(long key) {
    declareScope(key, -1);
  }

  private void declareScope(long parentKey, long key) {
    final ElementInstance parent = elementInstanceState.getInstance(parentKey);

    final TypedRecord<WorkflowInstanceRecord> record = mockTypedRecord(key, parentKey);
    elementInstanceState.newInstance(
        parent, key, record.getValue(), WorkflowInstanceIntent.ELEMENT_READY);
    elementInstanceState.flushDirtyState();
  }

  private TypedRecord<WorkflowInstanceRecord> mockTypedRecord(long key, long parentKey) {
    final WorkflowInstanceRecord workflowInstanceRecord = createWorkflowInstanceRecord(parentKey);

    final TypedRecord<WorkflowInstanceRecord> typedRecord = mock(TypedRecord.class);
    when(typedRecord.getKey()).thenReturn(key);
    when(typedRecord.getValue()).thenReturn(workflowInstanceRecord);
    final RecordMetadata metadata = new RecordMetadata();
    metadata.intent(WorkflowInstanceIntent.ELEMENT_ACTIVATED);
    when(typedRecord.getMetadata()).thenReturn(metadata);

    return typedRecord;
  }

  private WorkflowInstanceRecord createWorkflowInstanceRecord(long parentKey) {
    final WorkflowInstanceRecord workflowInstanceRecord = new WorkflowInstanceRecord();

    if (parentKey >= 0) {
      workflowInstanceRecord.setScopeInstanceKey(parentKey);
    }

    return workflowInstanceRecord;
  }
}
