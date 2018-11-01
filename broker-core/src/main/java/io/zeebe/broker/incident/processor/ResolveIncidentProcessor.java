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
package io.zeebe.broker.incident.processor;

import io.zeebe.broker.incident.data.IncidentRecord;
import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.ElementInstanceState;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.intent.IncidentIntent;

public final class ResolveIncidentProcessor implements TypedRecordProcessor<IncidentRecord> {

  private final ZeebeState zeebeState;
  private long incidentKey;

  public ResolveIncidentProcessor(ZeebeState zeebeState) {
    this.zeebeState = zeebeState;
  }

  @Override
  public void processRecord(
      TypedRecord<IncidentRecord> command,
      TypedResponseWriter responseWriter,
      TypedStreamWriter streamWriter) {

    incidentKey = command.getKey();
    final TypedBatchWriter typedBatchWriter = streamWriter.newBatch();

    final IncidentRecord incidentRecord =
        zeebeState.getIncidentState().getIncidentRecord(incidentKey);
    if (incidentRecord != null) {
      triggerProcessContinuation(incidentRecord, typedBatchWriter);
    }

    typedBatchWriter.addFollowUpEvent(incidentKey, IncidentIntent.RESOLVED, command.getValue());
  }

  public void triggerProcessContinuation(
      IncidentRecord incidentRecord, TypedBatchWriter typedBatchWriter) {
    final long jobKey = incidentRecord.getJobKey();
    final boolean isJobIncident = jobKey > 0;

    if (isJobIncident) {
      final JobRecord job = zeebeState.getJobState().getJob(jobKey);
      if (job != null) {
        // rewrite job in state
        //
        //          streamWriter.writeFollowUpEvent(
        //            jobKey,
        //            instance.getKey(), instance.getState(), instance.getValue(),
        // this::setIncidentKey);
      }
    } else {
      final ElementInstanceState elementInstanceState =
          zeebeState.getWorkflowState().getElementInstanceState();

      final ElementInstance instance =
          elementInstanceState.getInstance(incidentRecord.getElementInstanceKey());

      if (instance != null) {
        // rewrite record
        // do we need to rewrite with new payload from command?
        typedBatchWriter.addFollowUpEvent(
            instance.getKey(), instance.getState(), instance.getValue(), this::setIncidentKey);
      }
    }
  }

  private void setIncidentKey(RecordMetadata metadata) {
    metadata.incidentKey(incidentKey);
  }
}
