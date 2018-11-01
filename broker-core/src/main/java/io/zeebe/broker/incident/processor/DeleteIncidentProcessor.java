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
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.intent.IncidentIntent;

public final class DeleteIncidentProcessor implements TypedRecordProcessor<IncidentRecord> {

  private final ZeebeState zeebeState;

  public DeleteIncidentProcessor(ZeebeState zeebeState) {
    this.zeebeState = zeebeState;
  }

  @Override
  public void processRecord(
      TypedRecord<IncidentRecord> command,
      TypedResponseWriter responseWriter,
      TypedStreamWriter streamWriter) {
    final long incidentKey = command.getKey();
    final IncidentState incidentState = zeebeState.getIncidentState();
    final IncidentRecord incidentRecord = incidentState.getIncidentRecord(incidentKey);

    if (incidentRecord != null) {
      // TODO do we need to write the prior incident event ?
      streamWriter.writeFollowUpEvent(command.getKey(), IncidentIntent.DELETED, incidentRecord);
    } else {
      streamWriter.writeRejection(command, RejectionType.NOT_APPLICABLE, "Incident does not exist");
    }
  }
}
