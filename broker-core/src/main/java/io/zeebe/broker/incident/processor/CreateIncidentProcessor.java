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
import io.zeebe.broker.logstreams.processor.CommandProcessor;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.protocol.intent.IncidentIntent;

public final class CreateIncidentProcessor implements CommandProcessor<IncidentRecord> {

  private final ZeebeState zeebeState;

  public CreateIncidentProcessor(ZeebeState zeebeState) {
    this.zeebeState = zeebeState;
  }

  @Override
  public void onCommand(
      TypedRecord<IncidentRecord> command, CommandControl<IncidentRecord> commandControl) {
    final IncidentRecord incidentEvent = command.getValue();

    // TODO do I have to check this?
    //    if (isJobIncident
    //        && failedJobMap.get(incidentEvent.getJobKey(), -1L) !=
    // IncidentStreamProcessor.NON_PERSISTENT_INCIDENT) {
    //      commandControl.reject(RejectionType.NOT_APPLICABLE, "Job is not failed");
    //      return;
    //    }
    final long incidentKey = commandControl.accept(IncidentIntent.CREATED, incidentEvent);

    zeebeState.getIncidentState().createIncident(incidentKey, incidentEvent);
  }
}
