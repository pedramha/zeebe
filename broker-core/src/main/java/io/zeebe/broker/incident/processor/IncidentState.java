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

import static io.zeebe.logstreams.rocksdb.ZeebeStateConstants.STATE_BYTE_ORDER;

import io.zeebe.broker.incident.data.IncidentRecord;
import io.zeebe.broker.workflow.state.PersistenceHelper;
import io.zeebe.logstreams.rocksdb.ZbRocksDb;
import io.zeebe.logstreams.state.StateController;
import io.zeebe.logstreams.state.StateLifecycleListener;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyHandle;

public class IncidentState implements StateLifecycleListener {

  private static final byte[] INCIDENT_COLUMN_FAMILY_NAME = "incident".getBytes();

  private static final byte[][] COLUMN_FAMILY_NAMES = {INCIDENT_COLUMN_FAMILY_NAME};

  private ColumnFamilyHandle incidentColumnFamily;
  private ZbRocksDb db;

  private final MutableDirectBuffer keyBuffer = new UnsafeBuffer(new byte[Long.BYTES]);
  private final MutableDirectBuffer valueBuffer = new ExpandableArrayBuffer();

  private final IncidentRecord incidentRecord = new IncidentRecord();
  private PersistenceHelper persistenceHelper;

  public static List<byte[]> getColumnFamilyNames() {
    return Stream.of(COLUMN_FAMILY_NAMES).flatMap(Stream::of).collect(Collectors.toList());
  }

  @Override
  public void onOpened(StateController stateController) {
    db = stateController.getDb();

    persistenceHelper = new PersistenceHelper(stateController);
    incidentColumnFamily = stateController.getColumnFamilyHandle(INCIDENT_COLUMN_FAMILY_NAME);
  }

  public void createIncident(long incidentKey, IncidentRecord incident) {
    keyBuffer.putLong(0, incidentKey, STATE_BYTE_ORDER);

    final int length = incident.getLength();
    incident.write(valueBuffer, 0);

    db.put(
        incidentColumnFamily,
        keyBuffer.byteArray(),
        0,
        Long.BYTES,
        valueBuffer.byteArray(),
        0,
        length);
  }

  public IncidentRecord getIncidentRecord(long incidentKey) {
    final boolean successfulRead =
        persistenceHelper.readInto(incidentRecord, incidentColumnFamily, incidentKey);

    return successfulRead ? incidentRecord : null;
  }
}
