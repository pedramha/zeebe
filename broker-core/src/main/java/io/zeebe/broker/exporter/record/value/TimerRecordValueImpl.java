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
package io.zeebe.broker.exporter.record.value;

import io.zeebe.broker.exporter.ExporterObjectMapper;
import io.zeebe.broker.exporter.record.RecordValueImpl;
import io.zeebe.exporter.record.value.TimerRecordValue;

public class TimerRecordValueImpl extends RecordValueImpl implements TimerRecordValue {

  private final long elementInstanceKey;
  private final long dueDate;

  public TimerRecordValueImpl(
      ExporterObjectMapper objectMapper, long elementInstanceKey, long dueDate) {
    super(objectMapper);
    this.elementInstanceKey = elementInstanceKey;
    this.dueDate = dueDate;
  }

  @Override
  public long getElementInstanceKey() {
    return elementInstanceKey;
  }

  @Override
  public long getDueDate() {
    return dueDate;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (elementInstanceKey ^ (elementInstanceKey >>> 32));
    result = prime * result + (int) (dueDate ^ (dueDate >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final TimerRecordValueImpl other = (TimerRecordValueImpl) obj;
    if (elementInstanceKey != other.elementInstanceKey) {
      return false;
    }
    return dueDate == other.dueDate;
  }

  @Override
  public String toString() {
    return "TimerRecordValueImpl [elementInstanceKey="
        + elementInstanceKey
        + ", dueDate="
        + dueDate
        + "]";
  }
}
