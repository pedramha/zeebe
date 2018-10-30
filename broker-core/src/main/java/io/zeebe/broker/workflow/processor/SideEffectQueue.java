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
package io.zeebe.broker.workflow.processor;

import io.zeebe.broker.logstreams.processor.SideEffectProducer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Consumer;

public class SideEffectQueue implements SideEffectProducer, Consumer<SideEffectProducer> {
  private Queue<SideEffectProducer> sideEffectProducers = new LinkedList<>();

  public void add(SideEffectProducer producer) {
    sideEffectProducers.add(producer);
  }

  public void clear() {
    sideEffectProducers.clear();
  }

  @Override
  public void accept(SideEffectProducer sideEffectProducer) {
    sideEffectProducers.add(sideEffectProducer);
  }

  @Override
  public boolean flush() {
    SideEffectProducer sideEffectProducer;

    while ((sideEffectProducer = sideEffectProducers.poll()) != null) {
      if (!sideEffectProducer.flush()) {
        sideEffectProducers.add(sideEffectProducer);
        return false;
      }
    }

    return true;
  }
}
