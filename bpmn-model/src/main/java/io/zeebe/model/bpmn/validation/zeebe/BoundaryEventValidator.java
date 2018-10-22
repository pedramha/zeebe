/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.model.bpmn.validation.zeebe;

import io.zeebe.model.bpmn.instance.Activity;
import io.zeebe.model.bpmn.instance.BoundaryEvent;
import io.zeebe.model.bpmn.instance.EventDefinition;
import io.zeebe.model.bpmn.instance.Message;
import io.zeebe.model.bpmn.instance.MessageEventDefinition;
import java.util.Collection;
import org.camunda.bpm.model.xml.validation.ModelElementValidator;
import org.camunda.bpm.model.xml.validation.ValidationResultCollector;

public class BoundaryEventValidator implements ModelElementValidator<BoundaryEvent> {

  @Override
  public Class<BoundaryEvent> getElementType() {
    return BoundaryEvent.class;
  }

  @Override
  public void validate(BoundaryEvent element, ValidationResultCollector validationResultCollector) {
    // todo(npepinpe): not sure if necessary
    final Activity activity = element.getAttachedTo();
    if (activity == null) {
      validationResultCollector.addError(0, "Must have be attached to an activity!");
    }

    final Collection<EventDefinition> eventDefinitions = element.getEventDefinitions();
    if (eventDefinitions.size() != 1) {
      validationResultCollector.addError(0, "Must have exactly one event definition!");
    } else {
      final EventDefinition eventDefinition = eventDefinitions.iterator().next();

      // todo(npepinpe): extract common parts from IntermediateCatchEvent and here to
      // CatchEventValidator?
      if (eventDefinition instanceof MessageEventDefinition) {
        final Message message = ((MessageEventDefinition) eventDefinition).getMessage();
        if (message == null) {
          validationResultCollector.addError(0, "Must reference a message");
        }
      } else {
        validationResultCollector.addError(
            0, "Event definition not supported; must be one of: message!");
      }
    }
  }
}
