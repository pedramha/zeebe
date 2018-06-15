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
package io.zeebe.util.sched.ordering;

import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorControl;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

public class ActionRecordingActor extends Actor {
  public final List<String> actions = new ArrayList<>();

  protected BiConsumer<Void, Throwable> futureConsumer(String label) {
    return (v, t) -> {
      actions.add(label);
    };
  }

  protected Runnable runnable(String label) {
    return () -> {
      actions.add(label);
    };
  }

  protected Callable<Void> callable(String label) {
    return () -> {
      actions.add(label);
      return null;
    };
  }

  public ActorControl actorControl() {
    return actor;
  }
}
