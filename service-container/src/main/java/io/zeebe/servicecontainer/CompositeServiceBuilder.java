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
package io.zeebe.servicecontainer;

import io.zeebe.servicecontainer.impl.ServiceContainerImpl;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Can be used to install a list of services "transactionally" (in the sense that if one of the
 * services fails to install, all services are removed again).
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CompositeServiceBuilder {
  private final ServiceContainerImpl container;
  private final ServiceName<Void> compositeServiceName;

  private final Map<ServiceName, ActorFuture<Object>> installFutures = new HashMap<>();

  public CompositeServiceBuilder(
      ServiceName<Void> name,
      ServiceContainer container,
      ServiceName<?>... additionalDependencies) {
    this.compositeServiceName = name;
    this.container = (ServiceContainerImpl) container;

    final ServiceBuilder<Void> installServiceBuilder =
        container.createService(compositeServiceName, new CompositeService());
    for (ServiceName<?> serviceName : additionalDependencies) {
      installServiceBuilder.dependency(serviceName);
    }
    installFutures.put(compositeServiceName, (ActorFuture) installServiceBuilder.install());
  }

  public ActorFuture<Void> install() {
    final CompletableActorFuture<Void> compositeInstallFuture = new CompletableActorFuture<>();

    container
        .getActorScheduler()
        .submitActor(new CompositeInstallCompletion(compositeInstallFuture, compositeServiceName));

    return compositeInstallFuture;
  }

  public <S> ActorFuture<S> installAndReturn(ServiceName<S> returnedServiceName) {
    final CompletableActorFuture<S> compositeInstallFuture = new CompletableActorFuture<>();

    container
        .getActorScheduler()
        .submitActor(new CompositeInstallCompletion(compositeInstallFuture, returnedServiceName));

    return compositeInstallFuture;
  }

  public <S> ServiceBuilder<S> createService(ServiceName<S> name, Service<S> service) {
    return new ComposedServiceBuilder<>(name, service, container);
  }

  private final class CompositeInstallCompletion<S> extends Actor {
    private final CompletableActorFuture<S> future;
    private final ServiceName<S> returnedServiceName;

    CompositeInstallCompletion(
        CompletableActorFuture<S> future, ServiceName<S> returnedServiceName) {
      this.future = future;
      this.returnedServiceName = returnedServiceName;
    }

    @Override
    protected void onActorStarted() {
      final Collection<ActorFuture<Object>> allFutures = installFutures.values();
      for (Entry<ServiceName, ActorFuture<Object>> futures : installFutures.entrySet()) {
        actor.runOnCompletion(
            futures.getValue(),
            (val, t) -> {
              if (t != null) {
                actor.runOnCompletion(
                    container.removeService(compositeServiceName),
                    (v, e) -> {
                      final String errorMessage =
                          "Could not complete installation of "
                              + compositeServiceName.getName()
                              + ": Failed to install "
                              + futures.getKey();
                      future.completeExceptionally(new RuntimeException(errorMessage, t));
                      actor.close();
                    });
              } else {
                // check all complete
                if (!future.isDone() && allFutures.stream().allMatch(f -> f.isDone())) {
                  future.completeWith(
                      (CompletableActorFuture<S>) installFutures.get(returnedServiceName));
                  actor.close();
                }
              }
            });
      }
    }
  }

  class ComposedServiceBuilder<S> extends ServiceBuilder<S> {
    private ServiceName<S> serviceName;

    ComposedServiceBuilder(
        ServiceName<S> name, Service<S> service, ServiceContainerImpl serviceContainer) {
      super(name, service, serviceContainer);
      serviceName = name;
      dependency(compositeServiceName);
    }

    @Override
    public ActorFuture<S> install() {
      final ActorFuture<S> installFuture = super.install();
      installFutures.put(serviceName, (ActorFuture) installFuture);
      return (ActorFuture) installFuture;
    }
  }

  class CompositeService implements Service<Void> {
    @Override
    public void start(ServiceStartContext startContext) {}

    @Override
    public void stop(ServiceStopContext stopContext) {}

    @Override
    public Void get() {
      return null;
    }
  }

  public static ServiceName<Void> compositeServiceName(String name) {
    return ServiceName.newServiceName(String.format("%s.install", name), Void.class);
  }
}
