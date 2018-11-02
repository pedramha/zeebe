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
package io.zeebe.broker.benchmarks.state.prototype;

import io.zeebe.broker.workflow.state.prototype.VariableStateImpl;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class GetVariableBenchmark {

  @Benchmark
  @Threads(1)
  public void getVariableLocal(final GetBenchmarkPreparation ctx, final Blackhole blackhole) {
    final int iteration = ctx.countIteration();

    final VariableStateImpl variableState = ctx.getVariableState();

    final long scopeKey = ctx.getRootScopeKey(iteration);
    final DirectBuffer name = ctx.getVariableName(iteration);

    final DirectBuffer value = variableState.getVariableLocal(scopeKey, name);
    blackhole.consume(value);
  }

  @Benchmark
  @Threads(1)
  public void getVariable(final GetBenchmarkPreparation ctx, final Blackhole blackhole) {
    final int iteration = ctx.countIteration();

    final VariableStateImpl variableState = ctx.getVariableState();

    final long scopeKey = ctx.getLeafScopeKey(iteration);
    final DirectBuffer name = ctx.getVariableName(iteration);

    final DirectBuffer value = variableState.getVariable(scopeKey, name);
    blackhole.consume(value);
  }
}
