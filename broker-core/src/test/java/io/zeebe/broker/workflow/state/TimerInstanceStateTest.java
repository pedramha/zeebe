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
package io.zeebe.broker.workflow.state;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.test.util.AutoCloseableRule;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TimerInstanceStateTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Rule public AutoCloseableRule closeables = new AutoCloseableRule();

  private TimerInstanceState state;

  @Before
  public void setUp() throws Exception {
    final ZeebeState zeebeState = new ZeebeState();
    zeebeState.open(folder.newFolder("rocksdb"), false);
    state = zeebeState.getWorkflowState().getTimerState();
    closeables.manage(zeebeState);
  }

  @Test
  public void shouldInsertTimer() {
    // given
    final TimerInstance timer = new TimerInstance();
    timer.setElementInstanceKey(1L);
    timer.setKey(2L);
    timer.setDueDate(1000L);
    state.put(timer);

    // when
    final List<TimerInstance> timers = new ArrayList<>();
    state.findTimersWithDueDateBefore(1000L, timers::add);

    // then
    assertThat(timers).hasSize(1);

    final TimerInstance readTimer = timers.get(0);
    assertThat(readTimer.getElementInstanceKey()).isEqualTo(1L);
    assertThat(readTimer.getKey()).isEqualTo(2L);
    assertThat(readTimer.getDueDate()).isEqualTo(1000L);
  }

  @Test
  public void shouldRemoveTimer() {
    // given
    final TimerInstance timer1 = new TimerInstance();
    timer1.setElementInstanceKey(1L);
    timer1.setDueDate(1000L);
    state.put(timer1);

    final TimerInstance timer2 = new TimerInstance();
    timer2.setElementInstanceKey(2L);
    timer2.setDueDate(2000L);
    state.put(timer2);

    // when
    final TimerInstance timer = new TimerInstance();
    timer.setElementInstanceKey(1L);
    timer.setDueDate(1000L);
    state.remove(timer);

    // then
    final List<TimerInstance> timers = new ArrayList<>();
    state.findTimersWithDueDateBefore(2000L, timers::add);

    assertThat(timers).hasSize(1);
    assertThat(timers.get(0).getElementInstanceKey()).isEqualTo(2L);
  }

  @Test
  public void shouldGetTimerByElementInstanceKey() {
    // given
    final TimerInstance timer = new TimerInstance();
    timer.setElementInstanceKey(1L);
    timer.setKey(2L);
    timer.setDueDate(1000L);
    state.put(timer);

    // when
    final TimerInstance readTimer = state.get(1L);

    // then
    assertThat(readTimer).isNotNull();
    assertThat(readTimer.getElementInstanceKey()).isEqualTo(1L);
    assertThat(readTimer.getKey()).isEqualTo(2L);
    assertThat(readTimer.getDueDate()).isEqualTo(1000L);

    // and
    assertThat(state.get(2L)).isNull();
  }

  @Test
  public void shouldFindTimersWithDueDate() {
    // given
    final TimerInstance timer1 = new TimerInstance();
    timer1.setElementInstanceKey(1L);
    timer1.setDueDate(1000L);
    state.put(timer1);

    final TimerInstance timer2 = new TimerInstance();
    timer2.setElementInstanceKey(2L);
    timer2.setDueDate(2000L);
    state.put(timer2);

    final TimerInstance timer3 = new TimerInstance();
    timer3.setElementInstanceKey(3L);
    timer3.setDueDate(3000L);
    state.put(timer3);

    // when
    final List<Long> keys = new ArrayList<>();
    state.findTimersWithDueDateBefore(2000L, t -> keys.add(t.getElementInstanceKey()));

    // then
    assertThat(keys).hasSize(2);
    assertThat(keys).containsExactly(1L, 2L);
  }

  @Test
  public void shouldReturnNextDueDate() {
    // given
    final TimerInstance timer1 = new TimerInstance();
    timer1.setElementInstanceKey(1L);
    timer1.setDueDate(1000L);
    state.put(timer1);

    final TimerInstance timer2 = new TimerInstance();
    timer2.setElementInstanceKey(2L);
    timer2.setDueDate(2000L);
    state.put(timer2);

    final TimerInstance timer3 = new TimerInstance();
    timer3.setElementInstanceKey(3L);
    timer3.setDueDate(3000L);
    state.put(timer3);

    // when
    final long nextDueDate = state.findTimersWithDueDateBefore(2000L, t -> true);

    // then
    assertThat(nextDueDate).isEqualTo(3000L);
  }

  @Test
  public void shouldReturnNegativeDueDateIfEmpty() {

    // when
    final long nextDueDate = state.findTimersWithDueDateBefore(2000L, t -> true);

    // then
    assertThat(nextDueDate).isEqualTo(-1L);
  }

  @Test
  public void shouldReturnNegativeDueDateIfNoMoreTimers() {
    // given
    final TimerInstance timer1 = new TimerInstance();
    timer1.setElementInstanceKey(1L);
    timer1.setDueDate(1000L);
    state.put(timer1);

    final TimerInstance timer2 = new TimerInstance();
    timer2.setElementInstanceKey(1L);
    timer2.setDueDate(1000L);
    state.put(timer2);

    final TimerInstance timer3 = new TimerInstance();
    timer3.setElementInstanceKey(3L);
    timer3.setDueDate(3000L);
    state.put(timer3);

    // when
    final long nextDueDate = state.findTimersWithDueDateBefore(3000L, t -> true);

    // then
    assertThat(nextDueDate).isEqualTo(-1L);
  }

  @Test
  public void shouldFindTimersWithDueDateUntilNotConsumed() {
    // given
    final TimerInstance timer1 = new TimerInstance();
    timer1.setElementInstanceKey(1L);
    timer1.setDueDate(1000L);
    state.put(timer1);

    final TimerInstance timer2 = new TimerInstance();
    timer2.setElementInstanceKey(2L);
    timer2.setDueDate(2000L);
    state.put(timer2);

    // when
    final List<Long> keys = new ArrayList<>();
    final long nextDueDate =
        state.findTimersWithDueDateBefore(
            2000L,
            t -> {
              keys.add(t.getElementInstanceKey());
              return false;
            });

    // then
    assertThat(keys).hasSize(1);
    assertThat(keys).contains(1L);
    assertThat(nextDueDate).isEqualTo(timer1.getDueDate());
  }
}
