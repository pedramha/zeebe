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

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.subscription.message.state.WorkflowInstanceSubscriptionState;
import io.zeebe.test.util.AutoCloseableRule;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WorkflowInstanceSubscriptionStateTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Rule public AutoCloseableRule closeables = new AutoCloseableRule();

  private WorkflowInstanceSubscriptionState state;

  @Before
  public void setUp() throws Exception {
    final ZeebeState stateController = new ZeebeState();
    stateController.open(folder.newFolder("rocksdb"), false);

    state = stateController.getWorkflowInstanceSubscriptionState();

    closeables.manage(stateController);
  }

  @Test
  public void shouldNotExist() {
    // given
    state.put(subscriptionWithElementInstanceKey(1));

    // when
    final boolean exist = state.existSubscriptionForElementInstance(2);

    // then
    assertThat(exist).isFalse();
  }

  @Test
  public void shouldExistSubscription() {
    // given
    state.put(subscriptionWithElementInstanceKey(1));

    // when
    final boolean exist = state.existSubscriptionForElementInstance(1);

    // then
    assertThat(exist).isTrue();
  }

  @Test
  public void shouldNoVisitSubscriptionBeforeTime() {
    // given
    final WorkflowInstanceSubscription subscription1 = subscriptionWithElementInstanceKey(1L);
    state.put(subscription1);
    state.updateSentTime(subscription1, 1_000);

    final WorkflowInstanceSubscription subscription2 = subscriptionWithElementInstanceKey(2L);
    state.put(subscription2);
    state.updateSentTime(subscription2, 3_000);

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(1_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).isEmpty();
  }

  @Test
  public void shouldVisitSubscriptionBeforeTime() {
    // given
    final WorkflowInstanceSubscription subscription1 = subscriptionWithElementInstanceKey(1L);
    state.put(subscription1);
    state.updateSentTime(subscription1, 1_000);

    final WorkflowInstanceSubscription subscription2 = subscriptionWithElementInstanceKey(2L);
    state.put(subscription2);
    state.updateSentTime(subscription2, 3_000);

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(1).contains(1L);
  }

  @Test
  public void shouldFindSubscriptionBeforeTimeInOrder() {
    // given
    final WorkflowInstanceSubscription subscription1 = subscriptionWithElementInstanceKey(1L);
    state.put(subscription1);
    state.updateSentTime(subscription1, 1_000);

    final WorkflowInstanceSubscription subscription2 = subscriptionWithElementInstanceKey(2L);
    state.put(subscription2);
    state.updateSentTime(subscription2, 2_000);

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(3_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(2).containsExactly(1L, 2L);
  }

  @Test
  public void shouldNotVisitSubscriptionIfOpened() {
    // given
    final WorkflowInstanceSubscription subscription1 = subscriptionWithElementInstanceKey(1L);
    state.put(subscription1);

    final WorkflowInstanceSubscription subscription2 = subscriptionWithElementInstanceKey(2L);
    state.put(subscription2);
    state.updateToOpenedState(subscription2, 3);

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(1).contains(1L);
  }

  @Test
  public void shouldUpdateSubscriptionSentTime() {
    // given
    final WorkflowInstanceSubscription subscription = subscriptionWithElementInstanceKey(1L);
    state.put(subscription);

    // when
    state.updateSentTime(subscription, 1_000);

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(1).contains(1L);
  }

  @Test
  public void shouldUpdateOpenState() {
    // given
    final WorkflowInstanceSubscription subscription = subscriptionWithElementInstanceKey(1L);
    state.put(subscription);

    assertThat(subscription.isOpening()).isTrue();

    // when
    state.updateToOpenedState(subscription, 3);

    // then
    assertThat(subscription.isOpening()).isFalse();

    // and
    assertThat(state.getSubscription(1L).getSubscriptionPartitionId()).isEqualTo(3);

    // and
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).isEmpty();
  }

  @Test
  public void shouldUpdateCloseState() {
    // given
    final WorkflowInstanceSubscription subscription = subscriptionWithElementInstanceKey(1L);
    state.put(subscription);

    state.updateToOpenedState(subscription, 3);

    assertThat(subscription.isClosing()).isFalse();

    // when
    state.updateToClosingState(subscription, 1_000);

    // then
    assertThat(state.getSubscription(1L).isClosing()).isTrue();
    // and
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(1).contains(1L);
  }

  @Test
  public void shouldRemoveSubscription() {
    // given
    final WorkflowInstanceSubscription subscription = subscriptionWithElementInstanceKey(1L);
    state.put(subscription);
    state.updateSentTime(subscription, 1_000);

    // when
    state.remove(1L);

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).isEmpty();

    // and
    assertThat(state.existSubscriptionForElementInstance(1L)).isFalse();
  }

  @Test
  public void shouldNotFailOnRemoveSubscriptionTwice() {
    // given
    state.put(subscriptionWithElementInstanceKey(1L));

    // when
    state.remove(1L);
    state.remove(1L);

    // then
    assertThat(state.existSubscriptionForElementInstance(1L)).isFalse();
  }

  @Test
  public void shouldNotRemoveSubscriptionOnDifferentKey() {
    // given
    state.put(subscription("messageName", "correlationKey", 1L));
    state.put(subscription("messageName", "correlationKey", 2L));

    // when
    state.remove(2L);

    // then
    assertThat(state.existSubscriptionForElementInstance(1L)).isTrue();
  }

  private WorkflowInstanceSubscription subscriptionWithElementInstanceKey(long elementInstanceKey) {
    return subscription("messageName", "correlationKey", elementInstanceKey);
  }

  private WorkflowInstanceSubscription subscription(
      String name, String correlationKey, long elementInstanceKey) {
    return new WorkflowInstanceSubscription(
        1L, elementInstanceKey, wrapString(name), wrapString(correlationKey), 1_000);
  }
}
