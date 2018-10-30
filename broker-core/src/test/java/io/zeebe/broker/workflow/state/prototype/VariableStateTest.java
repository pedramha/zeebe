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
package io.zeebe.broker.workflow.state.prototype;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.test.util.AutoCloseableRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class VariableStateTest {

  private static final long SCOPE1 = 123;
  private static final long SCOPE2 = 456;
  private static final long SCOPE3 = 789;

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule public AutoCloseableRule closeables = new AutoCloseableRule();

  private VariableStateImpl state;

  @Before
  public void setUp() {
    state = new VariableStateImpl(tempFolder.getRoot(), 128, 128);
    closeables.manage(state);

    state.addScope(SCOPE1, SCOPE2);
    state.addScope(SCOPE2, SCOPE3);
  }

  @Test
  public void shouldGetLocal() {
    // given
    state.setVariableLocal(SCOPE2, "key", "val");

    // when
    final String value = state.getVariableLocal(SCOPE2, "key");

    // then
    assertThat(value).isEqualTo("val");
  }

  @Test
  public void shouldGet() {
    // given
    state.setVariableLocal(SCOPE1, "key", "val");

    // when
    final String value = state.getVariable(SCOPE3, "key");

    // then
    assertThat(value).isEqualTo("val");
  }

  @Test
  public void shouldSet() {
    // when
    state.setVariable(SCOPE3, "key", "val");

    // then
    final String value = state.getVariableLocal(SCOPE1, "key");
    assertThat(value).isEqualTo("val");
  }

  @Test
  public void shouldOverwrite() {
    // given
    state.setVariableLocal(SCOPE2, "key", "val1");

    // when
    state.setVariable(SCOPE3, "key", "val2");

    // then
    final String value = state.getVariableLocal(SCOPE2, "key");
    assertThat(value).isEqualTo("val2");
  }

  @Test
  public void shouldHideVariable() {
    // given
    state.setVariableLocal(SCOPE1, "key", "val1");
    state.setVariableLocal(SCOPE2, "key", "val2");

    // when
    final String value = state.getVariable(SCOPE3, "key");

    // then
    assertThat(value).isEqualTo("val2");
  }

  @Test
  public void shouldGetVariablesFromDifferentScopes() {
    // given
    state.setVariableLocal(SCOPE1, "key1", "val1");
    state.setVariableLocal(SCOPE2, "key2", "val2");
    state.setVariableLocal(SCOPE3, "key3", "val3");

    // when
    final String value1 = state.getVariable(SCOPE3, "key1");
    final String value2 = state.getVariable(SCOPE3, "key2");
    final String value3 = state.getVariable(SCOPE3, "key3");

    // then
    assertThat(value1).isEqualTo("val1");
    assertThat(value2).isEqualTo("val2");
    assertThat(value3).isEqualTo("val3");
  }
}
