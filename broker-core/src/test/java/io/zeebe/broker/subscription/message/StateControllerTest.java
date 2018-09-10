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
package io.zeebe.broker.subscription.message;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.logstreams.state.StateController;
import io.zeebe.util.collection.Tuple;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;

public class StateControllerTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private StateController stateController;

  @Before
  public void setUp() throws Exception {
    stateController = new StateController();
    stateController.open(folder.newFolder("rocksdb"), false);
  }

  @After
  public void close() {
    stateController.close();
  }

  @Test
  public void shouldIterateAsc() {
    // given
    // when
    stateController.put("foobar".getBytes(), "bar".getBytes());
    stateController.put("abc".getBytes(), "123".getBytes());
    stateController.put("foo".getBytes(), "lulz".getBytes());
    stateController.put("123".getBytes(), "lulz".getBytes());

    // then
    final List<Tuple<byte[], byte[]>> keyValuePairs = new ArrayList<>();
    stateController.foreach(
        (key, value) -> {
          keyValuePairs.add(new Tuple<>(key, value));
        });

    assertThat(keyValuePairs)
        .extracting(t -> t.getLeft())
        .containsExactly("123".getBytes(), "abc".getBytes(), "foo".getBytes(), "foobar".getBytes());
  }

  @Test
  public void shouldIterateOnlyValuesFromColumnFamily() throws Exception {
    // given
    final ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor("foo".getBytes());
    final ColumnFamilyHandle columnFamilyHandle =
        stateController.getDb().createColumnFamily(descriptor);

    // when
    stateController.put("abc".getBytes(), "123".getBytes());
    stateController.put("foobar".getBytes(), "bar".getBytes());
    stateController.put("foo".getBytes(), "lulz".getBytes());
    stateController.getDb().put(columnFamilyHandle, "123".getBytes(), "lulz".getBytes());

    // then
    final List<Tuple<byte[], byte[]>> keyValuePairs = new ArrayList<>();
    stateController.foreach(
        columnFamilyHandle,
        (key, value) -> {
          keyValuePairs.add(new Tuple<>(key, value));
        });

    assertThat(keyValuePairs).extracting(t -> t.getLeft()).containsExactly("123".getBytes());
  }

  @Test
  public void shouldOverwrite() throws Exception {
    // given
    // when
    final byte[] keyBytes = "abc".getBytes();
    stateController.put(keyBytes, "123".getBytes());
    stateController.put(keyBytes, "456".getBytes());

    // then
    assertThat(stateController.getDb().get(keyBytes)).containsExactly("456".getBytes());
  }

  @Test
  public void shouldIterateSubset() {
    // given
    stateController.put("abc".getBytes(), "bar".getBytes());
    stateController.put("name13223".getBytes(), "bar".getBytes());
    stateController.put("name123".getBytes(), "bar".getBytes());
    stateController.put("namecorrelation123".getBytes(), "bar".getBytes());
    stateController.put("namecorrelation1234".getBytes(), "bar".getBytes());
    stateController.put("namecorrelation2323".getBytes(), "bar".getBytes());
    stateController.put("namecorrelation3243".getBytes(), "bar".getBytes());
    stateController.put("namecorrelation14223".getBytes(), "bar".getBytes());

    // when
    final List<Tuple<byte[], byte[]>> keyValuePairs = new ArrayList<>();
    stateController.foreach(
        "namecorrelation".getBytes(),
        (key, value) -> {
          keyValuePairs.add(new Tuple<>(key, value));
        });

    // then
    assertThat(keyValuePairs)
        .extracting(t -> t.getLeft())
        .containsExactly(
            "namecorrelation123".getBytes(),
            "namecorrelation1234".getBytes(),
            "namecorrelation14223".getBytes(),
            "namecorrelation2323".getBytes(),
            "namecorrelation3243".getBytes());
  }
}
