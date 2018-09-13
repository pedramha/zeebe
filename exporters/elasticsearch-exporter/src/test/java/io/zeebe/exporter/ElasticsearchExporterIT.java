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
package io.zeebe.exporter;

import io.zeebe.gateway.ZeebeClient;
import io.zeebe.gateway.api.events.JobEvent;
import io.zeebe.test.ZeebeTestRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ElasticsearchExporterIT {

  @Rule public ZeebeTestRule testRule = new ZeebeTestRule();

  private ZeebeClient client;

  @Before
  public void setUp() {
    client = testRule.getClient();
  }

  @Test
  public void test() throws InterruptedException {
    final JobEvent job = client.jobClient().newCreateCommand().jobType("test-job").send().join();

    Thread.sleep(10_000);
  }
}
