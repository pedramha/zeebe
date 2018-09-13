package io.zeebe.exporter;

import io.zeebe.gateway.ZeebeClient;
import io.zeebe.gateway.api.events.JobEvent;
import io.zeebe.test.ZeebeTestRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ElasticsearchExporterTest {

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
