package io.zeebe.broker.subscription.message;

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.util.sched.clock.ActorClock;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MessageStateControllerTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private MessageStateController stateController;

  @Before
  public void setUp() throws Exception {
    stateController = new MessageStateController();
    stateController.open(folder.newFolder("rocksdb"), false);
  }

  @After
  public void close() {
    stateController.close();
  }

  @Test
  public void shouldStoreMessage() {
    // given
    final Message message = new Message("messageName", "correlationKey", "{\"foo\":\"bar\"}", 1234);

    // when
    stateController.put(message);

    // then no error
  }

  @Test
  public void shouldFindMessage() {
    // given
    final long now = ActorClock.currentTimeMillis();
    final Message message = new Message("messageName", "correlationKey", "{\"foo\":\"bar\"}", 1234);
    stateController.put(message);

    // when
    final Message readMessage =
        stateController.findMessage(wrapString("messageName"), wrapString("correlationKey"));

    // then
    assertThat(readMessage.getName()).isEqualTo(message.getName());
    assertThat(readMessage.getCorrelationKey()).isEqualTo(message.getCorrelationKey());
    assertThat(readMessage.getPayload()).isEqualTo(message.getPayload());
    assertThat(readMessage.getTimeToLive()).isEqualTo(1234);
    assertThat(readMessage.getDeadline()).isGreaterThanOrEqualTo(now + 1234);
  }

  @Test
  public void shouldNotFindMessageBeforeTime() {
    // given
    final long now = ActorClock.currentTimeMillis();

    final Message message = new Message("messageName", "correlationKey", "{\"foo\":\"bar\"}", 1234);
    final Message message2 =
        new Message("messageName", "correlationKey", "{\"foo\":\"bar\"}", 4567);
    stateController.put(message);
    stateController.put(message2);

    // when
    final long deadline = now + 1_000L;

    // then
    final List<Message> readMessage = stateController.findMessageBefore(deadline);
    assertThat(readMessage).isEmpty();
  }

  @Test
  public void shouldFindMessageBeforeTime() {
    // given
    final long now = ActorClock.currentTimeMillis();

    final Message message = new Message("messageName", "correlationKey", "{\"foo\":\"bar\"}", 1234);
    final Message message2 = new Message("otherName", "otherKey", "{\"foo\":\"bar\"}", 4567);
    stateController.put(message);
    stateController.put(message2);

    // when
    final long deadline = now + 2_000L;

    // then
    final List<Message> readMessage = stateController.findMessageBefore(deadline);
    assertThat(readMessage.size()).isEqualTo(1);
    assertThat(readMessage.get(0).getName()).isEqualTo(wrapString("messageName"));
    assertThat(readMessage.get(0).getCorrelationKey()).isEqualTo(wrapString("correlationKey"));
    assertThat(readMessage.get(0).getPayload()).isEqualTo(wrapString("{\"foo\":\"bar\"}"));
  }
}
