package ru.hh.rabbitmq.client.test.receiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

public final class SpringConsumerTest {

  public static void main(String[] args) {
    final CachingConnectionFactory connectionFactory = createConnectionFactory();
    playWithConnectionFactory(connectionFactory);
  }

  private static CachingConnectionFactory createConnectionFactory() {
    final CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
    connectionFactory.setConnectionTimeout(1000);
    connectionFactory.setRequestedHeartBeat(1);
    return connectionFactory;
  }

  private static void playWithConnectionFactory(final CachingConnectionFactory connectionFactory) {
    final SimpleMessageListenerContainer simpleMessageListenerContainer = new SimpleMessageListenerContainer(connectionFactory);
    simpleMessageListenerContainer.setQueueNames("test");
    simpleMessageListenerContainer.setMessageListener(new MyMessageListener());
    simpleMessageListenerContainer.start();
  }

  static final class MyMessageListener implements MessageListener {

    @Override
    public void onMessage(Message message) {
      log.info("received '{}'", new String(message.getBody()));
    }

    private static final Logger log = LoggerFactory.getLogger(MyMessageListener.class);
  }

  private SpringConsumerTest() {
  }
}
