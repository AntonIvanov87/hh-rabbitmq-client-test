package ru.hh.rabbitmq.client.test.receiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import ru.hh.rabbitmq.spring.ClientFactory;
import ru.hh.rabbitmq.spring.Receiver;

import java.util.Properties;

public final class HHConsumerTest {

  public static void main(String[] args) {
    final Properties properties = createProperties();

    final ClientFactory clientFactory = new ClientFactory(properties);
    final Receiver receiver = clientFactory.createReceiver();
    receiver.forQueues("test");
    receiver.withListener(new MyMessageListener());
    receiver.start();
  }

  private static Properties createProperties() {
    final Properties properties = new Properties();
    properties.setProperty("hosts", "127.0.0.1");
    properties.setProperty("username", "guest");
    properties.setProperty("password", "guest");
    return properties;
  }

  static final class MyMessageListener implements MessageListener {

    @Override
    public void onMessage(Message message) {
      log.info("received '{}'", new String(message.getBody()));
    }

    private static final Logger log = LoggerFactory.getLogger(MyMessageListener.class);
  }

  private HHConsumerTest() {
  }
}
