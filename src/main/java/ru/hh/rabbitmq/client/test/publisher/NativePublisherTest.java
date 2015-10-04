package ru.hh.rabbitmq.client.test.publisher;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static java.lang.System.currentTimeMillis;

public final class NativePublisherTest {

  public static void main(String[] args) throws InterruptedException, IOException, TimeoutException {

    final ConnectionFactory connectionFactory = createConnectionFactory("localhost");

    final Connection connection = connectionFactory.newConnection();
    try {
      playWithConnection(connection);
    } finally {
      if (connection.isOpen()) {
        try {
          connection.close(2000);
        } catch (ShutdownSignalException e) {
          log.warn("failed to close connection", e);
        }
      }
    }
  }

  private static ConnectionFactory createConnectionFactory(final String host) {
    final ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost(host);
    connectionFactory.setConnectionTimeout(1000);
    connectionFactory.setRequestedHeartbeat(1);
    return connectionFactory;
  }

  private static void playWithConnection(final Connection connection) throws IOException, InterruptedException, TimeoutException {

    final Channel channel = connection.createChannel();
    try {
      playWithChannel(channel);
    } finally {
      if (channel.isOpen()) {
        try {
          channel.close();
        } catch (ShutdownSignalException e) {
          log.warn("failed to close channel", e);
        }
      }
    }
  }

  private static void playWithChannel(final Channel channel) throws IOException {

    for (int i=0; i<10; i++) {
      channel.basicPublish("", "test", createMessageProperties(), "warm up".getBytes());
    }
    log.info("warmed up, starting...");

    final long start = currentTimeMillis();

    for (int i=0; i<10000; i++) {
      final String message = "message " + i;
      channel.basicPublish("", "test", createMessageProperties(), message.getBytes());
      System.out.println("published '" + message + "'");

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    final long end = currentTimeMillis();
    log.info("sent {} messages in {} ms", 10000, end - start);
  }

  private static AMQP.BasicProperties createMessageProperties() {
    return COMMON_PROPERTIES.builder()
            .timestamp(new Date())
            .messageId(UUID.randomUUID().toString())
            .build();
  }

  private static final AMQP.BasicProperties COMMON_PROPERTIES = MessageProperties.PERSISTENT_TEXT_PLAIN.builder()
          .appId("native-publisher-test")
          .build();

  private static final Logger log = LoggerFactory.getLogger(NativePublisherTest.class);

  private NativePublisherTest() {
  }
}
