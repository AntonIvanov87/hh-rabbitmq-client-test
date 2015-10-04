package ru.hh.rabbitmq.client.test.receiver;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public final class NativeConsumerTest {

  public static void main(String[] args) throws InterruptedException {
    final ConnectionFactory connectionFactory = createConnectionFactory("localhost");
    playWithConnectionFactory(connectionFactory);
  }

  private static ConnectionFactory createConnectionFactory(final String host) {
    final ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost(host);
    connectionFactory.setConnectionTimeout(1000);
    connectionFactory.setRequestedHeartbeat(1);
    return connectionFactory;
  }

  private static void playWithConnectionFactory(final ConnectionFactory connectionFactory) throws InterruptedException {
    final ConnectionOpener connectionOpener = connectionFactory::newConnection;
    final Consumer<Connection> consumerCreator = connection -> {
      try {
        playWithConnection(connection);
      } catch (IOException e) {
        throw new RuntimeException("failed to play with connection", e);
      }
    };
    recoveringFromNetworkFailures(connectionOpener, consumerCreator);
  }

  @FunctionalInterface
  private interface ConnectionOpener {
    Connection open() throws IOException, TimeoutException;
  }

  private static void recoveringFromNetworkFailures(
          final ConnectionOpener connectionOpener,
          final Consumer<Connection> consumerCreator) throws InterruptedException {

    final Connection connection = openConnectionUntilSuccess(connectionOpener);
    connection.addShutdownListener(cause -> {
      log.info("connection received shutdown signal: {}, reopening", cause.toString(), cause);
      try {
        recoveringFromNetworkFailures(connectionOpener, consumerCreator);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    consumerCreator.accept(connection);
  }

  private static Connection openConnectionUntilSuccess(final ConnectionOpener connectionOpener) throws InterruptedException {
    while (true) {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }

      try {
        final Connection connection = connectionOpener.open();
        testConnection(connection);
        return connection;
      } catch (IOException | TimeoutException e) {
        log.warn("failed to open and test connection: {}, retrying after sleep", e.toString(), e);
        Thread.sleep(2000);
      }
    }
  }

  private static void testConnection(final Connection connection) throws IOException, TimeoutException {
    final Channel channel = connection.createChannel();
    channel.close();
  }

  private static void playWithConnection(final Connection connection) throws IOException {
    final Channel channel = connection.createChannel();
    playWithChannel(channel);
  }

  private static void playWithChannel(final Channel channel) throws IOException {
    channel.basicConsume("test", new TestConsumer(channel));
    log.info("created consumer");
  }

  private static final class TestConsumer extends DefaultConsumer {

    TestConsumer(final Channel channel) {
      super(channel);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
      if (!getChannel().isOpen()) {
        // log.warn("channel is not open, skipping incoming message");
        return;
      }
      final long deliveryTag = envelope.getDeliveryTag();
      getChannel().basicAck(deliveryTag, false);
      log.info("consumed, delivery tag {}, body '{}'", deliveryTag, new String(body));
      try {
        Thread.sleep(1_000L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  static final Logger log = LoggerFactory.getLogger(NativeConsumerTest.class);

  private NativeConsumerTest() {
  }
}
