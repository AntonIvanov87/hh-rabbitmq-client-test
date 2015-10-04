package ru.hh.rabbitmq.client.test.publisher;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.spring.ClientFactory;
import ru.hh.rabbitmq.spring.send.Publisher;
import ru.hh.rabbitmq.spring.send.Destination;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.System.currentTimeMillis;

public final class HHPublisherTest {

  public static void main(String[] args) throws InterruptedException, TimeoutException, ExecutionException {

    final Properties properties = createProperties();

    final Publisher publisher = new ClientFactory(properties).createPublisherBuilder().build();
    publisher.startAsync().awaitRunning(5L, TimeUnit.SECONDS);
    try {
      playWithPublisher(publisher);
    } finally {
      publisher.stopAsync().awaitTerminated(5L, TimeUnit.SECONDS);
    }
  }

  private static Properties createProperties() {
    final Properties properties = new Properties();
    properties.setProperty("hosts", "127.0.0.1");
    properties.setProperty("username", "guest");
    properties.setProperty("password", "guest");
    return properties;
  }

  private static void playWithPublisher(final Publisher publisher) throws ExecutionException, InterruptedException {

    final Destination destination = new Destination("", "test");

    for (int i=0; i<10; i++) {
      publisher.send(destination, "warm up").get();
    }
    log.info("warmed up, starting...");

    final long start = currentTimeMillis();

    for (int i = 0; i<1000; i++) {

      final String message = "message " + i;
      final ListenableFuture<Void> future = publisher.send(destination, message);
      future.get();
      log.info("sent '{}'", message);

      Thread.sleep(1000L);
    }

    final long end = currentTimeMillis();
    log.info("published {} messages in {} ms", 1000, end-start);
  }

  private static final Logger log = LoggerFactory.getLogger(HHPublisherTest.class);

  private HHPublisherTest() {
  }
}
