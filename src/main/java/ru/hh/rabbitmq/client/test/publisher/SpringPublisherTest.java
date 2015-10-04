package ru.hh.rabbitmq.client.test.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static java.lang.System.currentTimeMillis;

public final class SpringPublisherTest {

  public static void main(String[] args) {
    final CachingConnectionFactory connectionFactory = createConnectionFactory();
    try {
      final RabbitTemplate rabbitTemplate = createRabbitTemplate(connectionFactory);
      playWithAmqpTemplate(rabbitTemplate);
    } finally {
      connectionFactory.destroy();
    }
  }

  private static CachingConnectionFactory createConnectionFactory() {
    final CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
    connectionFactory.setConnectionTimeout(1000);
    connectionFactory.setRequestedHeartBeat(1);
    return connectionFactory;
  }

  private static RabbitTemplate createRabbitTemplate(final ConnectionFactory connectionFactory) {
    final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
    return rabbitTemplate;
  }

  private static void playWithAmqpTemplate(final RabbitTemplate rabbitTemplate) {

    for(int i=0; i<10; i++) {
      rabbitTemplate.convertAndSend("test", "warm up");
    }
    log.info("warmed up, starting...");

    final long start = currentTimeMillis();

    for(int i=0; i<1000; i++) {
      final String messageText = "message " + i;
      rabbitTemplate.convertAndSend("test", messageText);
      // log.info("sent '{}'", messageText);

      // Thread.sleep(2000L);
    }

    final long end = currentTimeMillis();
    log.info("sent {} messages in {} ms", 1000, end-start);
  }

  private static final Logger log = LoggerFactory.getLogger(SpringPublisherTest.class);

  private SpringPublisherTest() {
  }
}
