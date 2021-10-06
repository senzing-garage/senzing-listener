package com.senzing.listener.senzing.communication;

import com.senzing.listener.senzing.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.senzing.communication.rabbitmq.RabbitMQConsumer;
import com.senzing.listener.senzing.communication.sqs.SQSConsumer;

/**
 * A factory class for creating instances of {@link MessageConsumer}.
 */
public class MessageConsumerFactory {
  /**
   * Generates a message consumer based on consumer type.
   * 
   * @param consumerType The consumer type.
   *
   * @param config The configuration string for the {@link MessageConsumer}.
   *
   * @return The {@link MessageConsumer} that was created.
   * 
   * @throws MessageConsumerSetupException If a failure occurs.
   */
  public static MessageConsumer generateMessageConsumer(
      ConsumerType consumerType,
      String config)
      throws MessageConsumerSetupException
  {
    MessageConsumer consumer = null;

    switch (consumerType) {
      case RABBIT_MQ:
        consumer = RabbitMQConsumer.generateRabbitMQConsumer();
        break;
      case SQS:
        consumer = SQSConsumer.generateSQSConsumer();
        break;
    }
    if (consumer == null) {
      StringBuilder errorMessage
          = new StringBuilder("Invalid message consumer specified: ")
          .append(consumerType);
      throw new MessageConsumerSetupException(errorMessage.toString());
    }
    else {
        consumer.init(config);
        return consumer;
    }
  }
}
