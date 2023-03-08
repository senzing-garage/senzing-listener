package com.senzing.listener.communication;

import com.senzing.listener.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.communication.rabbitmq.RabbitMQConsumer;
import com.senzing.listener.communication.sqs.SQSConsumer;

import javax.json.JsonObject;

/**
 * A factory class for creating instances of {@link MessageConsumer}.
 */
public class MessageConsumerFactory {
  /**
   * Generates a message consumer based on consumer type.
   * 
   * @param consumerType The consumer type.
   *
   * @param config The {@link JsonObject} descrbing the configuration for the
   *               {@link MessageConsumer}.
   *
   * @return The {@link MessageConsumer} that was created.
   * 
   * @throws MessageConsumerSetupException If a failure occurs.
   */
  public static MessageConsumer generateMessageConsumer(
      ConsumerType  consumerType,
      JsonObject    config)
      throws MessageConsumerSetupException
  {
    MessageConsumer consumer = null;

    switch (consumerType) {
      case RABBIT_MQ:
        consumer = new RabbitMQConsumer();
        break;
      case SQS:
        consumer = new SQSConsumer();
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
