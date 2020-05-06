package com.senzing.listener.senzing.communication;

import com.senzing.listener.senzing.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.senzing.communication.rabbitmq.RabbitMQConsumer;

public class MessageConsumerFactory {

  /**
   * Generates a message consumer based on consumer type.
   * 
   * @param consumerType
   * 
   * @return
   * 
   * @throws MessageConsumerSetupException
   */
  public static MessageConsumer generateMessageConsumer(ConsumerType consumerType, String config) throws MessageConsumerSetupException {
    if (consumerType == ConsumerType.rabbitmq) {
      RabbitMQConsumer consumer = RabbitMQConsumer.generateRabbitMQConsumer();
      consumer.init(config);
      return consumer;
    }

    StringBuilder errorMessage = new StringBuilder("Invalid message consumer specified: ").append(consumerType.toString());
    throw new MessageConsumerSetupException(errorMessage.toString());
  }
}
