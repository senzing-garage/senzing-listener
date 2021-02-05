package com.senzing.listener.senzing.communication;

import com.senzing.listener.senzing.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.senzing.communication.rabbitmq.RabbitMQConsumer;
import com.senzing.listener.senzing.communication.sqs.SQSConsumer;

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
    MessageConsumer consumer = null;

    switch (consumerType) {
      case rabbitmq:
        consumer = RabbitMQConsumer.generateRabbitMQConsumer();
        break;
      case sqs:
        consumer = SQSConsumer.generateSQSConsumer();
        break;
    }
    if (consumer == null) {
      StringBuilder errorMessage = new StringBuilder("Invalid message consumer specified: ").append(consumerType.toString());
      throw new MessageConsumerSetupException(errorMessage.toString());
    }
    else {
        consumer.init(config);
        return consumer;
    }
  }
}
