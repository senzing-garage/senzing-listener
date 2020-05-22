package com.senzing.listener.senzing.communication.rabbitmq;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.senzing.listener.senzing.communication.MessageConsumer;
import com.senzing.listener.senzing.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.senzing.data.ConsumerCommandOptions;
import com.senzing.listener.senzing.service.ListenerService;
import com.senzing.listener.senzing.service.exception.ServiceExecutionException;

/**
 * A consumer for RabbidMQ.
 */
public class RabbitMQConsumer implements MessageConsumer {

  private String queueName;
  private String queueHost;
  private String userName;
  private String password;

  ListenerService service;

  private final String UTF8_ENCODING = "UTF-8";

  /**
   * Generates a Rabbit MQ consumer.
   * 
   * @return
   * 
   * @throws MessageConsumerSetupException
   */
  public static RabbitMQConsumer generateRabbitMQConsumer() {
    return new RabbitMQConsumer();
  }

  private RabbitMQConsumer() {
  }

  /**
   * Initializes the object. It sets the object up based on configuration passed in.
   * 
   * @param config Configuration string containing the needed information to connect to RabbitMQ.
   * The configuration is in JSON format:
   * {
   *   "mqQueue":"<queue name>",              # required value
   *   "mqHost":"<host name or IP address>",  # required value
   *   "mqUser":"<user name>",                # not required
   *   "mqPassword":"<password>"              # not required
   * }
   *
   * @throws MessageConsumerSetupException
   */
  public void init(String config) throws MessageConsumerSetupException {
    try {
      JsonReader reader = Json.createReader(new StringReader(config));
      JsonObject configObject = reader.readObject();
      queueName = getConfigValue(configObject, ConsumerCommandOptions.MQ_QUEUE, true);
      queueHost = getConfigValue(configObject, ConsumerCommandOptions.MQ_HOST, true);
      userName = getConfigValue(configObject, ConsumerCommandOptions.MQ_USER, false);
      password = getConfigValue(configObject, ConsumerCommandOptions.MQ_PASSWORD, false);
    } catch (RuntimeException e) {
      throw new MessageConsumerSetupException(e);
    }
  }

  /**
   * Sets up a RabbitMQ consumer and then receives messages from RabbidMQ and
   * feeds to service.
   * 
   * @param service Processes messages
   * 
   * @throws MessageConsumerSetupException
   */
  @Override
  public void consume(ListenerService service) throws MessageConsumerSetupException {

    this.service = service;

    try {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(queueHost);
      if (userName != null && !userName.isEmpty()) {
        factory.setUsername(userName);
        factory.setPassword(password);
      }
      Connection connection = factory.newConnection();
      Channel channel = getChannel(connection, queueName);

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), UTF8_ENCODING);
        try {
          processMessage(message);
        } finally {
          boolean ackMultiple = false;
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), ackMultiple);
        }
      };

      boolean autoAck = false;
      channel.basicConsume(queueName, autoAck, deliverCallback, consumerTag -> {
      });

    } catch (IOException | TimeoutException e) {
      throw new MessageConsumerSetupException(e);
    }

  }

  private Channel getChannel(Connection connection, String queueName) throws IOException {
    try {
      return declareQueue(connection, queueName, true, false, false, null);
    } catch (IOException e) {
      // Possibly the queue is already declared and as non-durable. Retry with durable = false.
      return declareQueue(connection, queueName, false, false, false, null);
    }
  }

  private Channel declareQueue(Connection connection, String queueName, boolean durable, boolean exclusive,
      boolean autoDelete, Map<String, Object> arguments) throws IOException {
    Channel channel = connection.createChannel();
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);
    return channel;
  }

  private void processMessage(String message) {
    try {
      service.process(message);
    } catch (ServiceExecutionException e) {
      e.printStackTrace();
    }
  }

  private String getConfigValue(JsonObject configObject, String key, boolean required) throws MessageConsumerSetupException {
    String configValue = configObject.getString(key, null);
    if (required && (configValue == null || configValue.isEmpty())) {
      StringBuilder message = new StringBuilder("Following configuration parameter missing: ").append(key);
      throw new MessageConsumerSetupException(message.toString());
    }
    return configValue;
  }
}
