package com.senzing.listener.communication.rabbitmq;

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
import com.senzing.listener.communication.MessageConsumer;
import com.senzing.listener.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.service.ListenerService;
import com.senzing.listener.service.exception.ServiceExecutionException;

/**
 * A consumer for RabbitMQ.
 */
public class RabbitMQConsumer implements MessageConsumer {
  /**
   * The initialization parameter for the RabbitMQ host.
   */
  public static final String MQ_HOST = "mqHost";

  /**
   * The initialization parameter for the RabbitMQ user name.
   */
  public static final String MQ_USER = "mqUser";

  /**
   * The initialization parameter for the RabbitMQ password.
   */
  public static final String MQ_PASSWORD = "mqPassword";

  /**
   * The initialization parameter for the RabbitMQ queue name.
   */
  public static final String MQ_QUEUE = "mqQueue";

  /**
   * The name of the queue.
   */
  private String queueName;

  /**
   * The host or IP address for the queue.
   */
  private String queueHost;

  /**
   * The user name for authenticating with the queue host.
   */
  private String userName;

  /**
   * The password for the authenticating with the queue host.
   */
  private String password;

  /**
   * The {@link ListenerService} for receiving messages.
   */
  private ListenerService service;

  /**
   * Constant for UTF-8 encoding.
   */
  private static final String UTF8_ENCODING = "UTF-8";

  /**
   * Generates a Rabbit MQ consumer.
   * 
   * @return The created {@link RabbitMQConsumer}.
   */
  public static RabbitMQConsumer generateRabbitMQConsumer() {
    return new RabbitMQConsumer();
  }

  /**
   * Private default constructor.
   */
  private RabbitMQConsumer() {
    // do nothing
  }

  /**
   * Initializes the object. It sets the object up based on configuration
   * passed in.
   * <p>
   * The configuration is in JSON format:
   * <pre>
   * {
   *   "mqQueue": "&lt;queue name&gt;",              # required value
   *   "mqHost": "&lt;host name or IP address&gt;",  # required value
   *   "mqUser": "&lt;user name&gt;",                # not required
   *   "mqPassword": "&lt;password&gt;"              # not required
   * }
   * </pre>
   * @param config Configuration string containing the needed information to
   *               connect to RabbitMQ.
   *
   * @throws MessageConsumerSetupException If a failure occurs.
   */
  public void init(String config) throws MessageConsumerSetupException {
    try {
      JsonReader reader = Json.createReader(new StringReader(config));
      JsonObject configObject = reader.readObject();
      queueName = getConfigValue(configObject, MQ_QUEUE, true);
      queueHost = getConfigValue(configObject, MQ_HOST, true);
      userName = getConfigValue(configObject, MQ_USER, false);
      password = getConfigValue(configObject, MQ_PASSWORD, false);
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
   * @throws MessageConsumerSetupException If a failure occurs.
   */
  @Override
  public void consume(ListenerService service)
      throws MessageConsumerSetupException
  {
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

  private Channel getChannel(Connection connection, String queueName)
      throws IOException
  {
    try {
      return declareQueue(connection, queueName, true, false, false, null);
    } catch (IOException e) {
      // Possibly the queue is already declared and as non-durable. Retry with durable = false.
      return declareQueue(connection, queueName, false, false, false, null);
    }
  }

  private Channel declareQueue(Connection           connection,
                               String               queueName,
                               boolean              durable,
                               boolean              exclusive,
                               boolean              autoDelete,
                               Map<String, Object>  arguments)
      throws IOException
  {
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

  private String getConfigValue(JsonObject  configObject,
                                String      key,
                                boolean     required)
      throws MessageConsumerSetupException
  {
    String configValue = configObject.getString(key, null);
    if (required && (configValue == null || configValue.isEmpty())) {
      StringBuilder message
          = new StringBuilder("Following configuration parameter missing: ")
          .append(key);
      throw new MessageConsumerSetupException(message.toString());
    }
    return configValue;
  }
}
