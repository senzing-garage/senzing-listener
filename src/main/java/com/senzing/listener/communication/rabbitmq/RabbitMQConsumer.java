package com.senzing.listener.communication.rabbitmq;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.json.JsonObject;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.senzing.listener.communication.AbstractMessageConsumer;
import com.senzing.listener.communication.exception.MessageConsumerException;
import com.senzing.listener.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.service.ListenerService;

import static com.senzing.io.IOUtilities.UTF_8;

/**
 * A consumer for RabbitMQ.  The initialization parameters include:
 * <ul>
 *   <li>{@link #MQ_HOST_KEY} (required)</li>
 *   <li>{@link #MQ_PORT_KEY} (optional, default port used if not specified)</li>
 *   <li>{@link #MQ_VIRTUAL_HOST_KEY} (optional)</li>
 *   <li>{@link #MQ_QUEUE_KEY} (required)</li>
 *   <li>{@link #MQ_USER_KEY} (optional, no authentication if not specified)</li>
 *   <li>{@link #MQ_PASSWORD_KEY} (required if {@link #MQ_USER_KEY} is specified)</li>
 * </ul>
 */
public class RabbitMQConsumer extends AbstractMessageConsumer<Delivery> {
  /**
   * Constant for the "auto ack" parameter.
   */
  private static final boolean AUTO_ACK = false;

  /**
   * Constant for "ack multiple" parameter.
   */
  private static final boolean MULTI_ACK = false;

  /**
   * The initialization parameter for the RabbitMQ host.
   */
  public static final String MQ_HOST_KEY = "mqHost";

  /**
   * The initialization parameter for the RabbitMQ port.
   */
  public static final String MQ_PORT_KEY = "mqPort";

  /**
   * The initialization parameter for the RabbitMQ user name.
   */
  public static final String MQ_USER_KEY = "mqUser";

  /**
   * The initialization parameter for the RabbitMQ password.
   */
  public static final String MQ_PASSWORD_KEY = "mqPassword";

  /**
   * The initialization parameter for the RabbitMQ queue name.
   */
  public static final String MQ_QUEUE_KEY = "mqQueue";

  /**
   * The initialization parameter for the RabbitMQ virtual host.
   */
  public static final String MQ_VIRTUAL_HOST_KEY = "mqVirtualHost";

  /**
   * The name of the queue.
   */
  private String queueName = null;

  /**
   * The host or IP address for the queue.
   */
  private String queueHost = null;

  /**
   * The port to use for connecting to RabbitMQ if not the default.
   */
  private Integer queuePort = null;

  /**
   * The optional virtual host.
   */
  private String virtualHost = null;

  /**
   * The user name for authenticating with the queue host.
   */
  private String userName = null;

  /**
   * The password for the authenticating with the queue host.
   */
  private String password = null;

  /**
   * The RabbitMQ {@link Channel} to use.
   */
  private Channel channel = null;

  /**
   * The consumer tag when consuming, or <code>null</code> when not.
   */
  private String consumerTag = null;

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
  public RabbitMQConsumer() {
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
   *   "mqPort:L "&lt;port&gt;",                     # not required
   *   "mqVirtualHost": "&lt;virtual host name&gt;", # not required
   *   "mqUser": "&lt;user name&gt;",                # not required
   *   "mqPassword": "&lt;password&gt;"              # not required
   * }
   * </pre>
   * @param config Configuration string containing the needed information to
   *               connect to RabbitMQ.
   *
   * @throws MessageConsumerSetupException If a failure occurs.
   */
  @Override
  protected void doInit(JsonObject config) throws MessageConsumerSetupException
  {
    try {
      // get the queue name
      this.queueName = getConfigString(config, MQ_QUEUE_KEY, true);

      // get the queue host
      this.queueHost = getConfigString(config, MQ_HOST_KEY, true);

      // get the queue port
      this.queuePort = getConfigInteger(config,
                                        MQ_PORT_KEY,
                                        false,
                                        1);

      // get the virtual host
      this.virtualHost = getConfigString(config,
                                         MQ_VIRTUAL_HOST_KEY,
                                         false);

      // get the user name (optional)
      this.userName = getConfigString(config, MQ_USER_KEY, false);
      if (this.userName != null && this.userName.trim().length() == 0) {
        this.userName = null;
      }

      // get the password (optional)
      this.password = getConfigString(config, MQ_PASSWORD_KEY, false);
      if (this.password != null && this.password.trim().length() == 0) {
        this.password = null;
      }

      // check if the and user name and password are not consistent
      if ((this.userName != null && this.password == null)
          || (this.userName == null && this.password != null))
      {
        throw new MessageConsumerSetupException(
            "Either both or neither of the " + MQ_USER_KEY + " and "
            + MQ_PASSWORD_KEY + " configuration parameters must be provided.");
      }

    } catch (MessageConsumerSetupException e) {
      throw e;

    } catch (Exception e) {
      throw new MessageConsumerSetupException(e);
    }
  }

  /**
   * Sets up a RabbitMQ consumer and then receives messages from RabbidMQ and
   * feeds to service.
   * 
   * @param service Processes messages
   * 
   * @throws MessageConsumerException If a failure occurs.
   */
  @Override
  protected void doConsume(ListenerService service)
      throws MessageConsumerException
  {
    try {
      // construct the factory
      ConnectionFactory factory = new ConnectionFactory();

      // set the host
      factory.setHost(this.queueHost);

      // optionally set the port, otherwise uses RabbitMQ default
      if (this.queuePort != null) {
        factory.setPort(this.queuePort);
      }

      // optionally set the virtual host
      if (this.virtualHost != null) {
        factory.setVirtualHost(this.virtualHost);
      }

      // if we have credentials then set them
      if (this.userName != null && !this.userName.isEmpty()) {
        factory.setUsername(this.userName);
        factory.setPassword(this.password);
      }

      Connection connection = factory.newConnection();
      this.channel = this.getChannel(connection, queueName);

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        // enqueue the next message for processing -- this call may wait
        // for enough room in the queue for the messages to be enqueued
        this.enqueueMessages(service, delivery);
      };

      // this call will run in the background until basicCancel() is called
      this.consumerTag = channel.basicConsume(
          queueName, AUTO_ACK, deliverCallback, consumerTag -> { });

    } catch (IOException | TimeoutException e) {
      throw new MessageConsumerSetupException(e);
    }
  }

  @Override
  protected String extractMessageBody(Delivery message) {
    try {
      return new String(message.getBody(), UTF_8);

    } catch (UnsupportedEncodingException cannotHappen) {
      throw new IllegalStateException(
          "UTF-8 encoding should always be supported, but is not.");
    }
  }

  @Override
  protected void disposeMessage(Delivery message) {
    try {
      this.channel.basicAck(message.getEnvelope().getDeliveryTag(), MULTI_ACK);
    } catch (IOException e) {
      System.err.println();
      System.err.println("***************************************************");
      System.err.println("Ignoring exception while acknowledging message:");
      e.printStackTrace();
    }
  }

  @Override
  protected void doDestroy() {
    if (this.channel != null && this.consumerTag != null) {
      try {
        this.channel.basicCancel(this.consumerTag);

      } catch (IOException e) {
        System.err.println();
        System.err.println("***************************************************");
        System.err.println("Ignoring exception while destroying:");
        e.printStackTrace();

      } finally {
        this.consumerTag = null;
      }
    }
  }

  private Channel getChannel(Connection connection, String queueName)
      throws IOException
  {
    try {
      return this.declareQueue(connection,
                               queueName,
                               true,
                               false,
                               false,
                               null);

    } catch (IOException e) {
      // Possibly the queue is already declared and as non-durable.
      // Retry with durable = false.
      return declareQueue(connection,
                          queueName,
                          false,
                          false,
                          false,
                          null);
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
}
