package com.senzing.listener.communication.sql;

import java.io.File;
import java.util.List;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.naming.NameNotFoundException;

import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.DatabaseType;
import com.senzing.sql.Connector;
import com.senzing.sql.SQLiteConnector;
import com.senzing.sql.PostgreSqlConnector;
import com.senzing.sql.ConnectionPool;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.PoolConnectionProvider;

import com.senzing.text.TextUtilities;
import com.senzing.listener.communication.AbstractMessageConsumer;
import com.senzing.listener.communication.exception.MessageConsumerException;
import com.senzing.listener.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.service.MessageProcessor;
import com.senzing.util.AccessToken;
import com.senzing.util.JsonUtilities;
import com.senzing.naming.Registry;

import static java.lang.Boolean.*;
import static com.senzing.sql.SQLUtilities.*;
import static com.senzing.util.LoggingUtilities.*;
import static com.senzing.listener.communication.MessageConsumer.State.*;

/**
 * A consumer for a SQL-based message queue using a database table to hold
 * the pending messages.
 */
public class SQLConsumer extends AbstractMessageConsumer<LeasedMessage> {
  /**
   * Provides an interface for interacting with the message queue used
   * by the associated {@link SQLConsumer}. 
   */
  public static interface MessageQueue {
    /**
     * Checks if the message queue is empty.
     * 
     * @throws SQLException If a database failure occurs.
     */
    boolean isEmpty() throws SQLException;

    /**
     * Gets the number of messages currently in the message queue.  The
     * returned value will include leased messages.
     * 
     * @return The number of messages in the message queue (including leased
     *         messages).
     * 
     * @throws SQLException If a database failure occurs.
     */
    int getMessageCount() throws SQLException;

    /**
     * Enqueues a message on this {@link MessageQueue} so the associated
     * {@link SQLConsumer} can consume it.
     * 
     * @param message The message to enqueue.
     * 
     * @throws SQLException If a SQL failure occurs.
     */
    void enqueueMessage(String message) throws SQLException;

    /**
     * Gets the associated {@link SQLConsumer}.
     * 
     * @return The associated {@link SQLConsumer}.
     */
    SQLConsumer getSQLConsumer();
  }

  /**
   * Provides a {@link MessageQueue} implementation that is backed
   * by the {@link SQLClient} for the associated {@link SQLConsumer}.
   * 
   */
  protected class SimpleMessageQueue implements MessageQueue {
    /**
     * The {@link SQLClient} backing this instance.
     */
    private SQLClient client = null;

    /**
     * Constructs with the {@link SQLClient} to use.
     * 
     * @param client The {@link SQLClient} to use.
     */
    protected SimpleMessageQueue(SQLClient client) {
      this.client = client;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implemented to call {@link SQLClient#isQueueEmpty(Connection)}
     * on the backing {@link SQLClient}.
     * </p>
     */
    public boolean isEmpty() throws SQLException {
      Connection conn = null;
      try {
        conn = SQLConsumer.this.getConnection();

        return this.client.isQueueEmpty(conn);

      } finally {
        conn = close(conn);
      }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implemented to call {@link SQLClient#getMessageCount(Connection)}
     * on the backing {@link SQLClient}.
     * </p>
     */
    public int getMessageCount() throws SQLException {
      Connection conn = null;
      try {
        conn = SQLConsumer.this.getConnection();

        return this.client.getMessageCount(conn);

      } finally {
        conn = close(conn);
      }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implemented to call {@link SQLClient#insertMessage(Connection,String)}
     * on the backing {@link SQLClient}.
     * </p>
     */
    public void enqueueMessage(String message) throws SQLException
    {
      Connection conn = null;
      try {
        conn = SQLConsumer.this.getConnection();

        this.client.insertMessage(conn, message);

        conn.commit();

      } finally {
        conn = close(conn);
      }
    }

    /**
     * {@inheritDoc}
     */
    public SQLConsumer getSQLConsumer() {
      return SQLConsumer.this;
    }
  }

  /**
   * {@link Registry} used to register the {@link MessageQueue} instances
   * assocaited with each {@link SQLConsumer}.  In order to register the
   * {@link MessageQueue} the {@link #QUEUE_REGISTRY_NAME_KEY} initialization 
   * parameter must be provided for the {@link SQLConsumer}.
   */
  public static Registry<MessageQueue> MESSAGE_QUEUE_REGISTRY 
      = new Registry<>(false);

  /**
   * The initialization parameter key used to obtain the name for binding 
   * the {@link MessageQueue} instance for interacting with the backing
   * message queue of the {@link SQLConsumer} in the {@link 
   * #MESSAGE_QUEUE_REGISTRY}.  There is no default value for this
   * initialization parameter, if it is not specified then the {@link
   * MessageQueue} is not registered in the {@link #MESSAGE_QUEUE_REGISTRY}.
   * The {@link MessageQueue} is unbound when the {@link SQLConsumer} is
   * destroyed.
   */
  public static final String QUEUE_REGISTRY_NAME_KEY = "queueRegistryName";

  /**
   * The initialization parameter key for checking if the persistent store
   * of messages should be dropped / deleted and recreated during
   * initialization.  Values should be <code>true</code> or <code>false</code>.
   */
  public static final String CLEAN_DATABASE_KEY = "cleanDatabase";

  /**
   * The initialization parameter key for obtaining the {@link
   * ConnectionProvider} to use for connecting to the database from the
   * {@link ConnectionProvider#REGISTRY}.
   */
  public static final String CONNECTION_PROVIDER_KEY = "connectionProvider";

  /**
   * The initialization parameter to configure the maximum number of times to
   * retry a failed attempt to select messages from the database before
   * aborting consumption.
   */
  public static final String MAXIMUM_RETRIES_KEY = "maximumRetries";

  /**
   * The initialization parameter to configure the number of milliseconds to
   * wait to retry when a failure occurs.  This is only matters if the
   * configured {@linkplain #MAXIMUM_RETRIES_KEY failure threshold} is
   * greater than one (1).
   */
  public static final String RETRY_WAIT_TIME_KEY = "retryWaitTime";

  /**
   * The initialization parameter to configure the number of <b>seconds</b>
   * messages are leased on the database table before they become available
   * to another consumer instance.  If not configured then {@link 
   * #DEFAULT_LEASE_TIME} is used.  Specifying this initialization parameter
   * allows the clients to override.
   */
  public static final String LEASE_TIME_KEY = "leaseTime";

  /**
   * The initialization parameter to configure the maximum number of messages
   * to be leased from the database table at one time.  If not configured
   * then {@link #DEFAULT_MAXIMUM_LEASE_COUNT} is used.  Specifying this
   * initialization parameter allows clients to override.
   */
  public static final String MAXIMUM_LEASE_COUNT_KEY = "maximumLeaseCount";

  /**
   * The initialization parameter to configure the maximum number of 
   * <b>seconds</b> to sleep when the database queue is found to be empty 
   * in order to avoid a busy loop of constant queries.  The actual amount
   * of time used for sleep will progessively increase as the message
   * queue continues to be empty until it equals the configured maximum
   * number of seconds.  If not configured then {@link 
   * #DEFAULT_MAXIMUM_SLEEP_TIME} is used.  Specifying this initialization
   * parameter allows clients to override.
   */
  public static final String MAXIMUM_SLEEP_TIME_KEY = "maximumSleepTime";

  /**
   * The default number of times to retry failed SQS requests before aborting
   * consumption.  The default value is {@value}.  A different value can be set
   * via the {#link #MAXIMUM_RETRIES_KEY} initialization parameter.
   */
  public static final int DEFAULT_MAXIMUM_RETRIES = 0;

  /**
   * The default number of milliseconds to wait before retrying the SQS request
   * if the previous request failed.  The default value is {@value}.  A
   * different value can be set via the {@link #RETRY_WAIT_TIME_KEY} 
   * initialization parameter.
   */
  public static final long DEFAULT_RETRY_WAIT_TIME = 1000L;

 /**
   * The default number of seconds to lease a message in the database table,
   * preventing other consumers from obtaining it.  The default value is {@value}.
   * A different value can be set via the {@link #LEASE_TIME_KEY} initialization
   * parameter.
   */
  public static final int DEFAULT_LEASE_TIME = 1800;

  /**
   * The default maximum number of messages to be leased from the database table
   * at one time.  The default value is {@value}.  A different value can be set
   * via the {@link #MAXIMUM_LEASE_COUNT_KEY} initialization parameter.
   */
  public static final int DEFAULT_MAXIMUM_LEASE_COUNT = 100;

  /**
   * The default maximum number of second to sleep when an empty queue is
   * encountered in order to avoid a busy loop of querying the database.
   * The actual amount of time used for sleep will progessively increase 
   * as the message queue continues to be empty until it equals the
   * configured maximum number of seconds.  The default value is {@value}.
   * A different value can be set via the {@link #MAXIMUM_SLEEP_TIME_KEY}
   * initialization parameter.
   */
  public static final int DEFAULT_MAXIMUM_SLEEP_TIME = 10;

  /**
   * Defined constant for one second in milliseconds.
   */
  private static final long ONE_SECOND = 1000L;

  /**
   * The {@link ConnectionProvider} to use for obtaining
   * {@link Connection} instances.
   */
  private ConnectionProvider connectionProvider;

  /**
   * The {@link MessageQueue} for this instance.
   */
  private MessageQueue messageQueue;

  /**
   * The name for binding the {@link #messageQueue} in the {@link
   * #MESSAGE_QUEUE_REGISTRY}.
   */
  private String queueRegistryName = null;

  /**
   * The {@link AccessToken} for unbinding the {@link #messageQueue} from the
   * {@link #MESSAGE_QUEUE_REGISTRY}.
   */
  private AccessToken registryToken = null;

  /**
   * The {@link SQLClient} to use for interacting with the database.
   */
  private SQLClient sqlClient;
  
  /**
   * The consumption thread for this instance.
   */
  private Thread consumptionThread = null;

  /**
   * The maximum number of times to retry failed SQS requests before aborting
   * consumption.
   */
  private int maximumRetries = DEFAULT_MAXIMUM_RETRIES;

  /**
   * The number of milliseconds to wait before retrying the SQS request if the
   * previous request failed.
   */
  private long retryWaitTime = DEFAULT_RETRY_WAIT_TIME;

  /**
   * The configured number of seconds to lease messages from the database 
   * queue before they become available to other consumers.
   */
  private int leaseTime = DEFAULT_LEASE_TIME;

  /**
   * The configured maximum number of messages to lease at one time from the
   * database queue.
   */
  private int maximumLeaseCount = DEFAULT_MAXIMUM_LEASE_COUNT;

  /**
   * The configured maximum number of second to sleep when an empty queue is
   * encountered in order to avoid a busy loop of querying the database.
   * The actual amount of time used for sleep will progessively increase 
   * as the message queue continues to be empty until it equals the
   * configured maximum number of seconds.
   */
  private int maximumSleepTime = DEFAULT_MAXIMUM_SLEEP_TIME;

  /**
   * Private default constructor.
   */
  public SQLConsumer() {
    // do nothing
  }

  /**
   * Initializes the object. It sets the object up based on configuration
   * passed in.
   * <p>
   * The configuration is in JSON format:
   * <pre>
   * {
   *   "connectionProvider": "&lt;provider-registry-name&gt;",
   *   "cleanDatabase": "&lt;true|false&gt;",
   *   "maximumRetries": "&lt;retry-count&gt;"
   *   "retryWaitTime": "&lt;pause-milliseconds&gt;",
   *   "leaseTime": "&lt;lease-time-seconds&gt;",
   *   "maximumLeaseCount": "&lt;message-count&gt;",
   *   "maximumSleepTime": "&lt;sleep-time-seconds&gt;"
   * }
   * </pre>
   *
   * @param config Configuration string containing the needed information to
   *               connect to connect to the backing database to lease 
   *               messages and consume them.
   *
   * @throws MessageConsumerSetupException If an initialization failure occurs.
   */
  @Override
  protected void doInit(JsonObject config) throws MessageConsumerSetupException
  {
    try {
      // check if we are cleaning the database
      Boolean clean = getConfigBoolean(config, CLEAN_DATABASE_KEY, FALSE);

      // get the connection provider name
      String providerKey = getConfigString(config,
                                           CONNECTION_PROVIDER_KEY,
                                           true);


      try {
        this.connectionProvider = ConnectionProvider.REGISTRY.lookup(providerKey);
      } catch (NameNotFoundException e) {
        throw new MessageConsumerSetupException(
            "No ConnectionProvider was registered to the name specified by the "
            + "\"" + CONNECTION_PROVIDER_KEY + "\" initialization parameter: "
            + providerKey);
      }

      // get the failure threshold
      this.maximumRetries = getConfigInteger(config,
                                             MAXIMUM_RETRIES_KEY,
                                             0,
                                             DEFAULT_MAXIMUM_RETRIES);

      // get the retry wait time
      this.retryWaitTime = getConfigLong(config,
                                         RETRY_WAIT_TIME_KEY,
                                         0L,
                                         DEFAULT_RETRY_WAIT_TIME);

      // get the lease time
      this.leaseTime = getConfigInteger(config,
                                        LEASE_TIME_KEY,
                                        1,
                                        DEFAULT_LEASE_TIME);

      // get the maximum lease count
      this.maximumLeaseCount = getConfigInteger(config,
                                                MAXIMUM_LEASE_COUNT_KEY,
                                                1,
                                                DEFAULT_MAXIMUM_LEASE_COUNT);

      // get the maximum sleep time
      this.maximumSleepTime = getConfigInteger(config,
                                               MAXIMUM_SLEEP_TIME_KEY,
                                               1,
                                               DEFAULT_MAXIMUM_SLEEP_TIME);

      // initialize the SQLClient
      this.sqlClient = this.initSQLClient();

      // initialize the message queue interface
      this.messageQueue = this.initMessageQueue();

      // ensure the schema exists
      this.ensureSchema(clean);

      // optionally register the MessageQueue interface
      this.queueRegistryName = getConfigString(config, 
                                               QUEUE_REGISTRY_NAME_KEY,
                                               false);

      if (this.queueRegistryName != null) {
        this.registryToken = MESSAGE_QUEUE_REGISTRY.bind(
          this.queueRegistryName, this.messageQueue);
      }

    } catch (Exception e) {
      throw new MessageConsumerSetupException(e);
    }
  }

  /**
   * Gets a JDBC {@link Connection} to use.  Typically these are obtained from
   * a backing pool so repeated calls to this function without closing the
   * previously obtained {@link Connection} instances could exhaust the pool.
   * This may block until a {@link Connection} is available.
   *
   * @return The {@link Connection} that was obtained.
   *
   * @throws SQLException If a JDBC failure occurs.
   */
  protected Connection getConnection() throws SQLException {
    return this.connectionProvider.getConnection();
  }

  /**
   * Determines the {@link SQLClient} to use from the metadata obtained
   * from the JDBC {@link Connection} via {@link #getConnection()} and
   * returns the {@link SQLClient} instance.
   * 
   * @return The {@link SQLClient} to use.
   * 
   * @throws MessageConsumerSetupException If a failure occurs.
   */
  protected SQLClient initSQLClient() throws MessageConsumerSetupException
  {
    Connection conn = null;
    try {
      // get a connection
      conn = this.getConnection();

      // set the database type
      DatabaseType databaseType = DatabaseType.detect(conn);

      // create the SQLClient instance
      switch (databaseType) {
        case POSTGRESQL:
          return new PostgreSQLClient();
        case SQLITE:
          return new SQLiteClient();
        default:
          throw new MessageConsumerSetupException(
            "The configured ConnectionProvider is associated with unsupported "
            + "database type.  databaseType=[ " + databaseType + " ]");
      }

    } catch (SQLException e) {
      throw new MessageConsumerSetupException(
        "Encountered a SQL failure during initialization.", e);

    } finally {
      conn = close(conn);
    }
  }

  /**
   * Gets the {@link SQLClient} used by this instance for interacting with the 
   * backing database.  This returns <code>null</code> if the {@link SQLClient}
   * has not yet been initialized.
   * 
   * @return The {@link SQLClient} used by this instance for interacting with 
   *         the backing database.
   */
  protected SQLClient getSQLClient() {
    return this.sqlClient;
  }

  /**
   * Creates and initializes the {@link MessageQueue} instance to use with
   * this {@link SQLConsumer}.
   * 
   * @return The {@link MessageQueue} instance that was created to be used
   *         with this {@link SQLConsumer}.
   */
  protected MessageQueue initMessageQueue() {
    return new SimpleMessageQueue(this.getSQLClient());
  }

  /**
   * Gets the {@link MessageQueue} interface for interacting with the 
   * backing message queue for this {@link SQLConsumer}.
   * 
   * @return The {@link MessageQueue} interface for interacting with the 
   *         backing message queue for this {@link SQLConsumer}.
   */
  public MessageQueue getMessageQueue() {
    return this.messageQueue;
  }

  /**
   * Ensures the schema exists and alternatively drops the existing the schema
   * and recreates it.  This is called from {@link #doInit(JsonObject)}.
   *
   * @param recreate <code>true</code> if the existing schema should be
   *                 dropped, otherwise <code>false</code>.
   *
   * @throws SQLException If a failure occurs.
   */
  protected void ensureSchema(boolean recreate) throws SQLException {
    Connection conn = null;
    try {
      // get the connection
      conn = this.getConnection();

      // get the SQLClient
      SQLClient sqlClient = this.getSQLClient();

      // ensure the schema exists
      sqlClient.ensureSchema(conn, recreate);

    } finally {
      conn = close(conn);
    }
  }

  /**
   * Returns the maximum number times failed attempts to connect to the database
   * will be retried before aborting message consumption.  This defaults to {@link
   * #DEFAULT_MAXIMUM_RETRIES} and can be configured via the
   * {@link #MAXIMUM_RETRIES_KEY} configuration parameter.
   *
   * @return The maximum number of times failed attempts to connect to the database
   *         will be retried before aborting message consumption.
   */
  public int getMaximumRetries() {
    return this.maximumRetries;
  }

  /**
   * Returns the number of milliseconds to wait between database query retries
   * when a failure occurs.  This defaults to {@link #DEFAULT_RETRY_WAIT_TIME}
   * and can be configured via the {@link #RETRY_WAIT_TIME_KEY} configuration
   * parameter.
   *
   * @return The number of milliseconds to wait between databae query retries
   *         when a failure occurs.
   */
  public long getRetryWaitTime() {
    return this.retryWaitTime;
  }

  /**
   * Gets the number of <b>seconds</b> messages will be leased from the
   * database queue, preventing other processores from consuming those same
   * messages until the lease has expired.
   *
   * @return The number of <b>seconds</b> messages will be leased from the
   *         database queue, preventing other processores from consuming those
   *         same messages until the lease has expired.
   */
  public int getLeaseTime() { return this.leaseTime; }

  /**
   * Gets the maximum number of messages to lease from the database queue
   * at one time.
   * 
   * @return The maximum number of messages to lease from the database 
   *         queue at one time.
   */
  public int getMaximumLeaseCount() { 
    return this.maximumLeaseCount;
  }

  /**
   * Gets the maximum number of <b>seconds</b> to sleep when an empty queue
   * is encountered.   The actual amount of time used for sleep will 
   * progessively increase as the message queue continues to be empty until
   * it equals the configured maximum number of seconds.
   * 
   * @return The maximum number of <b>seconds</b> to sleep when an empty queue
   *         is encountered.
   */
  public int getMaximumSleepTime() {
    return this.maximumSleepTime;
  }

  /**
   * Creates a virtually unique lease ID.
   *
   * @return A new lease ID to use.
   */
  protected String generateLeaseId() {
    long pid = ProcessHandle.current().pid();
    StringBuilder sb = new StringBuilder();
    sb.append(pid).append("|").append(Instant.now().toString()).append("|");
    sb.append(TextUtilities.randomAlphanumericText(50));
    return sb.toString();
  }

  /**
   * Handles an SQL failure and checks if consumption should be aborted.
   *
   * @param failureCount The number of consecutive failures so far.
   * @param failure The {@link Exception} that was thrown if available,
   *                otherwise <code>null</code>.
   * @return <code>true</code> if consumption should abort, otherwise
   *         <code>false</code>.
   */
  protected boolean handleFailure(int failureCount, Exception failure)
  {
    // get the maximum number of retries
    int maxRetries = this.getMaximumRetries();

    logWarning(failure,
               "FAILURE DETECTED: " + failureCount + " of " + maxRetries
                   + " consecutive failure(s)");

    // check if we have exceeded the maximum failure count
    if (failureCount > maxRetries) {
      // return true to indicate that we should abort consumption
      return true;

    } else {
      // looks like we can retry
      try {
        Thread.sleep(this.getRetryWaitTime());
      } catch (InterruptedException ignore) {
        // ignore the exception
      }
      return false;
    }
  }

  /**
   * Implemented to launch a background thread that will read messages from
   * the database queue and process them.
   * 
   * @param processor Processes messages
   * 
   * @throws MessageConsumerException If a failure occurs.
   */
  @Override
  protected void doConsume(MessageProcessor processor)
      throws MessageConsumerException
  {
    this.consumptionThread = new Thread(() -> {
      int   failureCount  = 0;
      long  sleepTime     = 1000L;
      while (this.getState() == CONSUMING) {
        // get the SQLClient
        SQLClient sqlClient = this.getSQLClient();

        // generate a lease ID
        String leaseId = this.generateLeaseId();

        // get the lease time (in seconds)
        int leaseTime = this.getLeaseTime();

        // get the maximum lease count
        int maxLeaseCount = this.getMaximumLeaseCount();

        // initialize the messages list
        List<LeasedMessage> messages = null;

        // initialize the connection
        Connection conn = null;
          
        try {
          // get the connection
          conn = this.getConnection();

          // first release any expired leases so we can lease those messages
          int count = sqlClient.releaseExpiredLeases(conn, leaseTime);
          
          // commit the transaction
          conn.commit();

          if (count > 0) {
            logInfo("expired leases on " + count + " messages");
          }

          // lease messages
          count = sqlClient.leaseMessages(
            conn, leaseId, leaseTime, maxLeaseCount);

          // commit and close the connection so the leases are marked
          // and the connection is available
          conn.commit();
          conn = close(conn);

          // check if we have an empty queue
          if (count == 0) {
            failureCount = 0;
            try {
              Thread.sleep(sleepTime);
            } catch (InterruptedException ignore) {
              // do nothing
            }
            sleepTime = sleepTime * 2L;
            long maxSleepTime = 1000L * ((long) this.getMaximumSleepTime());

            if (sleepTime > maxSleepTime) {
              sleepTime = maxSleepTime;
            }
            
            // try again
            continue;
          }

          // we got a non-empty queue so retore the sleep time to one second
          sleepTime = ONE_SECOND;

          // get the connection 
          conn = this.getConnection();

          // get the list of leased messages
          messages = sqlClient.getLeasedMessages(conn, leaseId);

          // close the connection
          conn = close(conn);

        } catch (SQLException e) {
          if (this.handleFailure(++failureCount, e)) {
            // destroy and then return to abort consumption
            this.destroy();
            return;

          } else {
            // let's retry
            continue;
          }
        } finally {
          // close the connection
          conn = close(conn);
        }
          
        // if we get here then we have leased messages without a failure
        // so we reset the failure count
        failureCount = 0;

        // get the messages from the response
        for (LeasedMessage message : messages) {
          // enqueue the next message for processing -- this call may wait
          // for enough room in the queue for the messages to be enqueued
          this.enqueueMessages(processor, message);
        }
      }
    });

    // start the thread
    this.consumptionThread.start();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Overridden to renew the lease when a message is dequeued.
   * </p>
   */
  @Override
  protected synchronized InfoMessage<LeasedMessage> dequeueMessage(
    MessageProcessor processor)
  {
    // get the message
    InfoMessage<LeasedMessage> message = super.dequeueMessage(processor);

    // check if we got a message and renew its lease
    if (message != null) {
      Connection conn = null;
      try {
        // get a connection
        conn = this.getConnection();

        // get the SQLClient
        SQLClient sqlClient = this.getSQLClient();

        // get the lease time
        int leaseTime = this.getLeaseTime();

        // get the leased message
        LeasedMessage leasedMessage = message.getBatch().getMessage();

        // renew the lease
        sqlClient.renewLease(conn, leasedMessage, leaseTime);

        // commit the connection
        conn.commit();

      } catch (SQLException e) {
        logWarning(e, "Ignoring exception while renewing message lease:", message);

      } finally {
        conn = close(conn);
      }
    }

    // return the message
    return message;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String extractMessageBody(LeasedMessage message) {
    return message.getMessageText();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void disposeMessage(LeasedMessage message) {
    Connection conn = null;
    try {
      // get the connection
      conn = this.getConnection();

      // get the SQLClient
      SQLClient sqlClient = this.getSQLClient();

      // get the message ID and lease ID
      long    messageId = message.getMessageId();
      String  leaseId   = message.getLeaseId();

      // delete the message
      sqlClient.deleteMessage(conn, messageId, leaseId);

      // commit the transaction
      conn.commit();

    } catch (Exception e) {
      logWarning(e, "Ignoring exception while acknowledging message:", message);

    } finally {
      conn = close(conn);
    }
  }

  @Override
  protected void doDestroy() {
    // join to the consumption thread
    try {
      this.consumptionThread.join();
      synchronized (this) {
        this.consumptionThread = null;
      }

      // unregister the the message queue if registered
      if (this.registryToken != null && this.queueRegistryName != null
          && MESSAGE_QUEUE_REGISTRY.isBound(this.queueRegistryName))
      {
        try {
          MESSAGE_QUEUE_REGISTRY.unbind(this.queueRegistryName, 
                                        this.registryToken);
          this.registryToken      = null;
          this.queueRegistryName  = null;
        } catch (Exception e) {
          logWarning(e, "Ignoring exception while unbinding MessageQueue.");
        }
      }

    } catch (InterruptedException ignore) {
      // ignore
    }
  }

  /**
   * Provides a means to test this class from the command-line.
   * 
   * @param args The command-line arguments.
   */
  public static void main(String[] args) {
    // check if no arguments specified
    if (args.length != 1 && args.length !=2 && args.length != 6) {
      System.err.println("Unexpected number of command-line arguments.");
      printUsage();
      System.exit(1);
    }

    try {
      DatabaseType dbType = DatabaseType.valueOf(args[0]);

      Connector connector   = null;
      int       minPoolSize = 1;
      int       maxPoolSize = 1;
      switch (dbType) {
        case SQLITE:
          if (args.length == 1) {
            SQLiteConnector conn = new SQLiteConnector();
            File file = conn.getSqliteFile();
            System.out.println("SQLite File: " + file);
            connector = conn;

          } else if (args.length == 2) {
            connector = new SQLiteConnector(args[1]);
          } else {
            System.err.println("Unexpected number of command-line arguments.");
            printUsage();
            System.exit(1);
          }
          break;
        case POSTGRESQL:
          if (args.length != 6) {
            System.err.println("Unexpected number of command-line arguments.");
            printUsage();
            System.exit(1);
          }
          String  host      = args[1];
          int     port      = Integer.parseInt(args[2]);
          String  database  = args[3];
          String  user      = args[4];
          String  password  = args[5];

          connector = new PostgreSqlConnector(
            host, port, database, user, password);
          minPoolSize = 2;
          maxPoolSize = 5;

          break;
        default:
          System.err.println("Unsupported database type: " + dbType);
          printUsage();
          System.exit(1);
          break;
      }

      ConnectionPool pool 
        = new ConnectionPool(connector, minPoolSize, maxPoolSize);

      ConnectionProvider provider = new PoolConnectionProvider(pool);
      
      ConnectionProvider.REGISTRY.bind("test-provider", provider);
      
      JsonObjectBuilder builder = Json.createObjectBuilder();
      builder.add(CLEAN_DATABASE_KEY, false);
      builder.add(CONNECTION_PROVIDER_KEY, "test-provider");
      builder.add(QUEUE_REGISTRY_NAME_KEY, "message-queue");

      JsonObject config = builder.build();

      SQLConsumer consumer = new SQLConsumer();
      consumer.init(config);

      consumer.consume((jsonMessage) -> {
        String recordId = jsonMessage.getString("RECORD_ID");
        JsonArray array = jsonMessage.getJsonArray("AFFECTED_ENTITIES");
        for (JsonObject obj : array.getValuesAs(JsonObject.class)) {
          long entityId = obj.getJsonNumber("ENTITY_ID").longValue();

          System.out.println();
          System.out.println(
            "ENTITY ID: " + entityId + " / RECORD ID: " + recordId);
        }
      });

      MessageQueue messageQueue 
        = SQLConsumer.MESSAGE_QUEUE_REGISTRY.lookup("message-queue");

      int entityId = 10;
      int recordId = 100000;
      int messageCount = 0;
      for (int index1 = 0; index1 < 100; index1++) {
        JsonArrayBuilder jab = Json.createArrayBuilder();
        for (int index2 = 0; index2 < 15; index2++) {
          JsonObjectBuilder job = Json.createObjectBuilder();
          job.add("DATA_SOURCE", "CUSTOMERS");
          job.add("RECORD_ID", String.valueOf(recordId++));

          JsonArrayBuilder jab2 = Json.createArrayBuilder();

          JsonObjectBuilder job2 = Json.createObjectBuilder();
          job2.add("ENTITY_ID", entityId++);
          jab2.add(job2);
          job.add("AFFECTED_ENTITIES", jab2);
          
          jab.add(job);
        }
        String message = JsonUtilities.toJsonText(jab);
        messageQueue.enqueueMessage(message);
        messageCount++;
      }

      System.out.println();
      System.out.println("ENQUEUED " + messageCount + " MESSAGES");

      // wait until the queue is empty
      for (int index = 0; !messageQueue.isEmpty(); index++) {
        Thread.sleep(1000L);
        if (index % 10 == 0) {
          java.util.Map<Statistic,Number> statistics = consumer.getStatistics();
          System.out.println();
          System.out.println("------------------------------");
          statistics.forEach((stat, value) -> {
            System.out.println(
              stat.getName() + " : " + value + " " + stat.getUnits());
          });
          System.out.println();
          System.out.println("QUEUE SIZE  : " + messageQueue.getMessageCount());
          System.out.println("------------------------------");
        }
      }

      System.out.println();
      System.out.println("QUEUE EMPTY : " + messageQueue.isEmpty());
      System.out.println("QUEUE SIZE  : " + messageQueue.getMessageCount());

      // destroy the consumer
      consumer.destroy();
      pool.shutdown();

    } catch (Exception e) {
      e.printStackTrace();
      printUsage();
      System.exit(1);
    }
  }

  /**
   * 
   */
  private static void printUsage() {
    System.err.println();
    System.err.println("COMMAND-LINE ARGUMENT OPTIONS: ");
    System.err.println(
      "  - For SQLite with an auto-created temporary file:");
    System.err.println(
      "       SQLITE");
    System.err.println(
      "  - For SQLite with a specific database file:");
    System.err.println(
      "       SQLITE <sqlite-file-path>");
    System.err.println(
      "  - For PostrgreSQL:");
    System.err.println(
      "       POSTGRESQL <db-host> <db-port> <db-name> <db-user> <db-password>");
    System.err.println();
  }
}
