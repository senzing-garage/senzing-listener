package com.senzing.listener.communication.sqs;

import java.util.List;

import javax.json.JsonObject;

import com.senzing.listener.communication.AbstractMessageConsumer;
import com.senzing.listener.communication.exception.MessageConsumerException;
import com.senzing.listener.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.service.MessageProcessor;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import static com.senzing.util.LoggingUtilities.*;
import static com.senzing.listener.communication.MessageConsumer.State.*;

/**
 * A consumer for SQS.
 */
public class SQSConsumer extends AbstractMessageConsumer<Message> {
  /**
   * The initialization parameter for the SQS URL.  There is no default value
   * so this configuration parameter is required.
   */
  public static final String SQS_URL_KEY = "sqsUrl";

  /**
   * The initialization parameter to configure the maximum number of times to
   * retry a failed SQS request before aborting consumption.
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
   * messages on the SQS queue are hidden from subsequent retrieve requests
   * after having been retrieved.  If not configured then the value configured
   * on the queue itself is used.  Specifying this initialization parameter
   * allows the client to override.
   */
  public static final String VISIBILITY_TIMEOUT_KEY = "visibilityTimeout";

  /**
   * The default number of times to retry failed SQS requests before aborting
   * consumption.  The default value is {@value}.  A different value can be set
   * via the {#link #MAXIMUM_RETRIES_KEY} parameter.
   */
  public static final int DEFAULT_MAXIMUM_RETRIES = 0;

  /**
   * The default number of milliseconds to wait before retrying the SQS request
   * if the previous request failed.  The default value is {@value}.  A
   * different value can be set via the {@link #RETRY_WAIT_TIME_KEY} parameter.
   */
  public static final long DEFAULT_RETRY_WAIT_TIME = 1000L;

  /**
   * The SQS URL.
   */
  private String sqsUrl;

  /**
   * The {@link SqsClient} for the connection to SQS.
   */
  private SqsClient sqsClient;

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
   * The configured visibility timeout or <code>null</code> if the queue's
   * configured value should be used.
   */
  private Integer visibilityTimeout = null;

  /**
   * Wait parameter in seconds to SQS in case no messages are waiting to be collected.
   */
  private static final int SQS_WAIT_SECS = 10;

  /**
   * Generates a SQS consumer.
   * 
   * @return The created {@link SQSConsumer} instance.
   */
  public static SQSConsumer generateSQSConsumer() {
    return new SQSConsumer();
  }

  /**
   * Private default constructor.
   */
  public SQSConsumer() {
    // do nothing
  }

  /**
   * Initializes the object. It sets the object up based on configuration
   * passed in.
   * <p>
   * The configuration is in JSON format:
   * <pre>
   * {
   *   "sqsUrl": "&lt;URL&gt;",
   *   "concurrency": "&lt;thread-count&gt;",
   *   "failureThreshold": "&lt;failure-threshold&gt;",
   *   "retryWaitTime": "&lt;pause-milliseconds&gt;",
   *   "visibilityTimeout": "&lt;timeout-seconds&gt;"
   * }
   * </pre>
   *
   * @param config Configuration string containing the needed information to
   *               connect to SQS.
   *
   * @throws MessageConsumerSetupException If an initialization failure occurs.
   */
  @Override
  protected void doInit(JsonObject config) throws MessageConsumerSetupException
  {
    try {
      // get the SQS URL
      this.sqsUrl = getConfigString(config, SQS_URL_KEY, true);

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

      // get the visibility timeout
      this.visibilityTimeout = getConfigInteger(config,
                                                VISIBILITY_TIMEOUT_KEY,
                                                1,
                                                null);

      this.sqsClient = SqsClient.builder().build();

    } catch (RuntimeException e) {
      throw new MessageConsumerSetupException(e);
    }
  }

  /**
   * Returns the maximum number times failed SQS requests will be retried before
   * aborting message consumption.  This defaults to {@link
   * #DEFAULT_MAXIMUM_RETRIES} and can be configured via the
   * {@link #MAXIMUM_RETRIES_KEY} configuration parameter.
   *
   * @return The maximum number of times failed SQS requests will be retried
   *         before aborting message consumption.
   */
  public int getMaximumRetries() {
    return this.maximumRetries;
  }

  /**
   * Returns the number of milliseconds to wait between SQS request retries
   * when a failure occurs.  This defaults to {@link #DEFAULT_RETRY_WAIT_TIME}
   * and can be configured via the {@link #RETRY_WAIT_TIME_KEY} configuration
   * parameter.
   *
   * @return The number of milliseconds to wait between SQS request retries
   *         when a failure occurs.
   */
  public long getRetryWaitTime() {
    return this.retryWaitTime;
  }

  /**
   * Returns the number of <b>seconds</b> messages on the SQS queue are hidden
   * from subsequent retrieve requests after having been retrieved.  If this
   * returns <code>null</code> then the value configured on the queue itself
   * is used.
   *
   * @return The number of <b>seconds</b> messages on the SQS queue are hidden
   *         from subsequent retrieve requests after having been retrieved, or
   *         <code>null</code> if the queue's configured value is used.
   */
  public Integer getVisibilityTimeout() { return this.visibilityTimeout; }

  /**
   * Returns the configured SQS URL.
   *
   * @return The configured SQS URL.
   */
  public String getSqsUrl() {
    return this.sqsUrl;
  }

  /**
   * Handles an SQS failure and checks if consumption should be aborted.
   *
   * @param failureCount The number of consecutive failures so far.
   * @param response The SQS response, or <code>null</code> if not known.
   * @param failure The {@link Exception} that was thrown if available,
   *                otherwise <code>null</code>.
   * @return <code>true</code> if consumption should abort, otherwise
   *         <code>false</code>.
   */
  protected boolean handleFailure(int                     failureCount,
                                  ReceiveMessageResponse  response,
                                  Exception               failure)
  {
    // get the maximum number of retries
    int maxRetries = this.getMaximumRetries();
    
    logWarning(failure,
               "FAILURE DETECTED: " + failureCount + " of " + maxRetries
                   + " consecutive failure(s)",
               ((response != null)
                   ? ("Received SQS HTTP error response code: "
                   + response.sdkHttpResponse().statusCode()
                   + " / " + response.sdkHttpResponse().statusText())
                   : "*** No HTTP Response ***"),
               "SQS URL: " + this.getSqsUrl());

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
   * Sets up a SQS consumer and then receives messages from SQS and
   * feeds to service.
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
      int failureCount = 0;
      while (this.getState() == CONSUMING) {
        try {
          ReceiveMessageRequest request = ReceiveMessageRequest.builder()
              .queueUrl(this.getSqsUrl())
              .waitTimeSeconds(SQS_WAIT_SECS)
              .visibilityTimeout(this.getVisibilityTimeout())
              .build();

          ReceiveMessageResponse response = sqsClient.receiveMessage(request);

          // failed obtaining a response
          if (!response.sdkHttpResponse().isSuccessful()) {
            int responseCode = response.sdkHttpResponse().statusCode();
            if (this.handleFailure(++failureCount, response, null)) {
              // destroy and then return to abort consumption
              this.destroy();
              return;

            } else {
              // let's retry
              continue;
            }

          } else {
            // reset the consecutive failure count
            failureCount = 0;
          }

          // get the messages from the response
          List<Message> messages = response.messages();
          for (Message message : messages) {
            // enqueue the next message for processing -- this call may wait
            // for enough room in the queue for the messages to be enqueued
            this.enqueueMessages(processor, message);
          }

        } catch (SdkException e) {
          e.printStackTrace();
          failureCount++;

        }
      }
    });

    // start the thread
    this.consumptionThread.start();
  }

  @Override
  protected String extractMessageBody(Message message) {
    return message.body();
  }

  @Override
  protected void disposeMessage(Message message) {
    String receiptHandle = message.receiptHandle();

    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
        .queueUrl(this.getSqsUrl())
        .receiptHandle(receiptHandle)
        .build();

    sqsClient.deleteMessage(deleteMessageRequest);
  }

  @Override
  protected void doDestroy() {
    // join to the consumption thread
    try {
      this.consumptionThread.join();
      synchronized (this) {
        this.consumptionThread = null;
      }
    } catch (InterruptedException ignore) {
      // ignore
    }
  }
}
