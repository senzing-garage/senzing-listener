package com.senzing.listener.communication;

import com.senzing.listener.communication.exception.MessageConsumerException;
import com.senzing.listener.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.service.MessageProcessor;
import com.senzing.listener.service.locking.LockingService;
import com.senzing.listener.service.locking.ProcessScopeLockingService;
import com.senzing.util.AsyncWorkerPool;
import com.senzing.util.JsonUtilities;
import com.senzing.util.LoggingUtilities;
import com.senzing.util.Timers;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.*;

import static java.util.Collections.*;
import static com.senzing.util.JsonUtilities.parseJsonObject;
import static com.senzing.util.JsonUtilities.parseJsonArray;
import static com.senzing.util.JsonUtilities.toJsonText;
import static com.senzing.listener.communication.MessageConsumer.State.*;
import static com.senzing.util.AsyncWorkerPool.*;
import static com.senzing.listener.communication.AbstractMessageConsumer.Stat.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Base class for {@link MessageConsumer} implementations.
 *
 * @param <M> The type for the framework-specific messages received.
 */
public abstract class AbstractMessageConsumer<M> implements MessageConsumer
{
  /**
   * The default concurrency.  The default is to serialize message handling in
   * a single thread.
   */
  public static final int DEFAULT_CONCURRENCY = 1;

  /**
   * The default number of milliseconds for the {@link #TIMEOUT_KEY}
   * initialization parameter if not otherwise specified.
   */
  public static final long DEFAULT_TIMEOUT = 1500L;

  /**
   * The config property key for configuring the concurrency.
   */
  public static final String CONCURRENCY_KEY = "concurrency";

  /**
   * The initialization parameter to specify the number of milliseconds to
   * sleep between checking to see if message processing should cease.  If not
   * configured then the value is set to {@link #DEFAULT_TIMEOUT}.
   * If the value is specified it should be non-negative.
   */
  public static final String TIMEOUT_KEY = "timeout";

  /**
   * Millisecond units constant for {@link Stat} instances.
   */
  private static final String MILLISECOND_UNITS = "ms";

  /**
   * Thread units constant for {@link Stat} instances.
   */
  private static final String THREAD_UNITS = "threads";

  /**
   * Message units constant for {@link Stat} instances.
   */
  private static final String MESSAGE_UNITS = "messages";

  /**
   * Call units constant for {@link Stat} instances.
   */
  private static final String CALL_UNITS = "calls";

  /**
   * The various keys used for timing operations.
   */
  public enum Stat implements Statistic {
    /**
     * The number of worker threads used to asynchronously consume the messages.
     */
    concurrency(THREAD_UNITS),

    /**
     * The timeout to use when waiting for new messages to show up  and to
     * check to see if message processing has ceased.
     */
    timeout(MILLISECOND_UNITS),

    /**
     * The average number of milliseconds for a message to be pulled from the
     * vendor message queue until it has been completely processed.  For batches
     * this means that every message in the batch has been processed.
     */
    averageRoundTrip(MILLISECOND_UNITS),

    /**
     * The longest amount of time (in milliseconds) for when a message was
     * pulled from the vendor message queue until it was completely processed.
     * For batches this means the number of milliseconds it took until every
     * message in the batch was processed.
     */
    longestRoundTrip(MILLISECOND_UNITS),

    /**
     * The average number of milliseconds for an info message to be processed
     * by the {@link MessageProcessor} via {@link
     * MessageProcessor#process(JsonObject)}.
     */
    averageServiceProcess(MILLISECOND_UNITS),

    /**
     * The number of messages that have made the round trip from the vendor
     * message queue to the point where they are completely processed (for
     * batches this means all contained info messages are processed).  Some
     * messages may make the round trip more than once if a failure occurs in
     * processing part or all of the message.
     */
    roundTripCount(MESSAGE_UNITS),

    /**
     * The number of times the {@link MessageProcessor#process(JsonObject)}
     * method has been called to process an info message.
     */
    processCount(CALL_UNITS),

    /**
     * The number of times that the {@link MessageProcessor#process(JsonObject)}
     * has been called successfully (i.e.: without any exceptions).
     */
    processSuccessCount(CALL_UNITS),

    /**
     * The number of times that the {@link MessageProcessor#process(JsonObject)}
     * has been called unsuccessfully (i.e.: with an exceptions being thrown).
     */
    processFailureCount(CALL_UNITS),

    /**
     * The number of times that the {@link MessageProcessor#process(JsonObject)}
     * has been or will be retried for the same info message due to failures.
     * This will be more than the number of failures since a single info
     * message failing in a batch will trigger the whole batch to be retried.
     */
    processRetryCount(CALL_UNITS),

    /**
     * The number of messages from the vendor message queue that will be
     * retried.  The messages from the queue may be batches and a failure from
     * one or more messages within the batch will cause the batch to be retried,
     * so the number of message retries could actually be less than the number
     * of failures.
     */
    messageRetryCount(MESSAGE_UNITS),

    /**
     * The ratio of cumulative {@link MessageProcessor} processing time across
     * all threads to actual active processing time.
     */
    parallelism(null),

    /**
     * The ratio of the number of times the {@link
     * #dequeueMessage(MessageProcessor)} function is called and a message is
     * ready to be returned without waiting.
     */
    dequeueHitRatio(null),

    /**
     * The cumulative time spent (in milliseconds) in the {@link
     * #processMessages(MessageProcessor)} function.
     */
    processMessages(MILLISECOND_UNITS),

    /**
     * The cumulative time spent (in milliseconds) actively processing messages.
     * This excludes time waiting for messages to arrive.
     */
    activelyProcessing(MILLISECOND_UNITS),

    /**
     * The cumulative time spent (in milliseconds) waiting for messages to
     * arrive from the vendor message queue and get moved to the internal queue.
     */
    waitingForMessages(MILLISECOND_UNITS),

    /**
     * The time spent (in milliseconds) between handing a message off to a
     * worker for processing and obtaining the next message to be processed.
     */
    betweenMessages(MILLISECOND_UNITS),

    /**
     * The time spent (in milliseconds) calling {@link
     * #dequeueMessage(MessageProcessor)} function to dequeue a message from the
     * internal queue.  This includes time waiting for the first message to
     * arrive or the next message to arrive after the last message has been
     * handled.
     */
    dequeue(MILLISECOND_UNITS),

    /**
     * The time spent (in milliseconds) waiting to obtain the synchronized lock
     * on the consumer in order to call the {@link
     * #dequeueMessage(MessageProcessor)} function.
     */
    dequeueBlocking(MILLISECOND_UNITS),

    /**
     * The time spent (in milliseconds) in the "wait loop" of
     * {@link #dequeueMessage(MessageProcessor)} waiting for a message to become
     * available for processing.
     */
    dequeueMessageWaitLoop(MILLISECOND_UNITS),

    /**
     * The time spent (in milliseconds) in the synchronization wait of
     * {@link #dequeueMessage(MessageProcessor)} waiting for a message to become
     * available for processing.  This should be the majority of the time spent
     * in {@link #dequeueMessageWaitLoop}, but isolates the non-busy sleeping
     * time awaiting notification of message arrival.
     */
    dequeueMessageWait(MILLISECOND_UNITS),

    /**
     * The number of milliseconds spent calling {@link #init(JsonObject)}.
     */
    initialize(MILLISECOND_UNITS),

    /**
     * The number of milliseconds spent calling {@link
     * #enqueueMessages(MessageProcessor, Object)}.  This can be high if we have
     * to wait for the pending queue shrink before we can add more messages to
     * it.  This built-in wait is done to throttle pulling from the vendor
     * message queue when we have enough messages already pending processing.
     */
    enqueue(MILLISECOND_UNITS),

    /***
     * The number of milliseconds spent waiting for the pending queue to shrink
     * so more messages can be added to it when calling {@link
     * #enqueueMessages(MessageProcessor, Object)}.
     */
    throttleEnqueue(MILLISECOND_UNITS),

    /**
     * A subset of {@link #throttleEnqueue}, this is specifically the number of
     * milliseconds spent in non-busy sleep awaiting notification that the
     * pending queue has shrunk in size and more messages can be added to it.
     * This should be the majority of the time logged for {@link
     * #throttleEnqueue}.
     */
    throttleWait(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent waiting for an available
     * worker thread to process an info message that has been pulled from the
     * pending queue.
     */
    waitForWorker(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent calling {@link
     * MessageProcessor#process(JsonObject)}.
     */
    serviceProcess(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent calling {@link
     * InfoMessage#markProcessed(boolean)}.
     */
    markProcessed(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent calling {@link
     * #disposeMessage(Object)}.
     */
    disposeMessage(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent calling {@link
     * #postProcess(InfoMessage)}.
     */
    postProcess(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent calling {@link
     * #destroy()}.
     */
    destroy(MILLISECOND_UNITS);

    /**
     * Constructs with the specified units.
     *
     * @param units The units to construct with.
     */
    Stat(String units) {
      this.units = units;
    }

    /**
     * The units for this instance.
     */
    private String units;

    /**
     * Returns the units associated with this statistic.
     *
     * @return The units associated with this statistic.
     */
    public String getUnits() {
      return this.units;
    }
  }

  /**
   * The {@link State} of the {@link MessageConsumer}.
   */
  private State state = UNINITIALIZED;

  /**
   * Flag indicating if we are currently processing messages.  This is used to
   * synchronize destruction and wait until in-flight messages that have been
   * postponed are handled.
   */
  private boolean processing = false;

  /**
   * The concurrency for this instance.
   */
  private int concurrency = DEFAULT_CONCURRENCY;

  /**
   * The worker pool for this instance.
   */
  private AsyncWorkerPool<ProcessResult<M>> workerPool = null;

  /**
   * The number of milliseconds to sleep between checking to see if message
   * processing should cease.
   */
  private long timeout = DEFAULT_TIMEOUT;

  /**
   * The {@link List} of pending messages.
   */
  private List<InfoMessage<M>> pendingMessages;

  /**
   * The background thread used for processing messages.
   */
  private Thread processingThread = null;

  /**
   * The total of the number of milliseconds each of the message batches
   * round trip from enqueueing until all contained messages in the batch have
   * been processed.
   */
  private long totalRoundTripMillis = 0L;

  /**
   * The longest time it has taken a message to round-trip from the vendor
   * message queue to being completed.
   */
  private long longestRoundTripMillis = 0L;

  /**
   * The total number of milliseconds spent processing messages.
   */
  private long totalProcessMillis = 0L;

  /**
   * The number of info message batches that have been processed.
   */
  private long processedBatchCount = 0L;

  /**
   * The number of info messages that have been processed.
   */
  private long processedMessageCount = 0L;

  /**
   * The number of times the {@link MessageProcessor#process(JsonObject)}
   * method has been successfully called.
   */
  private long processSuccessCount = 0L;

  /**
   * The number of times the {@link MessageProcessor#process(JsonObject)}
   * method has been called and thrown an exception.
   */
  private long processFailureCount = 0L;

  /**
   * The number of batches that have to be retried because of a failure from
   * at least one message contained within the batch.
   */
  private long batchRetryCount = 0L;

  /**
   * The total number of messages for the batches that are being retried.
   */
  private long processRetryCount = 0L;

  /**
   * The statistics monitor for synchronizing without blocking on message
   * handling.
   */
  private final Object statsMonitor = new Object();

  /**
   * The {@link LockingService} to use.
   */
  private LockingService lockingService = null;

  /**
   * The total number of times an attempt was made to dequeue a message and
   * one was ready.
   */
  private long dequeueHitCount = 0L;

  /**
   * The total number of times an attempt was made to dequeue a message and one
   * was not ready to be dequeued.
   */
  private long dequeueMissCount = 0L;

  /**
   * The processing {@link Timers}.
   */
  private final Timers timers = new Timers();

  /**
   * Flag to use to suppress checking if already processing when backgrounding
   * message processing.
   */
  private static final ThreadLocal<Boolean> SUPPRESS_PROCESSING_CHECK
      = new ThreadLocal<>();

  /**
   * Implemented to return the {@link State} of this instance.
   *
   * @return The {@link State} of this instance.
   */
  @Override
  public synchronized State getState() {
    return this.state;
  }

  /**
   * Provides a means to set the {@link State} for this instance as a
   * synchronized method that will notify all upon changing the state.
   *
   * @param state The {@link State} for this instance.
   */
  protected synchronized void setState(State state) {
    Objects.requireNonNull(state,"State cannot be null");
    this.state = state;
    this.notifyAll();
  }

  /**
   * Gets the number of queued messages that are pending.  These are messages
   * from message batches that have been pulled from the MQ framework and
   * added to the queue.
   *
   * @return The number of pending messages.
   */
  protected synchronized int getPendingMessageCount() {
    return this.pendingMessages.size();
  }

  /**
   * Returns the maximum number of messages allowed in the pending queue.  When
   * this limit is reached enqueueing additional messages will be blocked until
   * the queue reduces in size.
   *
   * @return The maximum number of messages allowed in the pending queue before
   *         throttling consumption.
   */
  protected synchronized int getMaximumPendingCount() {
    return this.concurrency * 1000;
  }

  /**
   * Checks if this instance is current processing messages.  This is used to
   * synchronize destruction.  The {@link #doDestroy()} method is not called
   * until processing ceases.
   *
   * @return <code>true</code> if this instance is still processing messages,
   *         otherwise <code>false</code>.
   */
  protected synchronized boolean isProcessing() {
    return this.processing;
  }

  /**
   * The {@link Object} to synchronize on when computing and recording
   * statistics in a thread-safe manner.
   *
   * @return The {@link Object} to synchronize on when computing and recording
   *         statistics in a thread-safe manner.
   */
  protected final Object getStatisticsMonitor() {
    return this.statsMonitor;
  }

  /**
   * Call this to increment the number of times dequeue has been called with
   * or without a message ready to be dequeued.  This function is thread-safe
   * with respect to other statistics.
   *
   * @param hit <code>true</code> if we have a "hit" and there is a message
   *            ready to be dequeued, otherwise <code>false</code> for a "miss".
   */
  protected void updateDequeueHitRatio(boolean hit) {
    synchronized (this.getStatisticsMonitor()) {
      if (hit) {
        this.dequeueHitCount++;
      } else {
        this.dequeueMissCount++;
      }
    }
  }

  /**
   * Returns the "hit ratio" for attempting to dequeue a message from the
   * internal queue and finding a message ready to be dequeued.  If this is
   * low then the internal queue needs to be filled at a faster rate from
   * the vendor-specific message queue.  This returns <code>null</code> if
   * no attempt have been made to dequeue a message.
   *
   * @return The "hit ratio" of attempts to dequeue a message and finding one
   *         ready to the total number of attempts to dequeue a message, or
   *         <code>null</code> if no attempts have been made to dequeue a
   *         message.
   */
  public Double getDequeueHitRatio() {
    synchronized (this.getStatisticsMonitor()) {
      double hits = (double) this.dequeueHitCount;
      double misses = (double) this.dequeueMissCount;
      double total = hits + misses;
      return (hits / total);
    }
  }

  /**
   * Gets the {@link Map} of {@link Stat} keys to their {@link Number}
   * values in an atomic thread-safe manner.
   *
   * @return The {@link Map} of {@link Stat} keys to their {@link Number}
   *         values.
   */
  @Override
  public Map<Statistic, Number> getStatistics() {
    synchronized (this.getStatisticsMonitor()) {
      Map<String, Long> timings = this.timers.getTimings();

      Map<Statistic, Number> statsMap = new LinkedHashMap<>();

      statsMap.put(Stat.concurrency, getConcurrency());
      statsMap.put(averageRoundTrip, this.getAverageRoundTripMillis());
      statsMap.put(longestRoundTrip, this.getLongestRoundTripMillis());
      statsMap.put(averageServiceProcess, this.getAverageProcessMillis());
      statsMap.put(roundTripCount, this.getCompletedMessageCount());
      statsMap.put(messageRetryCount, this.getMessageRetryCount());
      statsMap.put(processCount, this.getProcessedInfoMessageCount());
      statsMap.put(Stat.processSuccessCount,
                   this.getInfoMessageSuccessCount());
      statsMap.put(Stat.processFailureCount,
                   this.getInfoMessageFailureCount());
      statsMap.put(Stat.processRetryCount, this.getInfoMessageRetryCount());
      statsMap.put(parallelism, this.getParallelism());
      statsMap.put(dequeueHitRatio, this.getDequeueHitRatio());

      for (Stat statistic : Stat.values()) {
        Number value = timings.get(statistic.toString());
        if (value != null) {
          statsMap.put(statistic, value);
        }
      }

      return statsMap;
    }
  }

  /**
   * Implemented to parse the specified {@link String} as a {@link JsonObject}.
   * This will set up the internal {@link AsyncWorkerPool}.
   *
   * @param config The JSON config text.
   *
   * @throws MessageConsumerSetupException If a failure occurs during
   *                                       initialization.
   */
  @Override
  public void init(JsonObject config) throws MessageConsumerSetupException {
    synchronized (this) {
      if (this.getState() != UNINITIALIZED) {
        throw new IllegalStateException(
            "Cannot initialize if not in the " + UNINITIALIZED + " state: "
                + this.getState());
      }
      this.timerStart(initialize);
      this.setState(INITIALIZING);
    }

    try {
      synchronized (this) {
        // create the locking service
        this.lockingService = new ProcessScopeLockingService();
        this.lockingService.init(null);

        this.concurrency = getConfigInteger(config,
                                            CONCURRENCY_KEY,
                                            1,
                                            DEFAULT_CONCURRENCY);

        // get the standard timeout
        this.timeout = getConfigLong(config,
                                     TIMEOUT_KEY,
                                     0L,
                                     DEFAULT_TIMEOUT);

        // create the list of pending messages
        this.pendingMessages = new LinkedList<>();
      }

      // defer additional configuration
      this.doInit(config);

    } catch (MessageConsumerSetupException e) {
      throw e;

    } catch (Exception e) {
      throw new MessageConsumerSetupException(e);

    } finally {
      this.setState(INITIALIZED);
      this.timerPause(initialize);
    }
  }

  /**
   * Called by the {@link #init(JsonObject)} implementation after handling the
   * base configuration parameters and parsing the specified {@link String} as
   * a {@link JsonObject}.
   *
   * @param config The {@link JsonObject} describing the configuration.
   *
   * @throws MessageConsumerSetupException If a failure occurs during
   *                                       initialization.
   */
  protected abstract void doInit(JsonObject config)
    throws MessageConsumerSetupException;

  /**
   * Implemented to verify that the state of this instance is currently
   * {@link State#INITIALIZED}, transitions to {@link State#CONSUMING},
   * calls {@link #backgroundProcessMessages(MessageProcessor)} and then
   * delegates to {@link #doConsume(MessageProcessor)}.
   *
   * @param processor The {@link MessageProcessor} for processing the messages.
   *
   * @throws MessageConsumerException If a failure occurs.
   */
  @Override
  public void consume(MessageProcessor processor)
      throws MessageConsumerException
  {
    synchronized (this) {
      if (this.getState() != State.INITIALIZED) {
        throw new IllegalStateException(
            "Can only transition to " + CONSUMING + " state from "
                + INITIALIZED + " state.  Current state: " + this.getState());
      }

      // set the state
      this.setState(CONSUMING);
    }

    // startup background processing of enqueued messages
    this.backgroundProcessMessages(processor);

    // delegate
    this.doConsume(processor);
  }

  /**
   * Implement this to initiate consumption.  The implementation should return
   * immediately and should not loop indefinitely while consuming messages.
   * This may require launching a background thread to loop for message
   * consumption.
   *
   * @param processor The {@link MessageProcessor} to use for processing.
   * @throws MessageConsumerException If a failure occurs.
   */
  protected abstract void doConsume(MessageProcessor processor)
      throws MessageConsumerException;

  /**
   * Implemented as a synchronized method to {@linkplain #setState(State)
   * set the state} to {@link State#DESTROYING}, call {@link #doDestroy()} and
   * then perform {@link #notifyAll()} and set the state to {@link
   * State#DESTROYED}.
   */
  public void destroy() {
    this.timerStart(destroy);
    synchronized (this) {
      this.setState(DESTROYING);

      // wait until no longer processing
      while (this.isProcessing()) {
        try {
          this.wait(this.getTimeout());
        } catch (InterruptedException ignore) {
          // do nothing
        }
      }
    }

    // join against the processing thread
    try {
      this.processingThread.join();
    } catch (InterruptedException ignore) {
      // ignore the exception
    }
    synchronized (this) {
      this.processingThread = null;
    }

    try {
      // now complete the destruction / cleanup
      this.doDestroy();

    } finally {
      this.setState(DESTROYED); // this should notify all as well
      this.timerPause(destroy);
    }
  }

  /**
   * This is called from the {@link #destroy()} implementation and should be
   * overridden by the concrete sub-class.
   */
  protected abstract void doDestroy();

  /**
   * Gets the concurrency of the consumer.  The returned value will be a
   * positive number greater than or equal to one (1).
   *
   * @return The concurrency of the consumer.
   */
  protected int getConcurrency() {
    return this.concurrency;
  }

  /**
   * Gets the number of milliseconds to sleep between checking to see if message
   * processing should cease.  This timeout is used when there are no postponed
   * messages due to contention.
   *
   * @return The number of milliseconds to sleep between checking to see if
   *         message processing should cease.  This timeout is used when there
   *         are no postponed messages due to contention.
   */
  protected long getTimeout() {
    return this.timeout;
  }

  /**
   * Returns the average number of milliseconds required for the round trip
   * of a message from the time it is dequeued from the vendor message queue
   * and its info messages are enqueued for processing until they have all been
   * processed (for non-batch messages then consider it a batch of one).  This
   * returns <code>null</code> if no batches have been completed.
   *
   * @return The average number of milliseconds required for the round trip of
   *         a message from the time it is dequeued from the vendor message
   *         queue and its info messages are enqueued for processing until they
   *         have all been processed (for non-batch messages then consider it a
   *         batch of one), or <code>null</code> if no batches have been
   *         completed.
   */
  public Long getAverageRoundTripMillis() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.processedBatchCount == 0L) return null;
      return this.totalRoundTripMillis / this.processedBatchCount;
    }
  }

  /**
   * Returns the longest number of milliseconds required for the round trip
   * of a single message from the time it is dequeued from the vendor message
   * queue and its info messages are enqueued for processing until they have
   * all been processed (for non-batch messages then consider it a batch of
   * one).  This returns <code>null</code> if no batches have been completed.
   *
   * @return longest number of milliseconds required for the round trip of a
   *         single message from the time it is dequeued from the vendor message
   *         queue and its info messages are enqueued for processing until they
   *         have all been processed (for non-batch messages then consider it a
   *         batch of one), or <code>null</code> if no batches have been
   *         completed.
   */
  public Long getLongestRoundTripMillis() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.processedBatchCount == 0L) return null;
      return this.longestRoundTripMillis;
    }
  }

  /**
   * Returns the number of MQ-vendor messages that have been dequeued from
   * the messaging service and have completed processing.  Each message may or
   * may not be a batch of info messages.  For a batch to be completed, each
   * of the info messages contained in the batch must have been processed.
   *
   * @return The number of MQ-vendor messages (i.e.: batches) that have been
   *         dequeued from the messaging service and have completed processing.
   */
  public long getCompletedMessageCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.processedBatchCount;
    }
  }

  /**
   * Returns the number of MQ-vendor messages that will be or have been retried
   * because of a failure that prevents them from being acknowledged to or
   * deleted from the MQ-vendor queue.  Each message may or may not be a batch
   * of info messages, any of which may fail and trigger retry for the entire
   * batch when the message is not acknowledged or deleted from the MQ-vendor
   * queue.  For a batch to not be retried, then every info messages contained
   * in the batch must have be processed without a failure.
   *
   * @return The number of MQ-vendor messages (i.e.: batches) that have been
   *         dequeued from the messaging service and have had at least one
   *         failure during the processing of the contained info messages.
   */
  public long getMessageRetryCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.batchRetryCount;
    }
  }

  /**
   * Returns the number of info messages that will be or have been retried
   * because of a failure in processing that info message or due to a failure
   * in processing another info message that belongs to the same batch.  This
   * number will exceed the number of MQ-vendor messages if those mesages are
   * batches of more than one info message.  Further, successfully processed
   * messages may still be retried if another info message from the same batch
   * experiences a processing failure that prevents the MQ-vendor message from
   * being acknowledged to or deleted from the MQ-vendor message queue.
   *
   * @return The number of info messages that will be have been
   *         dequeued from the messaging service and have had at least one
   *         failure during the processing of the contained info messages.
   */
  public long getInfoMessageRetryCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.processRetryCount;
    }
  }

  /**
   * Returns the average number of milliseconds required to process the info
   * messages.  This returns <code>null</code> if no messages have been
   * processed.
   *
   * @return The average number of milliseconds required to process the info
   *         messages, or <code>null</code> if no messages have been processed.
   */
  public Long getAverageProcessMillis() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.processedMessageCount == 0L) return null;
      return this.totalProcessMillis / this.processedMessageCount;
    }
  }

  /**
   * Gets the ratio of the total processing time across all threads to the
   * total active processing of the message scheduler to indicate the level
   * of parallelism achieved.  This returns <code>null</code> if the actively
   * processing time is zero.
   *
   * @return The ratio of the total processing time across all threads to the
   *         total active processing of the message scheduler.
   */
  public Double getParallelism() {
    synchronized (this.getStatisticsMonitor()) {
      String timerKey = activelyProcessing.toString();
      Long activeTime = this.timers.getElapsedTime(timerKey);
      if (activeTime == 0L) return null;
      return (((double)this.totalProcessMillis) / ((double) activeTime));
    }
  }

  /**
   * Returns the number of info messages that have been processed.  This may be
   * equal to or greater than the number of MQ-vendor messages that have been
   * completed because some MQ-vendor messages are batches of info messages.
   *
   * @return The number of info messages that have been processed.
   */
  public long getProcessedInfoMessageCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.processedMessageCount;
    }
  }

  /**
   * Returns the number of info messages that have been processed successfully
   * without an exception.  This may be equal to or greater than the number of
   * MQ-vendor messages that have been completed because some MQ-vendor messages
   * are batches of info messages and some messages are retried after success
   * because they belong to a batch that gets retried.
   *
   * @return The number of info messages that have been processed successfully.
   */
  public long getInfoMessageSuccessCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.processSuccessCount;
    }
  }

  /**
   * Returns the number of info messages that experienced a failure during
   * processing in the form of an exception being thrown.  This may be equal to
   * or greater than the number of MQ-vendor messages that have been completed
   * because some MQ-vendor messages are batches of info messages and failed
   * messages will get retried eventually.
   *
   * @return The number of info messages that experienced a failure during
   *         processing.
   */
  public long getInfoMessageFailureCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.processFailureCount;
    }
  }

  /**
   * Enqueues the one or more info messages contained in the specified
   * framework-specific message.  If the message text is <code>null</code>
   * or empty-string then this method does nothing.  If the message text
   * contains text that cannot be parsed as JSON then the unrecognized message
   * is logged and no messages are enqueued.
   *
   * @param processor The {@link MessageProcessor} to enqueue with.
   * @param message The framework-specific message that was received.
   */
  protected void enqueueMessages(MessageProcessor processor, M message) {
    if (this.getState() != CONSUMING) {
      throw new IllegalStateException(
          "Cannot enqueue messages in not in the " + CONSUMING + " state.  "
              + "Current state is " + this.getState());
    }
    this.timerStart(enqueue);
    try {
      // get the message text and ensure it is non-empty and non-null
      String messageText = this.extractMessageBody(message);

      logDebug("RECEIVED MESSAGE: ", messageText);

      if (messageText == null) return;
      messageText = messageText.trim();
      if (messageText.length() == 0) return;

      List<InfoMessage<M>> infoMessages = null;
      try {
        // construct the batch
        MessageBatch<M> batch = new MessageBatch<>(message, messageText);

        // get the info messages
        infoMessages = batch.getInfoMessages();

      } catch (Exception e) {
        logWarning(e, "Ignoring unrecognized message body:", messageText);
        return;
      }

      // add to the queue
      synchronized (this) {
        int totalCount = this.pendingMessages.size() + infoMessages.size();
        this.pendingMessages.addAll(infoMessages);
        this.notifyAll();
        if (totalCount >= this.getMaximumPendingCount()) {
          this.throttleConsumption();
        }
      }
    } finally {
      this.timerPause(enqueue);
    }
  }

  /**
   * Throttles comsumption until the number of pending messages is less than
   * half the maximum pending count.
   */
  protected synchronized void throttleConsumption() {
    this.timerStart(throttleEnqueue);
    // wait until we work down to half the maximum pending count
    while (this.pendingMessages.size() >= (this.getMaximumPendingCount()/2)) {
      try {
        long timeout = this.getTimeout();
        this.timerStart(throttleWait);
        this.wait(timeout); // wait at most the timeout milliseconds
        this.timerPause(throttleWait);

      } catch (InterruptedException ignore) {
        // ignore the interruption
      }
    }
    this.timerPause(throttleEnqueue);
  }

  /**
   * Calls the {@link #processMessages(MessageProcessor)} function in a
   * background thread after validating the current state of this instance.
   *
   * @param processor The {@link MessageProcessor} to use for processing.
   */
  protected synchronized void backgroundProcessMessages(
      MessageProcessor  processor)
  {
    // first check if we are even consuming
    synchronized (this) {
      // check if not consuming messages
      if (this.getState() != CONSUMING) {
        throw new IllegalStateException(
            "Cannot call processMessages() if not in the " + CONSUMING
                + " state.  Current state is " + this.getState());
      }

      // check if already processing
      if (this.processing) {
        throw new IllegalStateException(
            "Cannot call processMessages() when it has already been called "
                + "and is still processing messages.");
      }

      // set the processing flag
      this.processing = true;

      // verify the processing thread is null
      if (this.processingThread != null) {
        throw new IllegalStateException(
            "Processing thread seems to already exist.");
      }

      // create the thread
      this.processingThread = new Thread(() -> {
        SUPPRESS_PROCESSING_CHECK.set(true);
        this.processMessages(processor);
      });

      // start the thread
      this.processingThread.start();
    }
  }

  /**
   * Provides a loop that continues to schedule and process messages as long as
   * the {@link State} of this instance obtained from {@link #getState()} is
   * {@link State#CONSUMING}.  If the state transitions out of {@link
   * State#CONSUMING} then only previously postponed messages will be handled
   * before the processing terminates.  This method does not return until
   * processing is complete.
   *
   * @param processor The {@link MessageProcessor} to use for consuming the
   *                  messages and optionally providing cross-process cluster
   *                  locking.
   */
  protected void processMessages(MessageProcessor processor) {
    try {
      // check if we should validate the current state
      if (!SUPPRESS_PROCESSING_CHECK.get()) {
        // first check if we are even consuming
        synchronized (this) {
          // check if not consuming messages
          if (this.getState() != CONSUMING) {
            throw new IllegalStateException(
                "Cannot call processMessages() if not in the " + CONSUMING
                    + " state.  Current state is " + this.getState());
          }

          // check if already processing
          if (this.processing) {
            throw new IllegalStateException(
                "Cannot call processMessages() when it has already been called "
                    + "and is still processing messages.");
          }

          // set the processing flag
          this.processing = true;
        }
      }

      // create the worker pool
      synchronized (this) {
        this.workerPool = new AsyncWorkerPool<>(this.getConcurrency());
      }

      // start the processing timer
      this.timerStart(processMessages, betweenMessages);

      // loop over the messages
      while (this.getState() == CONSUMING || this.getPendingMessageCount() > 0)
      {
        // initialize the message
        this.timerStart(dequeue, dequeueBlocking);
        InfoMessage<M> msg = this.dequeueMessage(processor);
        this.timerPause(dequeue);

        // check if we have a message
        if (msg != null) {
          this.timerPause(betweenMessages);
          this.timerStart(activelyProcessing);

          // send the message to a worker to be processed
          InfoMessage<M> infoMsg = msg;
          final Timers timers = new Timers();
          timers.start(waitForWorker.toString());
          AsyncResult<ProcessResult<M>> result = this.workerPool.execute(() -> {
            timers.pause(waitForWorker.toString());
            try {
              // process the message
              timers.start(serviceProcess.toString());
              processor.process(infoMsg.getMessage());
              timers.pause(serviceProcess.toString());

              // in case of success mark it as processed and disposable
              timers.start(markProcessed.toString());
              infoMsg.markProcessed(true);
              timers.pause(markProcessed.toString());

            } catch (Exception e) {
              // in case of exception mark it as processed and non-disposable
              timers.start(markProcessed.toString());
              infoMsg.markProcessed(false);
              timers.pause(markProcessed.toString());

            } finally {
              // get the associated message batch
              MessageBatch<M> batch = infoMsg.getBatch();

              // check if disposable
              if (batch.isDisposable()) {
                // dispose the message associated with the batch
                timers.start(disposeMessage.toString());
                this.disposeMessage(batch.getMessage());
                timers.pause(disposeMessage.toString());
              }

              this.recordStatistics(infoMsg, timers);
            }

            return new ProcessResult<>(infoMsg, timers);
          });

          // handle any result that was received
          this.handleAsyncResult(result);
        }

        this.timerStart(betweenMessages);
      }

      // when done, close out the worker pool
      try {
        // if we get here then all postponed messages have been processed and we
        // are no longer consuming messages -- time to wait for completion of
        // in-flight messages so they can be disposed
        List<AsyncResult<ProcessResult<M>>> results = this.workerPool.close();
        for (AsyncResult<ProcessResult<M>> result : results) {
          this.handleAsyncResult(result);
        }

      } finally {
        this.timerPause(processMessages,
                        activelyProcessing,
                        waitingForMessages);

        synchronized (this) {
          this.processing = false;
          this.workerPool = null;
          this.notifyAll();
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Dequeues a previously enqueued {@link InfoMessage}.
   *
   * @param processor The {@link MessageProcessor} that is being used for
   *                  consumption.
   *
   * @return The {@link InfoMessage} that was dequeued.
   */
  protected synchronized InfoMessage<M> dequeueMessage(
      MessageProcessor  processor)
  {
    this.timerPause(dequeueBlocking);
    this.timerStart(dequeueMessageWaitLoop);

    // set the hit flag to true
    boolean hit = true;

    // wait for a message to be available
    while ((this.getState() == CONSUMING) && (this.pendingMessages.size() == 0))
    {
      // if we get here then no message was ready, set hit flag to false
      hit = false;

      // toggle the timers
      this.toggleActiveAndWaitingTimers(this.pendingMessages.size(),
                                        this.workerPool.isBusy());

      // determine how long to wait
      long timeout = this.getTimeout();

      // wait for the designated duration
      this.timerStart(dequeueMessageWait);
      try {
        this.wait(timeout);

      } catch (InterruptedException ignore) {
        // ignore the interruption
      } finally {
        this.timerPause(dequeueMessageWait);
      }
    }
    this.timerPause(dequeueMessageWaitLoop);

    InfoMessage<M> msg = null;

    // if none ready then check if we can grab a pending message
    // NOTE: we do not get more pending messages if state is not CONSUMING
    while (this.pendingMessages.size() > 0) {
      // get the candidate message
      msg = this.pendingMessages.remove(0);

      // ensure the timers are toggled correctly
      this.timerPause(waitingForMessages);
      this.timerStart(activelyProcessing);
      this.updateDequeueHitRatio(hit);

      // this will short-circuit the loop
      return msg;
    }

    // toggle the timers
    this.toggleActiveAndWaitingTimers(this.pendingMessages.size(),
                                      this.workerPool.isBusy());

    this.updateDequeueHitRatio(false);

    // return null if we get here
    return null;
  }

  /**
   * Handles recording statistics for the specified completed
   * {@link InfoMessage} and {@link Timers} in a thread-safe manner.
   *
   * @param message The completed {@link InfoMessage}.
   * @param timers The {@link Timers} used to process the {@link InfoMessage}.
   */
  protected void recordStatistics(InfoMessage<M> message, Timers timers) {
    synchronized (this.getStatisticsMonitor()) {
      MessageBatch<M> batch           = message.getBatch();
      boolean         lastInBatch     = message.isLastInBatch();
      boolean         firstFailure    = message.isFirstFailure();
      String          timerKey        = serviceProcess.toString();
      long            serviceMillis   = timers.getElapsedTime(timerKey);
      this.processedMessageCount++;
      this.totalProcessMillis += serviceMillis;
      if (firstFailure) {
        this.batchRetryCount++;
        this.processRetryCount += batch.getInfoMessages().size();
      }
      if (message.isDisposable()) {
        this.processSuccessCount++;
      } else {
        this.processFailureCount++;
      }
      if (lastInBatch) {
        this.processedBatchCount++;
        long roundTripMillis = batch.getLifespanNanos() / 1000000L;
        this.totalRoundTripMillis += roundTripMillis;
        if (roundTripMillis > this.longestRoundTripMillis) {
          this.longestRoundTripMillis = roundTripMillis;
        }
      }
      this.timerMerge(timers);
    }
  }

  /**
   * Handles the {@link AsyncResult} from the {@link AsyncWorkerPool} after it
   * is received.  This extracts the {@link ProcessResult} value and traps any
   * exceptions (there should be none).  It records the timings from the
   * processing and calls {@link #postProcess(InfoMessage)}.
   *
   * @param result The {@link AsyncResult} to handle, or <code>null</code> if
   *               no result was returned.
   */
  protected void handleAsyncResult(AsyncResult<ProcessResult<M>> result)
  {
    if (result == null) return;
    ProcessResult<M> processResult = null;
    try {
      processResult = result.getValue();

    } catch (Exception cannotHappen) {
      // exceptions should be logged and consumed during processing and used
      // to determine the disposability of the message/batch.
      logError(cannotHappen, "UNEXPECTED EXCEPTION: ");
      throw new IllegalStateException(cannotHappen);
    }
    InfoMessage<M>  message = processResult.getInfoMessage();
    this.timerStart(postProcess);
    this.postProcess(message);
    this.timerPause(postProcess);
  }

  /**
   * This method does nothing, but provides a hook so that it may be overridden
   * to do any special handling on the {@link InfoMessage} after it has been
   * processed by the {@link MessageProcessor}.
   *
   * @param infoMessage The {@link InfoMessage} that was processed.
   */
  protected void postProcess(InfoMessage<M> infoMessage) {
    // do nothing
  }

  /**
   * Extracts the JSON {@link String} message body from the specified
   * framework-specific message.
   *
   * @param message The framework-specific message from which to extract the
   *                message body.
   * @return The message body JSON text as a {@link String}.
   */
  protected abstract String extractMessageBody(M message);

  /**
   * Disposes the specified framework-specific message. This method is called
   * for framework-specific messages that have been successfully handled or
   * failed but cannot be retried.
   *
   * @param message The framework-specific message to dispose of.
   */
  protected abstract void disposeMessage(M message);

  /**
   * Encapsulates a message for a message queue type along with the flags for
   * each sub-message in a batch indicating if the parent message can be
   * disposed.
   *
   * @param <M> The framework-specific message type.
   */
  protected static class MessageBatch<M> {
    /**
     * The message for the message queue type.
     */
    private M message;

    /**
     * The array of {@link InfoMessage} instances for the batch.
     */
    private List<InfoMessage<M>> infoMessages;

    /**
     * The number of {@link InfoMessage} instances still pending completion.
     */
    private int pendingCount;

    /**
     * Used to flag whether at least one message from the batch has failed.
     */
    private boolean failed = false;

    /**
     * The timestamp when the message batch was enqueued.
     */
    private long enqueueTimeNanos;

    /**
     * The timestamp when all the messages in the batch have been processed.
     */
    private long completedTimeNanos;

    /**
     * Constructs with the framework-specific message object and the text of
     * the message body.
     *
     * @param message The framework-specific message object.
     * @param messageText The text of the message body.
     */
    public MessageBatch(M message, String messageText) {
      this.message = message;

      // check if an array or an object
      try {
        if (messageText.charAt(0) == '{') {
          // we have an object -- parse it
          JsonObject jsonObject = parseJsonObject(messageText);
          InfoMessage<M> pending = new InfoMessage<>(this, jsonObject);
          this.infoMessages = List.of(pending);

        } else {
          // assume we have a JSON array of JSON objects
          JsonArray jsonArray = parseJsonArray(messageText);
          this.infoMessages = new ArrayList<>(jsonArray.size());
          for (JsonObject jsonObject : jsonArray.getValuesAs(JsonObject.class))
          {
            InfoMessage<M> pending = new InfoMessage<>(this, jsonObject);
            this.infoMessages.add(pending);
          }
          this.infoMessages = unmodifiableList(this.infoMessages);
        }

        // set the pending count
        this.enqueueTimeNanos   = System.nanoTime();
        this.pendingCount       = this.infoMessages.size();
        this.completedTimeNanos = -1L;

      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Gets the framework-specific message object associated with this
     * {@link MessageBatch} instance.
     *
     * @return The framework-specific message object associated with this
     *        {@link MessageBatch} instance.
     */
    public M getMessage() {
      return this.message;
    }

    /**
     * Gets the <b>unmodifiable</b> {@link List} of {@link InfoMessage}
     * instances from the message batch described by the associated MQ message.
     *
     * @return The <b>unmodifiable</b> {@link List} of associated {@link
     *         InfoMessage} instances.
     * }
     */
    public List<InfoMessage<M>> getInfoMessages() {
      return this.infoMessages;
    }
    
    /**
     * Checks if the entire batch of messages has been processed and all are
     * flagged disposable.
     *
     * @return <code>true</code> if the entire batch of messages is disposable,
     *         otherwise <code>false</code>.
     */
    public synchronized boolean isDisposable() {
      for (InfoMessage msg: this.infoMessages) {
        if (!msg.isDisposable()) {
          return false;
        }
      }
      return true;
    }

    /**
     * Checks if any of the messages in the have not yet been processed.
     *
     * @return <code>true</code> if any message in the batch has not yet been
     *         processed, and <code>false</code> if the entire batch has been
     *         processed.
     */
    public synchronized boolean isPending() {
      return (this.pendingCount > 0);
    }

    /**
     * Gets the number of nanoseconds since this instance was constructed up
     * until all messages in the batch have been processed (or failed) or
     * up until the current time if some messages are still pening.
     *
     * @return The number of nanoseconds
     */
    public synchronized long getLifespanNanos() {
      long end = (this.completedTimeNanos < 0L)
          ? System.nanoTime() : this.completedTimeNanos;
      return (end - this.enqueueTimeNanos);
    }

    /**
     * Decrements the pending count.  This is a private message called by the
     * {@link InfoMessage} when it is marked as processed.  The return value
     * is negative if pending info messages remain and this is the first time
     * it is called with <code>true</code> for the failed parameter.  The
     * return value is <code>null</code> if there are no remaining pending info
     * messages and this is the first time it is called with <code>true</code>
     * for the failed parameter.  In all other cases the return value is a
     * non-negative integer indicating how many info messages remain pending.
     *
     * @param failed <code>true</code> if the calling info message has failed
     *               processing, otherwise <code>false</code>.
     *
     * @return The pending count for the batch after decrementing.
     */
    private synchronized Integer decrementPendingCount(boolean failed) {
      boolean failed0 = this.failed;
      if (failed) this.failed = true;
      this.pendingCount--;
      if (this.pendingCount == 0) {
        this.completedTimeNanos = System.nanoTime();
      }
      boolean firstFailure = (!failed0 && this.failed);
      if (firstFailure) {
        return (this.pendingCount != 0) ? (-1 * this.pendingCount) : null;
      }
      return this.pendingCount;
    }
  }

  /**
   * Describes a single pending info message which is associated with a
   * batch.  If a single message then a batch of one.
   *
   * @param <M> The message type.
   */
  protected static class InfoMessage<M> {
    /**
     * The associated {@link MessageBatch}.
     */
    private MessageBatch<M> batch;

    /**
     * The {@link JsonObject} describing the info message.
     */
    private JsonObject message;

    /**
     * Flag indicating if this message is processed and is now disposable.
     * This value is <code>null</code> if the message has not yet been
     * processed.
     */
    private Boolean disposable;

    /**
     * Flag indicating if the completion of this {@link InfoMessage} completes
     * the batch to which it belongs.
     */
    private boolean lastInBatch = false;

    /**
     * Flag indicating if this is the first {@link InfoMessage} in the
     * associated batch that has failed.
     */
    private boolean firstFailure = false;

    /**
     * Constructs a pending message.
     *
     * @param batch The {@link MessageBatch} to associate.
     * @param message The {@link JsonObject} for the sub-message.
     */
    public InfoMessage(MessageBatch<M> batch, JsonObject message)

    {
      this.batch        = batch;
      this.message      = message;
      this.disposable   = null;
      this.lastInBatch  = false;
    }

    /**
     * Gets the associated {@link MessageBatch}.
     *
     * @return The associated {@link MessageBatch}.
     */
    public MessageBatch<M> getBatch() {
      return this.batch;
    }

    /**
     * Gets the associated {@link JsonObject} message.
     *
     * @return The {@link JsonObject} describing the message.
     */
    public JsonObject getMessage() {
      return this.message;
    }

    /**
     * Checks if this {@link InfoMessage} was the last one that was completed
     * in the batch to which it belongs.
     *
     * @return <code>true</code> if this info message is the last one completed
     *         in the batch to which it belongs, and <code>false</code> if not.
     */
    public synchronized boolean isLastInBatch() {
      return this.lastInBatch;
    }

    /**
     * Checks if this {@link InfoMessage} failed processing <b>and</b> was the
     * first one within the associated batch to fail.
     *
     * @return <code>true</code> if this info message failed processing
     *         <b>and</b> was the first one within the associated batch to fail,
     *         otherwise <code>false</code>.
     */
    public synchronized boolean isFirstFailure() {
      return this.firstFailure;
    }

    /**
     * Checks if this message has been processed (whether or not processing
     * succeeded or failed).
     *
     * @return <code>true</code> if the message has been processed, otherwise
     *         <code>false</code>.
     */
    public synchronized boolean isPending() {
      return (this.disposable == null);
    }

    /**
     * Checks if this message can be disposed after processing.  If the message
     * has not yet been processed (i.e.: it is still pending) then this method
     * returns <code>null</code>.
     *
     * @return <code>true</code> if this message has been processed and can
     *         be disposed, and <code>false</code> if not yet processed or
     *         processed and should be retried.
     */
    public synchronized boolean isDisposable() {
      return Boolean.TRUE.equals(this.disposable);
    }

    /**
     * Marks this message as having been processed and sets whether or not
     * it is disposable or should be retried.
     *
     * @param disposable <code>true</code> if the message can be disposed of,
     *                   and <code>false</code> if it should be retried.
     */
    public void markProcessed(boolean disposable) {
      boolean decrement = false;
      synchronized (this) {
        decrement = (this.disposable == null);
        this.disposable = (disposable ? Boolean.TRUE : Boolean.FALSE);
      }
      if (decrement) {
        Integer pending = this.batch.decrementPendingCount(!disposable);
        this.lastInBatch = (pending == null || pending == 0);
        this.firstFailure = (pending == null || pending < 0);
      }
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this
     * instance.
     *
     * @return A diagnostic {@link String} describing this instance.
     */
    public String toString() {
      return "disposable=[ " + this.isDisposable() + " ]: "
          + toJsonText(this.getMessage());
    }
  }

  /**
   * Utility method for obtaining a {@link String} configuration parameter
   * with options to check if missing and required.  This will throw
   * a {@link MessageConsumerSetupException} if it fails.  Any {@link String}
   * value that is obtained will be trimmed of leading and trailing whitespace
   * and if empty will be returned as <code>null</code>.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param required <code>true</code> if required, otherwise
   *                 <code>false</code>.
   * @return The {@link String} configuration value.
   * @throws MessageConsumerSetupException If the parameter value is required
   *                                       but is missing.
   */
  protected static String getConfigString(JsonObject  config,
                                          String      key,
                                          boolean     required)
      throws MessageConsumerSetupException
  {
    return getConfigString(config, key, required, true);
  }

  /**
   * Utility method for obtaining a {@link String} configuration parameter
   * with options to check if missing and required.  This will throw
   * a {@link MessageConsumerSetupException} if it fails.  Any {@link String}
   * value that is obtained will be trimmed of leading and trailing whitespace.
   * Resultant empty-string values will optionally be converted to
   * <code>null</code> if the normalization parameter is set to
   * <code>true</code> and will be returned as-is if <code>false</code>.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param required <code>true</code> if required, otherwise
   *                 <code>false</code>.
   * @param normalize <code>true</code> if empty or pure whitespace strings
   *                  should be returned as <code>null</code>, otherwise
   *                  <code>false</code> to return them as-is.
   * @return The {@link String} configuration value.
   * @throws MessageConsumerSetupException If a failure occurs in obtaining the
   *                                       parameter value.
   */
  protected static String getConfigString(JsonObject  config,
                                          String      key,
                                          boolean     required,
                                          boolean     normalize)
      throws MessageConsumerSetupException
  {
    // check if required and missing
    if (required && !config.containsKey(key)) {
      throw new MessageConsumerSetupException(
          "Following configuration parameter missing: " + key);
    }

    String result = getConfigString(config, key, null, normalize);

    // check if required and missing
    if (required && normalize && result == null) {
      throw new MessageConsumerSetupException(
          "Following configuration parameter is specified as null "
          + "or empty string: " + key);
    }

    // return the result
    return result;
  }

  /**
   * Utility method for obtaining a {@link String} configuration parameter
   * with option to return a default value if missing.  This will throw
   * a {@link MessageConsumerSetupException} if it fails.  Any {@link String}
   * value that is obtained will be trimmed of leading and trailing whitespace
   * and if empty will be returned as <code>null</code>.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param defaultValue The default value to return if the value is missing.
   * @return The {@link String} configuration value.
   * @throws MessageConsumerSetupException If the value is required but not
   *                                       present.
   */
  protected static String getConfigString(JsonObject  config,
                                          String      key,
                                          String      defaultValue)
      throws MessageConsumerSetupException
  {
    try {
      return getConfigString(config, key, defaultValue, true);

    } catch (Exception e) {
      throw new MessageConsumerSetupException(
          "Failed to parse JSON configuration parameter (" + key + "): "
              + e.getMessage());
    }
  }

  /**
   * Utility method for obtaining a {@link String} configuration parameter
   * with option to return a default value if missing.  This will throw
   * a {@link MessageConsumerSetupException} if it fails. Any {@link String}
   * value that is obtained will be trimmed of leading and trailing whitespace.
   * Resultant empty-string values will optionally be converted to
   * <code>null</code> if the normalization parameter is set to
   * <code>true</code> and will be returned as-is if <code>false</code>.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param defaultValue The default value to return if the value is missing.
   * @param normalize <code>true</code> if empty or pure whitespace strings
   *                  should be returned as <code>null</code>, otherwise
   *                  <code>false</code> to return them as-is.
   * @return The {@link String} configuration value.
   * @throws MessageConsumerSetupException If the value could not be
   *                                       interpreted as a {@link String}
   *                                       for some reason.
   */
  protected static String getConfigString(JsonObject  config,
                                          String      key,
                                          String      defaultValue,
                                          boolean     normalize)
      throws MessageConsumerSetupException
  {
    try {
      String result = JsonUtilities.getString(config, key, defaultValue);

      // trim the whitespace (regardless of normalization)
      if (result != null) result = result.trim();

      // optionally normalize empty string to null
      if (normalize && result != null && result.length() == 0) {
        result = null;
      }

      // return the result
      return result;

    } catch (Exception e) {
      throw new MessageConsumerSetupException(
          "Failed to parse JSON configuration parameter (" + key + "): "
              + e.getMessage());
    }
  }

  /**
   * Utility method for obtaining an {@link Integer} configuration parameter
   * with options to check if missing and required or if it is less than an
   * optional minimum value.  This will throw {@link
   * MessageConsumerSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param required <code>true</code> if required, otherwise
   *                 <code>false</code>.
   * @param minimum The minimum integer value allowed, or <code>null</code>
   *                if no minimum is enforced.
   * @return The {@link String} configuration value.
   * @throws MessageConsumerSetupException If the value is required and not
   *                                       present or if it is present and less
   *                                       than the optionally specified minimum
   *                                       value or could not an integer.
   */
  protected static Integer getConfigInteger(JsonObject   config,
                                            String       key,
                                            boolean      required,
                                            Integer      minimum)
      throws MessageConsumerSetupException
  {
    // check if required and missing
    if (required && !config.containsKey(key)) {
      throw new MessageConsumerSetupException(
          "Following configuration parameter missing: " + key);
    }
    return getConfigInteger(config, key, minimum, null);
  }

  /**
   * Utility method for obtaining an {@link Integer} configuration parameter
   * with options to check if missing and required or if it is less than an
   * optional minimum value.  This will throw {@link
   * MessageConsumerSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param minimum The minimum integer value allowed, or <code>null</code>
   *                if no minimum is enforced.
   * @param defaultValue The default value to return if the value is missing.
   * @return The {@link String} configuration value.
   * @throws MessageConsumerSetupException If the value is present and less
   *                                       than the optionally specified minimum
   *                                       value or could not an integer.
   */
  protected static Integer getConfigInteger(JsonObject   config,
                                            String       key,
                                            Integer      minimum,
                                            Integer      defaultValue)
      throws MessageConsumerSetupException
  {
    Integer result = null;
    try {
      result = JsonUtilities.getInteger(config, key, defaultValue);

    } catch (Exception e) {
      throw new MessageConsumerSetupException(
          "Failed to parse JSON configuration parameter (" + key + "): "
              + e.getMessage());
    }
    // check the result
    if (result != null && minimum != null && result < minimum) {
      throw new MessageConsumerSetupException(
          "The " + key + " configuration parameter cannot be less than "
          + minimum + ": " + result);
    }
    return result;
  }

  /**
   * Utility method for obtaining a {@link Long} configuration parameter
   * with options to check if missing and required or if it is less than an
   * optional minimum value.  This will throw {@link
   * MessageConsumerSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param required <code>true</code> if required, otherwise
   *                 <code>false</code>.
   * @param minimum The minimum integer value allowed, or <code>null</code>
   *                if no minimum is enforced.
   * @return The {@link String} configuration value.
   * @throws MessageConsumerSetupException If the value is required and not
   *                                       present or if it is present and less
   *                                       than the optionally specified minimum
   *                                       value or could not a long integer.
   */
  protected static Long getConfigLong(JsonObject  config,
                                      String      key,
                                      boolean     required,
                                      Long        minimum)
      throws MessageConsumerSetupException
  {
    // check if required and missing
    if (required && !config.containsKey(key)) {
      throw new MessageConsumerSetupException(
          "Following configuration parameter missing: " + key);
    }

    return getConfigLong(config, key, minimum, null);
  }

  /**
   * Utility method for obtaining a {@link Long} configuration parameter
   * with options to check if missing and required or if it is less than an
   * optional minimum value.  This will throw {@link
   * MessageConsumerSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param minimum The minimum integer value allowed, or <code>null</code>
   *                if no minimum is enforced.
   * @param defaultValue The default value to return if the value is missing.
   * @return The {@link String} configuration value.
   * @throws MessageConsumerSetupException If the value is less than the
   *                                       optionally specified minimum value
   *                                       or if it is not a long integer.
   */
  protected static Long getConfigLong(JsonObject  config,
                                      String      key,
                                      Long        minimum,
                                      Long        defaultValue)
      throws MessageConsumerSetupException
  {
    Long result = null;
    try {
      result = JsonUtilities.getLong(config, key, defaultValue);

    } catch (Exception e) {
      throw new MessageConsumerSetupException(
          "Failed to parse JSON configuration parameter (" + key + "): "
              + e.getMessage());
    }
    // check the result
    if (result != null && minimum != null && result < minimum) {
      throw new MessageConsumerSetupException(
          "The " + key + " configuration parameter cannot be less than "
              + minimum + ": " + result);
    }
    return result;
  }

  /**
   * Utility method for obtaining a {@link Boolean} configuration parameter
   * with options to check if missing and required.  This will throw {@link
   * MessageConsumerSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param required <code>true</code> if required, otherwise
   *                 <code>false</code>.
   * @return The {@link String} configuration value.
   * @throws MessageConsumerSetupException If the value is required and not
   *                                       present or if it is present and could
   *                                       not be interpreted as a boolean.
   */
  protected static Boolean getConfigBoolean(JsonObject  config,
                                            String      key,
                                            boolean     required)
      throws MessageConsumerSetupException {
    // check if required and missing
    if (required && !config.containsKey(key)) {
      throw new MessageConsumerSetupException(
          "Following configuration parameter missing: " + key);
    }

    return getConfigBoolean(config, key, null);
  }

  /**
   * Utility method for obtaining a {@link Long} configuration parameter
   * with options to check if missing and required or if it is less than an
   * optional minimum value.  This will throw {@link
   * MessageConsumerSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param defaultValue The default value to return if the value is missing.
   * @return The {@link String} configuration value.
   *
   * @throws MessageConsumerSetupException If the value is present but could not
   *                                       be interpreted as a boolean.
   */
  protected static Boolean getConfigBoolean(JsonObject  config,
                                            String      key,
                                            Boolean     defaultValue)
    throws MessageConsumerSetupException
  {
    try {
      return JsonUtilities.getBoolean(config, key, defaultValue);

    } catch (Exception e) {
      throw new MessageConsumerSetupException(
          "Failed to parse JSON configuration parameter (" + key + "): "
              + e.getMessage());
    }
  }

  /**
   * Converts the specified {@link Stat} instances to an array of
   * {@link String} instances.
   * @param statistics The {@link Stat} instances to convert.
   * @return The array of {@link String} instances describing the specified
   *         {@link Stat} instances.
   */
  private String[] convertTimerKeys(Stat... statistics) {
    String[] names = (statistics == null || statistics.length == 0)
        ? null : new String[statistics.length];
    if (names != null) {
      for (int index = 0; index < statistics.length; index++) {
        names[index] = statistics[index].toString();
      }
    }
    return names;
  }

  /**
   * Merges the specified {@link Timers} with this instances {@link Timers}
   * in a thread safe manner.
   * @param timers The {@link Timers} to merge.
   */
  protected void timerMerge(Timers timers) {
    synchronized (this.getStatisticsMonitor()) {
      this.timers.mergeWith(timers);
    }
  }

  /**
   * Toggles the active and waiting timers.
   * @param pendingCount The number of pending messages.
   * @param busy <code>true</code> if the worker pool is busy, otherwise
   *             <code>false</code>.
   */
  protected void toggleActiveAndWaitingTimers(int     pendingCount,
                                              boolean busy)
  {
    synchronized (this.getStatisticsMonitor()) {
      // check if there are messages
      if (busy) {
        this.timerPause(waitingForMessages);
        this.timerStart(activelyProcessing);

      } else if (pendingCount == 0) {
        // no messages pending
        this.timerPause(activelyProcessing);
        this.timerStart(waitingForMessages);

      } else if (pendingCount > 0) {
        // messages pending
        this.timerPause(waitingForMessages);
        this.timerStart(activelyProcessing);
      }
    }
  }

  /**
   * Resumes the associated {@link Timers} in a thread-safe manner.
   * @param statistic The {@link Stat} to resume.
   * @param addlTimers The additional {@link Stat} instances to resume.
   */
  protected void timerResume(Stat statistic, Stat... addlTimers) {
    String[] names = this.convertTimerKeys(addlTimers);
    synchronized (this.getStatisticsMonitor()) {
      if (names == null) {
        this.timers.resume(statistic.toString());
      } else {
        this.timers.resume(statistic.toString(), names);
      }
    }
  }

  /**
   * Starts the associated {@link Timers} in a thread-safe manner.
   * @param statistic The {@link Stat} to start.
   * @param addlTimers The additional {@link Stat} instances to start.
   */
  protected void timerStart(Stat statistic, Stat... addlTimers) {
    String[] names = this.convertTimerKeys(addlTimers);
    synchronized (this.getStatisticsMonitor()) {
      if (names == null) {
        this.timers.start(statistic.toString());
      } else {
        this.timers.start(statistic.toString(), names);
      }
    }
  }

  /**
   * Pauses the associated {@link Timers} in a thread-safe manner.
   * @param statistic The {@link Stat} to pause.
   * @param addlTimers The additional {@link Stat} instances to pause.
   */
  protected void timerPause(Stat statistic, Stat... addlTimers) {
    String[] names = this.convertTimerKeys(addlTimers);
    synchronized (this.getStatisticsMonitor()) {
      if (names == null) {
        this.timers.pause(statistic.toString());
      } else {
        this.timers.pause(statistic.toString(), names);
      }
    }
  }

  /**
   * The encapsulation of the result from the async workers.
   * @param <M> The vendor-specific message type.
   */
  protected static class ProcessResult<M> {
    /**
     * The {@link InfoMessage} associated with the result.
     */
    private InfoMessage<M> infoMessage;

    /**
     * The {@link Timers} used to time activity within the worker.
     */
    private Timers timers;

    /**
     * Constructs with the specified parameters.
     * @param infoMessage The {@link InfoMessage} to associate with the result.
     * @param timers The {@link Timers} to associate with the result.
     */
    public ProcessResult(InfoMessage<M> infoMessage, Timers timers) {
      this.infoMessage = infoMessage;
      this.timers = timers;
    }

    /**
     * Gets the associated {@link InfoMessage}.
     * @return The associated {@link InfoMessage}.
     */
    public InfoMessage<M> getInfoMessage() {
      return this.infoMessage;
    }

    /**
     * Gets the associated {@link Timers}.
     * @return The associated {@link Timers}.
     */
    public Timers getTimers() {
      return this.timers;
    }
  }
}
