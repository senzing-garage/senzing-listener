package com.senzing.listener.communication;

import com.senzing.listener.communication.exception.MessageConsumerException;
import com.senzing.listener.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.service.ListenerService;

/**
 * Interface for a queue consumer.
 */

public interface MessageConsumer {
  /**
   * Enumerates the state of the {@link MessageConsumer}.
   */
  enum State {
    /**
     * The {@link MessageConsumer} has not yet been initialized.
     */
    UNINITIALIZED,

    /**
     * The {@link MessageConsumer} is initializing, but has not finished
     * initializing.
     */
    INITIALIZING,

    /**
     * The {@link MessageConsumer} has completed initialization, but is not
     * yet consuming messages.
     */
    INITIALIZED,

    /**
     * The {@link MessageConsumer} is consuming messages.
     */
    CONSUMING,

    /**
     * The {@link MessageConsumer} has begun destruction, but may still be
     * processing whatever messages were in progress.
     */
    DESTROYING,

    /**
     * The {@link MessageConsumer} is no longer processing messages and has
     * been destroyed.
     */
    DESTROYED;
  }

  /**
   * Obtains the {@link State} of this {@link MessageConsumer}.  Whenever the
   * state changes the implementation should perform a {@link
   * Object#notifyAll()} on this instance to notify any thread awaiting the
   * state change.
   *
   * @return The {@link State} of this {@link MessageConsumer}.
   */
  State getState();

  /**
   * Initializes the consumer.
   * 
   * @param config Configuration string.  It can be in JSON or other appropriate
   *               format.
   * 
   * @throws MessageConsumerSetupException If a failure occurs.
   */
  void init(String config) throws MessageConsumerSetupException;

  /**
   * Consumer main function.  This method receives messages from the message
   * source (i.e.: vendor-specific framework) and delegates processing to the
   * specified {@link ListenerService}.  This method returns immediately, but
   * consumption continues in the background until the {@link #destroy()} method
   * is called.  Usage might look like:
   *
   * <p>
   *   Example 1 (idle wait):
   * <pre>
   *   consumer.consume(listenerService);
   *
   *   synchronized (consumer) {
   *     while (consumer.getState() != DESTROYED) {
   *       try {
   *          consumer.wait(timeoutPeriod);
   *       } catch (InterruptedException ignore) {
   *         // ignore the exception
   *       }
   *     }
   *   }
   * </pre>
   *
   * <p>
   *   Example 2 (busy wait):
   * <pre>
   *   consumer.consume(listenerService);
   *
   *   while (consumer.getState() == CONSUMING) {
   *     try {
   *        Thread.sleep(timeout);
   *     } catch (InterruptedException ignore) {
   *       // ignore
   *     }
   *   }
   * </pre>
   *
   * <p>
   *   Example 3 (active destroy):
   * <pre>
   *   consumer.consume(listenerService);
   *
   *   try {
   *      Thread.sleep(timeout);
   *   } catch (InterruptedException ignore) {
   *     // ignore
   *   }
   *
   *   consumer.destroy();
   * </pre>
   *
   * @param service Processes messages
   * 
   * @throws MessageConsumerException If a failure occurs.
   */
	void consume(ListenerService service) throws MessageConsumerException;

  /**
   * Closes the consumer and completes message processing.
   */
  void destroy();
}
