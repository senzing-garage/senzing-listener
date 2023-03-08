package com.senzing.listener.service;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.util.Quantified;

import javax.json.JsonObject;

/**
 * Defines an interface for a {@link ListenerService} that can process
 * messages that are received.
 */
public interface ListenerService extends MessageProcessor, Quantified {
  /**
   * Enumerates the states of a {@link ListenerService}.
   */
  enum State {
    /**
     * The {@link ListenerService} has not yet been initialized.
     */
    UNINITIALIZED,

    /**
     * The {@link ListenerService} is initializing, but has not finished
     * initializing.
     */
    INITIALIZING,

    /**
     * The {@link ListenerService} has completed initialization, and is ready
     * to process messages.
     */
    AVAILABLE,

    /**
     * The {@link ListenerService} has begun destruction, but may still be
     * processing whatever messages were in progress.
     */
    DESTROYING,

    /**
     * The {@link ListenerService} is no longer processing messages and has
     * been destroyed.
     */
    DESTROYED;
  }

  /**
   * Gets the {@link State} of this instance.
   *
   * @return The {@link State} of this instance.
   */
  State getState();

  /**
   * For initializing any needed resources before processing
   * 
   * @param config Configuration needed for the processing
   * 
   * @throws ServiceSetupException If a failure occurs.
   */
  void init(JsonObject config) throws ServiceSetupException;

  /**
   * This method is called by the consumer.  Processes the message passed to
   * the service from the consumer.
   * 
   * @param message The message to process.
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  void process(JsonObject message) throws ServiceExecutionException;

  /**
   * For cleaning up after processing, e.g. free up resources.
   */
  void destroy();
}
