package com.senzing.listener.service;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.util.AccessToken;
import com.senzing.listener.communication.MessageConsumer;

import java.util.Set;

/**
 * Defines an interface for a {@link ListenerService} that can process
 * messages that are received.
 */
public interface ListenerService {
  /**
   * For initializing any needed resources before processing
   * 
   * @param config Configuration needed for the processing
   * 
   * @throws ServiceSetupException If a failure occurs.
   */
  void init(String config) throws ServiceSetupException;
  
  /**
   * This method is called by the consumer.  Processes the message passed to
   * the service from the consumer.
   * 
   * @param message The message to process.
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  void process(String message) throws ServiceExecutionException;

  /**
   * For cleaning up after processing, e.g. free up resources.
   */
  void destroy();
}
