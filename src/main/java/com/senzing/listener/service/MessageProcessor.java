package com.senzing.listener.service;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;

import javax.json.JsonObject;

/**
 * Defines an interface for a {@link MessageProcessor} that can process
 * messages that are received.
 */
public interface MessageProcessor {
  /**
   * This method is called by the consumer.  Processes the message passed to
   * the service from the consumer.
   * 
   * @param message The message to process.
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  void process(JsonObject message) throws ServiceExecutionException;
}
