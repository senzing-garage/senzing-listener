package com.senzing.listener.communication;

import com.senzing.listener.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.service.ListenerService;

/**
 * Interface for a queue consumer.
 */

public interface MessageConsumer {
  /**
   * Initializes the consumer.
   * 
   * @param config Configuration string.  It can be in JSON or other appropriate format.
   * 
   * @throws MessageConsumerSetupException If a failure occurs.
   */
  void init(String config) throws MessageConsumerSetupException;

  /**
   * Consumer main function.  Receives messages from message source and processes.
   * 
   * @param service Processes messages
   * 
   * @throws Exception If a failure occurs.
   */
	void consume(ListenerService service) throws Exception;
}
