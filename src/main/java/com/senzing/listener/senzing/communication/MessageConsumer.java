package com.senzing.listener.senzing.communication;

import com.senzing.listener.senzing.communication.exception.MessageConsumerSetupException;
import com.senzing.listener.senzing.service.ListenerService;

/**
 * Interface for a queue consumer.
 */

public interface MessageConsumer {

  /**
   * Initializes the consumer.
   * 
   * @param config Configuration string.  It can be in JSON or other appropriate format.
   * 
   * @throws MessageConsumerSetupException
   */
  public void init(String config) throws MessageConsumerSetupException;

  /**
   * Consumer main function.  Receives messages from message source and processes.
   * 
   * @param service Processes messages
   * 
   * @throws Exception
   */
	public void consume(ListenerService service) throws Exception;
}
