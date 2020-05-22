package com.senzing.listener.senzing.service;

import com.senzing.listener.senzing.service.exception.ServiceExecutionException;
import com.senzing.listener.senzing.service.exception.ServiceSetupException;

public interface ListenerService {

  /**
   * For initializing any needed resources before processing
   * 
   * @param config Configuration needed for the processing
   * 
   * @throws ServiceSetupException
   */
  public void init(String config) throws ServiceSetupException;

  /**
   * This method is called by the consumer.  Processes messages passed to the service from the consumer.
   * 
   * @param message
   * 
   * @throws ServiceExecutionException
   */
  public void process(String message) throws ServiceExecutionException;

  /**
   * For cleaning up after processing, e.g. free up resources.
   */
  public void cleanUp();
}
