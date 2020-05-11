package com.senzing.listener.senzing.service;

import com.senzing.listener.senzing.service.exception.ServiceExecutionException;
import com.senzing.listener.senzing.service.exception.ServiceSetupException;

public interface ListenerService {

  public void init(String config) throws ServiceSetupException;

  public void process(String message) throws ServiceExecutionException;
}
