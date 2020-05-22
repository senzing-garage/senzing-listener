package com.senzing.listener.senzing.service.exception;

public class ServiceExecutionException extends Exception {

  public ServiceExecutionException(String message) {
    super(message);
  }

  public ServiceExecutionException(Exception e) {
    super(e);
  }
}
