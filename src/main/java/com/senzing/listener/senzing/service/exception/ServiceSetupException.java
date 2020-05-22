package com.senzing.listener.senzing.service.exception;

public class ServiceSetupException extends Exception {

  private static final long serialVersionUID = 1L;

  public ServiceSetupException(String message) {
    super(message);
  }

  public ServiceSetupException(Exception e) {
    super(e);
  }

  public ServiceSetupException(String message, Exception e) {
    super(message, e);
  }
}
