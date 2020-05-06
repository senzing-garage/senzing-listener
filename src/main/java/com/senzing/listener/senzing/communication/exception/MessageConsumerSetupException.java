package com.senzing.listener.senzing.communication.exception;

public class MessageConsumerSetupException extends Exception {

  private static final long serialVersionUID = 1L;

  public MessageConsumerSetupException(String message) {
    super(message);
  }

  public MessageConsumerSetupException(Exception e) {
    super(e);
  }
}
