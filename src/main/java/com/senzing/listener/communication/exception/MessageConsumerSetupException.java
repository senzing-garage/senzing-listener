package com.senzing.listener.communication.exception;

/**
 * Exception thrown when message consumer fails initialization.
 */
public class MessageConsumerSetupException extends Exception {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs with the specified message.
   * @param message The message with which to construct.
   */
  public MessageConsumerSetupException(String message) {
    super(message);
  }

  /**
   * Constructs with the specified {@link Exception} describing the
   * underlying failure that occurred.
   *
   * @param e The {@link Exception} descrbing the underlying failure that
   *          occurred.
   */
  public MessageConsumerSetupException(Exception e) {
    super(e);
  }
}
