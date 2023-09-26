package com.senzing.listener.communication.exception;

/**
 * Exception thrown when message consumer encounters a failure.
 */
public class MessageConsumerException extends Exception {
  /**
   * Constructs with the specified message.
   * @param message The message with which to construct.
   */
  public MessageConsumerException(String message) {
    super(message);
  }

  /**
   * Constructs with the specified {@link Exception} describing the
   * underlying failure that occurred.
   *
   * @param cause The {@link Exception} descrbing the underlying failure
   *              that occurred.
   */
  public MessageConsumerException(Exception cause) {
    super(cause);
  }

  /**
   * Constructs with the specified message and {@link Exception}
   * describing the underlying failure that occurred.
   *
   * @param message The message with which to construct.
   * @param cause The {@link Exception} descrbing the underlying failure
   *              that occurred.
   */
  public MessageConsumerException(String     message,
                                  Exception  cause)
  {
    super(message, cause);
  }
}
