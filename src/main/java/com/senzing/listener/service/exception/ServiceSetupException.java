package com.senzing.listener.senzing.service.exception;

import com.senzing.listener.senzing.service.ListenerService;

/**
 * Exception that can be thrown if a {@link ListenerService} fails
 * initialization.
 */
public class ServiceSetupException extends Exception {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs with the specified message describing why the failure occurred.
   *
   * @param message The message describing why the failure occurred.
   */
  public ServiceSetupException(String message) {
    super(message);
  }

  /**
   * Constructs with the specified {@link Exception} describing the underlying
   * cause of the failure.
   *
   * @param e The {@link Exception} describing the underlying cause of the
   *          failure.
   */
  public ServiceSetupException(Exception e) {
    super(e);
  }

  /**
   * Constructs with the specified message describing the failure and the
   * {@link Exception} describing the underlying cause of the failure.
   *
   * @param message The message describing why the failure occurred.
   *
   * @param e The {@link Exception} describing the underlying cause of the
   *          failure.
   */
  public ServiceSetupException(String message, Exception e) {
    super(message, e);
  }
}
