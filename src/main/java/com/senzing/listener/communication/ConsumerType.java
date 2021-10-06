package com.senzing.listener.senzing.communication;

/**
 * Enumerates the supported consumer types.
 */
public enum ConsumerType {
  /**
   * Used for Rabbit MQ.
   */
  RABBIT_MQ,

  /**
   * Used for Amazon SQS.
   */
  SQS
}
