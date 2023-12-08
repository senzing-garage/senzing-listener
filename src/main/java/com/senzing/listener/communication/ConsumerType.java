package com.senzing.listener.communication;

/**
 * Enumerates the supported consumer types.
 */
public enum ConsumerType {
  /**
   * Uses for database message queuue via <code>sz_message_queue</code> table.
   */
  DATABASE,

  /**
   * Used for Rabbit MQ.
   */
  RABBIT_MQ,

  /**
   * Used for Amazon SQS.
   */
  SQS
}
