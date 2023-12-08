package com.senzing.listener.communication.sql;

import java.io.Serializable;

/**
 * Provides a simple message implementation representing a message
 * that has been stored in a database table and leased for consumption.
 */
public class LeasedMessage implements Serializable {
    /**
     * The unique ID identifying the message in the database.
     */
    private long messageId;

    /**
     * The lease ID identifying the lease for the message.
     */
    private String leaseId;

    /**
     * The millisecond UTC time for the expiration of the lease on
     * this message.
     */
    private long leaseExpiration;

    /**
     * The message text.
     */
    private String messageText;

    /**
     * Constructs with the specified parameters.
     * 
     * @param messageId The unique ID identifying the message in the database.
     * @param messageText The message text.
     * @param leaseId The lease ID identifying the lease for the message.
     * @param leaseExpiration The millisecond UTC time for the expiration of
     *                        the lease on this message.
     */
    public LeasedMessage(long       messageId, 
                         String     messageText,
                         String     leaseId,
                         long       leaseExpiration)   
    {
        this.messageId          = messageId;
        this.messageText        = messageText;
        this.leaseId            = leaseId;
        this.leaseExpiration    = leaseExpiration;
    }
    
    /**
     * Gets the unique message ID that has been assigned to this 
     * message in the database.
     * 
     * @return The unique message ID that has been assigned to this 
     *         message in the database.
     */
    public long getMessageId() {
        return this.messageId;
    }

    /**
     * Gets the lease ID for the lease on this message.
     * 
     * @return the lease ID for the lease on this message.
     */
    public String getLeaseId() {
        return this.leaseId;
    }

    /**
     * Gets the millisecond UTC time indicating when the lease on this message
     * will expire.
     * 
     * @return The millisecond UTC time indicating when the lease on this message
     *         will expire.
     */
    public long getLeaseExpiration() {
        return this.leaseExpiration;
    }
    
    /**
     * Sets the millisecond UTC time indicating the updated time when the lease
     * on this message will expire.
     * 
     * @param leaseExpiration The millisecond UTC time when the lease on the 
     *                        message will expire.
     */
    public void extendLease(long leaseExpiration) {
        this.leaseExpiration = leaseExpiration;
    }

    /**
     * Gets the text for the message.
     * 
     * @return The text for the message.
     */
    public String getMessageText() {
        return this.messageText;
    }
}
