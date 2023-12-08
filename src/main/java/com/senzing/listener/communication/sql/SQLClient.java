package com.senzing.listener.communication.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.List;
import java.util.LinkedList;
import java.util.Objects;

import com.senzing.sql.DatabaseType;

import static com.senzing.sql.SQLUtilities.*;

/**
 * Interface for adapting the {@link SQLConsumer} to a specific database.
 */
interface SQLClient {
    /**
     * Gets the {@link DatabaseType} for this client implementation.
     * 
     * @return The {@link DatabaseType} for this client implementation.
     */
    DatabaseType getDatabaseType();
    
    /**
     * Ensures the schema required for managing the message queue exists.
     * 
     * @param conn The {@link Connection} to use.
     * 
     * @param recreate <code>true</code> if the schema should be dropped and
     *                 recreated, <code>false</code> if it should just be
     *                 created if it does not exist.
     * 
     * @throws SQLException If a database failure occurs.
     */
    void ensureSchema(Connection conn, boolean recreate) throws SQLException;

    /**
     * Checks if the message queue is empty.
     * 
     * @param conn The {@link Connection} to use.
     * 
     * @throws SQLException If a database failure occurs.
     */
    default boolean isQueueEmpty(Connection conn) throws SQLException {
        PreparedStatement ps = null;
        ResultSet         rs = null;
        try {
            // check if we can find at least one row
            ps = conn.prepareStatement(
                "SELECT 1 FROM sz_message_queue LIMIT 1");

            rs = ps.executeQuery();

            return (!rs.next());

        } finally {
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Gets the number of messages currently in the message queue.  The
     * returned value will include leased messages.
     * 
     * @param conn The {@link Connection} to use.
     * 
     * @return The number of messages in the message queue (including leased
     *         messages).
     * 
     * @throws SQLException If a database failure occurs.
     */
    default int getMessageCount(Connection conn) throws SQLException {
        PreparedStatement ps = null;
        ResultSet         rs = null;
        try {
            // check if we can find at least one row
            ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM sz_message_queue");

            rs = ps.executeQuery();

            rs.next();

            return rs.getInt(1);

        } finally {
            rs = close(rs);
            ps = close(ps);
        }
    }

    /**
     * Inserts a message into the database queue.
     * 
     * @param conn The {@link Connection} to use.
     * 
     * @param messageText The text for the message.
     * 
     * @throws SQLException If a database failure occurs.
     */
    default void insertMessage(Connection conn, String messageText) throws SQLException 
    {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(
                "INSERT INTO sz_message_queue (message_text) VALUES (?)");

            ps.setString(1, messageText);
            
            int rowCount = ps.executeUpdate();

            if (rowCount != 1) {
                throw new SQLException(
                    "Failed to insert exactly one row: " + rowCount);
            }

        } finally {
            ps = close(ps);
        }
    }

    /**
     * Updates at most the specified number of the least recently received message 
     * rows with the specified lease ID and lease expiration time and returns the
     * number of message rows that were leased.
     * 
     * @param conn The {@link Connection} to use.
     * 
     * @param leaseId The lease ID to use.
     * 
     * @param leaseTime The number of <b>seconds</b> for which the messages should be 
     *                  leased.
     * 
     * @param maxLeaseCount The maximum number of info message rows to lease.
     * 
     * @return The number of message rows that were leased using the specified
     *         lease ID and lease time in seconds.  This returns zero (0) if 
     *         no rows were leased.
     * 
     * @throws SQLException If a database failure occurs.
     */
    default int leaseMessages(Connection    conn, 
                              String        leaseId, 
                              int           leaseTime,
                              int           maxLeaseCount)
        throws SQLException
    {
        PreparedStatement   ps      = null;
        DatabaseType        dbType  = this.getDatabaseType();

        try {
            // prepare the statement
            ps = conn.prepareStatement(
                "UPDATE sz_message_queue "
                + "SET lease_id = ?, "
                + "expire_lease_at = " + dbType.getTimestampBindingSQL() + " "
                + "WHERE message_id IN (SELECT message_id FROM sz_message_queue "
                + "WHERE lease_id IS NULL AND expire_lease_at IS NULL "
                + "ORDER BY created_on LIMIT ?)");
            
            // calculate the lease expiration
            long        now         = System.currentTimeMillis();
            long        leaseExpire = now + (leaseTime * 1000);
            Timestamp   expireTime  = new Timestamp(leaseExpire);

            // bind the statement
            ps.setString(1, leaseId);
            dbType.setTimestamp(ps, 2, expireTime);
            ps.setInt(3, maxLeaseCount);

            // update the row count having the lease ID
            return ps.executeUpdate();

        } finally {
            ps = close(ps);
        }
    }

    /**
     * Selects the info message rows that were previously leased with the specified
     * lease ID and returns a {@link List} of {@link LeasedMessage} instances describing
     * thoe info message rows.
     * 
     * @param conn The {@link Connection} to use.
     * 
     * @param leaseId The lease ID to use.
     * 
     * @return The {@link List} of {@link LeasedMessage} instances that were selected.
     * 
     * @throws SQLException If a database failure occurs.
     */
    default List<LeasedMessage> getLeasedMessages(Connection conn, String leaseId)
        throws SQLException
    {
        Objects.requireNonNull(leaseId, "Lease ID cannot be null");
        
        PreparedStatement   ps      = null;
        ResultSet           rs      = null;
        List<LeasedMessage> result  = new LinkedList<>();
        
        try {
            // prepare the statement for the query of leased messages
            ps = conn.prepareStatement(
                "SELECT message_id, expire_lease_at, message_text "
                + "FROM sz_message_queue "
                + "WHERE lease_id = ?");

            // bind the leae ID
            ps.setString(1, leaseId);
            
            // execute the query
            rs = ps.executeQuery();

            // read the leased messages
            while (rs.next()) {
                long        messageId   = rs.getLong(1);
                Timestamp   expTime     = rs.getTimestamp(2, UTC_CALENDAR);
                String      messageText = rs.getString(3);

                LeasedMessage message = new LeasedMessage(messageId,
                                                          messageText,
                                                          leaseId,
                                                          expTime.getTime());

                result.add(message);
            }

            // return the list of leased messages
            return result;

        } finally {
            rs = close(rs);
            ps = close(ps);
        }
    }
    
    /**
     * Releases any expired leases in the databae to make them available to 
     * this client for consumption.
     * 
     * @param conn The {@link Connection} to use.
     * 
     * @param leaseTime The number of seconds a message should be leased.
     * 
     * @return The number of messages for which the leases were expired.
     * 
     * @throws SQLException If a database failure occurs.
     */
    default int releaseExpiredLeases(Connection conn, int leaseTime) 
        throws SQLException
    {
        PreparedStatement   ps      = null;
        DatabaseType        dbType  = this.getDatabaseType();
        try {
            ps = conn.prepareStatement(
                "UPDATE sz_message_queue "
                + "SET lease_id = NULL, expire_lease_at = NULL "
                + "WHERE lease_id IS NOT NULL "
                + "AND expire_lease_at < " + dbType.getTimestampBindingSQL());

            // don't be too aggressive on expiring leases
            long        now         = System.currentTimeMillis();
            long        leaseExpire = now - ((leaseTime * 1000) / 2);
            Timestamp   expireTime  = new Timestamp(leaseExpire);

            dbType.setTimestamp(ps, 1, expireTime);

            return ps.executeUpdate();

        } finally {
            ps = close(ps);
        }
    }

    /**
     * Renews the lease on the specified {@link LeasedMessage}.  If the lease
     * has already expired or the message is leased by another client then 
     * this method returns negative-one <code>-1</code>, otherwise this method
     * returns the UTC millisecond time for the new lease expiration.  The 
     * specified {@link LeasedMessage} will also be modified with the new lease
     * expiration by having its {@link LeasedMessage#extendLease(long)} method
     * called.
     * 
     * @param conn The {@link Connection} to use.
     * 
     * @param message The {@link LeasedMessage} for which to renew the lease.
     * 
     * @param leaseTime The number of <b>seconds</b> to lease the message from
     *                  the point of renewal.
     * 
     * @return The new UTC millisecond time for the new lease expiration, or 
     *         negative-one (<code>-1</code>) if the lease could not be renewed.
     *  
     * @throws SQLException If a database failure occurs.
     */
    default long renewLease(Connection conn, LeasedMessage message, int leaseTime) 
        throws SQLException
    {
        PreparedStatement   ps      = null;
        DatabaseType        dbType  = this.getDatabaseType();
        try {
            // prepare the statement
            ps = conn.prepareStatement(
                "UPDATE sz_message_queue SET expire_lease_at = " 
                + dbType.getTimestampBindingSQL()
                + " WHERE message_id = ? AND lease_id = ?");
            
            // calculate the expiration time
            long        now         = System.currentTimeMillis();
            long        leaseExpire = now + (leaseTime * 1000);
            Timestamp   expireTime  = new Timestamp(leaseExpire);

            // bind the statement
            dbType.setTimestamp(ps, 1, expireTime);
            ps.setLong(2, message.getMessageId());
            ps.setString(3, message.getLeaseId());
            
            int rowCount = ps.executeUpdate();

            // check if the message is already expired or leased by another or missing
            if (rowCount == 0) return -1L;

            // check if somehow we updated more than row (should not be possible)
            if (rowCount > 1) {
                throw new SQLException(
                    "Unexpectedly updasted more than row for message and lease ID.  "
                    + "messageId=[ " + message.getMessageId() + " ], leaseId=[ "
                    + message.getLeaseId() + " ], rowCount=[ " + rowCount + " ]");
            }

            // extend the lease on the LeasedMessage
            message.extendLease(leaseExpire);

            // return the new lease expiration time
            return leaseExpire;

        } finally {
            ps = close(ps);
        }
    }

    /**
     * Deletes a message from the database message queue.
     * 
     * @param conn The {@link Connection} to use.
     * 
     * @param messageId The message ID for the message.
     * 
     * @param leaseId The optional lease ID to prevent deletion if the lease ID
     *                on the message has changed, or <code>null</code> if the message
     *                should be deleted regardless of the lease ID.
     *                 
     * @return <code>true</code> if a message was deleted, otherwise
     *         <code>false</code>.
     * 
     * @throws SQLException If a database failure occurs.
     */
    default boolean deleteMessage(Connection conn, long messageId, String leaseId)
        throws SQLException 
    {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(
                "DELETE FROM sz_message_queue WHERE message_id = ?"
                + ((leaseId != null) ? " AND lease_id = ?" : ""));

            ps.setLong(1, messageId);
            if (leaseId != null) {
                ps.setString(2, leaseId);
            }    

            int rowCount = ps.executeUpdate();

            if (rowCount > 1) {
                throw new SQLException(
                    "Somehow deleted more than one message row.  messageId=[ "
                    + messageId + " ], leaseId=[ " + leaseId + " ], rowCount=[ "
                    + rowCount + " ]");
            }

            return (rowCount == 1);

        } finally {
            ps = close(ps);
        }
    }

}
