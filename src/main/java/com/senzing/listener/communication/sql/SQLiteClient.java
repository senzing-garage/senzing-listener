package com.senzing.listener.communication.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.List;
import java.util.ArrayList;

import com.senzing.sql.DatabaseType;

import static com.senzing.sql.SQLUtilities.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Provides a SQLite implementation of {@link SQLClient}.
 */
public class SQLiteClient implements SQLClient {
    /**
     * {@inheritDoc}
     * <p>
     * Implemented to return {@link DatabaseType#SQLITE}.
     * </p>
     */
    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.SQLITE;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implemented to create the schema for the SQLite database.
     * <p>
     */
    @Override
    public void ensureSchema(Connection conn, boolean recreate) 
        throws SQLException 
    {
        String createTableSql = "CREATE TABLE IF NOT EXISTS sz_message_queue ( "
            + "message_id INTEGER PRIMARY KEY, "
            + "lease_id TEXT, "
            + "expire_lease_at TIMESTAMP,"
            + "message_text TEXT NOT NULL,"
            + "created_on TIMESTAMP NOT NULL "
            + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), "
            + "modified_on TIMESTAMP NOT NULL "
            + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')))";
    
       String dropTableSql = "DROP TABLE IF EXISTS sz_message_queue";

       String createIndexSql = "CREATE INDEX IF NOT EXISTS sz_msg_queue_lease "
            + "ON sz_message_queue (lease_id)";

       String dropIndexSql = "DROP INDEX IF EXISTS sz_msg_queue_lease";

       String createUpdateTriggerSql = "CREATE TRIGGER IF NOT EXISTS "
            + "sz_msg_queue_mod AFTER UPDATE "
            + "ON sz_message_queue FOR EACH ROW "
            + "BEGIN UPDATE sz_message_queue "
            + "SET created_on = old.created_on,"
            + " modified_on = (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')) "
            + " WHERE message_id = old.message_id; END;";

       String dropUpdateTriggerSql = "DROP TRIGGER IF EXISTS sz_msg_queue_mod";

       String createInsertTriggerSql = "CREATE TRIGGER IF NOT EXISTS "
            + "sz_msg_queue_create AFTER INSERT "
            + "ON sz_message_queue FOR EACH ROW "
            + "BEGIN UPDATE sz_message_queue "
            + "SET created_on = (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),"
            + " modified_on = (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')) "
            + " WHERE message_id = new.message_id; END;";

       String dropInsertTriggerSql = "DROP TRIGGER IF EXISTS sz_msg_queue_create";

       List<String> sqlList = new ArrayList<>();

       if (recreate) {
           sqlList.add(dropInsertTriggerSql);
           sqlList.add(dropUpdateTriggerSql);
           sqlList.add(dropIndexSql);
           sqlList.add(dropTableSql);
       }
       sqlList.add(createTableSql);
       sqlList.add(createIndexSql);
       sqlList.add(createUpdateTriggerSql);
       sqlList.add(createInsertTriggerSql);
      
       // execute the statements
       Statement stmt = null;
       try {
           // create the statement
           stmt = conn.createStatement();

           // execute the SQL statements
           for (String sql : sqlList) {
               try {
                   stmt.execute(sql);
               } catch (SQLException e) {
                   logError(e, "SQL Error Encountered: ", sql);
                   throw e;
               }
           }

           // commit the connection
           conn.commit();

       } finally {
           stmt = close(stmt);
       }
    }
}
