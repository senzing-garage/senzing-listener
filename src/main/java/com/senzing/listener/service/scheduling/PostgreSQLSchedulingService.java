package com.senzing.listener.service.scheduling;

import java.sql.*;
import java.util.*;

import static com.senzing.sql.SQLUtilities.close;
import static com.senzing.util.LoggingUtilities.logError;

/**
 * Implements {@link SchedulingService} using a SQLite database to handle
 * persisting the follow-up tasks by extending {@link
 * AbstractSQLSchedulingService}.
 */
public class PostgreSQLSchedulingService extends AbstractSQLSchedulingService {
  /**
   * The {@link Calendar} to use for retrieving timestamps from the database.
   */
  private static final Calendar UTC_CALENDAR
      = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  /**
   * Ensures the schema exists and alternatively drops the existing the schema
   * and recreates it.
   *
   * @param recreate <code>true</code> if the existing schema should be
   *                 dropped, otherwise <code>false</code>.
   *
   * @throws SQLException If a failure occurs.
   */
  protected void ensureSchema(boolean recreate) throws SQLException {
    String createTableSql = "CREATE TABLE IF NOT EXISTS sz_follow_up_tasks ("
        + "task_id BIGSERIAL PRIMARY KEY, "
        + "signature TEXT NOT NULL, "
        + "allow_collapse_flag NUMERIC(1,0) DEFAULT 0,"
        + "lease_id TEXT,"
        + "expire_lease_at TIMESTAMP,"
        + "multiplicity INTEGER DEFAULT 1,"
        + "json_text TEXT NOT NULL,"
        + "created_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        + "modified_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);";

    String dropTableSql = "DROP TABLE IF EXISTS sz_follow_up_tasks;";

    String createIndexSql1 =
        "CREATE INDEX IF NOT EXISTS sz_task_dup ON sz_follow_up_tasks ("
            + "signature, allow_collapse_flag, expire_lease_at);";

    String dropIndexSql1 = "DROP INDEX IF EXISTS sz_task_dup;";

    String createIndexSql2 =
        "CREATE INDEX IF NOT EXISTS sz_task_lease ON sz_follow_up_tasks ("
            + "lease_id);";

    String dropIndexSql2 = "DROP INDEX IF EXISTS sz_task_lease;";

    String createTriggerFunctionSql =
        "CREATE OR REPLACE FUNCTION sz_follow_up_timestamps() "
            + "RETURNS TRIGGER "
            + "LANGUAGE PLPGSQL "
            + "AS $$ "
            + "BEGIN "
            + "  IF (TG_OP = 'UPDATE') THEN "
            + "  BEGIN "
            + "    NEW.created_on := OLD.created_on; "
            + "    NEW.modified_on := CURRENT_TIMESTAMP; "
            + "    return NEW; "
            + "  END; "
            + "ELSIF (TG_OP = 'INSERT') THEN "
            + "  BEGIN "
            + "    NEW.created_on := CURRENT_TIMESTAMP; "
            + "    NEW.modified_on := CURRENT_TIMESTAMP; "
            + "    return NEW; "
            + "  END; "
            + "END IF; "
            + "RETURN NULL; "
            + "END; "
            + "$$;";

    String createTriggerSql =
        "CREATE TRIGGER sz_follow_up_tasks_trigger "
            + "  BEFORE INSERT OR UPDATE "
            + "  ON sz_follow_up_tasks "
            + "  FOR EACH ROW "
            + "  WHEN (pg_trigger_depth() = 0) "
            + "  EXECUTE PROCEDURE sz_follow_up_timestamps();";

    String dropTriggerFunctionSql =
        "DROP FUNCTION IF EXISTS sz_follow_up_timestamps;";

    String dropTriggerSql =
        "DROP TRIGGER IF EXISTS sz_follow_up_tasks_trigger "
            + "ON sz_follow_up_tasks;";

    List<String> sqlList = new ArrayList<>();

    if (recreate) {
      sqlList.add(dropTriggerSql);
      sqlList.add(dropTriggerFunctionSql);
      sqlList.add(dropIndexSql1);
      sqlList.add(dropIndexSql2);
      sqlList.add(dropTableSql);
    }
    sqlList.add(createTableSql);
    sqlList.add(createIndexSql1);
    sqlList.add(createIndexSql2);
    sqlList.add(createTriggerFunctionSql);
    sqlList.add(dropTriggerSql);
    sqlList.add(createTriggerSql);

    // execute the statements
    Connection  conn = null;
    Statement   stmt = null;
    try {
      conn = this.getConnection();

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
      conn = close(conn);
    }
  }
}
