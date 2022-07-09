package com.senzing.listener.service.scheduling;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.senzing.sql.SQLUtilities.*;

/**
 * Implements {@link SchedulingService} using a SQLite database to handle
 * persisting the follow-up tasks by extending {@link
 * AbstractSQLSchedulingService}.
 */
public class SQLiteSchedulingService extends AbstractSQLSchedulingService {
  /**
   * The {@link Calendar} to use for retrieving timestamps from the database.
   */
  private static final Calendar UTC_CALENDAR
      = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  /**
   * The date-time pattern used to bind timestamp values as strings.
   */
  private static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

  /**
   * The {@link ZoneId} for UTC time zone.
   */
  private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

  /**
   * The {@link DateTimeFormatter} used to bind timestamp values as strings.
   */
  private static final DateTimeFormatter DATE_TIME_FORMATTER
      = DateTimeFormatter.ofPattern(DATE_TIME_PATTERN);

  /**
   * {@inheritDoc}
   *
   * Overridden to bind as a {@link String} to match {@link
   * #formatTimestampBinding()}.
   */
  @Override
  protected void setTimestamp(PreparedStatement ps, int index, Timestamp value)
    throws SQLException
  {
    Instant         instant       = Instant.ofEpochMilli(value.getTime());
    ZonedDateTime   zonedDateTime = instant.atZone(UTC_ZONE);

    ps.setString(index, DATE_TIME_FORMATTER.format(zonedDateTime));
  }

  /**
   * {@inheritDoc}
   *
   * Overridden to return <code>"(STRFTIME('%Y-%m-%d %H:%M:%f', ?))"</code>.
   */
  @Override
  protected String formatTimestampBinding() {
    return "(STRFTIME('%Y-%m-%d %H:%M:%f', ?))";
  }

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
    String createTableSql = "CREATE TABLE IF NOT EXISTS sz_follow_up_tasks ( "
        + "task_id INTEGER PRIMARY KEY, "
        + "signature TEXT NOT NULL, "
        + "allow_collapse_flag INTEGER(1) DEFAULT 0, "
        + "lease_id TEXT, "
        + "expire_lease_at TIMESTAMP,"
        + "multiplicity INTEGER DEFAULT 1,"
        + "json_text TEXT NOT NULL,"
        + "created_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), "
        + "modified_on TIMESTAMP NOT NULL "
        + "DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')))";

    String dropTableSql = "DROP TABLE IF EXISTS sz_follow_up_tasks";

    String createIndexSql1 =
        "CREATE INDEX IF NOT EXISTS sz_task_dup ON sz_follow_up_tasks ("
            + "signature, allow_collapse_flag, expire_lease_at)";

    String dropIndexSql1 = "DROP INDEX IF EXISTS sz_task_dup";

    String createIndexSql2 =
        "CREATE INDEX IF NOT EXISTS sz_task_lease ON sz_follow_up_tasks ("
            + "lease_id)";

    String dropIndexSql2 = "DROP INDEX IF EXISTS sz_task_lease";

    String createUpdateTriggerSql =
        "CREATE TRIGGER IF NOT EXISTS sz_follow_up_tasks_mod AFTER UPDATE "
            + "ON sz_follow_up_tasks FOR EACH ROW "
            + "BEGIN UPDATE sz_follow_up_tasks "
            + "SET created_on = old.created_on,"
            + " modified_on = (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')) "
            + " WHERE task_id = old.task_id; END;";

    String dropUpdateTriggerSql =
        "DROP TRIGGER IF EXISTS sz_follow_up_tasks_mod";

    String createInsertTriggerSql =
        "CREATE TRIGGER IF NOT EXISTS sz_follow_up_tasks_create AFTER INSERT "
            + "ON sz_follow_up_tasks FOR EACH ROW "
            + "BEGIN UPDATE sz_follow_up_tasks "
            + "SET created_on = (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),"
            + " modified_on = (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')) "
            + " WHERE task_id = new.task_id; END;";

    String dropInsertTriggerSql =
        "DROP TRIGGER IF EXISTS sz_follow_up_tasks_create";

    List<String> sqlList = new ArrayList<>();

    if (recreate) {
      sqlList.add(dropInsertTriggerSql);
      sqlList.add(dropUpdateTriggerSql);
      sqlList.add(dropIndexSql1);
      sqlList.add(dropIndexSql2);
      sqlList.add(dropTableSql);
    }
    sqlList.add(createTableSql);
    sqlList.add(createIndexSql1);
    sqlList.add(createIndexSql2);
    sqlList.add(createUpdateTriggerSql);
    sqlList.add(createInsertTriggerSql);

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
          System.err.println();
          System.err.println(sql);
          e.printStackTrace();
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
