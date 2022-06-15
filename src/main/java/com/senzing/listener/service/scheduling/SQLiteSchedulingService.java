package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.text.TextUtilities;

import javax.json.JsonObject;
import java.io.File;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static com.senzing.listener.service.ServiceUtilities.*;
import static java.lang.Boolean.*;
import static com.senzing.sql.SQLUtilities.*;

/**
 * Implements {@link SchedulingService} using a SQLite database to handle
 * persisting the follow-up tasks by extending {@link
 * AbstractSchedulingService}.
 */
public class SQLiteSchedulingService extends AbstractSchedulingService {
  /**
   * The default SQLite file name to use for the persistent store.
   */
  public static final String DEFAULT_SQLITE_FILE_NAME = "sz_follow_tasks.db";

  /**
   * The initialization parameter key for obtaining the path to the SQLite
   * file to use.  The value should be a path to an existing SQLite file or
   * the path where the file should be created.  If not provided then a file
   * is created in the current working directory using the
   * {@linkplain #DEFAULT_SQLITE_FILE_NAME default SQLite file name}.
   */
  public static final String SQLITE_FILE_PATH_KEY= "sqliteFile";

  /**
   * The initialization parameter key for checking if the persistent store
   * of follow-up tasks should be dropped / deleted and recreated during
   * initialization.  Values should be <code>true</code> or <code>false</code>.
   */
  public static final String CLEAN_DATABASE_KEY = "cleanDatabase";

  /**
   * The <b>unmodifiable</b> {@link List}
   */
  private static final List<String> PRAGMA_FEATURES_SQL_LIST = List.of(
      "PRAGMA foreign_keys = ON;",
      "PRAGMA journal_mode = WAL;",
      "PRAGMA synchronous = 0;",
      "PRAGMA secure_delete = 0;",
      "PRAGMA automatic_index = 0;");

  /**
   * The configured SQLite database file.
   */
  private String sqliteFilePath;

  /**
   * The JDBC URL used to connect to the SQLite database.
   */
  private String jdbcUrl;

  /**
   * The JDBC {@link Connection} that was established.
   */
  private Connection connection = null;

  /**
   * The number of expired follow-up tasks.
   */
  private long totalExpiredFollowUpTaskCount = 0L;

  /**
   * Gets the JDBC {@link Connection} used by this instance.  Be sure not to
   * close it or else further calls to this method will obtain a closed
   * connection.
   */
  protected Connection getConnection() {
    return this.connection;
  }

  /**
   * Overridden to establish the JDBC {@link Connection} and to ensure the
   * schema exists (optionally dropping an existing schema).
   *
   * {@inheritDoc}
   *
   * @param config The {@link JsonObject} describing the configuration.
   */
  protected void doInit(JsonObject config)
    throws ServiceSetupException
  {
    try {
      Boolean clean     = getConfigBoolean(config, CLEAN_DATABASE_KEY, FALSE);
      String  filePath  = getConfigString(config, SQLITE_FILE_PATH_KEY, null);
      if (filePath == null) {
        filePath = new File(DEFAULT_SQLITE_FILE_NAME).toString();
      }
      this.sqliteFilePath = filePath;
      this.jdbcUrl        = "jdbc:sqlite:" + this.sqliteFilePath;

      this.establishConnection(jdbcUrl);

      this.ensureSchema(clean);

    } catch (SQLException e) {
      throw new ServiceSetupException(
          "Failed to connect to database or initialize schema.", e);
    }
  }

  /**
   * Gets the {@link List} of the SQL statements to run to initialize a
   * SQLite session after opening a {@link Connection}.
   *
   * @return The {@link List} of {@link String} SQL statements to run on a
   *         newly established SQLite JDBC {@link Connection}.
   */
  protected List<String> getPragmaFeatureStatements() {
    return PRAGMA_FEATURES_SQL_LIST;
  }

  /**
   * Establishes the JDBC connection to the SQLite database.  Additionally,
   * this runs any session initialization for the connection.
   *
   * @param jdbcUrl The JDBC URL to use for establishing the connection.
   *
   * @throws SQLException If a failure occurs.
   */
  protected synchronized void establishConnection(String jdbcUrl)
    throws SQLException
  {
    if (this.connection != null) {
      throw new IllegalStateException(
          "Connection is already established: " + this.jdbcUrl);
    }
    this.jdbcUrl    = jdbcUrl;
    this.connection = DriverManager.getConnection(jdbcUrl);
    this.connection.setAutoCommit(false);

    // initialize the connection before use
    for (String sql : this.getPragmaFeatureStatements()) {
      try (PreparedStatement ps = this.connection.prepareStatement(sql)) {
        ps.execute();
      }
    }
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
    Connection conn = this.getConnection();

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
            + "signature, allow_collapse, expire_lease_at)";

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
            + "SET created_on = = (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),"
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

    // execute the SQL statements
    for (String sql : sqlList) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.execute();
      }
    }
  }

  /**
   * Overridden to close the database connection.
   */
  @Override
  protected void doDestroy() {
    try {
      if (this.connection != null) this.connection.close();

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Implemented to save the specified follow-up tasks to the backing SQLite
   * persistent store.
   */
  @Override
  protected synchronized void enqueueFollowUpTask(Task task)
    throws ServiceExecutionException
  {
    PreparedStatement ps = null;
    try {
      // get the connection
      Connection conn = this.getConnection();

      // prepare the statement
      ps = conn.prepareStatement(
          "UPDATE sz_follow_up_tasks "
              + "SET multiplicity = multiplicity + 1 "
              + "WHERE signature = ? "
              + "AND allow_collapse_flag = 1 "
              + "AND expire_lease_at IS NULL "
              + "AND task_id = (SELECT MAX(task_id) WHERE signature = ? "
              + "AND allow_collapse_flag = 1 AND expire_lease_at IS NULL)");

      ps.setString(1, task.getSignature());
      ps.setString(2, task.getSignature());

      int rowCount = ps.executeUpdate();

      // check if we updated a row
      if (rowCount == 0) {
        ps = close(ps);
        ps = conn.prepareStatement(
            "INSERT INTO sz_follow_up_tasks ("
            + "signature, allow_collapse_flag, json_text) VALUES (?, ?, ?)");
        ps.setString(1, task.getSignature());
        ps.setInt(2, (task.isAllowingCollapse() ? 1 : 0));
        ps.setString(3, task.toJsonText());

        ps.executeUpdate();
        ps = close(ps);

      } else if (rowCount > 1) {
        System.err.println();
        System.err.println("********************************");
        System.err.println("MULTIPLE ROWS UPDATED FOR FOLLOW-UP TASK: " + task);
      }

      // commit the transaction
      conn.commit();

    } catch (SQLException e) {
      throw new ServiceExecutionException(
          "Failed to enqueue follow-up task", e);
    } finally {
      close(ps);
    }
  }

  /**
   * Fetches at most the specified number of follow-up tasks from the backing
   * SQLite persistent store.
   *
   * @param count The suggested number of follow-up tasks to retrieve from
   *              persistent storage.
   *
   * @return The {@link List} of dequeued follow-up tasks.
   *
   * @throws ServiceExecutionException
   */
  @Override
  protected synchronized List<ScheduledTask> dequeueFollowUpTasks(int count)
      throws ServiceExecutionException
  {
    PreparedStatement ps = null;
    ResultSet         rs = null;
    try {
      // get a connection
      Connection conn = this.getConnection();

      // first release the lease on any task where the lease has expired
      ps = conn.prepareStatement(
          "UPDATE sz_folow_up_tasks "
          + "SET lease_id = NULL, expire_lease_at = NULL "
          + "WHERE lease_id IS NOT NULL AND expire_lease_at < ?");

      long      now         = System.currentTimeMillis();
      long      leaseExpire = now - this.getFollowUpTimeout();
      Timestamp expireTime  = new Timestamp(leaseExpire);

      ps.setTimestamp(1, expireTime);

      int rowCount = ps.executeUpdate();
      if (rowCount > 0) {
        synchronized (this.getStatisticsMonitor()) {
          System.err.println("EXPIRED LEASE ON " + rowCount + " FOLLOW UP TASKS");
          this.totalExpiredFollowUpTaskCount += rowCount;
        }
      }

      // close the statement
      ps = close(ps);

      // generate a unique lease ID
      String leaseId = this.generateLeaseId();

      // now let's lease some new follow up tasks
      ps = conn.prepareStatement(
          "UPDATE sz_follow_up_tasks "
          + "SET lease_id = ?, expire_lease_at = ? "
          + "WHERE task_id IN (SELECT task_id FROM sz_follow_up_tasks "
          + "WHERE lease_id IS NULL AND expire_lease_at IS NULL "
          + "ORDER BY created_on "
          + "LIMIT ?)");

      leaseExpire = now + (2 * this.getFollowUpTimeout());
      expireTime = new Timestamp(leaseExpire);

      ps.setString(1, leaseId);
      ps.setTimestamp(2, expireTime);
      ps.setInt(3, count);

      rowCount = ps.executeUpdate();

      // close the statement
      ps = close(ps);

      // commit the changes
      conn.commit();

      // check if no rows were updated
      if (rowCount == 0) {
        return new ArrayList<>(0);
      }

      // set the leased rows
      ps = conn.prepareStatement(
          "SELECT "
              + "task_id, expire_lease_at, multiplicity, json_text, created_on "
              + "WHERE lease_id = ?");

      ps.setString(1, leaseId);

      List<ScheduledTask> result = new ArrayList<>(rowCount);
      rs = ps.executeQuery();
      while (rs.next()) {
        long taskId = rs.getLong(1);
        Timestamp expTime = rs.getTimestamp(4);
        int multiplicity = rs.getInt(5);
        String jsonText = rs.getString(6);
        Timestamp createdOn = rs.getTimestamp(7);

        String followUpId = taskId + ":" + leaseId;
        now = System.currentTimeMillis();
        long elapsedSinceCreation = now - createdOn.getTime();

        ScheduledTask task = new ScheduledTask(jsonText,
                                               followUpId,
                                               multiplicity,
                                               expTime.getTime(),
                                               elapsedSinceCreation);

        result.add(task);
      }

      // close the result set and prepared statement
      rs = close(rs);
      ps = close(ps);

      // return the result list
      return result;

    } catch (SQLException e) {
      throw new ServiceExecutionException(
          "Failed to enqueue follow-up task", e);

    } finally {
      close(rs);
      close(ps);
    }
  }

  /**
   * Creates a virtually unique lease ID.
   *
   * @return A new lease ID to use.
   */
  protected String generateLeaseId() {
    long pid    = ProcessHandle.current().pid();
    StringBuilder sb = new StringBuilder();
    sb.append(pid).append("|").append(Instant.now().toString()).append("|");
    sb.append(TextUtilities.randomAlphanumericText(50));
    return sb.toString();
  }

  /**
   * Implemented to renew the lease on the specified tasks as well as any others
   * that were dequeued with the same lease.
   *
   * {@inheritDoc}
   */
  protected synchronized void renewFollowUpTasks(List<ScheduledTask> tasks)
      throws ServiceExecutionException
  {
    PreparedStatement ps = null;
    try {
      Connection conn = this.getConnection();

      Set<String> leaseIdSet = new LinkedHashSet<>();
      for (ScheduledTask task : tasks) {
        String followUpId = task.getFollowUpId();

        int     index   = followUpId.indexOf(":");
        String  leaseId = followUpId.substring(index + 1);

        leaseIdSet.add(leaseId);
      }

      long      now         = System.currentTimeMillis();
      long      leaseExpire = now + (2 * this.getFollowUpTimeout());
      Timestamp expireTime  = new Timestamp(leaseExpire);
      int       count       = leaseIdSet.size();

      StringBuilder sb = new StringBuilder(
          "UPDATE sz_follow_up_tasks SET expire_lease_at = ? "
          + "WHERE lease_id IN (");
      String prefix = "";
      for (int index = 0; index < count; index++) {
        sb.append(prefix).append("?");
        prefix = ", ";
      }
      sb.append(")");

      ps = conn.prepareStatement(sb.toString());

      int index = 1;
      for (String leaseId : leaseIdSet) {
        ps.setString(index++, leaseId);
      }

      int rowCount = ps.executeUpdate();
      ps = close(ps);

      if (rowCount != tasks.size()) {
        System.err.println();
        System.err.println("---------------------------------------------");
        System.err.println("WARNING: Renewed lease on more follow-up tasks ("
                               + rowCount + ") than expected ("
                               + tasks.size() + "): " + leaseIdSet);
      }
    } catch (SQLException e) {
      throw new ServiceExecutionException(
          "Failed to enqueue follow-up task", e);

    } finally {
      close(ps);
    }
  }

  /**
   * Implemented to delete the specified follow-up task from persistent storage.
   *
   * {@inheritDoc}
   */
  protected synchronized void completeFollowUpTask(ScheduledTask task)
      throws ServiceExecutionException
  {
    PreparedStatement ps = null;
    try {
      Connection conn = this.getConnection();

      ps = conn.prepareStatement(
          "DELETE FROM sz_follow_up_task WHERE task_id = ?");

      String  followUpId  = task.getFollowUpId();
      int     index       = followUpId.indexOf(":");
      long    taskId      = Long.parseLong(followUpId.substring(0, index));

      ps.setLong(1, taskId);

      int rowCount = ps.executeUpdate();
      ps = close(ps);

      if (rowCount != 1) {
        System.err.println();
        System.err.println("------------------------------------------------");
        System.err.println("WARNING: Follow-up task was already completed");
      }

    } catch (SQLException e) {
      throw new ServiceExecutionException(
          "Failed to enqueue follow-up task", e);

    } finally {
      close(ps);
    }
  }

  /**
   * Gets the total number of follow-up tasks that were dequeued and expired
   * before being handled.
   *
   * @return The total number of follow-up tasks that were dequeued and expired
   *         before being handled.
   */
  public long getTotalExpiredFollowUpTaskCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.totalExpiredFollowUpTaskCount;
    }
  }

}
