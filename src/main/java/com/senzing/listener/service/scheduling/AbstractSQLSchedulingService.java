package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.sql.DatabaseType;
import com.senzing.text.TextUtilities;
import com.senzing.sql.ConnectionProvider;

import javax.json.JsonObject;
import javax.naming.NameNotFoundException;
import java.sql.*;
import java.time.Instant;
import java.util.*;

import static com.senzing.listener.service.ServiceUtilities.getConfigBoolean;
import static com.senzing.listener.service.ServiceUtilities.getConfigString;
import static com.senzing.sql.SQLUtilities.close;
import static com.senzing.sql.SQLUtilities.rollback;
import static java.lang.Boolean.FALSE;

/**
 * Implements {@link SchedulingService} using a SQLite database to handle
 * persisting the follow-up tasks by extending {@link
 * AbstractSchedulingService}.
 */
public abstract class AbstractSQLSchedulingService
    extends AbstractSchedulingService
{
  /**
   * The {@link Calendar} to use for retrieving timestamps from the database.
   */
  private static final Calendar UTC_CALENDAR
      = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  /**
   * The initialization parameter key for checking if the persistent store
   * of follow-up tasks should be dropped / deleted and recreated during
   * initialization.  Values should be <code>true</code> or <code>false</code>.
   */
  public static final String CLEAN_DATABASE_KEY = "cleanDatabase";

  /**
   * The initialization parameter key for obtaining the {@link
   * ConnectionProvider} to use for connecting to the database from the
   * {@link ConnectionProvider#REGISTRY}.
   */
  public static final String CONNECTION_PROVIDER_KEY = "connectionProvider";

  /**
   * The number of expired follow-up tasks.
   */
  private long totalExpiredFollowUpTaskCount = 0L;

  /**
   * The {@link ConnectionProvider} to use.
   */
  private ConnectionProvider connectionProvider;

  private Map<String, int[]> callCountMap = new HashMap<>();

  /**
   * The {@link DatabaseType} for this instance.
   */
  private DatabaseType databaseType = null;

  /**
   * Gets a JDBC {@link Connection} to use.  Typically these are obtained from
   * a backing pool so repeated calls to this function without closing the
   * previously obtained {@link Connection} instances could exhaust the pool.
   * This may block until a {@link Connection} is available.
   *
   * @return The {@link Connection} that was obtained.
   *
   * @throws SQLException If a JDBC failure occurs.
   */
  protected Connection getConnection() throws SQLException {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    StackTraceElement caller = null;
    for (int index = 0; index < stackTrace.length; index++) {
      StackTraceElement frame = stackTrace[index];
      if (frame.getMethodName().equals("getConnection")) {
        caller = stackTrace[index+1];
        break;
      }
    }
    synchronized (this.callCountMap) {
      int[] count = this.callCountMap.get(caller.getMethodName());
      if (count == null) {
        count = new int[1];
        count[0] = 0;
        this.callCountMap.put(caller.getMethodName(), count);
      }
      count[0]++;
    }

    return this.connectionProvider.getConnection();
  }

  /**
   *
   */
  @Override
  public Map<Statistic,Number> getStatistics() {
    synchronized (this.callCountMap) {
      System.err.println();
      System.err.println("==================================================");
      this.callCountMap.forEach((key,val) -> {
        System.err.println(key + " = " + val[0]);
      });
      System.err.println("==================================================");
      System.err.println();
    }
    return super.getStatistics();
  }

  /**
   * Overridden to obtain the {@link ConnectionProvider}.
   *
   * {@inheritDoc}
   *
   * @param config The {@link JsonObject} describing the configuration.
   */
  protected void doInit(JsonObject config)
    throws ServiceSetupException
  {
    try {
      Boolean clean = getConfigBoolean(config, CLEAN_DATABASE_KEY, FALSE);

      String providerKey = getConfigString(config,
                                           CONNECTION_PROVIDER_KEY,
                                           true);


      try {
        this.connectionProvider = ConnectionProvider.REGISTRY.lookup(providerKey);
      } catch (NameNotFoundException e) {
        throw new ServiceSetupException(
            "No ConnectionProvider was registered to the name specified by the "
            + "\"" + CONNECTION_PROVIDER_KEY + "\" initialization parameter: "
            + providerKey);
      }

      // set the database type
      this.databaseType = this.initDatabaseType();

      // ensure the schema exists
      this.ensureSchema(clean);

    } catch (SQLException e) {
      throw new ServiceSetupException(
          "Failed to connect to database or initialize schema.", e);
    }
  }

  /**
   * Initializes the {@link DatabaseType} to use for formatting SQL statements.
   *
   * @return The {@link DatabaseType} to use.
   *
   * @throws SQLException If a failure occurs.
   */
  protected DatabaseType initDatabaseType() throws SQLException {
    Connection conn = this.getConnection();
    try {
      return DatabaseType.detect(conn);
    } finally {
      conn = close(conn);
    }
  }

  /**
   * Gets the {@link DatabaseType} used by this instance.
   *
   * @return The {@link DatabaseType} used by this instance.
   */
  public DatabaseType getDatabaseType() {
    return this.databaseType;
  }

  /**
   * Ensures the schema exists and alternatively drops the existing the schema
   * and recreates it.  This is called from {@link #doInit(JsonObject)}.
   *
   * @param recreate <code>true</code> if the existing schema should be
   *                 dropped, otherwise <code>false</code>.
   *
   * @throws SQLException If a failure occurs.
   */
  protected abstract void ensureSchema(boolean recreate) throws SQLException;

  /**
   * Overridden to do nothing.
   */
  @Override
  protected void doDestroy() {
    // do nothing
  }

  /**
   * Implemented to save the specified follow-up tasks to the backing SQLite
   * persistent store.
   */
  @Override
  protected synchronized void enqueueFollowUpTask(Task task)
    throws ServiceExecutionException
  {
    Connection  conn    = null;
    boolean     success = false;
    try {
      // obtain the connection
      conn = this.getConnection();

      // update the multiplicity if the row exists
      boolean updated = this.incrementFollowUpMultiplicity(conn, task);

      // check if we updated a row
      if (!updated) {
        // insert a new row since none was updated
        this.insertNewFollowUpTask(conn, task);
      }

      // commit the connection
      conn.commit();
      success = true;

    } catch (SQLException e) {
      e.printStackTrace();
      throw new ServiceExecutionException("JDBC failure occurred", e);

    } finally {
      if (!success) rollback(conn);
      conn = close(conn);
    }
  }

  /**
   * Increments the multiplicity for the specified follow-up task in the
   * database by updating the associated row if it exists.  This
   * <code>true</code> if the row existed and was updated, otherwise
   * <code>false</code>
   *
   * @param conn The {@link Connection} to use to connect to the database.
   * @param task The {@link Task} describing the row to update.
   * @return <code>true</code> if the row existed and was updated, otherwise
   *         <code>false</code>.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected boolean incrementFollowUpMultiplicity(Connection conn, Task task)
    throws SQLException
  {
    PreparedStatement ps = null;
    try {
      // prepare the statement
      ps = conn.prepareStatement(
          "UPDATE sz_follow_up_tasks "
              + "SET multiplicity = multiplicity + 1 "
              + "WHERE signature = ? "
              + "AND allow_collapse_flag = 1 "
              + "AND expire_lease_at IS NULL "
              + "AND task_id = (SELECT MAX(task_id) FROM sz_follow_up_tasks "
              + "WHERE signature = ? AND allow_collapse_flag = 1 "
              + "AND expire_lease_at IS NULL)");

      ps.setString(1, task.getSignature());
      ps.setString(2, task.getSignature());

      int rowCount = ps.executeUpdate();

      if (rowCount == 0) {
        return false;
      } if (rowCount == 1) {
        return true;
      } else {
        System.err.println();
        System.err.println("********************************");
        System.err.println("MULTIPLE ROWS UPDATED FOR FOLLOW-UP TASK: " + task);
        throw new IllegalStateException(
            "Somehow updated multiple rows when updating task multiplicity.  "
                + "task=[ " + task + " ]");
      }

    } finally {
      ps = close(ps);
    }
  }

  /**
   * Inserts a new follow-up task in the database schema.
   *
   * @param conn The {@link Connection} to use to connect to the database.
   * @param task The {@link Task} describing the row to insert.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected void insertNewFollowUpTask(Connection conn, Task task)
      throws SQLException
  {
    PreparedStatement ps = null;
    try {
      ps = conn.prepareStatement(
          "INSERT INTO sz_follow_up_tasks ("
              + "signature, allow_collapse_flag, json_text) VALUES (?, ?, ?)");
      ps.setString(1, task.getSignature());
      ps.setInt(2, (task.isAllowingCollapse() ? 1 : 0));
      ps.setString(3, task.toJsonText());

      int rowCount = ps.executeUpdate();

      if (rowCount != 1) {
        throw new SQLException(
            "Unexpected row count on insert: " + rowCount);
      }
    } finally {
      ps = close(ps);
    }
  }

  /**
   * This message can be used for debugging to dump the contents of the
   * follow-up table to standard error.
   *
   * @throws SQLException If a JDBC failure occurs.
   */
  protected void dumpFollowUpTable() throws SQLException {
    Connection        conn  = null;
    PreparedStatement ps    = null;
    ResultSet         rs    = null;
    try {
      conn = this.getConnection();

      StringBuilder sb = new StringBuilder();
      sb.append("SELECT task_id, json_text, signature, multiplicity, ");
      sb.append("lease_id, expire_lease_at, modified_on, created_on ");
      sb.append("FROM sz_follow_up_tasks ");

      ps = conn.prepareStatement(sb.toString());

      rs = ps.executeQuery();

      long now          = System.currentTimeMillis();
      long delayTime    = now - this.getFollowUpDelay();
      long timeoutTime  = now - this.getFollowUpTimeout();

      System.err.println();
      System.err.println("-------------------------------------------------");
      while (rs.next()) {
        System.err.println(
            rs.getLong(1) + " / " + rs.getString(2)
                + " / " + rs.getInt(4)
                + " / " + rs.getString(5)
                + " / " + rs.getTimestamp(6, UTC_CALENDAR)
                + " / " + rs.getTimestamp(7, UTC_CALENDAR)
                + " vs " + (new Timestamp(delayTime))
                + " / " + rs.getTimestamp(8, UTC_CALENDAR)
                + " vs " + (new Timestamp(timeoutTime)));
      }
      rs = close(rs);
      ps = close(ps);

    } catch (SQLException e) {
      e.printStackTrace();
      throw e;

    } finally {
      rs = close(rs);
      ps = close(ps);
      conn = close(conn);
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
   * @throws ServiceExecutionException If a failure occurs.
   */
  @Override
  protected synchronized List<ScheduledTask> dequeueFollowUpTasks(int count)
      throws ServiceExecutionException
  {
    Connection  conn    = null;
    boolean     success = false;
    try {
      // get a connection
      conn = this.getConnection();

      // first release any expired leases
      int released = this.releaseExpiredLeases(conn);

      if (released > 0) {
        synchronized (this.getStatisticsMonitor()) {
          //System.err.println("EXPIRED LEASE ON " + rowCount + " FOLLOW UP TASKS");
          this.totalExpiredFollowUpTaskCount += released;
        }
      }

      // generate a unique lease ID
      String leaseId = this.generateLeaseId();

      // lease the follow-up tasks
      int leasedCount = this.leaseFollowUpTasks(conn, count, leaseId);

      // this.dumpFollowUpTable();

      // check if no rows were updated
      if (leasedCount == 0) {
        return new ArrayList<>(0);
      }

      // now get the leased rows
      List<ScheduledTask> result = this.getLeasedFollowUpTasks(conn, leaseId);

      // commit the transaction
      conn.commit();
      success = true;

      // return the result list
      return result;

    } catch (SQLException e) {
      throw new ServiceExecutionException(
          "Failed to dequeue follow-up task", e);

    } finally {
      if (!success) rollback(conn);
      conn = close(conn);
    }
  }

  /**
   * Releases any previously obtained leases on follow tasks that have expired.
   * This makes it possible to retrieve those follow-up tasks again from the
   * database.  The assumption is that if the lease has expired then they are
   * no longer enqueued for processing and the lease is probably from an
   * aborted process that is no longer running.
   *
   * @param conn The {@link Connection} to use.
   * @return The number of tasks for which the leases had expired.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int releaseExpiredLeases(Connection conn)
    throws SQLException
  {
    {
      DatabaseType dbType = this.getDatabaseType();

      PreparedStatement ps = null;
      try {
        // first release the lease on any task where the lease has expired
        ps = conn.prepareStatement(
            "UPDATE sz_follow_up_tasks "
                + "SET lease_id = NULL, expire_lease_at = NULL "
                + "WHERE lease_id IS NOT NULL "
                + "AND expire_lease_at < " + dbType.getTimestampBindingSQL());

        // don't be too aggressive on expiring leases
        long      now           = System.currentTimeMillis();
        long      leaseExpire   = now - (this.getFollowUpTimeout() / 2);
        Timestamp expireTime    = new Timestamp(leaseExpire);

        dbType.setTimestamp(ps, 1, expireTime);

        return ps.executeUpdate();

      } finally {
        ps = close(ps);
      }
    }
  }

  /**
   * Marks the specified number of unleased follow-up tasks as leased with the
   * specified lease ID using the specified {@link Connection}.
   *
   * @param conn The {@link Connection} to use.
   * @param limit The upper-limit on the number of follow-up tasks to lease.
   * @param leaseId The lease ID to use for marking the tasks as leased.
   * @return The actual number of follow-up tasks that were leased.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int leaseFollowUpTasks(Connection conn, int limit, String leaseId)
    throws SQLException
  {
    DatabaseType dbType = this.getDatabaseType();

    PreparedStatement ps = null;
    try {
      // don't be too aggressive on expiring leases
      long now              = System.currentTimeMillis();
      long delayMillis      = now - this.getFollowUpDelay();
      long timeoutMillis    = now - this.getFollowUpTimeout();
      Timestamp delayTime   = new Timestamp(delayMillis);
      Timestamp timeoutTime = new Timestamp(timeoutMillis);

      // now let's lease some new follow up tasks
      ps = conn.prepareStatement(
          "UPDATE sz_follow_up_tasks "
              + "SET lease_id = ?, "
              + "expire_lease_at = " + dbType.getTimestampBindingSQL() + " "
              + "WHERE task_id IN (SELECT task_id FROM sz_follow_up_tasks "
              + "WHERE lease_id IS NULL AND expire_lease_at IS NULL "
              + "AND (modified_on < " + dbType.getTimestampBindingSQL() + " "
              + "OR created_on < " + dbType.getTimestampBindingSQL() + ") "
              + "ORDER BY created_on "
              + "LIMIT ?)");

      long      leaseExpire = now + (2 * this.getFollowUpTimeout());
      Timestamp expireTime  = new Timestamp(leaseExpire);

      ps.setString(1, leaseId);
      dbType.setTimestamp(ps,2, expireTime);
      dbType.setTimestamp(ps,3, delayTime);
      dbType.setTimestamp(ps,4, timeoutTime);
      ps.setInt(5, limit);

      // execute the update and return the number of affected rows
      return ps.executeUpdate();

    } finally {
      ps = close(ps);
    }
  }


  /**
   * Gets the {@link List} of {@link ScheduledTask} instances describing the
   * follow-up tasks in the database that are marked as leased with the
   * specified lease ID.
   *
   * @param conn The {@link Connection} to use.
   * @param leaseId The lease ID of the follow-up tasks to retrieve.
   * @return The {@link List} of {@link ScheduledTask} instances describing
   *         the leased follow-up tasks.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected List<ScheduledTask> getLeasedFollowUpTasks(Connection conn,
                                                       String     leaseId)
    throws SQLException
  {
    PreparedStatement ps = null;
    ResultSet         rs = null;
    try {
      // set the leased rows
      ps = conn.prepareStatement(
          "SELECT "
              + "task_id, expire_lease_at, multiplicity, json_text, created_on "
              + "FROM sz_follow_up_tasks "
              + "WHERE lease_id = ?");

      ps.setString(1, leaseId);

      int                 fetchCount  = this.getFollowUpFetchCount();
      List<ScheduledTask> result      = new ArrayList<>(fetchCount);
      rs = ps.executeQuery();
      while (rs.next()) {
        long taskId = rs.getLong(1);
        Timestamp expTime = rs.getTimestamp(2, UTC_CALENDAR);
        int multiplicity = rs.getInt(3);
        String jsonText = rs.getString(4);
        Timestamp createdOn = rs.getTimestamp(5, UTC_CALENDAR);

        String followUpId = taskId + ":" + leaseId;
        long now = System.currentTimeMillis();
        long elapsedSinceCreation = now - createdOn.getTime();

        ScheduledTask task = new ScheduledTask(jsonText,
                                               followUpId,
                                               multiplicity,
                                               expTime.getTime(),
                                               elapsedSinceCreation);

        result.add(task);
      }

      // return the result list
      return result;

    } finally {
      rs = close(rs);
      ps = close(ps);
    }
  }

  /**
   * Creates a virtually unique lease ID.
   *
   * @return A new lease ID to use.
   */
  protected String generateLeaseId() {
    long pid = ProcessHandle.current().pid();
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
    Connection        conn    = null;
    boolean           success = false;
    try {
      conn = this.getConnection();

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

      int updateCount = this.updateLeaseExpiration(conn,
                                                   expireTime,
                                                   leaseIdSet);

      if (updateCount != tasks.size()) {
        System.err.println();
        System.err.println("---------------------------------------------");
        System.err.println("WARNING: Renewed lease on " + updateCount
                               + " follow-up tasks when expected to update "
                               + tasks.size() + " follow-up tasks: "
                               + leaseIdSet);
      }

      // commit the change
      conn.commit();
      success = true;

    } catch (SQLException e) {
      throw new ServiceExecutionException(
          "Failed to enqueue follow-up task", e);

    } finally {
      if (!success) rollback(conn);
      conn = close(conn);
    }
  }

  /**
   * Updates the expiration time on the follow-up tasks with lease ID's in the
   * specified {@link Set} to the specified expiration time using the specified
   * {@link Connection}.
   *
   * @param conn The {@link Connection} to use.
   * @param expireTime The new expiration time as a {@link Timestamp}.
   * @param leaseIdSet The {@link Set} of {@link String} lease ID's.
   * @return The number of follow-up tasks updated.
   * @throws SQLException If a JDBC failure occurs.
   */
  protected int updateLeaseExpiration(Connection  conn,
                                      Timestamp   expireTime,
                                      Set<String> leaseIdSet)
    throws SQLException
  {
    DatabaseType dbType = this.getDatabaseType();

    PreparedStatement ps = null;
    try {
      int count = leaseIdSet.size();

      // build the SQL
      StringBuilder sb = new StringBuilder(
          "UPDATE sz_follow_up_tasks "
              + "SET expire_lease_at = " + dbType.getTimestampBindingSQL() + " "
              + "WHERE lease_id IN (");
      String prefix = "";
      for (int index = 0; index < count; index++) {
        sb.append(prefix).append("?");
        prefix = ", ";
      }
      sb.append(")");

      // prepare the statement
      ps = conn.prepareStatement(sb.toString());

      // bind the timestamp for the new expire time
      dbType.setTimestamp(ps, 1, expireTime);

      // bind the lease ID's
      int index = 2;
      for (String leaseId : leaseIdSet) {
        ps.setString(index++, leaseId);
      }

      // execute the update and return the row count
      return ps.executeUpdate();

    } finally {
      ps = close(ps);
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
    Connection  conn    = null;
    boolean     success = false;
    try {
      conn = this.getConnection();

      boolean deleted = this.deleteFollowUpTask(conn, task);

      if (!deleted) {
        System.err.println();
        System.err.println("------------------------------------------------");
        System.err.println("WARNING: Follow-up task was already completed");
      }

      // commit the transaction
      conn.commit();
      success = true;

    } catch (SQLException e) {
      throw new ServiceExecutionException(
          "Failed to delete completed follow-up task", e);

    } finally {
      if (!success) rollback(conn);
      conn = close(conn);
    }
  }

  /**
   * Deletes a follow-up task (typically once it hqs been completed).  This is
   * called from the default {@link #completeFollowUpTask(ScheduledTask)}
   * implementation.
   *
   * @param conn The {@link Connection} to use.
   * @param task The {@link ScheduledTask} describing the task to delete.
   * @return <code>true</code> if a follow-up was deleted and <code>false</code>
   *         if not (usually because it was already deleted).
   * @throws SQLException If a JDBC failure occurs.
   */
  protected boolean deleteFollowUpTask(Connection    conn,
                                                ScheduledTask task)
    throws SQLException
  {
    PreparedStatement ps = null;
    try {
      ps = conn.prepareStatement(
          "DELETE FROM sz_follow_up_tasks WHERE task_id = ?");

      String  followUpId  = task.getFollowUpId();
      int     index       = followUpId.indexOf(":");
      long    taskId      = Long.parseLong(followUpId.substring(0, index));

      ps.setLong(1, taskId);

      int rowCount = ps.executeUpdate();

      if (rowCount > 1) {
        throw new SQLException(
            "Multiple follow-up rows deleted when one was expected: "
                + rowCount);
      }

      return (rowCount == 1);

    } finally {
      ps = close(ps);
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
