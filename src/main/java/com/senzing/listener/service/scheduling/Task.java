package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.locking.ResourceKey;
import com.senzing.util.JsonUtilities;

import javax.json.*;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static java.util.Collections.*;
import static com.senzing.io.IOUtilities.*;
import static com.senzing.util.JsonUtilities.*;
import static com.senzing.listener.service.scheduling.Task.State.*;
import static com.senzing.listener.service.scheduling.Task.Statistic.*;

/**
 * Describes a task to be scheduled and handled.
 */
public class Task {
  /**
   * Constant used for converting between nanoseconds and milliseconds.
   */
  private static final long ONE_MILLION = 1000000L;

  /**
   * The enumerated states of a task.
   */
  enum State {
    /**
     * The transient state that the task is in between when it is constructed
     * and when it is scheduled to be executed.
     */
    UNSCHEDULED,

    /**
     * The task is scheduled to be performed at some future time but has not
     * yet been begun working.
     */
    SCHEDULED,

    /**
     * The task is in the process of being handled at the current time and will
     * either succeed or fail.
     */
    STARTED,

    /**
     * The task was successfully handled.
     */
    SUCCESSFUL,

    /**
     * The task could not be handled due to a failure.
     */
    FAILED,

    /**
     * The task was aborted before being started because another task in the
     * group failed.
     */
    ABORTED;

    /**
     * The states that are valid predecessors for this state.
     */
    private Set<State> predecessors;

    /**
     * The states that are valid successors for this state.
     */
    private Set<State> successors;

    /**
     * Initializes the predecessors and successor states for each instance.
     */
    static {
      UNSCHEDULED.predecessors = Collections.emptySet();
      UNSCHEDULED.successors   = Set.of(SCHEDULED);

      SCHEDULED.predecessors  = Set.of(UNSCHEDULED);
      SCHEDULED.successors    = Set.of(STARTED);

      STARTED.predecessors    = Set.of(SCHEDULED);
      STARTED.successors      = Set.of(SUCCESSFUL, FAILED);

      SUCCESSFUL.predecessors = Set.of(STARTED);
      SUCCESSFUL.successors   = Collections.emptySet();

      FAILED.predecessors     = Set.of(STARTED);
      FAILED.successors       = Collections.emptySet();

      ABORTED.predecessors    = Set.of(UNSCHEDULED, SCHEDULED);
      ABORTED.successors      = Collections.emptySet();
    }

    /**
     * Gets the <b>unmodifiable</b> {@link Set} of predecessor states for this
     * instance.
     *
     * @return The <b>unmodifiable</b> {@link Set} of predecessor states for
     *         this instance.
     */
    public Set<State> getPredecessors() {
      return this.predecessors;
    }

    /**
     * Gets the <b>unmodifiable</b> {@link Set} of successor states for this
     * instance.
     *
     * @return The <b>unmodifiable</b> {@link Set} of successor states for
     *         this instance.
     */
    public Set<State> getSuccessors() {
      return this.successors;
    }
  }

  /**
   * The available statistic keys for this {@link Task}.  See the
   * {@link Task#getStatistics()} method.
   *
   */
  public enum Statistic {
    /**
     * The time spent (in milliseconds) between constructing the task and
     * scheduling the task.  If the task is not yet scheduled then it is the
     * time spent thus far.
     */
    unscheduledTime,

    /**
     * The time spent (in milliseconds) between scheduling the task and
     * beginning to handle the task.  If the task has not yet begun handling
     * then it is the time spent thus far waiting to be handled.
     */
    pendingTime,

    /**
     * The time spent (in milliseconds) handling the task once it was no longer
     * in a pending state.  This is zero (0) if not yet handled and if the task
     * is not yet completed it will be the time spent thus far.
     */
    handlingTime,

    /**
     * The number of milliseconds from the point in time from when the task was
     * scheduled until the time it completed either successfully, with a
     * failure or was aborted.
     */
    roundTripTime,

    /**
     * The total time (in milliseconds) from construction until completion
     * (whether successful or failed) or until the current time if not yet
     * completed.
     */
    lifespan;

    /**
     * Gets the unit of measure for this statistic.  This is the unit that
     * the {@link Number} value is measured in when calling {@link
     * Task#getStatistics()}}
     *
     * @return The unit of measure for this statistic.
     */
    public String getUnits() {
      return "ms";
    }
  }

  /**
   * The next task ID.
   */
  private static long nextTaskId = 0L;

  /**
   * Gets the next task ID.
   *
   * @return The next task ID.
   */
  private static synchronized long getNextTaskId() {
    return nextTaskId++;
  }

  /**
   * The task ID for this task.
   */
  private long taskId;

  /**
   * The state of this task.
   */
  private State state = UNSCHEDULED;

  /**
   * The action for this task.
   */
  private String action;

  /**
   * The parameters for this task.
   */
  private SortedMap<String, Object> parameters;

  /**
   * The resource keys for this task.
   */
  private SortedSet<ResourceKey> resourceKeys;

  /**
   * The {@link TaskGroup} that this task is associated with.
   */
  private TaskGroup taskGroup = null;

  /**
   * Flag indicating if this task instance allows tasks that are duplicates
   * of this one to be collapsed into a single task handling with an
   * incrementally increased multiplicity.
   */
  private boolean allowCollapse = true;

  /**
   * The associated {@link Exception} if the handling of this task encountered
   * a failure.
   */
  private Exception failure = null;

  /**
   * The nanosecond timestamp when this task was created.
   */
  private long createdTimeNanos = -1L;

  /**
   * The nanoseconds timestamp when this task was scheduled, or negative
   * one (-1) if not yet scheduled.
   */
  private long scheduledTimeNanos = -1L;

  /**
   * The nanosecond timestamp when this task was started, or negative
   * one (-1) if not yet started.
   */
  private long startedTimeNanos = -1L;

  /**
   * The nanosecond timestamp when this task was campleted, or negative
   * one (-1) if not yet completed.
   */
  private long completedTimeNanos = -1L;

  /**
   * Constructs a new {@link Task} with the specified parameters.
   *
   * @param action The action for the task.
   * @param parameters The {@link SortedMap} of parameters for the task.
   * @param resourceKeys The {@link SortedSet} of {@link ResourceKey} instances
   *                     for the task.
   * @param taskGroup The optional {@link TaskGroup} for the task, or
   *                  <code>null</code> if the task is a follow-up task either
   *                  being constructed directly or deserialized from the
   *                  database.
   * @param allowCollapse <code>true</code> if collapsing identical tasks of
   *                      this type is allowed, otherwise <code>false</code>.
   */
  Task(String                     action,
       SortedMap<String, Object>  parameters,
       SortedSet<ResourceKey>     resourceKeys,
       TaskGroup                  taskGroup,
       boolean                    allowCollapse)
  {
    this.taskId           = getNextTaskId();
    this.action           = action;
    this.parameters       = new TreeMap<>(parameters);
    this.parameters       = unmodifiableSortedMap(this.parameters);
    this.resourceKeys     = new TreeSet<>(resourceKeys);
    this.resourceKeys     = unmodifiableSortedSet(this.resourceKeys);
    this.taskGroup        = taskGroup;
    this.failure          = null;
    this.createdTimeNanos = System.nanoTime();
    this.allowCollapse    = allowCollapse;
  }

  /**
   * Returns the statistics for this {@link Task} instance.  The statistics
   * are returned as a {@link Map} of {@link Statistic} keys to {@link Number}
   * values whose units are measured in the associated units for the given the
   * key found via {@link Statistic#getUnits()}.
   *
   * @return The statistics for this {@link Task}.
   */
  public Map<Statistic, Number> getStatistics() {
    Map<Statistic, Number> result = new LinkedHashMap<>();
    synchronized (this) {
      result.put(unscheduledTime, this.getUnscheduledTime());
      long pendTime = this.getPendingTime();
      if (pendTime > 0L) {
        result.put(pendingTime, pendTime);
      }
      long handleTime = this.getHandlingTime();
      if (handleTime > 0L) {
        result.put(handlingTime, handleTime);
      }
      result.put(lifespan, this.getLifespan());
    }
    return result;
  }

  /**
   * Parses the specified JSON text as a task.  This is typically used for
   * deserializing a task from permanent storage.  A deserialized task is not
   * associated with a {@link TaskGroup}.
   *
   * @param jsonText The JSON text to parse.
   *
   * @return The deserialized {@link Task}.
   */
  static Task parse(String jsonText) {
    JsonObject jsonObject = parseJsonObject(jsonText);

    String action = getString(jsonObject,"action");

    JsonObject paramsObject = getJsonObject(jsonObject, "params");

    JsonArray resourceArray = getJsonArray(jsonObject, "resources");

    boolean allowCollapse = getBoolean(jsonObject, "allowCollapse", true);

    // get the parameters map
    SortedMap<String, Object> paramsMap = (paramsObject == null)
        ? Collections.emptySortedMap()
        : new TreeMap<>((Map<String, Object>) normalizeJsonValue(paramsObject));

    // get the list of resources
    List<String> resourceList = (resourceArray == null)
        ? Collections.emptyList()
        : (List<String>) normalizeJsonValue(resourceArray);

    // convert the resources from strings to objects
    SortedSet<ResourceKey> resourceSet = new TreeSet<>();
    for (String resourceKey : resourceList) {
      resourceSet.add(ResourceKey.parse(resourceKey));
    }

    return new Task(action, paramsMap, resourceSet, null, allowCollapse);
  }

  /**
   * Gets the current {@link State} for this task.
   *
   * @return The current {@link State} for this task.
   */
  public synchronized State getState() {
    return this.state;
  }

  /**
   * Sets the {@link State} of this task to the specified {@link State} which
   * must be a valid transition from the {@linkplain #getState() current state}.
   *
   * @param state The {@link State} to transition to.
   *
   * @throws IllegalArgumentException If the specified {@Link State} does not
   *                                  represent a valid transition from the
   *                                  {@linkplain #getState() current state}.`
   */
  private synchronized void setState(State state) {
    Set<State> predecessors = state.getPredecessors();
    Set<State> successors   = this.getState().getSuccessors();

    if ((!predecessors.contains(this.getState()))
        || (!successors.contains(state)))
    {
      throw new IllegalArgumentException(
          "Cannot transition to specified state (" + state + ") from "
          + "current state (" + this.getState() + ").");
    }
    this.state = state;
    this.notifyAll();
  }

  /**
   * Gets the associated {@link TaskGroup}, if any.  This returns
   * <code>null</code> if the task has no group.
   *
   * @return The associated {@link TaskGroup}, or <code>null</code> if this
   *         task has no group.
   */
  public TaskGroup getTaskGroup() {
    return this.taskGroup;
  }

  /**
   * Checks if this task can be collapsed with other collapsible tasks that
   * are identical to it for a single call to {@link
   * TaskHandler#handle(String, Map, int, Scheduler)} with an incrementally
   * increased multiplicity.
   *
   * @return <code>true</code> if this task can be collpased, otherwise
   *         <code>false</code>.
   */
  public boolean isAllowingCollapse() {
    return this.allowCollapse;
  }

  /**
   * Checks if this {@link Task} instance has been marked in a state of
   * completion either {@link State#SUCCESSFUL}, {@link State#FAILED} or
   * {@link State#ABORTED}.
   */
  public synchronized boolean isCompleted() {
    return (this.completedTimeNanos >= 0L);
  }

  /**
   * Marks this task as having been scheduled.
   */
  void markScheduled() {
    synchronized (this) {
      this.setState(SCHEDULED);
      this.scheduledTimeNanos = System.nanoTime();
    }
    TaskGroup taskGroup = this.getTaskGroup();
    if (taskGroup != null) {
      taskGroup.taskScheduled(this);
    }
  }

  /**
   * Marks this task has having begun being handled.
   */
  void beginHandling() {
    synchronized (this) {
      this.setState(STARTED);
      this.startedTimeNanos = System.nanoTime();
    }
    TaskGroup taskGroup = this.getTaskGroup();
    if (taskGroup != null) {
      taskGroup.taskStarted(this);
    }
  }

  /**
   * Marks this task as having succeeded.
   */
  void succeeded() {
    synchronized (this) {
      this.setState(SUCCESSFUL);
      this.completedTimeNanos = System.nanoTime();
    }
    if (this.getTaskGroup() != null) {
      this.getTaskGroup().taskSucceeded(this);
    }
  }

  /**
   * Marks this task as having failed with the specified {@link Exception}.
   *
   * @param failure The {@link Exception} describing the failure.
   */
  void failed(Exception failure) {
    synchronized (this) {
      this.setState(FAILED);
      this.completedTimeNanos = System.nanoTime();
      this.failure = failure;
    }
    if (this.getTaskGroup() != null) {
      this.getTaskGroup().taskFailed(this);
    }
  }

  /**
   * Marks this task as having been aborted.
   */
  void aborted() {
    synchronized (this) {
      this.setState(ABORTED);
      this.completedTimeNanos = System.nanoTime();
    }
    if (this.getTaskGroup() != null) {
      this.getTaskGroup().taskAborted(this);
    }
  }

  /**
   * Returns the task ID for this task.
   *
   * @return The task ID for this task.
   */
  public long getTaskId() {
    return this.taskId;
  }

  /**
   * Returns the action for this task.
   *
   * @return The action for this task.
   */
  public String getAction() {
    return this.action;
  }

  /**
   * Gets the <b>unmodifiable</b> {@link Map} of parameters for this task.
   *
   * @return The <b>unmodifiable</b> {@link Map} of parameters for this task.
   */
  public SortedMap<String, Object> getParameters() {
    return this.parameters;
  }

  /**
   * Gets the <b>unmodifiable</b> {@link Set} of {@link ResourceKey} instances
   * identifying the resources that will be modified by this task.
   *
   * @return The <b>unmodifiable</b> {@link Set} of {@link ResourceKey} instances
   *         identifying the resources that will be modified by this task.
   */
  public SortedSet<ResourceKey> getResourceKeys() {
    return this.resourceKeys;
  }

  /**
   * Gets the {@link Exception} describing any failure that may have occurred
   * while handling this {@link Task}.  This method returns <code>null</code>
   * if the {@link Task} has not yet been handled or was handled but completed
   * successfully.
   *
   * @return The {@link Exception} describing any failure that may have
   *         occurred while handling this {@link Task}, or <code>null</code>
   *         if the {@link Task} has not yet been handled or was handled but
   *         completed successfully.
   */
  public Exception getFailure() {
    return this.failure;
  }

  /**
   * Converts the specified {@link Task} to a {@link JsonObjectBuilder}
   * describing the action, parameters and associated resource keys.
   *
   * @param task The {@link Task} to be represented as a {@link JsonObject}.
   *
   * @return The {@link JsonObjectBuilder} describing the specified
   *         {@link Task}.
   */
  public static JsonObjectBuilder toJsonObjectBuilder(Task task) {
    JsonObjectBuilder job1 = Json.createObjectBuilder();
    job1.add("action", task.action);
    if (task.parameters.size() > 0) {
      JsonObjectBuilder job2 = Json.createObjectBuilder();
      task.getParameters().forEach((key, value) -> {
        addProperty(job2, key, value);
      });
      job1.add("params", job2);
    }
    if (task.getResourceKeys().size() > 0) {
      JsonArrayBuilder jab = Json.createArrayBuilder();
      for (ResourceKey key : task.resourceKeys) {
        jab.add(key.toString());
      }
      job1.add("resources", jab);
    }
    return job1;
  }

  /**
   * Converts this {@link Task} instance to a {@link JsonObjectBuilder}
   * describing the action, parameters and associated resource keys.
   *
   * @return The {@link JsonObjectBuilder} describing this {@link Task}
   *         instance.
   */
  public JsonObjectBuilder toJsonObjectBuilder() {
    return toJsonObjectBuilder(this);
  }

  /**
   * Converts the specified {@link Task} to a {@link JsonObject} describing the
   * action, parameters and associated resource keys.
   *
   * @param task The {@link Task} to be represented as a {@link JsonObject}.
   *
   * @return The {@link JsonObject} describing the specified {@link Task}.
   */
  public static JsonObject toJsonObject(Task task) {
    return toJsonObjectBuilder(task).build();
  }

  /**
   * Converts this {@link Task} instance to a {@link JsonObject} describing the
   * action, parameters and associated resource keys.
   *
   * @return The {@link JsonObject} describing this {@link Task} instance.
   */
  public JsonObject toJsonObject() {
    return toJsonObject(this);
  }

  /**
   * Converts the specified {@link Task} to a {@link JsonObject} describing the
   * action, parameters and associated resource keys.
   *
   * @param task The {@link Task} to be represented as a {@link JsonObject}.
   */
  public static String toJsonText(Task task) {
    return JsonUtilities.toJsonText(toJsonObject(task));
  }

  /**
   * Converts this task to a JSON representation of this task.
   *
   * @return The JSON representation of this task.
   */
  public String toJsonText() {
    return toJsonText(this);
  }

  /**
   * Gets a message digest signature which can be used to easily identify
   * a serialized text representation of this task.
   *
   * @param task The {@link Task} to convert to a signature.
   *
   * @return A message digest signature which can be used to easily identify
   *         this a serialized text representation of this task.
   */
  public static String toSignature(Task task) {
    return toSignature(toJsonText(task));
  }

  /**
   * Gets a message digest signature which can be used to easily identify
   * a serialized text representation of this task.
   *
   * @return A message digest signature which can be used to easily identify
   *         this a serialized text representation of this task.
   */
  public String getSignature() {
    return toSignature(this);
  }

  /**
   * Converts the specified JSON text to a message digest signature which can
   * be used to easily identify the specified serialized JSON text.
   *
   * @param jsonText The JSON text representation of the task.
   *
   * @return The message digest of the specifiex JSON text.
   */
  private static String toSignature(String jsonText) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(jsonText.getBytes(UTF_8));
      byte[] digest = md.digest();
      StringBuilder sb = new StringBuilder();
      for (byte b: digest) {
        sb.append(Integer.toHexString(0xFF & b));
      }
      return sb.toString();

    } catch (NoSuchAlgorithmException cannotHappen) {
      throw new IllegalStateException("SHA-256 is not supported by JVM");
    } catch (UnsupportedEncodingException cannotHappen) {
      throw new IllegalStateException("UTF-8 encoding is not supported");
    }
  }

  /**
   * Overridden to returns a diagnostic {@link String} describing this task.
   *
   * @return A diagnostic {@link String} describing this task.
   */
  @Override
  public String toString() {
    return this.toString(this.getState());
  }

  /**
   * Provided as a way to convert to a {@link String} without
   * synchronization to avoid possible dead locks.
   *
   * @param state The {@link Task.State} known to the caller.
   *
   * @return The {@link String} representation of this instance.
   */
  protected String toString(State state) {
    StringBuilder sb = new StringBuilder();
    sb.append("taskId=[ ").append(this.getTaskId()).append(" ], ");
    sb.append("state=[ " ).append(state).append(" ], ");

    String jsonText = this.toJsonText();
    String signature = toSignature(jsonText);

    sb.append("signature=[ ").append(signature).append(" ], ");
    sb.append("allowCollapse=[ ").append(this.isAllowingCollapse())
        .append(" ], ").append("task=[ ").append(jsonText).append(" ]");
    return sb.toString();
  }

  /**
   * Gets the number of milliseconds from the point in time at which this task
   * was creasted until it was scheduled to be handled.  If the task has not
   * yet been scheduled then the number of milliseconds since it was created
   * is returned.
   *
   * @return The duration of the unscheduled time of this task in milliseconds.
   */
  public synchronized long getUnscheduledTime() {
    if (this.scheduledTimeNanos < 0L) {
      return (System.nanoTime() - this.createdTimeNanos) / ONE_MILLION;
    } else {
      return (this.scheduledTimeNanos - this.createdTimeNanos) / ONE_MILLION;
    }
  }

  /**
   * Gets the number of milliseconds from the point in time at which this task
   * was scheduled until handling of the task was started.  If the task has not
   * yet been scheduled then negative one (-1) is returned.  If the task has
   * been scheduled, but has not yet been started then the number of
   * milliseconds since it was scheduled is returned.
   *
   * @return The duration of the pending time of this task in milliseconds.
   */
  public synchronized long getPendingTime() {
    if (this.scheduledTimeNanos < 0L) return -1L;
    if (this.startedTimeNanos < 0L) {
      return (System.nanoTime() - this.scheduledTimeNanos) / ONE_MILLION;
    } else {
      return (this.startedTimeNanos - this.scheduledTimeNanos) / ONE_MILLION;
    }
  }

  /**
   * Gets the number of milliseconds from the point in time at which handling
   * of this task was started until handling completed successfully or with
   * failures.  If the task has not yet been started then negative one (-1)
   * is returned.  If the task started, but has not yet completed then the
   * number of milliseconds since it was started is returned.
   *
   * @return The duration of the handling time of this task in milliseconds.
   */
  public synchronized long getHandlingTime() {
    if (this.startedTimeNanos < 0L) return -1L;
    if (this.completedTimeNanos < 0L) {
      return (System.nanoTime() - this.startedTimeNanos) / ONE_MILLION;
    } else {
      return (this.completedTimeNanos - this.startedTimeNanos) / ONE_MILLION;
    }
  }

  /**
   * Gets the number of milliseconds from the point in time from when this
   * task was scheduled until the time it was completed either successfully or
   * with failures (or aborted).  If not yet completed then the number of
   * milliseconds since the task was created is returned.
   *
   * @return The duration of the time from scheduling to completion of this
   *         task in milliseconds.
   */
  public synchronized long getRoundTripTime() {
    if (this.scheduledTimeNanos < 0L) {
      return (System.nanoTime() - this.scheduledTimeNanos) / ONE_MILLION;
    } else {
      return (this.completedTimeNanos - this.scheduledTimeNanos) / ONE_MILLION;
    }
  }

  /**
   * Gets the number of milliseconds from the point in time from when this
   * task was created until the time it was completed either successfully or
   * with failures.  If not yet completed then the number of milliseconds since
   * the task was created is returned.
   *
   * @return The duration of the lifespan of this task in milliseconds.
   */
  public synchronized long getLifespan() {
    if (this.completedTimeNanos < 0L) {
      return (System.nanoTime() - this.createdTimeNanos) / ONE_MILLION;
    } else {
      return (this.completedTimeNanos - this.createdTimeNanos) / ONE_MILLION;
    }
  }
}
