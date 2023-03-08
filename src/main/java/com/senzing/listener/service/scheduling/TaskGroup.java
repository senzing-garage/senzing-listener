package com.senzing.listener.service.scheduling;

import com.senzing.util.Quantified;

import java.util.*;

import static com.senzing.listener.service.scheduling.Task.State.UNSCHEDULED;
import static com.senzing.listener.service.scheduling.Task.State.STARTED;
import static com.senzing.listener.service.scheduling.TaskGroup.State.*;

/**
 * Describes a group of tasks that pertain to the same message.  This is used
 * for tracking when all tasks associated with the message have been completed.
 * Synchronizing and waiting on an instance of this class will allow for
 * periodic non-busy waiting for completion.
 */
public class TaskGroup implements Quantified {
  /**
   * Constant used for converting between nanoseconds and milliseconds.
   */
  private static final long ONE_MILLION = 1000000L;

  /**
   * The various states of a {@link TaskGroup}.
   */
  public enum State {
    /**
     * The {@link TaskGroup} is open to have more tasks added to it.
     */
    OPEN,

    /**
     * The {@link TaskGroup} cannot have any more tasks added to it, <b>but</b>
     * the associated tasks have <b>not</b> yet been completed.
     */
    CLOSED,

    /**
     * The {@link TaskGroup} cannot have any more tasks added to it <b>and</b>
     * has had at least one but <b>not</b> all of its associated tasks scheduled
     * for handling.
     */
    SCHEDULING,

    /**
     * The {@link TaskGroup} cannot have any more tasks added to it <b>and</b>
     * has had all of its associated tasks scheduled for handling.
     */
    SCHEDULED,

    /**
     * The {@link TaskGroup} cannot have any more tasks added to it <b>and</b>
     * at least one of the associated tasks has been scheduled and experienced
     * a failure in its handling.  This state does <b>NOT</b> imply or
     * guarantee completion since more tasks may remain that are scheduled to
     * be handled or are being handled.
     *
     * @see #isFastFail()
     * @see #setFastFail(boolean)
     * @see #isCompleted()
     * @see #getPendingCount()
     * @see #awaitCompletion()
     * @see #awaitCompletion(long, long)
     */
    FAILED,

    /**
     * The {@link TaskGroup} cannot have any more tasks added to it <b>and</b>
     * <b>all</b> of the associated tasks <b>have</b> been successfully
     * completed.
     */
    SUCCESSFUL;
  }

  /**
   * Enumerates the statistics available for a {@link TaskGroup}.
   */
  public enum Stat implements Statistic {
    /**
     * The number of milliseconds from the point in time from when this task
     * group was created until it was {@linkplain #close() closed} to further
     * tasks being added to it or if still open then until the current
     * timestamp.
     */
    openTime,

    /**
     * The number of milliseconds from the point in time from when this task
     * group was created until its first task was scheduled or if no tasks for
     * the group have been scheduled then until the current timestamp.
     */
    unscheduledTime,

    /**
     * The number of milliseconds between scheduling the first associated
     * task and the handling of the first associated task being started (NOTE:
     * these may be different tasks).  If the first associated task has not yet
     * been scheduled then the statistic is not available.  If the first has
     * been scheduled, but no associated tasks have yet been started then the
     * duration is from the first scheduled time to the current timestamp.
     */
    pendingTime,

    /**
     * The total number of milliseconds spent handling each of the tasks in
     * the group.  For those tasks that have been started this duration
     * includes the time spent thus far.  The returned time does not account
     * for concurrency (i.e.: overlapping time spent in concurrent threads).
     */
    totalHandlingTime,

    /**
     * The total number of milliseconds spent handling any task in the group.
     * For those tasks that have been started the handling time for that task
     * is taken to be the time spent thus far.  If no tasks have been started
     * for the group then the statistic is unavailable.
     */
    longestHandlingTime,

    /**
     * The number of milliseconds from the point in time from when the first
     * task from the group was scheduled until the time the task group was
     * considered to be {@linkplain #isCompleted() completed} or if not yet
     * completed until the current time.
     */
    roundTripTime,

    /**
     * The number of milliseconds from the point in time from when the task
     * group was created until the time it was considered to be {@linkplain
     * #isCompleted() completed} or if not yet completed until the current
     * time.
     */
    lifespan,

    /**
     * The number of tasks in the group.
     */
    taskCount,

    /**
     * The number of tasks in the group that have not yet been completed.
     */
    pendingCount,

    /**
     * The number of tasks in the group that have completed successfully.
     */
    successCount,

    /**
     * The number of tasks in the group that have completed with failure.
     */
    failureCount;

    /**
     * Gets the unit of measure for this statistic.  This is the unit that
     * the {@link Number} value is measured in when calling {@link
     * Task#getStatistics()}}
     *
     * @return The unit of measure for this statistic.
     */
    public String getUnits() {
      switch (this) {
        case taskCount:
        case pendingCount:
        case successCount:
        case failureCount:
          return "tasks";
        default:
          return "ms";
      }
    }
  }

  /**
   * The default number of milliseconds to wait between checks
   */
  public static final long DEFAULT_MAXIMUM_INTERVAL = 1000L;

  /**
   * The next task group ID.
   */
  private static long nextGroupId = 0L;

  /**
   * Gets the next group ID in a thred-safe manner.
   */
  private static synchronized long getNextGroupId() {
    return nextGroupId++;
  }

  /**
   * The group ID for this task group.
   */
  private long groupId;

  /**
   * The number of tasks that in the group that have been scheduled.
   */
  private int scheduledCount = 0;

  /**
   * The number of tasks associated wit this task group that have started
   * being handled.
   */
  private int startedCount = 0;

  /**
   * The number of tasks completed successfully in the group.
   */
  private int successCount = 0;

  /**
   * The number of tasks in the group that completed with failures.
   */
  private int failureCount = 0;

  /**
   * The number of tasks in the group that have been aborted.
   */
  private int abortedCount = 0;

  /**
   * Flag indicating if the failure of a single associated task should signal
   * that other associated tasks should be skipped or never handled.
   */
  private boolean fastFail = true;

  /**
   * The {@link IdentityHashMap} of tasks to the {@link TaskInfo} instance
   * describing the last recorded state of the task.
   */
  private IdentityHashMap<Task, TaskInfo> taskStateMap = null;

  /**
   * The {@link State} of the {@link TaskGroup}.
   */
  private State state = State.OPEN;

  /**
   * The first {@link Task} to be marked as failed for the group (if any).
   */
  private Task firstFailure = null;

  /**
   * The last {@link Task} to be marked as completed (successful or not).
   */
  private Task lastInGroup = null;

  /**
   * The cumulative time spent handling the tasks in this group.
   */
  private long handlingDuration = 0L;

  /**
   * The longest duration spent handling a task in this group.
   */
  private long longestHandlingTime = -1L;

  /**
   * The timestamp for when this task group was first created.
   */
  private long createdTimeNanos = -1L;

  /**
   * The timestamp for when task addition to this group was closed.
   */
  private long closedTimeNanos = -1L;

  /**
   * The timestamp for when the first task associated with this group was
   * scheduled.
   */
  private long firstScheduledTimeNanos = -1L;

  /**
   * The timestamp for when the last task associated with this group was
   * scheduled.
   */
  private long lastScheduledTimeNanos = -1L;

  /**
   * The timestamp for when the first task in the group was handled.
   */
  private long firstHandledTimeNanos = -1L;

  /**
   * The timestamp for when the tasks in this group have all been completed.
   */
  private long completedTimeNanos = -1L;

  /**
   * Constructs a new task group with the next sequential group ID.
   */
  protected TaskGroup() {
    this.groupId          = getNextGroupId();
    this.taskStateMap     = new IdentityHashMap<>();
    this.successCount     = 0;
    this.failureCount     = 0;
    this.scheduledCount   = 0;
    this.startedCount     = 0;
    this.fastFail         = true;
    this.createdTimeNanos = System.nanoTime();
    this.state            = OPEN;
  }

  /**
   * Gets the {@link State} for this {@link TaskGroup} instance.
   *
   * @return The {@link State} for this {@link TaskGroup} instance.
   */
  public synchronized State getState() {
    return this.state;
  }

  /**
   * Sets the {@link State} for this {@link TaskGroup}.
   *
   * @param state The {@link State} for this {@link TaskGroup} instance.
   */
  protected synchronized void setState(State state) {
    if (this.state != state) {
      this.state = state;
      this.notifyAll();
    }
  }

  /**
   * Gets the group ID for this task group.
   *
   * @return The group ID for this task group.
   */
  public long getGroupId() {
    return this.groupId;
  }

  /**
   * Adds the specified {@link Task} to this group.
   *
   * @param task The task to add to the group.
   *
   * @throws IllegalStateException If this {@link TaskGroup} is no longer in the
   *                               {@link State#OPEN} state.
   */
  protected void addTask(Task task) throws IllegalStateException
  {
    // get the task state
    Task.State taskState = task.getState();

    // check the task state
    if (taskState != UNSCHEDULED) {
      throw new IllegalStateException(
          "Only a Task in the " + UNSCHEDULED + " state can be added to a "
              + "TaskGroup: " + taskState);
    }

    // check the task group of the task
    if (task.getTaskGroup() != this) {
      throw new IllegalArgumentException(
          "The specified task does not have this TaskGroup as its group.  "
          + "task=[ " + task + " ], actualGroup=[ " + task.getTaskGroup()
          + " ], expectedGroup=[ " + this + " ]");
    }

    synchronized (this) {
      if (this.getState() != OPEN) {
        throw new IllegalStateException(
            "Cannot add a task to a TaskGroup that is no longer in the " + OPEN
                + " state: " + this.getState());
      }

      // add the task
      TaskInfo taskInfo = this.taskStateMap.get(task);
      if (taskInfo == null) {
        this.taskStateMap.put(task, new TaskInfo(taskState));
      } else {
        taskInfo.setTaskState(taskState);
      }

      // notify all of the added task
      this.notifyAll();
    }
  }

  /**
   * Checks if the failure of a single task associated with this {@link
   * TaskGroup} should trigger the attempted abort of other associated tasks
   * that have not already been started and the completion status of this
   * {@link TaskGroup}.
   *
   * @return <code>true</code> if the failure of a single task associated with
   *         this {@link TaskGroup} should trigger the attempted abort of other
   *         associated tasks that have not already been handled, otherwise
   *         <code>false</code>.
   */
  public synchronized boolean isFastFail() {
    return this.fastFail;
  }

  /**
   * Sets whether the failure of a single task associated with this {@link
   * TaskGroup} should trigger the attempted abort of other associated tasks
   * that have not already been started and the completion status of this
   * {@link TaskGroup}.  This method cannot be called if the {@link TaskGroup}
   * has already transitioned out of the {@link State#OPEN} state.
   *
   * @param failFast <code>true</code> if the failure of a single task
   *                 associated with this {@link TaskGroup} should trigger the
   *                 attempted abort of other associated tasks that have not
   *                 already been handled, otherwise <code>false</code>.
   *
   * @throws IllegalStateException If this {@link TaskGroup} is <b>not</b> in
   *                               the {@link State#OPEN} state.
   * @see #getState()
   */
  public synchronized void setFastFail(boolean failFast) {
    if (this.getState() != OPEN) {
      throw new IllegalStateException(
          "The fast-fail property can only be set while the TaskGroup is in "
          + "the " + OPEN + " state: " + this.getState());
    }
    this.fastFail = failFast;
  }

  /**
   * Prevents the addition of any more tasks to this group.  This method has no
   * effect if this {@link TaskGroup} instance has already been closed.
   */
  synchronized void close() {
    if (this.getState() != OPEN) return;
    this.setState(CLOSED);
    this.closedTimeNanos = System.nanoTime();
  }

  /**
   * Ensures the specified {@link Task} is present in this {@link TaskGroup}.
   *
   * @param task The {@link Task} to verify.
   * @throws IllegalArgumentException If the {@link TaskGroup} of the specified
   *                                  {@link Task} is not this instance.
   * @throws IllegalStateException If the specified {@link Task} has not been
   *                               added to this {@link TaskGroup}.
   */
  protected synchronized void ensureTaskPresent(Task task)
    throws IllegalArgumentException, IllegalStateException
  {
    // check the task group for the task
    if (task.getTaskGroup() != this) {
      throw new IllegalArgumentException(
          "The specified Task is not part of this TaskGroup ("
              + this.getGroupId() + "): " + task);
    }

    // ensure the task was found
    if (!this.taskStateMap.containsKey(task)) {
      throw new IllegalStateException(
          "The specified task is associated with this TaskGroup ("
              + this.getGroupId() + "), but the TaskGroup is not associated "
              + "with the task: " + task);
    }
  }

  /**
   * Marks the specified {@link Task} has having been scheduled.
   *
   * @param task The {@link Task} to mark as succeeded.
   *
   * @throws IllegalArgumentException If the {@link TaskGroup} of the specified
   *                                  {@link Task} is not this instance.
   * @throws IllegalStateException If the specified {@link Task} has not been
   *                               added to this {@link TaskGroup} or if the
   *                               states of this task group or the task are
   *                               inconsistent.
   *
   */
  protected void taskScheduled(Task task) {
    // check the task state
    Task.State taskState = task.getState();
    if (taskState != Task.State.SCHEDULED) {
      throw new IllegalStateException(
          "Task being marked as scheduled is not in the " + Task.State.SCHEDULED
              + " state: " + task.toString(taskState));
    }

    synchronized (this) {
      this.ensureTaskPresent(task);
      State state = this.getState();
      if (this.getScheduledCount() == 0) {
        if (state != CLOSED) {
          throw new IllegalStateException(
              "TaskGroup must be in a " + CLOSED + " state before its first task "
                  + "gets scheduled: " + state);
        }
      } else {
        if (state != SCHEDULING && state != FAILED) {
          throw new IllegalStateException(
              "TaskGroup must be in a " + SCHEDULING + " or " + FAILED
                  + " state if tasks are being scheduled: " + state);
        }
      }

      // get the previously recorded task state
      Task.State lastState = this.taskStateMap.get(task).getTaskState();

      // first check the last recorded state
      if (lastState != UNSCHEDULED) {
        throw new IllegalStateException(
            "Task is being marked as scheduled when previous state was "
                + "not " + UNSCHEDULED + ": " + task.toString(taskState));
      }

      // check if this is the first scheduled
      if (this.getScheduledCount() == 0) {
        this.firstScheduledTimeNanos = System.nanoTime();
      }

      // record the task state
      this.taskStateMap.get(task).setTaskState(taskState);

      // increment the scheduled count
      this.scheduledCount++;

      // set the state
      if (state != SCHEDULING) {
        this.setState(SCHEDULING);
      }

      // check if this was the last
      if (this.getScheduledCount() == this.getTaskCount()) {
        this.lastScheduledTimeNanos = System.nanoTime();
        this.setState(SCHEDULED);
      }

      // notify all
      this.notifyAll();
    }
  }

  /**
   * Marks the start of handling for the specified {@link Task}.
   *
   * @param task The {@link Task} that has been started.
   *
   * @throws IllegalArgumentException If the {@link TaskGroup} of the specified
   *                                  {@link Task} is not this instance.
   * @throws IllegalStateException If the specified {@link Task} has not been
   *                               added to this {@link TaskGroup} or if the
   *                               states of this task group or the task are
   *                               inconsistent.
   */
  protected void taskStarted(Task task) {
    // get the task state
    Task.State taskState = task.getState();
    // check if the task is started
    if (taskState != STARTED) {
      throw new IllegalArgumentException(
          "Cannot record a Task as started that is not in the " + STARTED
              + " state: " + task.toString(taskState));
    }

    synchronized (this) {
      // ensure the task is part of the group
      this.ensureTaskPresent(task);

      // check the state of the task group
      State state = this.getState();
      if (state != SCHEDULING && state != SCHEDULED && state != FAILED) {
        throw new IllegalStateException(
            "TaskGroup must be in a " + SCHEDULING + ", " + State.SCHEDULED
                + " or " + FAILED + " state when one of its task is being "
                + "marked as started: " + state);
      }

      // check the task state
      Task.State lastState = this.taskStateMap.get(task).getTaskState();

      // first check the last recorded state
      if (lastState != Task.State.SCHEDULED) {
        throw new IllegalStateException(
            "Task is being marked as started when previous state was "
                + "not " + Task.State.SCHEDULED + ": "
                + task.toString(taskState));
      }

      // record the task state
      this.taskStateMap.get(task).setTaskState(taskState);

      // increment the started count
      this.startedCount++;

      // notify all
      this.notifyAll();
    }
  }

  /**
   * Marks the specified {@link Task} has having completed successfully.
   *
   * @param task The {@link Task} to mark as succeeded.
   *
   * @throws IllegalArgumentException If the {@link TaskGroup} of the specified
   *                                  {@link Task} is not this instance.
   * @throws IllegalStateException If the specified {@link Task} is not in the
   *                               {@link Task.State#SUCCESSFUL} state or if the
   *                               previous record state for the task is not
   *                               a valid predecessor for {@link
   *                               Task.State#SUCCESSFUL}.
   */
  protected void taskSucceeded(Task task) {
    // check if the task is successful
    Task.State taskState = task.getState();
    if (taskState != Task.State.SUCCESSFUL) {
      throw new IllegalArgumentException(
          "Cannot record a Task as successful that is not in a "
              + Task.State.SUCCESSFUL + " state: " + task.toString(taskState));
    }

    long handlingTime = task.getHandlingTime();

    synchronized (this) {
      // ensure the task is part of the group
      this.ensureTaskPresent(task);

      // check the state of the task group
      State state = this.getState();
      if (state != SCHEDULING && state != SCHEDULED && state != FAILED) {
        throw new IllegalStateException(
            "TaskGroup must be in a " + SCHEDULING + ", " + SCHEDULED + " or "
                + FAILED + " state for a task to be marked as successfully "
                + "completed: " + state);
      }

      // check the task state
      Task.State lastState = this.taskStateMap.get(task).getTaskState();

      // first check the last recorded state
      if (!taskState.getPredecessors().contains(lastState)) {
        throw new IllegalStateException(
            "Task is being marked as successful (" + taskState
                + ") when previously recorded state (" + lastState
                + ") was not a valid predecessor.  Valid predecessors are: " +
                taskState.getPredecessors());
      }

      // keep track of the last completed task
      this.lastInGroup = task;

      // record the task state
      this.taskStateMap.get(task).setTaskState(taskState);

      // increment the success count
      this.successCount++;

      // increment the handling duration
      if (handlingTime > 0) this.handlingDuration += handlingTime;
      if (handlingTime > this.longestHandlingTime) {
        this.longestHandlingTime = handlingTime;
      }

      // record completion stats and state
      this.checkCompletion();

      // notify all
      this.notifyAll();
    }
  }

  /**
   * Marks the specified {@link Task} has having failed.
   *
   * @param task The {@link Task} to mark as failed.
   *
   * @throws IllegalArgumentException If the {@link TaskGroup} of the specified
   *                                  {@link Task} is not this instance.
   * @throws IllegalStateException If the specified {@link Task} is not in the
   *                               {@link Task.State#FAILED} state or if the
   *                               previous record state for the task is not
   *                               a valid predecessor for {@link
   *                               Task.State#FAILED}.
   */
  protected void taskFailed(Task task) {
    // check the task state
    Task.State taskState = task.getState();
    if (taskState != Task.State.FAILED) {
      throw new IllegalStateException(
          "Task being marked as failed is not in the " + Task.State.FAILED
              + " state: " + task.toString(taskState));
    }

    long handlingTime = task.getHandlingTime();

    synchronized (this) {
      // ensure the task is part of the group
      this.ensureTaskPresent(task);

      // check the state of the task group
      State state = this.getState();
      if (state != SCHEDULING && state != SCHEDULED && state != FAILED) {
        throw new IllegalStateException(
            "TaskGroup must be in a " + SCHEDULING + ", " + SCHEDULED + " or "
                + FAILED + " state for a task to be marked as failed: "
                + state);
      }

      // check the last recorded task state
      Task.State lastState = this.taskStateMap.get(task).getTaskState();

      // first check the last recorded state
      if (!taskState.getPredecessors().contains(lastState)) {
        throw new IllegalStateException(
            "Task is being marked as failed (" + taskState
                + ") when previously recorded state (" + lastState
                + ") was not a valid predecessor.  Valid predecessors are: " +
                taskState.getPredecessors());
      }

      // keep track of the last completed task
      this.lastInGroup = task;
      if (this.firstFailure == null) {
        this.firstFailure = task;
      }

      // mark the state
      this.taskStateMap.get(task).setTaskState(taskState);

      // record the success
      this.failureCount++;

      // increment the handling duration
      if (handlingTime > 0) this.handlingDuration += handlingTime;
      if (handlingTime > this.longestHandlingTime) {
        this.longestHandlingTime = handlingTime;
      }

      // record completion stats and state
      this.checkCompletion();

      // notify all
      this.notifyAll();
    }
  }

  /**
   * Marks the specified {@link Task} has having been aborted.
   *
   * @param task The {@link Task} to mark as aborted.
   *
   * @throws IllegalArgumentException If the {@link TaskGroup} of the specified
   *                                  {@link Task} is not this instance.
   * @throws IllegalStateException If the specified {@link Task} is not in the
   *                               {@link Task.State#ABORTED} state or if the
   *                               previous record state for the task is not
   *                               a valid predecessor for {@link
   *                               Task.State#ABORTED}.
   */
  protected void taskAborted(Task task) {
    // check the task state
    Task.State taskState = task.getState();
    if (taskState != Task.State.ABORTED) {
      throw new IllegalStateException(
          "Task being marked as aborted is not in the " + Task.State.ABORTED
              + " state: " + task.toString(taskState));
    }

    synchronized (this) {
      // ensure the task is part of the group
      this.ensureTaskPresent(task);

      // check the state of the task group
      State state = this.getState();
      if (state != FAILED || !this.isFastFail()) {
        throw new IllegalStateException(
            "TaskGroup must be in a " + FAILED + " state and configured for "
                + "fast-fail for a contained task to be aborted.  state=[ "
                + state + " ], fastFail=[ " + this.isFastFail() + " ]");
      }

      // check the last recorded task state
      Task.State lastState = this.taskStateMap.get(task).getTaskState();

      // first check the last recorded state
      if (!taskState.getPredecessors().contains(lastState)) {
        throw new IllegalStateException(
            "Task is being marked as aborted (" + taskState
                + ") when previously recorded state (" + lastState
                + ") was not a valid predecessor.  Valid predecessors are: " +
                taskState.getPredecessors());
      }

      // keep track of the last completed task
      this.lastInGroup = task;

      // mark the state
      this.taskStateMap.get(task).setTaskState(taskState);

      // record the success
      this.abortedCount++;

      // record completion stats and state
      this.checkCompletion();

      // notify all
      this.notifyAll();
    }
  }

  /**
   * Checks the number of completed tasks and records timestamps and sets
   * state appropriately.
   */
  private synchronized void checkCompletion() {
    // check if this the first or last completed
    int completedCount  = this.getCompletedCount();
    int abortedCount    = this.getAbortedCount();
    if (completedCount == 1) {
      this.firstHandledTimeNanos = System.nanoTime();
    }
    // check the failure count and mark as failed if needed
    int failureCount = this.getFailureCount();
    if ((failureCount > 0) && (this.getState() != FAILED)) {
      this.setState(FAILED);
    }

    // check if the task group is complete
    boolean fastFail = this.isFastFail();
    if (completedCount == this.getTaskCount() || (failureCount > 0 && fastFail))
    {
      // set the completion time if not already set
      if (this.completedTimeNanos < 0L) {
        this.completedTimeNanos = System.nanoTime();
      }

      // check if no failures
      if (failureCount == 0) {
         this.setState(SUCCESSFUL);
      }
    }
  }

  /**
   * Gets the number of tasks associated with this task group that have been
   * scheduled to be handled.
   *
   * @return The number of tasks associated with this task group that have been
   *         scheduled to be handled.
   */
  public synchronized int getScheduledCount() {
    return this.scheduledCount;
  }

  /**
   * Gets the number of tasks associated with this task group that have begun
   * being handled (some may have completed as well).
   *
   * @return The number of tasks associated with this task group that have
   *         begun being handled.
   */
  public synchronized int getStartedCount() {
    return this.startedCount;
  }

  /**
   * Gets the number of associated tasks that completed successfully.
   *
   * @return The number of associated tasks that completed successfully.
   */
  public synchronized int getSuccessCount() {
    return this.successCount;
  }

  /**
   * Gets the number of associated tasks that completed with failures.
   *
   * @return The number of associated tasks that completed with failures.
   */
  public synchronized int getFailureCount() {
    return this.failureCount;
  }

  /**
   * Gets the number of associated tasks that were aborted.
   *
   * @return The number of associated tasks that have been aborted.
   */
  public synchronized int getAbortedCount() {
    return this.abortedCount;
  }

  /**
   * Gets the number of associated tasks that have completed either successfully
   * or with failures.  This does <b>not</b> include aborted tasks.
   *
   * @return The number of associated tasks that have completed either
   *         successfully or with failures.
   */
  public synchronized int getCompletedCount() {
    return this.getSuccessCount() + this.getFailureCount();
  }

  /**
   * Gets the total number of tasks in the group.
   *
   * @return The total number of tasks in the group.
   */
  public synchronized int getTaskCount() {
    return this.taskStateMap.size();
  }

  /**
   * Gets the number of tasks pending completion.  Tasks that have been
   * aborted are <b>not</b> considered to be pending.
   *
   * @return The number of tasks pending completion.
   */
  public synchronized int getPendingCount() {
    int pendingCount = this.getTaskCount();
    pendingCount -= this.getCompletedCount();
    pendingCount -= this.getAbortedCount();
    return pendingCount;
  }

  /**
   * Checks if this {@link TaskGroup} should be considered completed.  This
   * can mean that the associated tasks have been scheduled and all have
   * completed successfully, but it can also mean that at least one associated
   * task has been scheduled and at least one failed to be handled and the
   * {@linkplain #isFastFail() fast-fail} property is set to <code>true</code>.
   *
   * @return <code>true</code> if the {@link TaskGroup} should be considered
   *         to be completed, otherwise <code>false</code>.
   */
  public synchronized boolean isCompleted() {
    State   state         = this.getState();
    int     pendingCount  = this.getPendingCount();
    int     failureCount  = this.getFailureCount();
    boolean fastFail      = this.isFastFail();

    // if still in the OPEN state then it cannot be completed
    if (state == OPEN) return false;

    // if not in the OPEN state and nothing is pending, then we are completed
    if (pendingCount == 0) return true;

    // if at least one failure and we are failing fast, then we are completed
    if (failureCount > 0 && fastFail) return true;

    // otherwise we are not completed
    return false;
  }

  /**
   * Indefinitely awaits completion of the associated tasks using the
   * {@linkplain #DEFAULT_MAXIMUM_INTERVAL default maximum interval} time
   * between checks of completion (note: a completed task will notify waiters
   * and interrupt the waiting).  If this {@link TaskGroup} is set to
   * {@linkplain #isFastFail() fail fast} then the first failure will trigger
   * the completion state.
   */
  public void awaitCompletion() {
    this.awaitCompletion(DEFAULT_MAXIMUM_INTERVAL, -1L);
  }

  /**
   * Awaits completion of the associated tasks for the specified maximum total
   * wait time using the specified maximum interval time between checks of
   * completion (note: a completed task will notify waiters and interrupt the
   * waiting).
   *
   * @param maxInterval The maximum interval wait time (in milliseconds) between
   *                    periodic wakeup from sleep for a forced check of
   *                    completion.
   * @param maxWait The maximum number of milliseconds to await completion.
   *
   * @return <code>true</code> if all associated tasks have completed, otherwise
   *         <code>false</code>.
   */
  public boolean awaitCompletion(long maxInterval, long maxWait) {
    long startTime = System.nanoTime();
    synchronized (this) {
      long elapsed = (System.nanoTime() - startTime) / ONE_MILLION;
      while (!this.isCompleted() && (maxWait < 0L || elapsed < maxWait))
      {
        long interval = (maxWait < 0L)
            ? maxInterval : Math.min(maxInterval, maxWait - elapsed);

        try {
          this.wait(interval);
        } catch (InterruptedException ignore) {
          // ignore the interruption
        }

        // update the elapsed time
        elapsed = (System.nanoTime() - startTime) / ONE_MILLION;
      }

      // return if we completed all tasks
      return (this.isCompleted());
    }
  }

  /**
   * Gets an <b>unmodifiable</b> {@link List} of the associated {@link
   * Task} instances.
   *
   * @return An <b>unmodifiable</b> {@link List} of the associated {@link Task}
   *         instances.
   */
  public synchronized List<Task> getTasks() {
    List<Task> result = new ArrayList<>(this.taskStateMap.size());
    result.addAll(this.taskStateMap.keySet());
    return Collections.unmodifiableList(result);
  }

  /**
   * Gets an <b>unmodifiable</b> {@link List} of the associated {@link
   * Task} instances that experienced a failure in handling.
   *
   * @return An <b>unmodifiable</b> {@link List} of the associated {@link Task}
   *         instances that experienced a failure in handling.
   */
  public synchronized List<Task> getFailedTasks() {
    List<Task> result = new ArrayList<>(this.getFailureCount());
    this.taskStateMap.forEach((task, info) -> {
      if (info.getTaskState() == Task.State.FAILED) {
        result.add(task);
      }
    });
    return Collections.unmodifiableList(result);
  }

  /**
   * Gets an <b>unmodifiable</b> {@link List} of the associated {@link
   * Task} instances that have been aborted.
   *
   * @return An <b>unmodifiable</b> {@link List} of the associated {@link Task}
   *         instances that have been aborted before being handled.
   */
  public synchronized List<Task> getAbortedTasks() {
    List<Task> result = new ArrayList<>(this.getAbortedCount());
    this.taskStateMap.forEach((task, info) -> {
      if (info.getTaskState() == Task.State.ABORTED) {
        result.add(task);
      }
    });
    return Collections.unmodifiableList(result);
  }

  /**
   * Gets the number of milliseconds from the point in time from when this
   * task group was scheduled until the time it was considered to be {@linkplain
   * #isCompleted() completed} or if not yet completed until the current
   * time.
   *
   * @return The duration of the round-trip of this task group in milliseconds
   *         from scheduling to completion.
   */
  public synchronized long getRoundTripTime() {
    if (this.completedTimeNanos < 0L) {
      return (System.nanoTime() - this.firstScheduledTimeNanos) / ONE_MILLION;
    } else {
      long completed  = this.completedTimeNanos;
      long scheduled  = this.firstScheduledTimeNanos;
      return (completed - scheduled) / ONE_MILLION;
    }
  }

  /**
   * Gets the number of milliseconds from the point in time from when this
   * task group was created until the time it was considered to be {@linkplain
   * #isCompleted() completed} or if not yet completed until the current
   * time.
   *
   * @return The duration of the lifespan of this task group in milliseconds.
   */
  public synchronized long getLifespan() {
    if (this.completedTimeNanos < 0L) {
      return (System.nanoTime() - this.createdTimeNanos) / ONE_MILLION;
    } else {
      return (this.completedTimeNanos - this.createdTimeNanos) / ONE_MILLION;
    }
  }

  /**
   * Gets the number of milliseconds from the point in time from when this
   * task group was created until it was {@linkplain #close() closed} to further
   * tasks being added to it or if still open then until the current timestamp.
   *
   * @return The duration of the open time for this task group in milliseconds.
   */
  public synchronized long getOpenTime() {
    if (this.closedTimeNanos < 0L) {
      return (System.nanoTime() - this.createdTimeNanos) / ONE_MILLION;
    } else {
      return (this.closedTimeNanos - this.createdTimeNanos) / ONE_MILLION;
    }
  }

  /**
   * Gets the number of milliseconds from the point in time from when this
   * task group was created until its first task was scheduled or if no tasks
   * for the group have been scheduled then until the current timestamp.
   *
   * @return The duration of the unscheduled time for this task group in
   *         milliseconds.
   */
  public synchronized long getUnscheduledTime() {
    if (this.firstScheduledTimeNanos < 0L) {
      return (System.nanoTime() - this.createdTimeNanos) / ONE_MILLION;
    } else {
      long scheduled  = this.firstScheduledTimeNanos;
      long created    = this.createdTimeNanos;
      return (scheduled - created) / ONE_MILLION;
    }
  }

  /**
   * Gets the number of milliseconds between scheduling the first associated
   * task and the last associated task.  If the first associated task has not
   * yet been scheduled then a negative number is returned.  If the first has
   * been scheduled, but the last has not yet been scheduled then the duration
   * is from the first scheduled time to the current timestamp.
   *
   * @return The number of milliseconds between scheduling the first associated
   *         task and the last associated task, or negative-one (-1) if the
   *         first associated task has not yet been scheduled.
   */
  public synchronized long getSchedulingTime() {
    if (this.firstScheduledTimeNanos < 0L) return -1L;
    if (this.lastScheduledTimeNanos < 0L) {
      return (System.nanoTime() - this.firstScheduledTimeNanos) / ONE_MILLION;
    } else {
      long first  = this.firstScheduledTimeNanos;
      long last   = this.lastScheduledTimeNanos;
      return ((last - first) / ONE_MILLION);
    }
  }

  /**
   * Gets the number of milliseconds between scheduling the first associated
   * task and the handling of the first associated task being started (NOTE:
   * these may be different tasks).  If the first associated task has not yet
   * been scheduled then a negative number is returned.  If the first has been
   * scheduled, but no associated tasks have yet been started then the duration
   * is from the first scheduled time to the current timestamp.
   *
   * @return The number of milliseconds between scheduling the first associated
   *         task and the start of handling the first associated task, or
   *         negative-one (-1) if the first associated task has not yet been
   *         scheduled.
   */
  public synchronized long getPendingTime() {
    if (this.firstScheduledTimeNanos < 0L) return -1L;
    if (this.firstHandledTimeNanos < 0L) {
      return (System.nanoTime() - this.firstScheduledTimeNanos) / ONE_MILLION;
    } else {
      long scheduled = this.firstScheduledTimeNanos;
      long handled = this.lastScheduledTimeNanos;
      return ((handled - scheduled) / ONE_MILLION);
    }
  }

  /**
   * Gets the total number of milliseconds spent handling each of the tasks in
   * this group.  For those tasks that have been started this duration includes
   * the time spent thus far.  The returned time does not account for
   * concurrency (i.e.: overlapping time spent in concurrent threads).
   *
   * @return The total number of milliseconds spent handling each of the tasks
   *         in this group.
   */
  public synchronized long getTotalHandlingTime() {
    long now = System.nanoTime();
    long result = this.handlingDuration;
    for (TaskInfo taskInfo : this.taskStateMap.values()) {
      if (taskInfo.getTaskState() == STARTED) {
        result += ((now - taskInfo.getStateChangedTimeNanos()) / ONE_MILLION);
      }
    }
    return result;
  }

  /**
   * Gets the total number of milliseconds spent handling any task in this
   * group.  For those tasks that have been started the handling time for that
   * task is taken to be the time spent thus far.  If no tasks have been started
   * or completed then this returns a negative number.
   *
   * @return The longest number of milliseconds spent handling any of the tasks
   *         in this group, or negative-one (-1) if no tasks in this group have
   *         been started.
   */
  public synchronized long getLongestHandlingTime() {
    long now = System.nanoTime();
    long result = this.longestHandlingTime;
    for (TaskInfo taskInfo : this.taskStateMap.values()) {
      if (taskInfo.getTaskState() == STARTED) {
        long duration
            = ((now - taskInfo.getStateChangedTimeNanos()) / ONE_MILLION);
        if (duration > result) {
          result = duration;
        }
      }
    }
    return result;
  }

  /**
   * Checks if the specified {@link Task} is the first failure in the group.
   * If the group has not failed then this always returns <code>false</code>.
   *
   * @param task The {@link Task} to chcek to see if it is the first failure.
   *
   * @return <code>true</code> if this task group has failed and the specified
   *         task is the first failure, otherwise <code>false</code>.
   */
  protected synchronized boolean isFirstFailure(Task task) {
    return (task != null && this.firstFailure == task);
  }

  /**
   * Checks if the specified {@link Task} is the last pending task that was
   * marked as successful, failed or aborted.
   *
   * @param task The {@link Task} to check.
   *
   * @return <code>true</code> if and only if there are no pending tasks and
   *         the specified {@link Task} was the last one to be marked as
   *         successful, failed, or aborted, otherwise <code>false</code>.
   */
  protected synchronized boolean isConcludingTask(Task task) {
    int pendingCount = this.getPendingCount();
    return (task != null && pendingCount == 0 && this.lastInGroup == task);
  }

  /**
   * Returns the statistics for this {@link Task} instance.  The statistics
   * are returned as a {@link Map} of {@link Task.Statistic} keys to {@link Number}
   * values whose units are measured in the associated units for the given the
   * key found via {@link Task.Statistic#getUnits()}.
   *
   * @return The statistics for this {@link Task}.
   */
  public Map<Statistic, Number> getStatistics() {
    Map<Statistic, Number> result = new LinkedHashMap<>();
    synchronized (this) {
      result.put(Stat.openTime, this.getOpenTime());
      result.put(Stat.unscheduledTime, this.getUnscheduledTime());
      long pendingTime = this.getPendingTime();
      if (pendingTime > 0L) {
        result.put(Stat.pendingTime, pendingTime);
      }
      result.put(Stat.totalHandlingTime, this.getTotalHandlingTime());
      long longestTime = this.getLongestHandlingTime();
      if (longestTime > 0L) {
        result.put(Stat.longestHandlingTime, longestTime);
      }
      result.put(Stat.roundTripTime, this.getRoundTripTime());
      result.put(Stat.lifespan, this.getLifespan());
      result.put(Stat.taskCount, this.getTaskCount());
      result.put(Stat.pendingCount, this.getPendingCount());
      result.put(Stat.successCount, this.getSuccessCount());
      result.put(Stat.failureCount, this.getFailureCount());
    }
    return result;
  }

  /**
   * The {@link TaskInfo} recorded for the associated task.
   */
  private static class TaskInfo {
    /**
     * The {@link Task.State} for this instance.
     */
    private Task.State taskState;

    /**
     * The System nanonsecond time when the last state changed was recorded.
     */
    private long stateChangedTimeNanos;

    /**
     * Constructs with the specified {@link Task.State}.
     *
     * @param state The {@link Task.State} to set for this instance.
     */
    public TaskInfo(Task.State state) {
      this.taskState = state;
      this.stateChangedTimeNanos = System.nanoTime();
    }

    /**
     * Gets the current recorded {@link Task.State}.
     *
     * @return The current recorded {@link Task.State}.
     */
    public synchronized Task.State getTaskState() {
      return this.taskState;
    }

    /**
     * Returns the System nanosecond time recorded when the associated
     * Task state was changed.
     *
     * @return The System nanosecond time recorded when the associated
     *         Task state was changed.
     */
    public long getStateChangedTimeNanos() {
      return this.stateChangedTimeNanos;
    }

    /**
     * If the specified {@link Task.State} signifies a change then this method
     * sets the associated {@link Task.State} to the specified state and records
     * the nanosecond timestamp for the change.
     *
     * @param state The {@link Task.State} to set.
     */
    public synchronized void setTaskState(Task.State state) {
      if (this.taskState != state) {
        this.taskState              = state;
        this.stateChangedTimeNanos  = System.nanoTime();
      }
    }
  }
}
