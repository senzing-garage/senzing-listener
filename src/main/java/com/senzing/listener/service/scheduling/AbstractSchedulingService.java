package com.senzing.listener.service.scheduling;

import com.ibm.icu.impl.UCaseProps;
import com.senzing.listener.communication.AbstractMessageConsumer;
import com.senzing.listener.service.MessageProcessor;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.listener.service.locking.LockToken;
import com.senzing.listener.service.locking.LockingService;
import com.senzing.listener.service.locking.ProcessScopeLockingService;
import com.senzing.listener.service.locking.ResourceKey;
import com.senzing.util.AsyncWorkerPool;
import com.senzing.util.AsyncWorkerPool.AsyncResult;
import com.senzing.util.JsonUtilities;
import com.senzing.util.LoggingUtilities;
import com.senzing.util.Timers;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.*;

import static com.senzing.util.JsonUtilities.*;
import static com.senzing.listener.service.scheduling.SchedulingService.State.*;
import static com.senzing.listener.service.scheduling.AbstractSchedulingService.Stat.*;
import static com.senzing.listener.service.ServiceUtilities.*;
import static java.lang.Boolean.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Provides an abstract base class for implementing {@link SchedulingService}.
 */
public abstract class AbstractSchedulingService implements SchedulingService {
  /**
   * The number of milliseconds to wait for the task handler to be ready.
   */
  private static final long READY_TIMEOUT = 2000L;

  /**
   * Constant for nonosecond/millisecond conversion.
   */
  private static final long ONE_MILLION = 1000000L;

  /**
   * The default concurrency.  The default is to serialize task handling in
   * a single thread.
   */
  public static final int DEFAULT_CONCURRENCY = 1;

  /**
   * The default number of milliseconds for the {@link #POSTPONED_TIMEOUT_KEY}
   * initialization parameter.
   */
  public static final long DEFAULT_POSTPONED_TIMEOUT = 50L;

  /**
   * The default number of milliseconds for the {@link #STANDARD_TIMEOUT_KEY}
   * initialization parameter if not otherwise specified.
   */
  public static final long DEFAULT_STANDARD_TIMEOUT = 1500L;

  /**
   * The default number of milliseconds for the {@link #FOLLOW_UP_DELAY_KEY}
   * initialization parameter if not otherwise specified.
   */
  public static final long DEFAULT_FOLLOW_UP_DELAY = 10000L;

  /**
   * The default maximum number of milliseconds for the {@link
   * #FOLLOW_UP_TIMEOUT_KEY} initialization parameter if not otherwise
   * specified.
   */
  public static final long DEFAULT_FOLLOW_UP_TIMEOUT = 60000L;

  /**
   * The default number of follow-up tasks to fetch from persistent storage at
   * a time.
   */
  public static final int DEFAULT_FOLLOW_UP_FETCH = 100;

  /**
   * The config property key for configuring the concurrency.
   */
  public static final String CONCURRENCY_KEY = "concurrency";

  /**
   * The initialization parameter to specify the number of milliseconds to
   * sleep between checks on the locks required for tasks that have been
   * postponed due to contention.  If not configured then the value is set to
   * {@link #DEFAULT_POSTPONED_TIMEOUT}.  If the value is specified it should
   * be non-negative.
   */
  public static final String POSTPONED_TIMEOUT_KEY = "postponedTimeout";

  /**
   * The initialization parameter to specify the number of milliseconds to
   * sleep between checking to see if task handling should cease.  This
   * timeout is used when there are no postponed tasks due to contention.
   * If not configured then the value is set to {@link
   * #DEFAULT_STANDARD_TIMEOUT}.  If the value is specified it should be
   * non-negative.
   */
  public static final String STANDARD_TIMEOUT_KEY = "standardTimeout";

  /**
   * The initialization parameter to specify the number of milliseconds to
   * delay before attempting to execute a follow-up task.  This delay is
   * used to give the opportunity to receive duplicate follow-up tasks that
   * can be collapsed.  Whenever a duplicate is collapsed, the delay timer
   * starts over unless the {@linkplain #FOLLOW_UP_TIMEOUT_KEY maximum
   * follow-up deferral time} has been reached.  If not configured then
   * the value is set to {@link #DEFAULT_FOLLOW_UP_DELAY}.  If the value is
   * specified it should be non-negative.
   */
  public static final String FOLLOW_UP_DELAY_KEY = "followUpDelay";

  /**
   * The initialization parameter to specify the maximum number of milliseconds
   * to defer a follow-up task.  Once a follow-up task has been deferred this
   * number of milliseconds it will no longer be purposely delayed to wait for
   * additional duplicates to be scheduled and collapsed.  This is also the
   * amount of time used to cache a follow-up task from persistent storage
   * before considering the cached version expired and make it available again
   * from persistent storage.  If not configured then the value is set to {@link
   * #DEFAULT_FOLLOW_UP_TIMEOUT}.  If the value is specified it should be
   * non-negative and must be <b>greater than</b> the delay time specified by
   * {@link #FOLLOW_UP_DELAY_KEY}.
   */
  public static final String FOLLOW_UP_TIMEOUT_KEY = "followUpTimeout";

  /**
   * The initialization parameter to specify the maximum number of follow-up
   * tasks to retrieve from persistent storage at a time to refill the
   * in-memory cache.  The retrieved tasks should not be returned from
   * persistent storage again until after the {@linkplain #FOLLOW_UP_TIMEOUT_KEY
   * follow-up timeout} has elapsed and after it has elapsed, the in-memory
   * cache should be considered expired.  If not configured then the value is
   * to {@link #DEFAULT_FOLLOW_UP_FETCH}.  If the value is specified it should
   * be a positive number.
   */
  public static final String FOLLOW_UP_FETCH_KEY = "followUpFetch";

  /**
   * The initialization parameter used by the default implementation of
   * {@link #initLockingService(JsonObject)} to specify the Java class name
   * of the {@link LockingService} to use.  If the default implementation of
   * {@link #initLockingService(JsonObject)} is overridden, then this key may
   * have no effect in the derived implementation.
   */
  public static final String LOCKING_SERVICE_CLASS_KEY = "lockingService";

  /**
   * The default value for the {@link #LOCKING_SERVICE_CLASS_KEY} if the value
   * is not specified.  This is the class name for {@link
   * ProcessScopeLockingService}.
   */
  public static final String DEFAULT_LOCKING_SERVICE_CLASS_NAME
      = ProcessScopeLockingService.class.getName();

  /**
   * The initialization parameter referencing a JSON object or {@link String}
   * that represents the configuration for the {@link LockingService} instance
   * created by the default implementation of {@link
   * #initLockingService(JsonObject)} using the {@link
   * #LOCKING_SERVICE_CLASS_KEY} init parameter.  If the default implementation
   * of {@link #initLockingService(JsonObject)} is overridden, then this key may
   * have no effect in the derived implementation.
   */
  public static final String LOCKING_SERVICE_CONFIG_KEY
      = "lockingServiceConfig";

  /**
   * Millisecond units constant for {@link Stat} instances.
   */
  private static final String MILLISECOND_UNITS = "ms";

  /**
   * Thread units constant for {@link Stat} instances.
   */
  private static final String THREAD_UNITS = "threads";

  /**
   * Task units constant for {@link Stat} instances.
   */
  private static final String TASK_UNITS = "tasks";

  /**
   * Task group units constant for {@link Stat} instances.
   */
  private static final String TASK_GROUP_UNITS = "task groups";

  /**
   * Call units constant for {@link Stat} instances.
   */
  private static final String CALL_UNITS = "calls";

  /**
   * Tasks per call units constant for {@link Stat} instances.
   */
  private static final String TASKS_PER_CALL_UNITS = "tasks per call";

  /**
   * Enumerates the various task types.
   */
  private enum TaskType {
    /**
     * Pending tasks that have never been handled.
     */
    PENDING,

    /**
     * Postponed tasks that were previously attempted but postponed.
     */
    POSTPONED,

    /**
     * Follow-up tasks that were scheduled in response to previous tasks.
     */
    FOLLOW_UP;
  }

  /**
   * The various keys used for timing operations.
   */
  public enum Stat implements Statistic {
    /**
     * The number of worker threads used to asynchronously handle the tasks.
     */
    concurrency(THREAD_UNITS),

    /**
     * The timeout to use when waiting for new tasks to show up when there
     * are no postponed tasks. When there are postponed tasks then
     * {@link #postponedTimeout} is used.
     */
    standardTimeout(MILLISECOND_UNITS),

    /**
     * The number of milliseconds to sleep between checks on the locks required
     * for tasks that have been postponed due to contention.
     */
    postponedTimeout(MILLISECOND_UNITS),

    /**
     * The number of milliseconds to delay a follow-up task initially (to allow
     * duplicates to be collapsed with it) and after each time a duplicate is
     * found.  The total deferral of the follow-up task is governed by the
     * {@link #followUpTimeout} value.
     */
    followUpDelay(MILLISECOND_UNITS),

    /**
     * The maximum number of milliseconds to defer a follow-up task while
     * waiting for duplicate tasks to be collapsed with it.
     */
    followUpTimeout(MILLISECOND_UNITS),

    /**
     * The average number of milliseconds from when a non-follow-up task is
     * scheduled until it has been handled.
     */
    averageTaskTime(MILLISECOND_UNITS),

    /**
     * The average number of milliseconds from when a task group has its first
     * task scheduled until all of its tasks have been handled.
     */
    averageTaskGroupTime(MILLISECOND_UNITS),

    /**
     * The longest amount of time (in milliseconds) for when a non-followup
     * task was scheduled until it was completely processed.
     */
    longestTaskTime(MILLISECOND_UNITS),

    /**
     * The longest amount of time (in milliseconds) for when a non-followup
     * task was scheduled until it was completely processed.
     */
    longestTaskGroupTime(MILLISECOND_UNITS),

    /**
     * The number of non-follow-up tasks that have made the round trip from
     * being scheduled to the point where they are completely handled.
     */
    taskCompleteCount(TASK_UNITS),

    /**
     * The number of non-follow-up tasks that have been completed successfully.
     */
    taskSuccessCount(TASK_UNITS),

    /**
     * The number of non-follow-up tasks that have been completed with a
     * failure.
     */
    taskFailureCount(TASK_UNITS),

    /**
     * The number of non-follow-up tasks that have been aborted.
     */
    taskAbortCount(TASK_UNITS),

    /**
     * The number of follow-up tasks that have made the round trip from
     * being scheduled to the point where they are completely handled.
     */
    followUpCompleteCount(TASK_UNITS),

    /**
     * The number of follow-up tasks that have been completed successfully.
     */
    followUpSuccessCount(TASK_UNITS),

    /**
     * The number of follow-up tasks that have been completed with a failure.
     */
    followUpFailureCount(TASK_UNITS),

    /**
     * The average number of milliseconds for a task to be handled by the
     * {@link TaskHandler} via {@link
     * TaskHandler#handleTask(String,Map,int,Scheduler)}.
     */
    averageHandleTask(MILLISECOND_UNITS),

    /**
     * The number of times the {@link
     * TaskHandler#handleTask(String,Map,int,Scheduler)} method has been called
     * to handle a task (follow-up or not).
     */
    handleTaskCount(CALL_UNITS),

    /**
     * The number of times that the {@link
     * TaskHandler#handleTask(String,Map,int,Scheduler)} has been called
     * successfully (i.e.: without any exceptions) to handle a task (follow-up
     * or not).
     */
    handleTaskSuccessCount(CALL_UNITS),

    /**
     * The number of times that the {@link
     * TaskHandler#handleTask(String,Map,int,Scheduler)} has been called
     * unsuccessfully (i.e.: with an exceptions being thrown) to handle a task
     * (follow-up or not).
     */
    handleTaskFailureCount(CALL_UNITS),

    /**
     * Gets the ratio of the number of times {@link
     * TaskHandler#handleTask(String,Map,int,Scheduler)} has been called  for
     * follow-up tasks to number of times it has been called for <b>all</b>
     * tasks that have been handled.
     */
    followUpHandleTaskRatio(null),

    /**
     * The number of non-followup tasks that have made the round trip from being
     * scheduled to the point where they are completely handled.  Some
     * messages may make the round trip more than once if a failure occurs in
     * processing part or all of the message.
     */
    taskGroupCompleteCount(TASK_GROUP_UNITS),

    /**
     * The number of task groups that had all of their tasks handled
     * successfully without any exceptions.
     */
    taskGroupSuccessCount(TASK_GROUP_UNITS),

    /**
     * The number of task groups that have completed but have had at least one
     * failure with one of the associated tasks.
     */
    taskGroupFailureCount(TASK_GROUP_UNITS),

    /**
     * The average compression ratio of duplicate non-follow-up tasks.  This is
     * the number of total non-follow-up tasks handled divided by the number of
     * times {@link TaskHandler#handleTask(String, Map, int, Scheduler)} was called
     * to handle those tasks.
     */
    averageCompression(TASKS_PER_CALL_UNITS),

    /**
     * The greatest compression ratio achieved by a single non-follow-up task.
     * This the greatest number of duplicate non-follow-up tasks that were
     * collapsed into a single task handling call to {@link
     * TaskHandler#handleTask(String, Map, int, Scheduler)}.
     */
    greatestCompression(TASKS_PER_CALL_UNITS),

    /**
     * The average compression ratio of duplicate follow-up tasks.  This is
     * the number of total follow-up tasks handled divided by the number of
     * times {@link TaskHandler#handleTask(String, Map, int, Scheduler)} was called
     * to handle those tasks.
     */
    averageFollowUpCompression(TASKS_PER_CALL_UNITS),

    /**
     * The greatest compression ratio achieved by a single follow-up task.
     * This the greatest number of duplicate follow-up tasks that were
     * collapsed into a single task handling call to {@link
     * TaskHandler#handleTask(String, Map, int, Scheduler)}.
     */
    greatestFollowUpCompression(TASKS_PER_CALL_UNITS),

    /**
     * The average number of tasks in each task group.  This only considers
     * non-follow-up tasks.
     */
    averageTaskGroupSize(TASK_UNITS),

    /**
     * The greatest number of tasks encountered in a completed task group.
     * This only considers non-follow-up tasks.
     */
    greatestTaskGroupSize(TASK_UNITS),

    /**
     * The ratio of cumulative {@link TaskHandler} handling time across
     * all threads to actual active handling time.
     */
    parallelism(null),

    /**
     * The ratio of the number of times the {@link #dequeueTask()} function is
     * called and a task is ready to be returned without waiting.
     */
    dequeueHitRatio(null),

    /**
     * The greatest number of tasks to be postponed at any given time
     * due to contention on the resources being acted upon.
     */
    greatestPostponedCount(TASK_UNITS),

    /**
     * The cumulative time spent (in milliseconds) in the {@link
     * #handleTasks()} function.
     */
    taskHandling(MILLISECOND_UNITS),

    /**
     * The cumulative time spent (in milliseconds) actively handling tasks.
     * This excludes time waiting for messages to arrive.
     */
    activelyHandling(MILLISECOND_UNITS),

    /**
     * The cumulative time spent (in milliseconds) waiting for tasks to be
     * scheduled.
     */
    waitingForTasks(MILLISECOND_UNITS),

    /**
     * The cumulative time spent (in milliseconds) not handling tasks
     * while waiting for locks to be released for postponed tasks.
     */
    waitingOnPostponed(MILLISECOND_UNITS),

    /**
     * The time spent (in milliseconds) between handing a task off to a
     * worker for processing and obtaining the next task to be processed.
     */
    betweenTasks(MILLISECOND_UNITS),

    /**
     * The time spent (in milliseconds) calling {@link #dequeueTask()} function
     * to dequeue a task from the internal queue.  This includes time waiting
     * for the first task to arrive or the next task to arrive after the
     * previous task has been handled.
     */
    dequeue(MILLISECOND_UNITS),

    /**
     * The time spent (in milliseconds) waiting to obtain the synchronized lock
     * on the scheduling service in order to call the {@link #dequeueTask()}
     * function.
     */
    dequeueBlocking(MILLISECOND_UNITS),

    /**
     * The time spent (in milliseconds) in the "wait loop" of
     * {@link #dequeueTask()} waiting for a task to become available for
     * processing.
     */
    dequeueTaskWaitLoop(MILLISECOND_UNITS),

    /**
     * The time spent (in milliseconds) in the synchronization wait of
     * {@link #dequeueTask()} waiting for a task to become available for
     * processing.  This should be the majority of the time spent in
     * {@link #dequeueTaskWaitLoop}, but isolates the non-busy sleeping
     * time awaiting notification of task arrival.
     */
    dequeueTaskWait(MILLISECOND_UNITS),

    /**
     * The time spent (in milliseconds) checking to see if a task on the
     * pending queue is locked and should be postponed for later processing.
     */
    dequeueCheckLocked(MILLISECOND_UNITS),

    /**
     * The number of milliseconds spent calling {@link
     * #init(JsonObject, TaskHandler)}.
     */
    initialize(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent checking pending tasks
     * to see if one is ready to be processed.
     */
    checkPending(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent checking follow-up tasks
     * to see if they are now ready to be processed.
     */
    checkFollowUp(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent checking postponed tasks
     * to see if they are now ready to be processed.
     */
    checkPostponed(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent obtaining locks on the
     * affected resources.
     */
    obtainLocks(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent waiting for an available
     * worker thread to handle a task that has been pulled from the pending
     * queue.
     */
    waitForWorker(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent calling {@link
     * TaskHandler#handleTask(String,Map,int,Scheduler)}.
     */
    handleTask(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent calling {@link
     * ScheduledTask#succeeded()} or {@link ScheduledTask#failed(Exception)}.
     */
    markComplete(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent calling {@link
     * #completeFollowUpTask(ScheduledTask)}.
     */
    completeFollowUp(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent releasing locks on affected
     * resources.
     */
    releaseLocks(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent calling {@link
     * #postProcess(ScheduledTask)}.
     */
    postProcess(MILLISECOND_UNITS),

    /**
     * The cumulative number of milliseconds spent calling {@link
     * #destroy()}.
     */
    destroy("ms");

    /**
     * Constructs the statistic instance with the associated units.
     * @param units The units that the statistic is measured in.
     */
    Stat(String units) {
      this.units = units;
    }

    /**
     * The units for the statistic.
     */
    private String units;

    /**
     * Gets the unit of measure for this statistic.  This is the unit that
     * the {@link Number} value is measured in when calling {@link
     * AbstractSchedulingService#getStatistics()}}
     *
     * @return The unit of measure for this statistic.
     */
    public String getUnits() {
      return this.units;
    }
  }

  /**
   * The {@link TaskHandler} for handling the tasks.
   */
  private TaskHandler taskHandler = null;

  /**
   * The {@link LockingService} to use.
   */
  private LockingService lockingService = null;

  /**
   * The {@link State} for this instance.
   */
  private State state = UNINITIALIZED;

  /**
   * The {@link List} of {@link TaskType} instances specifying the {@link
   * TaskType} order.
   */
  private List<TaskType> taskTypeOrder = null;

  /**
   * The current index into the {@link #taskTypeOrder} list.
   */
  private int taskTypeIndex = 0;

  /**
   * Flag indicating that {@link #handleTasks()} has been called and is
   * currently running to prevent more than one call for this object
   * process-wide.
   */
  private boolean handlingTasks = false;

  /**
   * The concurrency for this instance.  This is the maximum number of threads
   * used to handle tasks.
   */
  private int concurrency = DEFAULT_CONCURRENCY;

  /**
   * The {@link AsyncWorkerPool} returning {@link TaskResult} instances.
   */
  private AsyncWorkerPool<TaskResult> workerPool = null;

  /**
   * The number of milliseconds to sleep between checks on the locks required
   * for tasks that have been postponed due to contention.  This timeout is
   * used when there are pending tasks that have been postponed due to
   * contention.
   */
  private long postponedTimeout = DEFAULT_POSTPONED_TIMEOUT;

  /**
   * The number of milliseconds to sleep between checking to see if task
   * handling should cease.  This timeout is used when there are no postponed
   * tasks due to contention.
   */
  private long standardTimeout = DEFAULT_STANDARD_TIMEOUT;

  /**
   * The number of milliseconds to delay before attempting to execute a
   * follow-up task.  This delay is used to give the opportunity to receive
   * duplicate follow-up tasks that can be collapsed.  Whenever a duplicate is
   * collapsed, the delay timer starts over unless the {@link
   * #followUpTimeout} has been reached.
   */
  private long followUpDelay = DEFAULT_FOLLOW_UP_DELAY;

  /**
   * The number of milliseconds to defer a follow-up task.  Once a follow-up
   * task has been deferred this number of milliseconds it will no longer be
   * delayed to wait for additional duplicates to be scheduled and collapsed.
   */
  private long followUpTimeout = DEFAULT_FOLLOW_UP_TIMEOUT;

  /**
   * The number of follow-up tasks to retrieve from persistent storage on a
   * single retrieval.  These retrieved follow-up tasks should be handled
   * within the configured {@link #followUpTimeout}.
   */
  private int followUpFetch = DEFAULT_FOLLOW_UP_FETCH;

  /**
   * The {@link List} of pending tasks.
   */
  private List<ScheduledTask> pendingTasks;

  /**
   * The {@link List} of delayed/postponed tasks.
   */
  private List<ScheduledTask> postponedTasks;

  /**
   * The {@link Map} of {@link String} signature keys to {@link ScheduledTask}
   * instances.
   */
  private Map<String, ScheduledTask> taskCollapseLookup;

  /**
   * The {@link List} of follow-up tasks.
   */
  private List<ScheduledTask> followUpTasks;

  /**
   * The {@link List} of follow-up tasks that are currently being worked on.
   */
  private IdentityHashMap<ScheduledTask, Long> inProgressFollowUpTasks;

  /**
   * This is the scheduling thread that handles managing and dispatching tasks.
   */
  private Thread taskHandlingThread = null;

  /**
   * The nanosecond timestamp when the postponed tasks were last checked to
   * see if one was ready.
   */
  private long postponedNanoTime = -2 * (DEFAULT_POSTPONED_TIMEOUT * 1000000L);

  /**
   * The nanosecond timestamp when the follow-up tasks were last checked to
   * see if one was ready.
   */
  private long followUpNanoTime = -2 * (DEFAULT_FOLLOW_UP_DELAY * 1000000L);

  /**
   * The nano-second time at which to renew the lease on any dequeued follow-up
   * tasks.
   */
  private long followUpRenewNanos = 0L;

  /**
   * The total of the number of round-trip milliseconds for all task groups.
   */
  private long totalTaskGroupTime = 0L;

  /**
   * The longest time it has taken a task to round-trip from scheduling to
   * completion.
   */
  private long longestTaskGroupTime = -1L;

  /**
   * The total number of round-trip milliseconds for all tasks.
   */
  private long totalTaskTime = 0L;

  /**
   * The longest number of milliseconds to round-trip from scheduling to
   * completion of any given task.
   */
  private long longestTaskTime = -1L;

  /**
   * The total number of milliseconds spent in the handling of all tasks.
   * Keep in mind that this accounts for collapsed tasks so that the handling
   * of collapsed tasks is only counted once.
   */
  private long totalHandlingTime = 0L;

  /**
   * The longest number of milliseconds spent handling a task.  Keep in mind
   * that this handling time may have completed multiple collapsed tasks.
   */
  private long longestHandlingTime = -1L;

  /**
   * The number of task groups that have been completed, successful or not.
   */
  private long taskGroupCount = 0L;

  /**
   * The number of tasks that have been handled whether successful or failed.
   * This excluded aborted tasks.
   */
  private long taskCompleteCount = 0L;

  /**
   * The number of times the {@link MessageProcessor#process(JsonObject)}
   * method has been successfully called.
   */
  private long taskSuccessCount = 0L;

  /**
   * The number of times the {@link MessageProcessor#process(JsonObject)}
   * method has been called and thrown an exception.
   */
  private long taskFailureCount = 0L;

  /**
   * The number of times the {@link MessageProcessor#process(JsonObject)}
   * method has been successfully called.
   */
  private long taskAbortCount = 0L;

  /**
   * The number of follow-up tasks that have been handled whether successful or
   * failed.  This excluded aborted tasks.
   */
  private long followUpCompleteCount = 0L;

  /**
   * The number of times the {@link MessageProcessor#process(JsonObject)}
   * method has been successfully called.
   */
  private long followUpSuccessCount = 0L;

  /**
   * The number of times the {@link MessageProcessor#process(JsonObject)}
   * method has been called and thrown an exception.
   */
  private long followUpFailureCount = 0L;

  /**
   * The number of task groups that have successfully completed.
   */
  private long groupSuccessCount = 0L;

  /**
   * The greatest number of tasks encountered for a task group.
   */
  private int greatestGroupSize = 0;

  /**
   * The number of task groups that have completed with failures.
   */
  private long groupFailureCount = 0L;

  /**
   * The number of {@link ScheduledTask} instances handled.  Each {@link
   * ScheduledTask} may be backed by multiple duplicate actual {@link Task}
   * instances.
   */
  private long handleCount = 0L;

  /**
   * The number of non-follow-up {@link ScheduledTask} instances handled.  Each
   * {@link ScheduledTask} may be backed by multiple duplicate actual {@link
   * Task} instances.
   */
  private long standardHandleCount = 0L;

  /**
   * The number of follow-up {@link ScheduledTask} instances handled.  Each
   * {@link ScheduledTask} may be backed by multiple duplicate actual {@link
   * Task} instances.
   */
  private long followUpHandleCount = 0L;

  /**
   * The number of {@link ScheduledTask} instances handled successfully.  Each
   * {@link ScheduledTask} may be backed by multiple duplicate actual {@link
   * Task} instances.
   */
  private long handleSuccessCount = 0L;

  /**
   * The number of {@link ScheduledTask} instances handled unsuccessfully.  Each
   * {@link ScheduledTask} may be backed by multiple duplicate actual {@link
   * Task} instances.
   */
  private long handleFailureCount = 0L;

  /**
   * The greatest task multiplicity encountered for non-follow-up tasks.
   */
  private int greatestMultiplicity = 0;

  /**
   * The greatest task multiplicity encountered for follow-up tasks.
   */
  private int greatestFollowUpMultiplicity = 0;

  /**
   * The total number of times an attempt was made to dequeue a message and
   * one was ready.
   */
  private long dequeueHitCount = 0L;

  /**
   * The total number of times an attempt was made to dequeue a message and one
   * was not ready to be dequeued.
   */
  private long dequeueMissCount = 0L;

  /**
   * The greatest number of info messages that are postponed at any one time.
   */
  private int greatestPostponedCount = 0;

  /**
   * The greatest number of info messages that are postponed at any one time.
   */
  private int greatestFollowUpCount = 0;

  /**
   * The object used for synchronization when working with statistics.
   */
  private final Object statsMonitor = new Object();

  /**
   * The processing {@link Timers}.
   */
  private final Timers timers = new Timers();

  /**
   * Flag to use to suppress checking if already handling tasks when
   * backgrounding task handling.
   */
  private static final ThreadLocal<Boolean> SUPPRESS_HANDLING_CHECK
      = new ThreadLocal<>();

  /**
   * Default constructor.
   */
  protected AbstractSchedulingService() {
    this.taskHandler    = null;
    this.lockingService = null;
    this.state          = UNINITIALIZED;
    this.taskTypeIndex  = 0;
    this.taskTypeOrder  = Arrays.asList(TaskType.values());
  }

  /**
   * Gets the {@link State} of this instance.
   *
   * @return The {@link State} of this instance.
   */
  public synchronized State getState() {
    return this.state;
  }

  /**
   * Provides a means to set the {@link State} for this instance as a
   * synchronized method that will notify all upon changing the state.
   *
   * @param state The {@link State} for this instance.
   */
  protected synchronized void setState(State state) {
    Objects.requireNonNull(state,"State cannot be null");
    this.state = state;
    this.notifyAll();
  }

  /**
   * Checks if this instance is current handling tasks.  This is used to
   * synchronize destruction.  The {@link #doDestroy()} method is not called
   * until task handling ceases.
   *
   * @return <code>true</code> if this instance is still handling tasks,
   *         otherwise <code>false</code>.
   *
   */
  protected synchronized boolean isHandlingTasks() {
    return this.handlingTasks;
  }

  /**
   * The {@link Object} to synchronize on when computing and recording
   * statistics in a thread-safe manner.
   *
   * @return The {@link Object} to synchronize on when computing and recording
   *         statistics in a thread-safe manner.
   */
  protected final Object getStatisticsMonitor() {
    return this.statsMonitor;
  }

  /**
   * Gets the concurrency of the scheduler -- this is the number of threads it
   * will use to handle tasks.  The returned value will be a positive number
   * greater than or equal to one (1).
   *
   * @return The concurrency of the scheduler (i.e.: the number of threads it
   *         will use to handle tasks).
   */
  public int getConcurrency() {
    return this.concurrency;
  }

  /**
   * Gets the default concurrency with which to initialize if one is not specified
   * in the initialization configuration via the {@link #CONCURRENCY_KEY}
   * initialization parameter.  By default, this returns {@link
   * #DEFAULT_CONCURRENCY}, but it may be overridden to return something more
   * sensible for a derived implementation.
   *
   * @return The default concurrency with which to initialize.
   *
   * @see #getConcurrency()
   * @see #CONCURRENCY_KEY
   * @see #DEFAULT_CONCURRENCY
   */
  public int getDefaultConcurrency() {
    return DEFAULT_CONCURRENCY;
  }

  /**
   * Gets the number of milliseconds to sleep between checks on the locks
   * required for tasks that have been postponed due to contention.  This
   * timeout is used when there are pending tasks that have been postponed
   * due to contention.
   *
   * @return The number of milliseconds to sleep between checks on the locks
   *         required for tasks that have been postponed due to contention.
   */
  public long getPostponedTimeout() {
    return this.postponedTimeout;
  }

  /**
   * Gets the default postponed timeout with which to initialize if one is not
   * specified in the initialization configuration via the {@link
   * #POSTPONED_TIMEOUT_KEY} initialization parameter.  By default, this returns
   * {@link #DEFAULT_POSTPONED_TIMEOUT}, but it may be overridden to return
   * something more sensible for a derived implementation.
   *
   * @return The default postponed timeout with which to initialize.
   *
   * @see #getPostponedTimeout()
   * @see #POSTPONED_TIMEOUT_KEY
   * @see #DEFAULT_POSTPONED_TIMEOUT
   */
  public long getDefaultPostponedTimeout() {
    return DEFAULT_POSTPONED_TIMEOUT;
  }

  /**
   * Gets the number of milliseconds to delay before attempting to execute a
   * follow-up task.  This delay is used to give the opportunity to receive
   * duplicate follow-up tasks that can be collapsed.  Whenever a duplicate is
   * collapsed, the delay timer starts over unless the {@linkplain
   * #getFollowUpTimeout() maximum follow-up deferral time} has been reached.
   *
   * @return The number of milliseconds to delay before attempting to execute a
   *         follow-up task.
   */
  public long getFollowUpDelay() {
    return this.followUpDelay;
  }

  /**
   * Gets the default follow-up delay with which to initialize if one is not
   * specified in the initialization configuration via the {@link
   * #FOLLOW_UP_DELAY_KEY} initialization parameter.  By default, this returns
   * {@link #DEFAULT_FOLLOW_UP_DELAY}, but it may be overridden to return
   * something more sensible for a derived implementation.
   *
   * @return The default follow-up delay with which to initialize.
   *
   * @see #getFollowUpDelay()
   * @see #FOLLOW_UP_DELAY_KEY
   * @see #DEFAULT_FOLLOW_UP_DELAY
   */
  public long getDefaultFollowUpDelay() {
    return DEFAULT_FOLLOW_UP_DELAY;
  }

  /**
   * The maximum number of milliseconds to defer a follow-up task.  Once a
   * follow-up task has been deferred this number of milliseconds it will no
   * longer be purposely delayed to wait for additional duplicates to be
   * scheduled and collapsed.  It may be delayed because of a lack of resources
   * to handle it.
   *
   * @return The maximum number of milliseconds to defer a follow-up task.
   */
  public long getFollowUpTimeout() {
    return this.followUpTimeout;
  }

  /**
   * Gets the default follow-up timeout with which to initialize if one is not
   * specified in the initialization configuration via the {@link
   * #FOLLOW_UP_TIMEOUT_KEY} initialization parameter.  By default, this returns
   * {@link #DEFAULT_FOLLOW_UP_TIMEOUT}, but it may be overridden to return
   * something more sensible for a derived implementation.
   *
   * @return The default follow-up timeout with which to initialize.
   *
   * @see #getFollowUpTimeout()
   * @see #FOLLOW_UP_TIMEOUT_KEY
   * @see #DEFAULT_FOLLOW_UP_TIMEOUT
   */
  public long getDefaultFollowUpTimeout() {
    return DEFAULT_FOLLOW_UP_TIMEOUT;
  }

  /**
   * Gets the number of milliseconds to lease follow-up messages for handling
   * before they become available to be obtained again.  The default
   * implementation returns twice the {@linkplain #getFollowUpTimeout()
   * follow-up timeout}.
   *
   * @return The number of milliseconds to lease follow-up messages for handling
   *         before they become available to be obtained again.
   */
  public long getFollowUpLeaseTime() {
    return this.getFollowUpTimeout() * 2;
  }

  /**
   * The configured maximum number of follow-up tasks to retrieve from
   * persistent search on a single retrieval.  The retrieved follow-up tasks
   * should be handled within the {@linkplain #getFollowUpTimeout() follow-up
   * timeout} and so this number should not be so large that the tasks are not
   * handled or their retrieval is renewed within the allotted time.
   *
   * @return The configured maximum number of follow-up tasks to retrieve from
   *         persistent storage on a single retrieval.
   */
  public int getFollowUpFetchCount() {
    return this.followUpFetch;
  }

  /**
   * Gets the default follow-up fetch count with which to initialize if one is
   * not specified in the initialization configuration via the {@link
   * #FOLLOW_UP_FETCH_KEY} initialization parameter.  By default, this returns
   * {@link #DEFAULT_FOLLOW_UP_FETCH}, but it may be overridden to return
   * something more sensible for a derived implementation.
   *
   * @return The default follow-up fetch count with which to initialize.
   *
   * @see #getFollowUpFetchCount()
   * @see #FOLLOW_UP_FETCH_KEY
   * @see #DEFAULT_FOLLOW_UP_FETCH
   */
  public int getDefaultFollowUpFetchCount() {
    return DEFAULT_FOLLOW_UP_FETCH;
  }

  /**
   * Gets the number of milliseconds to sleep between checking to see if task
   * handling should cease.  This timeout is used when there are no postponed
   * tasks due to contention.
   *
   * @return The number of milliseconds to sleep between checking to see if
   *         task handling should cease.  This timeout is used when there
   *         are no postponed tasks due to contention.
   */
  public long getStandardTimeout() {
    return this.standardTimeout;
  }

  /**
   * Gets the default standard timeout with which to initialize if one is not
   * specified in the initialization configuration via the {@link
   * #STANDARD_TIMEOUT_KEY} initialization parameter.  By default, this returns
   * {@link #DEFAULT_STANDARD_TIMEOUT}, but it may be overridden to return
   * something more sensible for a derived implementation.
   *
   * @return The default standard timeout with which to initialize.
   *
   * @see #getStandardTimeout()
   * @see #STANDARD_TIMEOUT_KEY
   * @see #DEFAULT_STANDARD_TIMEOUT
   */
  public long getDefaultStandardTimeout() {
    return DEFAULT_STANDARD_TIMEOUT;
  }

  /**
   * Gets the {@link TaskHandler} for this instance.
   *
   * @return The {@link TaskHandler} for this instance.
   */
  @Override
  public TaskHandler getTaskHandler() {
    return this.taskHandler;
  }

  /**
   * Sets the {@link TaskHandler} for this instance.
   *
   * @param taskHandler The {@link TaskHandler} for this instance.
   */
  protected void setTaskHandler(TaskHandler taskHandler) {
    this.taskHandler = taskHandler;
  }

  /**
   * Gets the {@link LockingService} for this instance.
   *
   * @return The {@link LockingService} for this instance.
   */
  @Override
  public LockingService getLockingService() {
    return this.lockingService;
  }

  /**
   * Sets the {@link LockingService} for this instance.
   *
   * @param lockingService The {@link LockingService} for this instance.
   */
  protected void setLockingService(LockingService lockingService) {
    this.lockingService = lockingService;
  }

  /**
   * Gets the default {@link LockingService} class name with which to
   * initialize the backing {@link LockingService} if one is not specified in
   * the initialization configuration via the {@link #LOCKING_SERVICE_CLASS_KEY}
   * initialization parameter.  By default, this returns the {@link
   * #DEFAULT_LOCKING_SERVICE_CLASS_NAME}, but it may be overridden to return
   * something more sensible for a derived implementation.
   *
   * @return The default {@link LockingService} class name with which to
   *         initialize.
   *
   * @see #initLockingService(JsonObject)
   * @see #getDefaultLockingServiceConfig()
   * @see #LOCKING_SERVICE_CLASS_KEY
   * @see #LOCKING_SERVICE_CONFIG_KEY
   * @see #DEFAULT_LOCKING_SERVICE_CLASS_NAME
   */
  public String getDefaultLockingServiceClassName() {
    return DEFAULT_LOCKING_SERVICE_CLASS_NAME;
  }

  /**
   * Gets the default {@link JsonObject} configuration with which to initialize
   * the backing {@link LockingService} if one is not specified in the
   * initialization configuration via the {@link #LOCKING_SERVICE_CONFIG_KEY}
   * initialization parameter.  By default, this returns the <code>null</code>,
   * but it may be overridden to return something more sensible for a derived
   * implementation.
   *
   * @return The default {@link JsonObject} configuration with which to
   *         initialize the backing {@link LockingService}.
   *
   * @see #initLockingService(JsonObject)
   * @see #getDefaultLockingServiceClassName()
   * @see #LOCKING_SERVICE_CLASS_KEY
   * @see #LOCKING_SERVICE_CONFIG_KEY
   * @see #DEFAULT_LOCKING_SERVICE_CLASS_NAME
   */
  public JsonObject getDefaultLockingServiceConfig() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Scheduler createScheduler(boolean followUp) {
    if (followUp) {
      return new DefaultScheduler(this);
    } else {
      TaskGroup taskGroup = new TaskGroup();
      return new DefaultScheduler(this, taskGroup);
    }
  }

  /**
   * Creates a {@link Scheduler} for creating follow-up tasks to the specified
   * {@link ScheduledTask} unless follow-up tasks are not supported for the
   * specified {@link ScheduledTask}.  The default implementation will
   * always create a {@link DefaultScheduler} that will <b>not</b> have an
   * associated {@link TaskGroup}.
   *
   * @param task The {@link ScheduledTask} for which to create the follow-up
   *             scheduler.
   * @return The follow-up {@link Scheduler} or <code>null</code> if follow-up
   *         tasks are not allowed for the specified {@link ScheduledTask}.
   */
  protected Scheduler createFollowUpScheduler(ScheduledTask task) {
    // create a follow-up scheduler
    return new DefaultScheduler(this);
  }

  /**
   * Schedules the tasks in the specified {@link List}.
   *
   * @param tasks The {@link List} of {@link Task} instances.
   *
   * @throws ServiceExecutionException If a failure occurs in scheduling the
   *                                   tasks.  If a failure occurs then it
   *                                   should be assumed that the tasks will
   *                                   not be handled and the associated
   *                                   message should be retried later.
   */
  protected void scheduleTasks(List<Task> tasks)
    throws ServiceExecutionException
  {
    synchronized (this) {
      State state = this.getState();
      if (state != READY && state != ACTIVE) {
        throw new IllegalStateException(
            "Cannot schedule tasks if not in the " + READY + " or " + ACTIVE
                + " state: " + state);
      }
    }

    // loop through the tasks
    for (Task task : tasks) {
      synchronized (this) {
        // get the task group
        TaskGroup taskGroup = task.getTaskGroup();

        // check if this is a follow-up task
        if (taskGroup == null) {
          logDebug("ENQUEUEING FOLLOW-UP TASK: ", task);

          // enqueue the follow-up task for later retrieval
          this.enqueueFollowUpTask(task);

          // notify all that a new follow-up task was enqueued
          this.notifyAll();
          continue;
        }

        // get the task signature
        String signature = task.getSignature();

        // check if the specified task allows collapse
        if (task.isAllowingCollapse()) {
          // check for existing tasks by the same signature
          ScheduledTask scheduledTask = this.taskCollapseLookup.get(signature);
          if (scheduledTask != null) {
            logDebug("SCHEDULING TASK: ", task,
                     "COLLAPSING WITH: ", scheduledTask);

            // simply collapse with the existing scheduled task
            scheduledTask.collapseWith(task);

          } else {
            // create a scheduled task and add to the pending queue
            scheduledTask = new ScheduledTask(task);
            logDebug("SCHEDULING TASK: ", task);
            this.pendingTasks.add(scheduledTask);
            this.taskCollapseLookup.put(signature, scheduledTask);
          }

        } else {
          // the specified task cannot be collapsed with another
          logDebug("SCHEDULING NON-COLLAPSING TASK: ", task);
          ScheduledTask scheduledTask = new ScheduledTask(task);
          this.pendingTasks.add(scheduledTask);
        }

        // for good measure notify all that a new task was scheduled
        this.notifyAll();
      }
    }
  }

  /**
   * Dequeues a previously enqueued {@link ScheduledTask}.
   *
   * @return The {@link ScheduledTask} that was dequeued.
   */
  protected synchronized ScheduledTask dequeueTask() {
    this.timerPause(dequeueBlocking);
    this.timerStart(dequeueTaskWaitLoop);

    // set the hit flag to true
    boolean hit = true;

    int prevPendingCount = -1;
    int prevPostponedCount = -1;

    // wait for a task to be available
    while (this.getState().isAvailable()
          && (this.pendingTasks.size() == 0)
          && (!this.isFollowUpReadyCheckTime())
          && (!this.isPostponedReadyCheckTime()))
    {
      // if we get here then no task was ready so we have a miss
      hit = false;

      // toggle the timers
      this.toggleActiveAndWaitingTimers(this.pendingTasks.size(),
                                        this.postponedTasks.size(),
                                        this.workerPool.isBusy());

      // determine if postponed tasks exist
      boolean postponed = (this.getPostponedTaskCount() > 0);

      // determine how long to wait
      long timeout = (postponed)
          ? Math.min(this.getPostponedTimeout(), this.getStandardTimeout())
          : this.getStandardTimeout();

      // wait for the designated duration
      this.timerStart(dequeueTaskWait);
      try {
        logDebug("SLEEPING BEFORE RETRIEVING "
                     + (postponed ? "POSTPONED" : "FOLLOW-UP")
                     + " TASK: " + timeout);
        this.wait(timeout);

      } catch (InterruptedException ignore) {
        // ignore the interruption
      } finally {
        this.timerPause(dequeueTaskWait);
      }
    }
    this.timerPause(dequeueTaskWaitLoop);

    // grab a postponed task if available
    ScheduledTask task          = null;
    TaskType      taskType      = null;
    int           taskTypeCount = this.taskTypeOrder.size();
    for (int index = 0; index < taskTypeCount && task == null; index++) {
      taskType = this.taskTypeOrder.get(this.taskTypeIndex++);
      this.taskTypeIndex = this.taskTypeIndex % taskTypeCount;
      switch (taskType) {
        case PENDING:
          this.timerStart(checkPending);
          try {
            task = this.getReadyPendingTask();
          } catch (Exception e) {
            logWarning(e, "FAILED TO OBTAIN A TASK FROM THE PENDING QUEUE");
          } finally {
            this.timerPause(checkPending);
          }
          break;

        case POSTPONED:
          this.timerStart(checkPostponed);
          try {
            task = this.getReadyPostponedTask();
          } catch (Exception e) {
            logWarning(e, "FAILED TO OBTAIN A POSTPONED TASK, "
                + "DEFERRING POSTPONED TASKS FOR NOW");
          } finally {
            this.timerPause(checkPostponed);
          }
          break;

        case FOLLOW_UP:
          this.timerStart(checkFollowUp);
          try {
            task = this.getReadyFollowUpTask();
          } catch (ServiceExecutionException e) {
            logWarning(e, "FAILED TO OBTAIN A FOLLOW-UP TASK, "
                + "DEFERRING FOLLOW-UP TASKS FOR NOW");
          } finally {
            this.timerPause(checkFollowUp);
          }
          break;

        default:
          throw new IllegalStateException(
              "Unrecognized task type: " + taskType);
      }
    }

    // if not null then return the task
    if (task != null) {
      // ensure the timers toggled correctly
      this.timerPause(waitingOnPostponed, waitingForTasks);
      this.timerStart(activelyHandling);
      this.updateDequeueHitRatio(hit);

      // update the state
      if (this.getState() == READY) {
        this.setState(ACTIVE);
      }

      // check if we need to remove from the collapse lookup
      if (!task.isFollowUp() && task.isAllowingCollapse()) {
        ScheduledTask collapse
            = this.taskCollapseLookup.remove(task.getSignature());
        if (task != collapse) {
          throw new IllegalStateException(
              "Collapse lookup table did not contain the same task as was "
              + "dequeued.  expected=[ " + task + " ], actual=[ " + collapse
              + " ]");
        }
      }

      // return the task for handling
      return task;
    }

    this.toggleActiveAndWaitingTimers(this.pendingTasks.size(),
                                      this.postponedTasks.size(),
                                      this.workerPool.isBusy());
    this.updateDequeueHitRatio(false);

    // update the state
    if ((this.getState() == ACTIVE)
        && (this.pendingTasks.size() == 0)
        && (this.postponedTasks.size() == 0)
        && (!this.workerPool.isBusy()))
    {
      // no pending or postponed tasks, no tasks being handled and we have none
      // to return the user (e.g.: follow-up tasks), go from ACTIVE to READY
      this.setState(READY);

    } else if (this.getState() == READY) {
      // we are either busy handling tasks or we have pending or postponed tasks
      // and we are in the READY state so transition to ACTIVE
      this.setState(ACTIVE);
    }

    return null;
  }

  /**
   * Returns a {@link ScheduledTask} from the pending queue that is ready for
   * handling. This method will find the least-recently-scheduled task whose set
   * of affected resources (identified by {@link ResourceKey} instances) could
   * be locked without blocking and locks those resources.  If no such pending
   * task could be found then <code>null</code> is returned.
   *
   * @return The next pending {@link ScheduledTask} that is now ready to try, or
   *         <code>null</code> if none are ready to try.
   */
  protected synchronized ScheduledTask getReadyPendingTask() {
    this.timerStart(dequeueCheckLocked);
    try {
      // if none ready then check if we can grab a pending task
      while (this.pendingTasks.size() > 0) {
        // get the candidate task
        ScheduledTask task = this.pendingTasks.remove(0);

        // check if the task is aborted
        if (this.skipIfAborted(task)) {
          continue;
        }

        // attempt to lock the task resources
        this.timerStart(obtainLocks);
        boolean locked = task.acquireLocks(this.getLockingService());
        this.timerPause(obtainLocks);

        // if the lock was obtained, return the task
        if (locked) return task;

        // if not locked then postpone the task
        this.postponedTasks.add(task);

        // check the postponed count to see if this is now the greatest
        synchronized (this.getStatisticsMonitor()) {
          int postponedCount = this.postponedTasks.size();
          if (postponedCount > this.greatestPostponedCount) {
            this.greatestPostponedCount = postponedCount;
          }
        }
         // notify all
        this.notifyAll();
      }

      // if we get here then return null
      return null;

    } finally {
      this.timerPause(dequeueCheckLocked);
    }
  }

  /**
   * Returns a previously postponed {@link ScheduledTask} that is now ready to
   * be processed.  If the last time this method was called was less than the
   * {@linkplain #getPostponedTimeout() postpone timeout} then this method returns
   * <code>null</code> so that the previously postponed tasks are not checked
   * for readiness too frequently.  Otherwise, this method will find the least
   * recently postponed {@link ScheduledTask} whose set of affected resources
   * (identified by {@link ResourceKey} instances) are not currently locked.
   * If there are no postponed {@link ScheduledTask} instance that meet the
   * readiness criteria, then <code>null</code> is returned.
   *
   * @return The next postponed {@link ScheduledTask} that is now ready to try.
   */
  protected synchronized ScheduledTask getReadyPostponedTask()
  {
    // get the elapsed time and update the timestamp
    long now                = System.nanoTime();
    long elapsedNanos       = now - this.postponedNanoTime;
    long elapsedMillis      = elapsedNanos / ONE_MILLION;

    // check the timestamp
    if (elapsedMillis < this.getPostponedTimeout()) {
      return null;
    }

    // check if there are no postponed messages
    if (this.postponedTasks.size() == 0) {
      // since we have checked all the postponed messages (none) and none are
      // ready then we need to update the timestamp
      this.postponedNanoTime = now;

      return null;
    }

    // iterate through the postponed messages
    Iterator<ScheduledTask> iter = this.postponedTasks.iterator();
    try {
      while (iter.hasNext()) {
        ScheduledTask task = iter.next();

        // handle aborted tasks
        if (this.skipIfAborted(task)) {
          iter.remove();
          continue;
        }

        // attempt to lock the task resources
        this.timerStart(obtainLocks);
        boolean locked = task.acquireLocks(this.getLockingService());
        this.timerPause(obtainLocks);

        if (locked) {
          iter.remove();
          return task;
        }
      }

    } finally {
      // check if we checked all the messages
      if (!iter.hasNext()) {
        // since we have checked all the postponed messages for readiness we
        // can update the timestamp so we don't busy check again and again
        this.postponedNanoTime = now;
      }
    }

    // if we get here without returning a message then return null
    return null;
  }

  /**
   * Removes any aborted backing tasks from the specified {@link ScheduledTask},
   * tracks the aborted count and returns <code>true</code> if the specified
   * {@link ScheduledTask} can be fully removed from the queue and ignored
   * (i.e.: it has no more backing tasks).  If not all backing tasks are
   * aborted, then <code>false</code> is returned to indicate the task still
   * needs to be handled.
   *
   * @param task The {@link ScheduledTask} to check if fully aborted and remove
   *             aborted tasks from.
   * @return <code>true</code> if the specified {@link ScheduledTask} should be
   *         skipped because it is fully aborted, otherwise <code>false</code>.
   */
  protected boolean skipIfAborted(ScheduledTask task)
  {
    // remove any aborted tasks
    int abortCount = task.removeAborted();
    this.taskAbortCount += abortCount;

    // check if aborted
    if (task.getMultiplicity() == 0) {
      if (task.isAllowingCollapse()) {
        ScheduledTask collapse
            = this.taskCollapseLookup.get(task.getSignature());
        if (collapse == task) {
          this.taskCollapseLookup.remove(task.getSignature());
        } else {
          throw new IllegalStateException(
              "Unexpected collapsing task in lookup.  expected=[ "
                  + task + " ], found=[ " + collapse + " ]");
        }
      }
      return true;
    }

    // return false if we get here
    return false;
  }
  /**
   * Checks if a check should be performed against the readiness of the
   * postponed tasks.  This returns <code>true</code> if and only if there is
   * at least one postponed task and the readiness check has not been
   * performed within the configured postponed timeout.
   *
   * @return <code>true</code> if it is time to perform a postponed task
   *         readiness check, otherwise <code>false</code>.
   */
  protected synchronized boolean isPostponedReadyCheckTime() {
    // no need to do a ready check if no postponed messages
    if (this.postponedTasks.size() == 0) return false;

    // get the elapsed time and update the timestamp
    long now                = System.nanoTime();
    long elapsedNanos       = now - this.postponedNanoTime;
    long elapsedMillis      = elapsedNanos / ONE_MILLION;

    // check the timestamp
    return (elapsedMillis >= this.getPostponedTimeout());
  }

  /**
   * Returns a previously scheduled follow-up {@link ScheduledTask} that is now
   * ready to be processed.  If the resources that must be locked are not
   * available for the follow-up task then it is left on the queue.  If the
   * last time this method was called was less than the
   * {@linkplain #getPostponedTimeout() postpone timeout} then this method returns
   * <code>null</code> so that the previously postponed tasks are not checked
   * for readiness too frequently.  Otherwise, this method will find the least
   * recently postponed {@link ScheduledTask} whose set of affected resources
   * (identified by {@link ResourceKey} instances) are not currently locked.
   * If there are no postponed {@link ScheduledTask} instance that meet the
   * readiness criteria, then <code>null</code> is returned.
   *
   * @return The next postponed {@link ScheduledTask} that is now ready to try.
   * @throws ServiceExecutionException If a failure occurs in obtaining a
   *                                   follow-up task.
   */
  protected synchronized ScheduledTask getReadyFollowUpTask()
    throws ServiceExecutionException
  {
    // get the current timestamp
    long now = System.nanoTime();

    // check if there are no follow-up messages
    if (this.followUpTasks.size() <= 1) {
      // we have no follow-up tasks in the cache, let's get some
      List<ScheduledTask> tasks = this.dequeueFollowUpTasks(
          this.getFollowUpFetchCount());

      // add the follow-up tasks
      this.followUpTasks.addAll(tasks);
      this.followUpRenewNanos
          = now + ((this.getFollowUpLeaseTime() / 2) * ONE_MILLION);

      // check if we still have no follow-up tasks
      if (this.followUpTasks.size() == 0) {
        // since we have checked all the follow-up messages (none) and none are
        // ready then we need to update the timestamp
        this.followUpNanoTime = now;
        logDebug("RESET FOLLOW-UP CHECK TIME");

        // return null since there are no follow-up tasks
        return null;
      }
    } else if (now > this.followUpRenewNanos) {
      int size = this.followUpTasks.size()
          + this.inProgressFollowUpTasks.size();
      List<ScheduledTask> renewList = new ArrayList<>(size);
      renewList.addAll(this.inProgressFollowUpTasks.keySet());
      renewList.addAll(this.followUpTasks);

      // renew the leases on the follow-up tasks
      this.renewFollowUpTasks(renewList);
    }

    // iterate through the follow-up messages
    Iterator<ScheduledTask> iter = this.followUpTasks.iterator();
    try {
      while (iter.hasNext()) {
        // get the next follow-up task
        ScheduledTask task = iter.next();

        // attempt to lock the message resources
        this.timerStart(obtainLocks);
        boolean locked = task.acquireLocks(this.getLockingService());
        this.timerPause(obtainLocks);

        if (locked) {
          iter.remove();
          this.inProgressFollowUpTasks.put(task, System.nanoTime());
          return task;
        }
      }

    } finally {
      // check if we checked all the messages
      if (!iter.hasNext()) {
        // since we have checked all the follow-up messages for readiness we
        // can update the timestamp so we don't busy check again and again
        this.followUpNanoTime = now;
        logDebug("RESET FOLLOW-UP CHECK TIME");
      }
    }

    // if we get here without returning a message then return null
    return null;
  }

  /**
   * Checks if a check should be performed against the readiness of the
   * follow-up tasks.  This returns <code>true</code> if and only if there is
   * at least one follow-up task and the readiness check has not been
   * performed within the configured follow-up timeout.
   *
   * @return <code>true</code> if it is time to perform a postponed task
   *         readiness check, otherwise <code>false</code>.
   */
  protected synchronized boolean isFollowUpReadyCheckTime() {
    // get the elapsed time and update the timestamp
    long now                = System.nanoTime();
    long elapsedNanos       = now - this.followUpNanoTime;
    long elapsedMillis      = elapsedNanos / ONE_MILLION;

    // check the timestamp
    return (elapsedMillis >= (this.getFollowUpDelay()/2));
  }

  /**
   * Enqueues the specified follow-up {@link Task} instance and persists it for
   * future retrieval.  A follow-up {@link Task} does <b>not</b> belong to a
   * {@link TaskGroup} and therefore should have a <code>null</code>
   * {@linkplain Task#getTaskGroup() task group property}.
   *
   * @param task The follow-up {@link Task} to enqueue.
   *
   * @throws IllegalArgumentException If any of the specified {@link Task} belongs
   *                                  to a {@link TaskGroup}.
   *
   * @throws ServiceExecutionException If a failure occurs in persisting the
   *                                   specified {@link Task} instances
   *
   */
  protected abstract void enqueueFollowUpTask(Task task)
    throws ServiceExecutionException;

  /**
   * Retrieves a number of follow-up tasks from persistent storage.
   * This should mark the retrieved tasks as pending and should not return
   * them again until <b>at least</b> after {@link #getFollowUpTimeout()}
   * milliseconds has past.
   *
   * @param count The suggested number of follow-up tasks to retrieve from
   *              persistent storage.
   *
   * @return The {@link List} of follow-up {@link Task} instances retrieved
   *         from persistent storage.
   *
   * @throws ServiceExecutionException If a failure occurs in persisting the
   *                                   specified {@link Task} instances
   */
  protected abstract List<ScheduledTask> dequeueFollowUpTasks(int count)
      throws ServiceExecutionException;

  /**
   * Renews the leases on the specified follow-up tasks from persistent
   * storage.  This should mark the retrieved tasks as pending and update their
   * expiration timestamps accordingly.  The specified {@link ScheduledTask}
   * instances should be directly modified via {@link
   * ScheduledTask#setFollowUpExpiration(long)}.
   *
   * @param tasks The {@link ScheduledTask} instances for lease renewal.
   *
   * @throws ServiceExecutionException If a failure occurs in persisting the
   *                                   specified {@link Task} instances
   */
  protected abstract void renewFollowUpTasks(List<ScheduledTask> tasks)
    throws ServiceExecutionException;

  /**
   * Marks the specified follow-up task as complete and removes it from
   * persistent storage and is no longer available for dequeue.
   *
   * @param task The {@link ScheduledTask} to be marked as completed.
   *
   * @throws ServiceExecutionException If a failure occurs in persisting the
   *                                   specified {@link Task} instances
   */
  protected abstract void completeFollowUpTask(ScheduledTask task)
    throws ServiceExecutionException;

  /**
   * Calls the {@link #handleTasks()} function in a background thread after
   * validating the current state of this instance.
   */
  protected void backgroundHandleTasks() {
    synchronized (this) {
      // check if not "READY"
      if (this.getState() != READY && this.getState() != ACTIVE) {
        throw new IllegalStateException(
            "Cannot call backgroundHandleTasks() if not in the " + READY
                + " or " + ACTIVE + " state.  Current state is "
                + this.getState());
      }

      // check if already handling tasks
      if (this.handlingTasks) {
        throw new IllegalStateException(
            "Cannot call handleTasks() when it has already been called and is "
            + "still handling tasks.");
      }

      // set the handling tasks flag
      this.handlingTasks = true;

      // verify the handling thread is null
      if (this.taskHandlingThread != null) {
        throw new IllegalStateException(
            "Task handling thread seems to already exist.");
      }

      // create the thread
      this.taskHandlingThread = new Thread(() -> {
        TaskHandler taskHandler = this.getTaskHandler();
        Boolean     ready       = null;
        int         count       = 0;

        try {
          do {
            if (count > 0) {
              logInfo("****** STILL WAITING ON TASK HANDLER READINESS");
            }
            count++;
            ready = taskHandler.waitUntilReady(READY_TIMEOUT);
          } while (FALSE.equals(ready));

        } catch (InterruptedException e) {
          logWarning("****** INTERRUPTED WHILE WAITING ON TASK HANDLER "
                  + "READINESS");
          e.printStackTrace();
          return;
        }

        // check if ready state indicates a failure
        if (ready == null) {
          logWarning(
              "****** TASK HANDLER HAS INDICATED A FAILURE PREVENTING "
                  + "READINESS (CHECK LOGS)");
          return;
        }

        // check if ready state is false (should not get here)
        if (FALSE.equals(ready)) {
          logWarning(
              "****** TASK HANDLER NEVER BECAME READY TO HANDLE TASKS");
          return;
        }

        SUPPRESS_HANDLING_CHECK.set(true);
        this.handleTasks();
      });

      // start the thread
      this.taskHandlingThread.start();
    }
  }

  /**
   * Provides a loop that continues to schedule and handle tasks as long as
   * the {@link State} of this instance obtained from {@link #getState()} is
   * indicates the service is {@linkplain State#isAvailable() available} or
   * until there are no more pending or postponed tasks.  If the state
   * transitions such that the service is no longer {@linkplain
   * State#isAvailable() available} the only previously scheduled tasks will
   * be handled before the processing terminates.  This method does not return
   * until handling of the tasks is camplete.
   *
   */
  protected void handleTasks() {
    try {
      if (!SUPPRESS_HANDLING_CHECK.get()) {
        synchronized (this) {
          if (this.getState().isAvailable()) {
            throw new IllegalStateException(
                "Cannot call handleTasks() if not in the " + READY + " or "
                    + ACTIVE + " state.  Current state is " + this.getState());
          }

          // check if already handling tasks
          if (this.handlingTasks) {
            throw new IllegalStateException(
                "Cannot call handleTasks() when it has already been called and "
                    + "tasks are still being handled.");
          }

          // set the handling tasks flag
          this.handlingTasks = true;
        }
      }

      // create the worker pool
      synchronized (this) {
        this.workerPool = new AsyncWorkerPool<>(this.getConcurrency());
      }

      // start the handling timer
      this.timerStart(taskHandling, betweenTasks);

      // loop over the tasks
      while (this.getState().isAvailable()
             || this.getPendingTaskCount() > 0
             || this.getPostponedTaskCount() > 0
             || this.getLeasedFollowUpTaskCount() > 0)
      {
        // dequeue a message
        this.timerStart(dequeue, dequeueBlocking);
        ScheduledTask task = this.dequeueTask();
        this.timerPause(dequeue);

        // check if we have a task
        if (task != null) {
          this.timerPause(betweenTasks);
          this.timerStart(activelyHandling);

          // prep a task reference for the
          final ScheduledTask currentTask = task;
          final Timers timers = new Timers();
          timers.start(waitForWorker.toString());
          AsyncResult<TaskResult> result = this.workerPool.execute(() -> {
            try {
              // handle the task
              timers.start(handleTask.toString());
              currentTask.beginHandling();
              taskHandler.handleTask(currentTask.getAction(),
                                     currentTask.getParameters(),
                                     currentTask.getMultiplicity(),
                                     this.createFollowUpScheduler(currentTask));
              timers.pause(handleTask.toString());

              // in case of success mark it as handled
              timers.start(markComplete.toString());
              currentTask.succeeded();
              timers.pause(markComplete.toString());

            } catch (Exception e) {
              // in case of exception mark it as failed
              timers.start(markComplete.toString());
              currentTask.failed(e);
              timers.pause(markComplete.toString());

            } finally {
              // remove from persistent store (mark completed)
              if (currentTask.isFollowUp()) {
                timers.start(completeFollowUp.toString());
                synchronized (this) {
                  this.inProgressFollowUpTasks.remove(currentTask);
                }
                this.completeFollowUpTask(currentTask);
                timers.pause(completeFollowUp.toString());
              }

              // release any associated locks on the resources
              timers.start(releaseLocks.toString());
              currentTask.releaseLocks(this.getLockingService());
              timers.pause(releaseLocks.toString());

              // record statistics
              this.recordStatistics(task, timers);
            }

            return new TaskResult(currentTask, timers);
          });

          this.handleAsyncResult(result);
        }
        this.timerStart(betweenTasks);
      }

      // when done, close out the worker pool
      try {
        // if we get here then all postponed tasks have been handled and we
        // are no longer scheduling tasks -- time to wait for completion of
        // in-flight tasks so they can be disposed
        List<AsyncResult<TaskResult>> results = this.workerPool.close();
        for (AsyncResult<TaskResult> result: results) {
          this.handleAsyncResult(result);
        }
      } finally {
        this.timerPause(taskHandling,
                        activelyHandling,
                        waitingForTasks,
                        waitingOnPostponed);

        synchronized (this) {
          this.handlingTasks  = false;
          this.workerPool     = null;
          this.notifyAll();
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Handles the {@link AsyncResult} from the {@link AsyncWorkerPool} after it
   * is received.  This extracts the {@link TaskResult} value and traps any
   * exceptions (there should be none).  It records the timings from the
   * handling and calls {@link #postProcess(ScheduledTask)}.
   *
   * @param result The {@link AsyncResult} to handle, or <code>null</code> if
   *               no result was returned.
   */
  protected void handleAsyncResult(AsyncResult<TaskResult> result) {
    if (result == null) return;
    TaskResult taskResult = null;

    try {
      taskResult = result.getValue();

    } catch (Exception cannotHappen) {
      // exceptions should be logged and consumed during processing and used
      // to determine the disposability of the message/batch.
      logError(cannotHappen, "UNEXPECTED EXCEPTION");
      throw new IllegalStateException(cannotHappen);
    }

    ScheduledTask task = taskResult.getTask();
    this.timerStart(postProcess);
    try {
      this.postProcess(task);
    } finally {
      this.timerPause(postProcess);
    }
  }

  /**
   * This method does nothing, but provides a hook so that it may be overridden
   * to do any special handling on the {@link ScheduledTask} after it has been
   * handled by the {@link TaskHandler}.
   *
   * @param task The {@link ScheduledTask} that was handled.
   */
  protected void postProcess(ScheduledTask task) {
    // do nothing
  }

  /**
   * Records the statistics pertaining to the specified {@link ScheduledTask}
   * and using the specified {@link Timers} instance.
   *
   * @param scheduledTask The {@link ScheduledTask} that was completed.
   * @param timers The {@link Timers} associated with the specified {@link
   *               ScheduledTask}.
   */
  protected void recordStatistics(ScheduledTask scheduledTask, Timers timers) {
    if (scheduledTask.isSuccessful() == null) {
      logWarning("Statistics recorded for incomplete task: ",
                 scheduledTask);
      return;
    }
    synchronized (this.getStatisticsMonitor()) {
      // increment the scheduled task count
      this.handleCount++;
      if (scheduledTask.isSuccessful()) {
        this.handleSuccessCount++;
      } else {
        this.handleFailureCount++;
      }

      // check if this task is a follow-up
      boolean followUp = scheduledTask.isFollowUp();
      if (followUp) {
        this.followUpHandleCount++;
      } else {
        this.standardHandleCount++;
      }

      int multiplicity = scheduledTask.getMultiplicity();
      if (followUp) {
        // update the follow-up multiplicity stats
        if (multiplicity > this.greatestFollowUpMultiplicity) {
          this.greatestFollowUpMultiplicity = multiplicity;
        }
      } else {
        // update the greatest multiplicity
        if (multiplicity > this.greatestMultiplicity) {
          this.greatestMultiplicity = multiplicity;
        }
      }

      // get the handling time
      String    timerKey        = handleTask.toString();
      long      handlingMillis  = timers.getElapsedTime(timerKey);
      this.totalHandlingTime += handlingMillis;
      if (this.longestHandlingTime < handlingMillis) {
        this.longestHandlingTime = handlingMillis;
      }

      // iterate over the backing tasks
      scheduledTask.getBackingTasks().forEach(task -> {
        TaskGroup taskGroup = task.getTaskGroup();

        // if we have a task group then handle group statistics
        if (taskGroup != null) {
          boolean concluding = taskGroup.isConcludingTask(task);
          if (concluding) {
            this.taskGroupCount++;
            int taskCount = taskGroup.getTaskCount();
            if (this.greatestGroupSize < taskCount) {
              this.greatestGroupSize = taskCount;
            }
            long roundTrip = taskGroup.getRoundTripTime();
            this.totalTaskGroupTime += roundTrip;
            if (roundTrip > this.longestTaskGroupTime) {
              this.longestTaskGroupTime = roundTrip;
            }
            TaskGroup.State state = taskGroup.getState();
            if (state == TaskGroup.State.SUCCESSFUL) {
              this.groupSuccessCount++;
            } else if (state == TaskGroup.State.FAILED) {
              this.groupFailureCount++;
            }
          }
        }

        // handle task statistics
        if (followUp) {
          // since follow-up tasks are collapsed into a single backing task
          // then we need to add the multiplicity instead
          this.followUpCompleteCount += multiplicity;
          switch (task.getState()) {
            case SUCCESSFUL:
              this.followUpSuccessCount += multiplicity;
              break;
            case FAILED:
              this.followUpFailureCount += multiplicity;
              break;
          }
        } else {
          this.taskCompleteCount++;
          switch (task.getState()) {
            case SUCCESSFUL:
              this.taskSuccessCount++;
              break;
            case FAILED:
              this.taskFailureCount++;
              break;
            default:
              logWarning("UNEXPECTED POST-COMPLETION TASK STATE: "
                             + task.getState(), task);
          }

          long taskTime = task.getRoundTripTime();
          if (this.longestTaskTime < taskTime) {
            this.longestTaskTime = taskTime;
          }
          this.totalTaskTime += taskTime;
        }
      });
    }
  }

  /**
   * Gets the number of queued tasks that are pending.
   *
   * @return The number of pending tasks.
   */
  protected synchronized int getPendingTaskCount() {
    return this.pendingTasks.size();
  }

  /**
   * Gets the number of postponed tasks.
   *
   * @return The number of postponed tasks.
   */
  protected synchronized int getPostponedTaskCount() {
    return this.postponedTasks.size();
  }

  /**
   * Gets the number of follow-up tasks cached in memory.
   *
   * @return The number of follow-up tasks cached in memory.
   */
  protected synchronized int getLeasedFollowUpTaskCount() {
    return this.followUpTasks.size();
  }

  /**
   * Default implmentation of {@link
   * SchedulingService#init(JsonObject,TaskHandler)} that will initialize the
   * base properties and then call {@link #doInit(JsonObject)} to complete the
   * configuration.  This implementation will ensure that this function is
   * called in the {@link State#UNINITIALIZED} and that the service transitions
   * to the {@link State#READY} state at its conclusion.
   *
   * @param config The {@link JsonObject} describing the configuration.
   * @param taskHandler The {@link TaskHandler} to use for handling tasks.
   * @throws ServiceSetupException If a failure occurs.
   */
  @Override
  public void init(JsonObject config, TaskHandler taskHandler)
    throws ServiceSetupException
  {
    Objects.requireNonNull(taskHandler,
                           "The specified TaskHandler cannot be null");
    synchronized (this) {
      if (this.getState() != UNINITIALIZED) {
        throw new IllegalStateException(
            "Cannot initialize if not in the " + UNINITIALIZED + " state: "
            + this.getState());
      }
      this.timerStart(initialize);
      this.setState(INITIALIZING);
    }

    try {
      synchronized (this) {
        // default to an empty JSON object if null
        if (config == null) {
          config = Json.createObjectBuilder().build();
        }

        this.lockingService = this.initLockingService(config);
        this.setTaskHandler(taskHandler);

        this.concurrency = getConfigInteger(config,
                                            CONCURRENCY_KEY,
                                            1,
                                            this.getDefaultConcurrency());

        // get the postponed timeout
        this.postponedTimeout = getConfigLong(config,
                                              POSTPONED_TIMEOUT_KEY,
                                              0L,
                                              this.getDefaultPostponedTimeout());

        // get the standard timeout
        this.standardTimeout = getConfigLong(config,
                                             STANDARD_TIMEOUT_KEY,
                                             0L,
                                             this.getDefaultStandardTimeout());

        // get the follow-up delay
        this.followUpDelay = getConfigLong(config,
                                           FOLLOW_UP_DELAY_KEY,
                                           0L,
                                           this.getDefaultFollowUpDelay());

        // get the follow-up timeout
        this.followUpTimeout = getConfigLong(config,
                                             FOLLOW_UP_TIMEOUT_KEY,
                                             0L,
                                             this.getDefaultFollowUpTimeout());

        // get the follow-up fetch
        this.followUpFetch = getConfigInteger(
            config,
            FOLLOW_UP_FETCH_KEY,
            1,
            this.getDefaultFollowUpFetchCount());

        // check that the follow-up timeout is greater than follow-up delay
        if (this.followUpTimeout < this.followUpDelay) {
          throw new ServiceSetupException(
              "The configured value for " + FOLLOW_UP_TIMEOUT_KEY + " ("
                  + this.followUpTimeout + ") cannot be less than the "
                  + "configured value for " + FOLLOW_UP_DELAY_KEY + " ("
                  + this.followUpDelay + ").");
        }

        // create the queues
        this.pendingTasks             = new LinkedList<>();
        this.postponedTasks           = new LinkedList<>();
        this.followUpTasks            = new LinkedList<>();
        this.inProgressFollowUpTasks  = new IdentityHashMap<>();
        this.taskCollapseLookup       = new LinkedHashMap<>();
      }

      // defer additional configuration
      this.doInit(config);

      // set to the ready state
      this.setState(READY);
      this.backgroundHandleTasks();

    } catch (Exception e) {
      e.printStackTrace();
      this.setState(UNINITIALIZED);
      throw new RuntimeException(e);

    } finally {
      this.timerPause(initialize);
    }
  }

  /**
   * The default implementation of this method gets the class name from
   * the {@link #LOCKING_SERVICE_CLASS_KEY} parameter, constructs an instance
   * of that class using the default constructor and then initializes the
   * constructed {@link LockingService} instance using the {@link JsonObject}
   * found in the specified configuration via the {@link
   * #LOCKING_SERVICE_CONFIG_KEY} JSON property.
   *
   * @param jsonConfig The {@link JsonObject} describing the configuration
   *                   for this instance of scheduling service.
   *
   * @return The {@link LockingService} that was created and initialized.
   * @throws ServiceSetupException If a failure occurs in initializing the
   *                               backing {@link LockingService}.
   */
  @SuppressWarnings("unchecked")
  protected LockingService initLockingService(JsonObject jsonConfig)
    throws ServiceSetupException
  {
    try {
      // get the LockingService class name from the config
      String className = getConfigString(
          jsonConfig,
          LOCKING_SERVICE_CLASS_KEY,
          this.getDefaultLockingServiceClassName());

      // get the LockingService Class object from the class name
      Class lockServiceClass = Class.forName(className);

      if (!LockingService.class.isAssignableFrom(lockServiceClass)) {
        throw new ServiceSetupException(
            "The configured locking service class for the "
                + LOCKING_SERVICE_CLASS_KEY + " config parameter must "
                + "implement " + LockingService.class.getName());
      }

      // create an instance of the LockingService class
      LockingService lockService = (LockingService)
          lockServiceClass.getConstructor().newInstance();

      // get the locking service configuration
      JsonObject lockServiceConfig
          = (jsonConfig.containsKey(LOCKING_SERVICE_CONFIG_KEY))
          ? getJsonObject(jsonConfig, LOCKING_SERVICE_CONFIG_KEY)
          : this.getDefaultLockingServiceConfig();

      // initialize the locking service
      lockService.init(lockServiceConfig);

      // return the locking service
      return lockService;

    } catch (ServiceSetupException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSetupException(
          "Failed to initialize LockingService for SchedulingService", e);
    }
  }

  /**
   * Called by the {@link #init(JsonObject,TaskHandler)} implementation after handling the
   * base configuration parameters.
   *
   * @param config The {@link JsonObject} describing the configuration.
   *
   * @throws ServiceSetupException If a failure occurs during initialization.
   */
  protected abstract void doInit(JsonObject config)
      throws ServiceSetupException;

  /**
   * Implemented as a synchronized method to {@linkplain #setState(State)
   * set the state} to {@link State#DESTROYING}, call {@link #doDestroy()} and
   * then perform {@link #notifyAll()} and set the state to {@link
   * State#DESTROYED}.
   */
  public void destroy() {
    synchronized (this) {
      State state = this.getState();
      if (state == DESTROYED) return;

      if (state == DESTROYING) {
        while (this.getState() != DESTROYED) {
          try {
            this.wait(this.getStandardTimeout());
          } catch (InterruptedException e) {
            // ignore
          }
        }
        // once DESTROYED state is found, just return
        return;
      }

      // begin destruction
      this.setState(DESTROYING);
      this.timerStart(destroy);

      // wait until no longer handling tasks
      while (this.isHandlingTasks()) {
        try {
          this.wait(this.getStandardTimeout());

        } catch (InterruptedException ignore) {
          // do nothing
        }
      }
    }

    // join against the scheduler thread
    try {
      this.taskHandlingThread.join();

    } catch (InterruptedException ignore) {
      // ignore the exception
    }

    try {
      // now complete the destruction / cleanup
      this.doDestroy();

      // destroy the locking service
      this.lockingService.destroy();

    } finally {
      this.setState(DESTROYED); // this should notify all as well
      this.timerPause(destroy);
    }
  }

  /**
   * This is called from the {@link #destroy()} implementation and should be
   * overridden by the concrete sub-class.
   */
  protected abstract void doDestroy();

  /**
   * Converts the specified {@link Stat} instances to an array
   * of {@link String} instances.
   *
   * @param statistics The {@link Stat} instances to convert.
   *
   * @return The array of {@link String} instances describing the specified
   *         {@link Stat} instances.
   */
  private String[] convertTimerKeys(Stat... statistics) {
    String[] names = (statistics == null || statistics.length == 0)
        ? null : new String[statistics.length];
    if (names != null) {
      for (int index = 0; index < statistics.length; index++) {
        names[index] = statistics[index].toString();
      }
    }
    return names;
  }

  /**
   * Merges the specified {@link Timers} with this instances {@link Timers}
   * in a thread safe manner.
   * @param timers The {@link Timers} to merge.
   */
  protected void timerMerge(Timers timers) {
    synchronized (this.getStatisticsMonitor()) {
      this.timers.mergeWith(timers);
    }
  }

  /**
   * Toggles the active and waiting timers.
   * @param pendingCount The number of pending messages.
   * @param postponedCount The number of postponed messages.
   * @param busy <code>true</code> if the worker pool is busy, otherwise
   *             <code>false</code>.
   */
  protected void toggleActiveAndWaitingTimers(int     pendingCount,
                                              int     postponedCount,
                                              boolean busy)
  {
    synchronized (this.getStatisticsMonitor()) {
      // check if there are messages
      if (busy) {
        this.timerPause(waitingForTasks, waitingOnPostponed);
        this.timerStart(activelyHandling);

      } else if (pendingCount == 0 && postponedCount == 0) {
        // no tasks pending or postponed
        this.timerPause(activelyHandling, waitingOnPostponed);
        this.timerStart(waitingForTasks);

      } else if (pendingCount > 0) {
        // messages pending
        this.timerPause(waitingForTasks, waitingOnPostponed);
        this.timerStart(activelyHandling);

      } else if (postponedCount > 0) {
        // none pending, but some postponed
        this.timerPause(activelyHandling, waitingForTasks);
        this.timerStart(waitingOnPostponed);
      }
    }
  }

  /**
   * Resumes the associated {@link Timers} in a thread-safe manner.
   * @param statistic The {@link Stat} to resume.
   * @param addlTimers The additional {@link Stat} instances to
   *                   resume.
   */
  protected void timerResume(Stat statistic,
                             Stat... addlTimers)
  {
    String[] names = this.convertTimerKeys(addlTimers);
    synchronized (this.getStatisticsMonitor()) {
      if (names == null) {
        this.timers.resume(statistic.toString());
      } else {
        this.timers.resume(statistic.toString(), names);
      }
    }
  }

  /**
   * Starts the associated {@link Timers} in a thread-safe manner.
   * @param statistic The {@link AbstractMessageConsumer.Stat} to start.
   * @param addlTimers The additional {@link Stat} instances to
   *                   start.
   */
  protected void timerStart(Stat statistic,
                            Stat...  addlTimers)
  {
    String[] names = this.convertTimerKeys(addlTimers);
    synchronized (this.getStatisticsMonitor()) {
      if (names == null) {
        this.timers.start(statistic.toString());
      } else {
        this.timers.start(statistic.toString(), names);
      }
    }
  }

  /**
   * Pauses the associated {@link Timers} in a thread-safe manner.
   * @param statistic The {@link Stat} to pause.
   * @param addlTimers The additional {@link Stat} instances to
   *                   pause.
   */
  protected void timerPause(Stat statistic,
                            Stat...  addlTimers)
  {
    String[] names = this.convertTimerKeys(addlTimers);
    synchronized (this.getStatisticsMonitor()) {
      if (names == null) {
        this.timers.pause(statistic.toString());
      } else {
        this.timers.pause(statistic.toString(), names);
      }
    }
  }

  /**
   * Gets the {@link Map} of {@link AbstractMessageConsumer.Stat} keys to
   * their {@link Number} values in an atomic thread-safe manner.
   *
   * @return The {@link Map} of {@link AbstractMessageConsumer.Stat} keys
   *         to their {@link Number} values.
   */
  @Override
  public Map<Statistic, Number> getStatistics() {
    synchronized (this.getStatisticsMonitor()) {
      Number value = null;
      Map<String, Long> timings = this.timers.getTimings();

      Map<Statistic, Number> statsMap = new LinkedHashMap<>();

      statsMap.put(AbstractSchedulingService.Stat.concurrency, this.getConcurrency());
      statsMap.put(AbstractSchedulingService.Stat.standardTimeout, this.getStandardTimeout());
      statsMap.put(AbstractSchedulingService.Stat.postponedTimeout, this.getPostponedTimeout());
      statsMap.put(AbstractSchedulingService.Stat.followUpDelay, this.getFollowUpDelay());
      statsMap.put(AbstractSchedulingService.Stat.followUpTimeout, this.getFollowUpTimeout());

      value = this.getAverageTaskTime();
      if (value != null) statsMap.put(averageTaskTime, value);

      value = this.getAverageTaskGroupTime();
      if (value != null) statsMap.put(averageTaskGroupTime, value);

      value = this.getLongestTaskTime();
      if (value != null) statsMap.put(AbstractSchedulingService.Stat.longestTaskTime, value);

      value = this.getLongestTaskGroupTime();
      if (value != null) statsMap.put(AbstractSchedulingService.Stat.longestTaskGroupTime, value);

      statsMap.put(AbstractSchedulingService.Stat.taskCompleteCount, this.getCompletedTaskCount());
      statsMap.put(AbstractSchedulingService.Stat.taskSuccessCount, this.getSuccessfulTaskCount());
      statsMap.put(AbstractSchedulingService.Stat.taskFailureCount, this.getFailedTaskCount());
      statsMap.put(AbstractSchedulingService.Stat.taskAbortCount, this.getAbortedTaskCount());
      statsMap.put(AbstractSchedulingService.Stat.followUpCompleteCount,
                   this.getCompletedFollowUpCount());
      statsMap.put(AbstractSchedulingService.Stat.followUpSuccessCount,
                   this.getSuccessfulFollowUpCount());
      statsMap.put(AbstractSchedulingService.Stat.followUpFailureCount,
                   this.getFailedFollowUpCount());

      value = this.getAverageHandleTaskTime();
      if (value != null) statsMap.put(averageHandleTask, value);

      statsMap.put(handleTaskCount, this.getHandleTaskCount());
      statsMap.put(handleTaskSuccessCount, this.getSuccessfulHandleTaskCount());
      statsMap.put(handleTaskFailureCount, this.getFailedHandleTaskCount());

      value = this.getFollowUpHandleTaskRatio();
      if (value != null) statsMap.put(followUpHandleTaskRatio, value);

      statsMap.put(taskGroupCompleteCount, this.getCompletedTaskGroupCount());
      statsMap.put(taskGroupSuccessCount, this.getSuccessfulTaskGroupCount());
      statsMap.put(taskGroupFailureCount, this.getFailedTaskGroupCount());

      value = this.getAverageCompressionRatio();
      if (value != null) statsMap.put(averageCompression, value);

      value = this.getGreatestCompressionRatio();
      if (value != null) statsMap.put(greatestCompression, value);

      value = this.getAverageFollowUpCompressionRatio();
      if (value != null) statsMap.put(averageFollowUpCompression, value);

      value = this.getGreatestFollowUpCompressionRatio();
      if (value!= null) statsMap.put(greatestFollowUpCompression, value);

      value = this.getAverageTaskGroupSize();
      if (value != null) statsMap.put(averageTaskGroupSize, value);

      value = this.getGreatestTaskGroupSize();
      if (value != null) statsMap.put(greatestTaskGroupSize, value);

      value = this.getParallelism();
      if (value != null) statsMap.put(parallelism, value);

      value = this.getDequeueHitRatio();
      if (value != null) statsMap.put(dequeueHitRatio, value);

      statsMap.put(AbstractSchedulingService.Stat.greatestPostponedCount,
                   this.getGreatestPostponedCount());

      // now get the timings
      for (Stat statistic : AbstractSchedulingService.Stat.values()) {
        value = timings.get(statistic.toString());
        if (value != null) {
          statsMap.put(statistic, value);
        }
      }

      return statsMap;
    }
  }

  /**
   * Gets the average task compression from collapsing non-follow-up tasks
   * handled by the scheduling service.  This returns <code>null</code> if no
   * non-follow-up tasks have been handled.
   *
   * @return The average task compression from collapsing non-follow-up tasks
   *         handled by the scheduling service, or <code>null</code> if no
   *         non-follow-up tasks have been handled.
   */
  public Double getAverageCompressionRatio() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.standardHandleCount == 0) return null;
      double completeCount  = (double) this.taskCompleteCount;
      double handleCount    = (double) this.standardHandleCount;
      return completeCount / handleCount;
    }
  }

  /**
   * Gets the greatest task compression from collapsing non-follow-up tasks
   * handled by the scheduling service.  This returns <code>null</code> if
   * no tasks have been handled.
   *
   * @return The greatest task compression from collapsing non-follow-up tasks
   *         handled by the scheduling service, or <code>null</code> if no
   *         tasks have been handled.
   */
  public Integer getGreatestCompressionRatio() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.greatestMultiplicity <= 0) return null;
      return this.greatestMultiplicity;
    }
  }

  /**
   * Gets the average task compression from collapsing follow-up tasks
   * handled by the scheduling service.  This returns <code>null</code> if no
   * follow-up tasks have been handled.
   *
   * @return The average task compression from collapsing follow-up tasks
   *         handled by the scheduling service, or <code>null</code> if no
   *         follow-up tasks have been handled.
   */
  public Double getAverageFollowUpCompressionRatio() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.followUpHandleCount == 0) return null;
      double completeCount  = (double) this.followUpCompleteCount;
      double handleCount    = (double) this.followUpHandleCount;
      return completeCount / handleCount;
    }
  }

  /**
   * Gets the greatest task compression from collapsing follow-up tasks
   * handled by the scheduling service.  This returns <code>null</code> if
   * no follow-up tasks have been handled.
   *
   * @return The greatest task compression from collapsing follow-up tasks
   *         handled by the scheduling service, or <code>null</code> if no
   *         follow-up tasks have been handled.
   */
  public Integer getGreatestFollowUpCompressionRatio() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.greatestFollowUpMultiplicity <= 0) return null;
      return this.greatestFollowUpMultiplicity;
    }
  }

  /**
   * Gets the average number of tasks in all the completed task groups.  This
   * returns <code>null</code> if no task groups have been completed.
   *
   * @return The average number of tasks in all the completed task groups, or
   *         <code>null</code> if no task groups have been completed.
   */
  public Double getAverageTaskGroupSize() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.taskGroupCount == 0) return null;
      double completeCount  = (double) this.taskCompleteCount;
      double groupCount     = (double) this.taskGroupCount;
      return (completeCount / groupCount);
    }
  }

  /**
   * Gets the dequeue hit ratio.  This returns <code>null</code> if there have
   * been no attempts to dequeue a task.
   *
   * @return The dequeue hit ratio, or <code>null</code> if no attempts have
   *         been made to dequeue a task.
   */
  public Double getDequeueHitRatio() {
    synchronized (this.getStatisticsMonitor()) {
      if ((this.dequeueHitCount + this.dequeueMissCount) == 0) {
        return null;
      }
      double hits = (double) this.dequeueHitCount;
      double misses = (double) this.dequeueMissCount;
      double total = hits + misses;
      return (hits / total);
    }
  }

  /**
   * Call this to increment the number of times dequeue has been called with
   * or without a task ready to be dequeued.  This function is thread-safe
   * with respect to other statistics.
   *
   * @param hit <code>true</code> if we have a "hit" and there is a task ready
   *            to be dequeued, otherwise <code>false</code> for a "miss".
   */
  protected void updateDequeueHitRatio(boolean hit) {
    synchronized (this.getStatisticsMonitor()) {
      if (hit) {
        this.dequeueHitCount++;
      } else {
        this.dequeueMissCount++;
      }
    }
  }

  /**
   * The average time in milliseconds that non-follow-up tasks have taken from
   * scheduling until completion.  This returns <code>null</code> if no
   * non-follow-up tasks have been handled.
   *
   * @return The average time in milliseconds that non-follow-up tasks have
   *         taken from scheduling until completion, or <code>null</code> if
   *         no non-follow-up tasks have been handled.
   */
  public Double getAverageTaskTime() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.taskCompleteCount == 0) return null;
      double totalTime      = (double) this.totalTaskTime;
      double completeCount  = (double) this.taskCompleteCount;
      return totalTime / completeCount;
    }
  }

  /**
   * The longest time in milliseconds that a non-follow-up task has taken from
   * scheduling until completion.  This returns <code>null</code> if no
   * non-follow-up tasks have been handled.
   *
   * @return The longest time in milliseconds that a non-follow-up task has
   *         taken from scheduling until completion, or <code>null</code> if
   *         no non-follow-up tasks have been handled.
   */
  public Long getLongestTaskTime() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.longestTaskTime < 0) return null;
      return this.longestTaskTime;
    }
  }

  /**
   * Gets the average number of milliseconds from all task groups to be
   * handled from the time first task in the group was scheduled until the last
   * task was completed.  This returns <code>null</code> if no task groups
   * have been completed.
   *
   * @return The average number of milliseconds from all task groups to be
   *         handled from the time first task in the group was scheduled until
   *         the last task was completed, or <code>null</code> if no task
   *         groups have been completed.
   */
  public Double getAverageTaskGroupTime() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.taskGroupCount == 0) return null;
      double totalTime  = (double) this.totalTaskGroupTime;
      double groupCount = (double) this.taskGroupCount;
      return totalTime / groupCount;
    }
  }

  /**
   * Gets the greatest number of milliseconds for a task groups to be handled
   * from the time first task in the group was scheduled until the last task
   * was completed.  This returns <code>null</code> if no task groups have been
   * completed.
   *
   * @return The greatest number of milliseconds for a task groups to be handled
   *         from the time first task in the group was scheduled until the last
   *         task was completed, or <code>null</code> if no task groups have
   *         been completed.
   */
  public Long getLongestTaskGroupTime() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.longestTaskGroupTime < 0L) return null;
      return this.longestTaskGroupTime;
    }
  }

  /**
   * Gets the number of non-follow-up tasks that have been completed.
   *
   * @return The number of non-follow-up tasks that have been completed.
   */
  public long getCompletedTaskCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.taskCompleteCount;
    }
  }

  /**
   * Gets the number of non-follow-up tasks that have been completed
   * successfully.
   *
   * @return The number of non-follow-up tasks that have been completed
   *         successfully.
   */
  public long getSuccessfulTaskCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.taskSuccessCount;
    }
  }

  /**
   * Gets the number of non-follow-up tasks that have been completed
   * unsuccessfully (i.e.: with failures).
   *
   * @return The number of non-follow-up tasks that have been completed
   *         unsuccessfully (i.e.: with failures).
   */
  public long getFailedTaskCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.taskFailureCount;
    }
  }

  /**
   * Gets the number of non-follow-up tasks that were aborted.
   *
   * @return The number of non-follow-up tasks that were aborted.
   */
  public long getAbortedTaskCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.taskAbortCount;
    }
  }

  /**
   * Gets the number of follow-up tasks that have been completed.
   *
   * @return The number of follow-up tasks that have been completed.
   */
  public long getCompletedFollowUpCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.followUpCompleteCount;
    }
  }

  /**
   * Gets the number of follow-up tasks that have been completed successfully.
   *
   * @return The number of follow-up tasks that have been completed
   *         successfully.
   */
  public long getSuccessfulFollowUpCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.followUpSuccessCount;
    }
  }

  /**
   * Gets the number of follow-up tasks that have been completed unsuccessfully
   * (i.e.: with failures).
   *
   * @return The number of follow-up tasks that have been completed
   *         successfully (i.e.: with failures).
   */
  public long getFailedFollowUpCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.followUpFailureCount;
    }
  }

  /**
   * Get the average number of milliseconds spent calling {@link
   * TaskHandler#handleTask(String, Map, int, Scheduler)} for tasks (both follow-up
   * and non-follow-up).  If no tasks have been handled then <code>null</code>
   * is returned.
   *
   * @return The average number of milliseconds spent calling {@link
   *         TaskHandler#handleTask(String, Map, int, Scheduler)} for tasks, or
   *         <code>null</code> if no tasks have been handled.
   */
  public Double getAverageHandleTaskTime() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.handleCount == 0) return null;
      double  totalTime = ((double) this.totalHandlingTime);
      double  callCount = ((double) this.handleCount);
      return totalTime / callCount;
    }
  }

  /**
   * Get the total number of times {@link
   * TaskHandler#handleTask(String, Map, int, Scheduler)} has been called to
   * handle tasks (both follow-up and non-follow-up).
   *
   * @return The total number of times {@link
   *         TaskHandler#handleTask(String, Map, int, Scheduler)} has been called
   *         to handle tasks (both follow-up and non-follow-up).
   */
  public long getHandleTaskCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.handleCount;
    }
  }

  /**
   * Get the total number of times {@link
   * TaskHandler#handleTask(String, Map, int, Scheduler)} has been called to
   * handle tasks successfully (both follow-up and non-follow-up).
   *
   * @return The total number of times {@link
   *         TaskHandler#handleTask(String, Map, int, Scheduler)} has been called
   *         to handle tasks successfully (both follow-up and non-follow-up).
   */
  public long getSuccessfulHandleTaskCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.handleSuccessCount;
    }
  }

  /**
   * Get the total number of times {@link
   * TaskHandler#handleTask(String, Map, int, Scheduler)} has been called to
   * handle tasks unsuccessfully (both follow-up and non-follow-up).
   *
   * @return The total number of times {@link
   *         TaskHandler#handleTask(String, Map, int, Scheduler)} has been called
   *         to handle tasks unsuccessfully (both follow-up and non-follow-up).
   */
  public long getFailedHandleTaskCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.handleFailureCount;
    }
  }

  /**
   * Gets the ratio of the number of times {@link
   * TaskHandler#handleTask(String, Map, int, Scheduler)} has been called to handle
   * follow-up tasks to the number of times it has been called to handle
   * <b>all</b> tasks that have been handled.  This returns <code>null</code>
   * if no tasks have been handled.
   *
   * @return The ratio of the number of times {@link
   *         TaskHandler#handleTask(String, Map, int, Scheduler)} has been called
   *         to handle follow-up tasks to the number of times it has been
   *         called to handle <b>all</b> tasks that have been handled, or
   *         <code>null</code> if no tasks have been handled.
   */
  public Double getFollowUpHandleTaskRatio() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.handleCount == 0) return null;
      double followUp = ((double) this.followUpHandleCount);
      double all      = ((double) this.handleCount);
      return followUp / all;
    }
  }

  /**
   * Gets the number of {@link TaskGroup} instances that have been folly
   * handled (whether successful or not).
   *
   * @return The number of {@link TaskGroup} instances that have been folly
   *         handled (whether successful or not).
   */
  public long getCompletedTaskGroupCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.taskGroupCount;
    }
  }

  /**
   * Gets the number of task groups that have been successfully completed.
   *
   * @return The number of task groups that have been successfully completed.
   */
  public long getSuccessfulTaskGroupCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.groupSuccessCount;
    }
  }

  /**
   * Gets the number of task groups that have been completed with failures.
   *
   * @return The number of task groups that have been completed with failures.
   */
  public long getFailedTaskGroupCount() {
    synchronized (this.getStatisticsMonitor()) {
      return this.groupFailureCount;
    }
  }

  /**
   * The greatest number of tasks in the completed task groups.  This returns
   * <code>null</code> if no task groups have been completed.
   *
   * @return The greatest number of tasks in the completed task groups, or
   *         <code>null</code> if no task groups have been completed.
   */
  public Integer getGreatestTaskGroupSize() {
    synchronized (this.getStatisticsMonitor()) {
      if (this.greatestGroupSize <= 0) return null;
      return this.greatestGroupSize;
    }
  }

  /**
   * Gets the ratio of the total handling time across all threads to the
   * total active handling of the task scheduler to indicate the level
   * of parallelism achieved.  This returns <code>null</code> if no tasks have
   * yet been handled.
   *
   * @return The ratio of the total handling time across all threads to the
   *         total active handling time of the task scheduler, or
   *         <code>null</code> if no tasks have been handled.
   */
  public Double getParallelism() {
    synchronized (this.getStatisticsMonitor()) {
      String timerKey = activelyHandling.toString();
      Long activeTime = this.timers.getElapsedTime(timerKey);
      if (activeTime == 0L) return null;
      Double totalTime  = (double) this.totalHandlingTime;
      return (totalTime / ((double) activeTime));
    }
  }

  /**
   * Gets the greatest number of tasks that have been postponed.
   *
   * @return The greatest number of tasks that have been postponed.
   */
  public int getGreatestPostponedCount() {
    return this.greatestPostponedCount;
  }

  /**
   * Encapsulates a scheduled {@link Task} and all duplicates of that {@link
   * Task} assuming the tasks can be collapsed.
   *
   */
  protected static class ScheduledTask {
    /**
     * The original backing task ID.
     */
    private long origTaskId;

    /**
     * Flag indicating if this contains follow-up tasks or non-follow-up
     * tasks.
     */
    private boolean followUp;

    /**
     * The external follow-up ID to reference the task in persistent storage.
     */
    private String followUpId;

    /**
     * The follow-up multiplicity since the follow-up tasks lack backing tasks.
     */
    private Integer multiplicity = null;

    /**
     * The nanosecond when this scheduled task is considered to be expired.
     */
    private Long expirationNanos = null;

    /**
     * The action associated with the associated tasks.
     */
    private String action;

    /**
     * The parameters for the associated tasks.
     */
    private SortedMap<String, Object> parameters;

    /**
     * The resource keys for the associated tasks.
     */
    private SortedSet<ResourceKey> resourceKeys;

    /**
     * The {@link List} of duplicate {@link Task} instances.
     */
    private List<Task> backingTasks;

    /**
     * The signature for the tasks.
     */
    private String signature;

    /**
     * Flag indicating if this instance allows collapsing duplicate tasks.
     */
    private boolean allowCollapse = false;

    /**
     * Flag indicating if the task has succeeded.
     */
    private Boolean successful = null;

    /**
     * The {@link LockToken} for the resources that are locked for this task.
     */
    private LockToken lockToken = null;

    /**
     * Constructs with the first backing actual {@link Task}.
     *
     * @param task The actual {@link Task} that will back this instance.
     */
    public ScheduledTask(Task task) {
      this.origTaskId       = task.getTaskId();
      this.followUp         = task.getTaskGroup() == null;
      this.backingTasks     = new LinkedList<>();
      this.action           = task.getAction();
      this.parameters       = task.getParameters();
      this.resourceKeys     = task.getResourceKeys();
      this.signature        = task.getSignature();
      this.allowCollapse    = task.isAllowingCollapse();
      this.lockToken        = null;
      this.successful       = null;
      this.expirationNanos  = null;
      task.markScheduled();
      this.backingTasks.add(task);
    }

    /**
     * Gets the task ID of the original backing task with which this instance
     * was constructed.
     *
     * @return The task ID of the original backing task with which this
     *         instance was constructed.
     */
    public long getOriginalBackingTaskId() {
      return this.origTaskId;
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this
     * instance.
     *
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
      JsonObjectBuilder job = Json.createObjectBuilder();
      JsonUtilities.add(job, "originalBackingTaskId",
                        this.getOriginalBackingTaskId());
      JsonUtilities.add(job, "signature", this.getSignature());
      JsonUtilities.add(job, "allowCollapse", this.isAllowingCollapse());
      JsonUtilities.add(job, "multiplicity", this.getMultiplicity());
      JsonUtilities.add(job, "followUp", this.isFollowUp());
      if (this.expirationNanos != null) {
        JsonUtilities.add(job, "followUpId", this.getFollowUpId());
        long deltaNanos   = this.expirationNanos - System.nanoTime();
        long deltaMillis  = deltaNanos / ONE_MILLION;
        long expireMillis = System.currentTimeMillis() + deltaMillis;
        Date expireDate   = new Date(expireMillis);
        JsonUtilities.add(job, "expiration", expireDate.toString());
      }
      JsonUtilities.add(job, "successful", this.isSuccessful());
      JsonUtilities.add(job, "action", this.getAction());
      job.add("parameters",
              JsonUtilities.toJsonObjectBuilder(this.getParameters()));
      JsonArrayBuilder jab = Json.createArrayBuilder();
      for (ResourceKey resourceKey : this.getResourceKeys()) {
        jab.add(resourceKey.toString());
      }
      job.add("resources", jab);
      return "\n" + JsonUtilities.toJsonText(job.build());
    }

    /**
     * Constructor for deserializing a follow-up task from persistent storage.
     *
     * @param jsonText The serialized JSON representation of the follow-up task.
     * @param followUpId The optional external persistence ID for the follow-up
     *                   task so it can be later marked complete in and deleted
     *                   from persistent storage.
     * @param multiplicity The collapsed multiplicity from persistent storage,
     *                     which may be one (1) if the follow-up task did not
     *                     allow collapsing with duplicate tasks.
     * @param expirationTime The millisecond UTC time since then epoch when the
     *                       follow-up task is considered to be "expired".
     * @param elapsedMillisSinceCreation The number of milliseconds that have
     *                                   elapsed since the deserialized task
     *                                   was originally creasted, or
     *                                   <code>null</code> if unknown.
     */
    public ScheduledTask(String jsonText,
                         String followUpId,
                         int    multiplicity,
                         long   expirationTime,
                         long   elapsedMillisSinceCreation)
    {
      this(Task.deserialize(jsonText,
                            false,
                            elapsedMillisSinceCreation));

      this.followUp         = true;
      this.followUpId       = followUpId;
      this.allowCollapse    = false;
      this.multiplicity     = multiplicity;

      // determine the expiration in a consistent manner
      long now              = System.currentTimeMillis();
      long remainingNanos   = (expirationTime - now) * ONE_MILLION;
      this.expirationNanos  = System.nanoTime() + remainingNanos;
    }

    /**
     * Checks if the actual tasks backing this instance are follow-up tasks.
     * Either all the tasks are follow-up tasks or all are <b>not</b>
     * follow-up tasks.
     *
     * @return <code>true</code> if the tasks are follow-up tasks, otherwise
     *         <code>false</code>.
     */
    public boolean isFollowUp() {
      return this.followUp;
    }

    /**
     * This method always returns <code>false</code> if not a follow-up task.
     * If this is a follow-up task then this returns <code>true</code> if the
     * follow-up task is expired, otherwise <code>false</code>.
     *
     * @return <code>true</code> if this is an expired follow-up task, otherwise
     *         <code>false</code>.
     */
    public boolean isFollowUpExpired() {
      if (this.expirationNanos == null) return false;
      return System.nanoTime() > this.expirationNanos;
    }

    /**
     * Updates the expiration time to the specified number of milliseconds
     * since the epoch in UTC time coordinates.
     *
     * @param expiration The expiration time in number of milliseconds since
     *                   the epoch in UTC time coordinates.
     */
    public void setFollowUpExpiration(long expiration) {
      // determine the expiration in a consistent manner
      long now              = System.currentTimeMillis();
      long remainingNanos   = (expiration - now) * ONE_MILLION;
      this.expirationNanos  = System.nanoTime() + remainingNanos;
    }

    /**
     * Obtains the external ID used to identify the deserialized follow-up
     * task in persistent storage.  This should always return <code>null</code>
     * if {@link #isFollowUp()} is <code>false</code>.  This may return
     * <code>null</code> if {@link #isFollowUp()} is <code>true</code> if the
     * external persistent storage mechanism does not require an external ID.
     *
     * @return The external ID used to identify the deserizlied follow-up
     *         task in persistent storage.
     */
    public String getFollowUpId() {
      return this.followUpId;
    }

    /**
     * Removes all backing tasks that have been flagged as aborted and
     * returns the remaining number of backing tasks.  If no backing tasks
     * remain then this {@link ScheduledTask} should itself be aborted.
     *
     * @return The number of backing tasks that were removed because they were
     *         aborted.
     */
    public synchronized int removeAborted() {
      if (this.isFollowUp()) return 0;

      int removedCount = 0;
      Iterator<Task> iter = this.backingTasks.iterator();
      while (iter.hasNext()) {
        // get the next task
        Task task = iter.next();

        // get the task group, not a follow-up so we should always have one
        TaskGroup group = task.getTaskGroup();

        // check if the group is fast-fail, if not then no abort
        if (!group.isFastFail()) continue;

        // check if the group has failed
        if (group.getState() == TaskGroup.State.FAILED) {
          // mark the task as aborted
          task.aborted();

          // we have a fast-fail group that is marked as failed
          iter.remove(); // remove the aborted task
          removedCount++;
        }
      }

      // return the number of removed tasks
      return removedCount;
    }

    /**
     * Gets the action for the backing tasks for this instance.
     *
     * @return The action for the backing tasks for this instance.
     */
    public String getAction() {
      return this.action;
    }

    /**
     * Gets the <b>unmodifiable</b> {@link Map} describing the parameters for
     * the backing tasks for this instance.
     *
     * @return The <b>unmodifiable</b> {@link Map} describing the parameters
     *         for the backing tasks for this instance.
     */
    public SortedMap<String, Object> getParameters() {
      return this.parameters;
    }

    /**
     * Gets the <b>unmodifiable</b> {@link Set} containing the {@link
     * ResourceKey} instances identifying the resources for the backing tasks
     * for this instance.
     *
     * @return The <b>unmodifiable</b> {@link Set} containing the {@link
     *         ResourceKey} instances identifying the resources for the backing
     *         tasks for this instance.
     */
    public SortedSet<ResourceKey> getResourceKeys() {
      return this.resourceKeys;
    }

    /**
     * Gets the {@link List} of backing tasks associated with the scheduled
     * task.
     *
     * @return The {@link List} of {@link Task} instances describing the
     *         backing tasks for this instance.
     */
    public List<Task> getBackingTasks() {
      if (this.backingTasks == null) {
        return null;
      } else {
        return Collections.unmodifiableList(this.backingTasks);
      }
    }

    /**
     * Merges the specified {@link Task} with the other backing tasks of this
     * instance.
     *
     * @param task The {@link Task} to merge.
     */
    public void collapseWith(Task task) {
      // check if one the tasks does not allow collapse
      if (!this.isAllowingCollapse() || !task.isAllowingCollapse()) {
        throw new UnsupportedOperationException(
            "Cannot collapse specified task (" + task + ") with this task ("
            + this.backingTasks.get(0) + ") because at least one does not "
            + "allow collapse.");
      }

      // check if the task signatures do not match
      if (!this.getSignature().equals(task.getSignature())) {
        throw new IllegalArgumentException(
            "Cannot collapse the specified task (" + task + ") with this task ("
            + this.backingTasks.get(0) + ") because they are not duplicates.");
      }

      // add the backing tasks
      task.markScheduled();
      this.backingTasks.add(task);
    }

    /**
     * Gets the signature for the backing {@link Task} for this instance.
     *
     * @return The signature for the backing {@link Task} for this instance.
     */
    public String getSignature() {
      return this.signature;
    }

    /**
     * Checks whether the backing tasks allow collapsing duplicate tasks.
     *
     * @return <code>true</code> if the duplicate tasks can be collapsed with
     *         the backing task from this instance, and <code>false</code> if
     *         collapse is not allowed.
     */
    public boolean isAllowingCollapse() {
      return this.allowCollapse;
    }

    /**
     * Gets the number of duplicate tasks identical to this one that were
     * scheduled prior to the task being handled.
     *
     * @return The number of duplicate tasks like
     */
    public int getMultiplicity() {
      if (this.multiplicity != null) {
        return this.multiplicity;
      } else {
        return this.backingTasks.size();
      }
    }

    /**
     * Marks all the backing tasks to transition to the {@link
     * Task.State#STARTED} state via {@link Task#beginHandling()}.
     */
    public void beginHandling() {
      this.backingTasks.forEach((task) -> {
        task.beginHandling();
      });
    }

    /**
     * Marks this instance and the backing tasks as having succeeded.
     */
    public void succeeded() {
      this.successful = Boolean.TRUE;
      this.backingTasks.forEach((task) -> {
        task.succeeded();
      });
    }

    /**
     * Marks this instance and the backing tasks as having failed.
     *
     * @param failure The exception that occurred.
     */
    public void failed(Exception failure) {
      this.successful = Boolean.FALSE;
      this.backingTasks.forEach((task) -> {
        task.failed(failure);
      });
    }

    /**
     * Checks if this {@link ScheduledTask} has been flagged as successful.
     * This returns <code>null</code> if the {@link ScheduledTask} has not
     * yet been handled, otherwise it returns {@link Boolean#TRUE} or {@link
     * Boolean#FALSE}.
     *
     * @return {@link Boolean#TRUE} if successful, {@link Boolean#FALSE} if
     *         unsuccessful, and <code>null</code> if not yet completed.
     */
    public Boolean isSuccessful() {
      return this.successful;
    }

    /**
     * Acquires the locks on the resources required for this instance.  If
     * no locks are required this simply returns <code>true</code>.
     *
     * @param lockingService The {@link LockingService} to use.
     *
     * @return <code>true</code> if all required locks were obtained, otherwise
     *         <code>false</code>.
     */
    public synchronized boolean acquireLocks(LockingService lockingService) {
      if (this.lockToken != null) return true;

      Set<ResourceKey> resourceKeys = this.getResourceKeys();
      if (resourceKeys == null || resourceKeys.size() == 0) return true;

      try {
        this.lockToken = lockingService.acquireLocks(resourceKeys, 0L);

      } catch (ServiceExecutionException e) {
        throw new RuntimeException(e);
      }

      // check if the lock token is non-null
      return (this.lockToken != null);
    }

    /**
     * Releases any locks associated with the backing tasks.
     *
     * @param lockingService The {@link LockingService} with which to release
     *                       the locks.
     */
    public synchronized void releaseLocks(LockingService lockingService) {
      if (this.lockToken == null) return;

      Set<ResourceKey> resourceKeys = this.getResourceKeys();
      if (resourceKeys == null || resourceKeys.size() == 0) return;

      try {
        int count = lockingService.releaseLocks(this.lockToken);

        this.lockToken = null;

        if (this.resourceKeys.size() != count) {
          throw new IllegalStateException(
              "Wrong number of locks released.  released=[ " + count
                  + " ], expected=[ " + this.getResourceKeys().size() + " ]");
        }

      } catch (ServiceExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * The encapsulation of the result from the async workers.
   */
  protected static class TaskResult {
    /**
     * The {@link ScheduledTask} that was handled.
     */
    private ScheduledTask task;

    /**
     * The {@link Timers} associated with the handling of the associated task.
     */
    private Timers timers;

    /**
     * Constructs with the specified parameters.
     * @param task The {@link Task} that was handled.
     * @param timers The {@link Timers} for handling the task.
     */
    public TaskResult(ScheduledTask task,  Timers timers) {
      this.task   = task;
      this.timers = timers;
    }

    /**
     * Gets the associated {@link ScheduledTask}.
     * @return The associated {@link ScheduledTask}.
     */
    public ScheduledTask getTask() {
      return this.task;
    }

    /**
     * Gets the associated {@link Timers}.
     * @return The associated {@link Timers}.
     */
    public Timers getTimers() {
      return this.timers;
    }
  }
}
