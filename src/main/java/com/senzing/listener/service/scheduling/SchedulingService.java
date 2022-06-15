package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.listener.service.locking.LockingService;

import javax.json.JsonObject;

/**
 * Defines a service to handle scheduling tasks, locking resources for executing
 * those tasks, and then executing those tasks when once the resources have
 * been locked.
 */
public interface SchedulingService {
  /**
   * Enumerates the states of a {@link SchedulingService}.
   */
  enum State {
    /**
     * The {@link SchedulingService} has not yet been initialized.
     */
    UNINITIALIZED,

    /**
     * The {@link SchedulingService} is initializing, but has not finished
     * initializing.
     */
    INITIALIZING,

    /**
     * The {@link SchedulingService} has completed initialization, but has not
     * yet had a task scheduled and dispatched.
     */
    READY,

    /**
     * The {@link SchedulingService} is actively scheduling and handling tasks,
     */
    ACTIVE,

    /**
     * The {@link SchedulingService} has begun destruction, but may still be
     * processing whatever messages were in progress.
     */
    DESTROYING,

    /**
     * The {@link SchedulingService} is no longer processing messages and has
     * been destroyed.
     */
    DESTROYED;

    /**
     * Checks if in this instance describes a state in which the
     * {@link SchedulingService} would allow tasks to be scheduled.  The
     * states for which this returns <code>true</code> are {@link #READY}
     * and {@link #ACTIVE}, for all other states it returns <code>false</code>.
     *
     * @return <code>true</code> if a {@link SchedulingService} in this state
     *         is available for tasks to be scheduled, otherwise
     *         <code>false</code>.
     */
    public boolean isAvailable() {
      switch (this) {
        case READY:
        case ACTIVE:
          return true;
        default:
          return false;
      }
    }
  }

  /**
   * Gets the {@link State} of this instance.
   *
   * @return The {@link State} of this instance.
   */
  State getState();

  /**
   * Initializes the scheduling service with the specified configuration.
   *
   * @param config The {@link JsonObject} configuration.
   * @param taskHandler The {@link TaskHandler} to use for handling tasks.
   *
   * @throws ServiceSetupException If a failure occurs.
   */
  void init(JsonObject config, TaskHandler taskHandler)
      throws ServiceSetupException;

  /**
   * Creates a {@link Scheduler} with a unique {@link TaskGroup} that will
   * schedule tasks with this {@link SchedulingService} instance.  If you want
   * to schedule tasks that are part of different groups you can call this
   * method multiple times since each returned {@link Scheduler} will have a
   * different {@link TaskGroup}.
   *
   * @return A {@link Scheduler} instance that is backed by this instance.
   */
  Scheduler createScheduler();

  /**
   * Gets the {@link TaskHandler} for this instance.
   *
   * @return The {@link TaskHandler} for this instance.
   */
  TaskHandler getTaskHandler();

  /**
   * Gets the {@link LockingService} used to obtain resource locks before
   * executing the tasks that depend on those resources.
   *
   * @return The {@link LockingService} that is used to
   */
  LockingService getLockingService();

  /**
   * Prevents further tasks from being scheduled, handles any pending tasks,
   * persists any follow-up tasks that have not been persisted and releases
   * any resources that were allocated.  This method transitions this instance
   * to the {@link State#DESTROYING} state and then the {@link State#DESTROYED}
   * state.
   */
  void destroy();
}
