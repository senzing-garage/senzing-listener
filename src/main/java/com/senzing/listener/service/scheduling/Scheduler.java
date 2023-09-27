package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.exception.ServiceExecutionException;

/**
 * Provides an interface for creating new tasks to be scheduled and
 * scheduling them.
 */
public abstract class Scheduler {
  /**
   * Creates a new {@link TaskBuilder} for creating and scheduling tasks with
   * this instance.
   *
   * @param action The non-null {@link String} describing the action to take.
   * @return The {@link TaskBuilder} created to create and schedule a task with
   *         this instance.
   */
  public abstract TaskBuilder createTaskBuilder(String action);

  /**
   * Gets the associated {@link TaskGroup} (if any).  This method returns
   * <code>null</code> if there is no associated {@link TaskGroup} (e.g.: in
   * the case of creating follow-up tasks).
   *
   * @return The associated {@link TaskGroup}, or <code>null</code> if no
   *         {@link TaskGroup} is associated.
   */
  public abstract TaskGroup getTaskGroup();

  /**
   * Protected method scheduling a task that has been built.  This method
   * exists so that {@link TaskBuilder} can call back into the {@link Scheduler}
   * to actually schedule the task that has been built; however, the public
   * interface for scheduling tasks is through the {@link TaskBuilder} obtained
   * from {@link #createTaskBuilder(String)}.  The scheduled {@link Task} gets
   * added to the list of pending tasks that must be committed before having it
   * actually get scheduled.
   *
   * @param task The {@link Task} to schedule.
   */
  protected abstract void schedule(Task task);

  /**
   * Gets the number of tasks that have been scheduled that are pending
   * commit.  Do not call {@link #commit()} until you are done creating
   * tasks with the scheduler instance.
   *
   * @return The number of tasks that have been scheduled that are pending
   *         commit.
   */
  public abstract int getPendingCount();

  /**
   * Commits the tasks that have been scheduled to the underlying {@link
   * SchedulingService}.  Once committed the {@link Scheduler} can no longer
   * be used.  This method should be called at the conclusion of task creation
   * and scheduling.  If the {@link Scheduler} has already been committed then
   * this method does nothing.
   *
   * @return The number of tasks that were scheduled.
   *
   * @throws ServiceExecutionException If a failure occurs scheduling the tasks
   *                                   with the {@link SchedulingService}.
   */
  public abstract int commit() throws ServiceExecutionException;
}
