package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.exception.ServiceExecutionException;

import java.util.List;
import java.util.Objects;

/**
 * Provides a default implementation of {@link Scheduler} that works with
 * classes that extend {@link AbstractSchedulingService}.
 */
public class DefaultScheduler implements Scheduler {
  /**
   * The underlying {@link AbstractSchedulingService} to schedule the tasks
   * with.
   */
  private AbstractSchedulingService service;

  /**
   * The {@link TaskGroup} to associated with the scheduled tasks if there
   * should be a {@link TaskGroup}.  Generally, {@link TaskGroup} is used for
   * standard tasks while a <code>null</code> {@link TaskGroup} indicates a
   * follow-up {@link Task} that may be serialized and deferred until later.
   */
  private TaskGroup taskGroup;

  /**
   * The tasks that have been scheduled but are pending commit to the underlying
   * {@link SchedulingService}.
   */
  private List<Task> pendingTasks;

  /**
   * Constructs with the specified non-null {@link AbstractSchedulingService}.
   *
   * @param service The non-null {@link AbstractSchedulingService} to use.
   */
  protected DefaultScheduler(AbstractSchedulingService service) {
    this(service, null);
  }

  /**
   * Constructs with the specified non-null {@link AbstractSchedulingService}
   * and optional {@link TaskGroup}.
   *
   * @param service The non-null {@link AbstractSchedulingService} to use.
   * @param taskGroup The optional {@link TaskGroup} with which to construct.
   */
  protected DefaultScheduler(AbstractSchedulingService  service,
                             TaskGroup                  taskGroup)
  {
    Objects.requireNonNull(
        service, "The SchedulingService cannot be null.");
    this.service    = service;
    this.taskGroup  = taskGroup;
  }

  /**
   * Checks if this {@link DefaultScheduler} has already been committed and if so then
   * throws an {@link IllegalStateException}.
   */
  protected void checkState() throws IllegalStateException {
    if (this.service == null) {
      throw new IllegalStateException(
          "This Scheduler instance has already been committed.");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TaskBuilder createTaskBuilder(String action)
      throws NullPointerException, IllegalStateException
  {
    Objects.requireNonNull(action, "The action cannot be null");
    this.checkState();
    return new TaskBuilder(this, action);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TaskGroup getTaskGroup() {
    return this.taskGroup;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getPendingCount() {
    return this.pendingTasks.size();
  }

  /**
   * {@inheritDoc}
   */
  public int commit() throws ServiceExecutionException {
    if (this.service == null) return 0;
    if (this.taskGroup == null)  return 0;

    this.service.scheduleTasks(this.pendingTasks);
    int count = this.pendingTasks.size();
    this.pendingTasks.clear();
    this.service = null;
    return count;
  }
}
