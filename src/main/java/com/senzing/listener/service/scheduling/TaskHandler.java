package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;

import javax.json.JsonObject;
import java.util.Map;

/**
 * An interface for converting messages into scheduled tasks and then handling
 * those tasks.
 */
public interface TaskHandler {
  /**
   * Checks if this {@link TaskHandler} is ready to handle tasks and waits for
   * it to be ready for the specified maximum number of milliseconds.  Specify
   * a negative number of milliseconds to wait indefinitely or zero (0) to
   * simply check if ready with no waiting.  This is used by the {@link
   * SchedulingService} to know when to begin handling tasks.
   *
   * @param timeoutMillis The maximum number of milliseconds to wait for this
   *                      task handler to become ready, a negative number to
   *                      wait indefinitely, or zero (0) to simply poll without
   *                      waiting.
   *
   * @return {@link Boolean#TRUE} if ready to handle tasks, {@link
   *         Boolean#FALSE} if not yet ready, and <code>null</code> if due to
   *         some failure we will never be ready to handle tasks.
   *
   * @throws InterruptedException If interrupted while waiting.
   */
  Boolean waitUntilReady(long timeoutMillis) throws InterruptedException;

  /**
   * Called to handle the specified {@link Task} with an optional {@link
   * Scheduler} for scheduling follow-up tasks if that is allowed.
   * Additionally, a multiplicity is specified which, if greater than one (1),
   * may require that the task be handled in a different way depending on the
   * {@linkplain Task#getAction() action} associated with the {@link Task}.
   * Typically, follow-up tasks may not be allowed if the specified
   * {@link Task} is itself a follow-up {@link Task}.
   *
   * @param action The action from the {@link Task} to be handled.
   * @param parameters The {@link Map} of parameters to use with the action to
   *                   be taken.
   * @param multiplicity The number of times an identical task was scheduled.
   * @param followUpScheduler The {@link Scheduler} for scheduling follow-up
   *                          tasks, or <code>null</code> if follow-up tasks
   *                          cannot be scheduled.
   * @throws ServiceExecutionException If a failure occurred in handling the
   *                                   task.
   */
  void handleTask(String              action,
                  Map<String, Object> parameters,
                  int                 multiplicity,
                  Scheduler           followUpScheduler)
    throws ServiceExecutionException;
}
