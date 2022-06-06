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
   * Initializes this instance with the configuration described by the specifed
   * {@link JsonObject}.
   *
   * @param config The {@link JsonObject} describing the configuration.
   * @throws ServiceSetupException If a failure occurs in initialization.
   */
  void init(JsonObject config) throws ServiceSetupException;

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
  void handle(String              action,
              Map<String, Object> parameters,
              int                 multiplicity,
              Scheduler           followUpScheduler)
    throws ServiceExecutionException;

  /**
   * Destroys this {@link TaskHandler} and performs any required cleanup.
   */
  void destroy();
}
