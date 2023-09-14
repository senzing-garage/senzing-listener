package com.senzing.listener.service;

import com.senzing.listener.communication.MessageConsumer;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.listener.service.model.SzInfoMessage;
import com.senzing.listener.service.model.SzInterestingEntity;
import com.senzing.listener.service.model.SzNotice;
import com.senzing.listener.service.model.SzSampleRecord;
import com.senzing.listener.service.scheduling.*;
import com.senzing.util.JsonUtilities;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

import static com.senzing.util.JsonUtilities.*;
import static com.senzing.listener.service.ListenerService.State.*;
import static com.senzing.listener.service.ServiceUtilities.*;
import static com.senzing.listener.service.model.SzInfoMessage.*;
import static java.lang.Boolean.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * Provides a base class for {@link ListenerService} implementations.
 */
public abstract class AbstractListenerService implements ListenerService
{
  /**
   * The initialization parameter used by the default implementation of
   * {@link #initSchedulingService(JsonObject)} to specify the Java class name
   * of the {@link SchedulingService} to use.  If the default implementation of
   * {@link #initSchedulingService(JsonObject)} is overridden, then this key
   * may have no effect in the derived implementation.
   */
  public static final String SCHEDULING_SERVICE_CLASS_KEY = "schedulingService";

  /**
   * The default class name for the {@link #SCHEDULING_SERVICE_CLASS_KEY}
   * initialziation parameter if none is specified.  The
   */
  public static final String DEFAULT_SCHEDULING_SERVICE_CLASS_NAME
      = SQLiteSchedulingService.class.getName();

  /**
   * The initialization parameter referencing a JSON object or {@link String}
   * that represents the configuration for the {@link SchedulingService}
   * instance created by the default implementation of {@link
   * #initSchedulingService(JsonObject)} using the {@link
   * #SCHEDULING_SERVICE_CLASS_KEY} init parameter.  If the default
   * implementation of {@link #initSchedulingService(JsonObject)} is overridden,
   * then this key may have no effect in the derived implementation.
   */
  public static final String SCHEDULING_SERVICE_CONFIG_KEY
      = "schedulingServiceConfig";

  /**
   * Enumerates the various parts of the info message that can be scheduled
   * as actions when parsed.  These are used as keys in a {@link Map} provided
   * by the sub-class to map the message part to a scheduled action upon
   * construction.  Any message part that is excluded from the provided map on
   * construction will not have any associated scheduled tasks in the default
   * implementation of the task-scheduling functions, though they can be
   * overridden.
   */
  public enum MessagePart {
    /**
     * The record part of the message.
     */
    RECORD,

    /**
     * The part of the message associated with <b>each</b> affected entity
     * (i.e.: one per affected entity).
     */
    AFFECTED_ENTITY,

    /**
     * The part of the message associated with <b>each</b> interesting entity
     * (i.e.: one per interesting entity).
     */
    INTERESTING_ENTITY,

    /**
     * The part of the message associated with <b>each</b> notiece (i.e.: one
     * per notice).
     */
    NOTICE;
  }

  /**
   * The task parameter key for the entity ID.
   */
  public static final String ENTITY_ID_PARAMETER_KEY = "ENTITY_ID";

  /**
   * The task parameter key for the record ID.
   */
  public static final String RECORD_ID_PARAMETER_KEY = "RECORD_ID";

  /**
   * The task parameter key for the data source.
   */
  public static final String DATA_SOURCE_PARAMETER_KEY = "DATA_SOURCE";

  /**
   * The task parameter key for the interesting entity degrees of separation.
   */
  public static final String DEGREES_PARAMETER_KEY = "DEGREES";

  /**
   * The task parameter key for the interesting entity flags.
   */
  public static final String FLAGS_PARAMETER_KEY = "FLAGS";

  /**
   * The task parameter key for the interesting entity sample records.
   */
  public static final String SAMPLE_RECORDS_PARAMETER_KEY = "SAMPLE_RECORDS";

  /**
   * The task parameter key for the code parameter of notices.
   */
  public static final String CODE_PARAMETER_KEY = "CODE";

  /**
   * The task parameter for the description parameter of notices.
   */
  public static final String DESCRIPTION_PARAMETER_KEY = "DESCRIPTION";

  /**
   * The resourece key for locking a record.
   */
  public static final String RECORD_RESOURCE_KEY = "RECORD";

  /**
   * The resource key for locking an entity.
   */
  public static final String ENTITY_RESOURCE_KEY = "ENTITY";

  /**
   * A {@link TaskHandler} implementation that simply delegates to
   * {@link AbstractListenerService#handleTask(String, Map, int, Scheduler)}
   */
  protected class ListenerTaskHandler implements TaskHandler {
    /**
     * Default constructor.
     */
    public ListenerTaskHandler() {
      // do nothing
    }

    /**
     * Overridden to call {@link AbstractListenerService#waitUntilReady(long)}
     * on the parent object.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public Boolean waitUntilReady(long timeoutMillis)
      throws InterruptedException {
      return AbstractListenerService.this.waitUntilReady(timeoutMillis);
    }

    /**
     * Overridden to call {@link
     * AbstractListenerService#handleTask(String, Map, int, Scheduler)} on the
     * parent object.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void handleTask(String               action,
                           Map<String, Object>  parameters,
                           int                  multiplicity,
                           Scheduler            followUpScheduler)
        throws ServiceExecutionException
    {
      AbstractListenerService.this.handleTask(
          action, parameters, multiplicity, followUpScheduler);
    }
  }

  /**
   * The {@link Map} of {@link MessagePart} keys to {@link String} values
   */
  private Map<MessagePart, String> messagePartMap = null;

  /**
   * The {@link State} of this instance.
   */
  private State state = UNINITIALIZED;

  /**
   * The backing {@link SchedulingService}.
   */
  private SchedulingService schedulingService;

  /**
   * The {@link TaskHandler} for this instance.
   */
  private TaskHandler taskHandler = null;

  /**
   * Constructs with the {@link Map} that maps {@link MessagePart} keys to
   * {@link String} action names.
   *
   * @param messagePartMap The {@link Map} that maps {@link MessagePart}'s to
   *                       {@link String} action keys.
   */
  protected AbstractListenerService(Map<MessagePart, String> messagePartMap) {
    this.messagePartMap = new LinkedHashMap<>();
    this.messagePartMap.putAll(messagePartMap);
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
   *
   */
  @Override
  public synchronized Map<Statistic, Number> getStatistics() {
      return this.schedulingService.getStatistics();
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
   * This returns the {@link TaskHandler} that was given to the backing {@link
   * SchedulingService} for handling tasks during initialization.  If this is
   * called prior to initialization then this returns <code>null</code>.
   *
   * @return The {@link TaskHandler} that was obtained via {@link
   *         #initTaskHandler(JsonObject)} during initialization, or
   *         <code>null</code> if this instnace has not yet been initialized.
   */
  protected TaskHandler getTaskHandler() {
    return this.taskHandler;
  }

  /**
   * Creates the {@link TaskHandler} to use with the backing {@link
   * SchedulingService} for handling tasks.  This is called from {@link
   * #init(JsonObject)}. By default this returns a new instance of {@link
   * ListenerTaskHandler}.
   *
   * @param config The {@link JsonObject} describing the initialization config.
   * @return The {@link TaskHandler} that was created / initialized.
   * @throws ServiceExecutionException If a failure occurs in creating the
   *                                   {@link TaskHandler}.
   */
  protected TaskHandler initTaskHandler(JsonObject config)
    throws ServiceExecutionException
  {
    return new ListenerTaskHandler();
  }

  /**
   * Checks if this instance is ready to handle tasks and waits for
   * it to be ready for the specified maximum number of milliseconds.  Specify
   * a negative number of milliseconds to wait indefinitely or zero (0) to
   * simply check if ready with no waiting.  This is used so the {@link
   * SchedulingService} can delay handling tasks until ready.
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
  protected synchronized Boolean waitUntilReady(long timeoutMillis)
      throws InterruptedException
  {
    switch (this.getState()) {
      case AVAILABLE:
        return Boolean.TRUE;
      case DESTROYING:
      case DESTROYED:
        return null;
      default:
        if (timeoutMillis < 0L) {
          this.wait();
        } else if (timeoutMillis > 0L) {
          this.wait(timeoutMillis);
        }
        return (this.getState() == State.AVAILABLE) ? TRUE : FALSE;
    }
  }

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
  protected abstract void handleTask(String              action,
                                     Map<String, Object> parameters,
                                     int                 multiplicity,
                                     Scheduler           followUpScheduler)
      throws ServiceExecutionException;

  /**
   * Default implementation of {@link ListenerService#init(JsonObject)} that
   *
   * @throws ServiceSetupException If a failure occurs.
   */
  @Override
  public void init(JsonObject config) throws ServiceSetupException  {
    synchronized (this) {
      if (this.getState() != UNINITIALIZED) {
        throw new IllegalStateException(
            "Cannot initialize if not in the " + UNINITIALIZED + " state: "
                + this.getState());
      }
      this.setState(INITIALIZING);
    }

    try {
      synchronized (this) {
        // default to an empty JSON object if null
        if (config == null) {
          config = Json.createObjectBuilder().build();
        }

        // initializes the task handler
        this.taskHandler = this.initTaskHandler(config);

        // initialize the scheduling service
        this.schedulingService = this.initSchedulingService(config);
      }

      this.doInit(config);

      this.setState(AVAILABLE);

    } catch (ServiceSetupException e) {
      this.setState(UNINITIALIZED);
      throw e;

    } catch (Exception e) {
      this.setState(UNINITIALIZED);
      throw new ServiceSetupException(e);
    }
  }

  /**
   * Utility method to convert the task being handled to JSON for logging
   * purposes or for serialization.  The specified {@link Map} of parameters
   * should have values that can be converted to JSON via {@link
   * JsonUtilities#toJsonObjectBuilder(Map)}
   *
   * @param action The action for the task.
   * @param parameters The {@link Map} of parameters for the task.
   * @param multiplicity The multiplicity for the task.
   *
   * @return The JSON text describing the task.
   */
  protected String taskAsJson(String              action,
                              Map<String, Object> parameters,
                              int                 multiplicity)
  {
    JsonObjectBuilder job = Json.createObjectBuilder();
    job.add("action", action);
    job.add("parameters", toJsonObjectBuilder(parameters));
    job.add("multiplicity", multiplicity);
    return toJsonText(job.build());
  }

  /**
   * Override this to perform whatever other initialization is required.
   *
   * @param config The {@link JsonObject} describing the initialization
   *               configuration.
   *
   * @throws ServiceSetupException If a failure occurs.
   */
  protected abstract void doInit(JsonObject config)
    throws ServiceSetupException;

  /**
   * The default implementation of this method gets the class name from
   * the {@link #SCHEDULING_SERVICE_CLASS_KEY} parameter, constructs an instance
   * of that class using the default constructor and then initializes the
   * constructed {@link SchedulingService} instance using the {@link JsonObject}
   * found in the specified configuration via the {@link
   * #SCHEDULING_SERVICE_CONFIG_KEY} JSON property.
   * #SCHEDULING_SERVICE_CONFIG_KEY} JSON property.
   *
   * @param config The {@link JsonObject} describing the configuration for this
   *               instance of scheduling service.
   *
   * @return The {@link SchedulingService} that was created and initialized.
   * @throws ServiceSetupException If an initialziation failure occurs.
   */
  protected SchedulingService initSchedulingService(JsonObject config)
    throws ServiceSetupException
  {
    try {
      String className = getConfigString(
          config,
          SCHEDULING_SERVICE_CLASS_KEY,
          this.getDefaultSchedulingServiceClassName());

      // get the scheduling service Class object from the class name
      Class schedServiceClass = Class.forName(className);

      if (!SchedulingService.class.isAssignableFrom(schedServiceClass)) {
        throw new ServiceSetupException(
            "The configured scheduling service class for the "
            + SCHEDULING_SERVICE_CLASS_KEY + " config parameter must "
            + "implement " + SchedulingService.class.getName());
      }

      // create an instance of the SchedulingService class
      SchedulingService schedulingService = (SchedulingService)
          schedServiceClass.getConstructor().newInstance();

      // get the scheduling service configuration
      JsonObject schedServiceConfig
          = config.containsKey(SCHEDULING_SERVICE_CONFIG_KEY)
          ? getJsonObject(config, SCHEDULING_SERVICE_CONFIG_KEY)
          : this.getDefaultSchedulingServiceConfig();

      // initialize the scheduling service
      schedulingService.init(schedServiceConfig, this.getTaskHandler());

      // return the scheduling service
      return schedulingService;

    } catch (ServiceSetupException e) {
      throw e;

    } catch (Exception e) {
      throw new ServiceSetupException(
          "Failed to initialize SchedulingService for ListenerService", e);
    }
  }

  /**
   * Gets the default {@link SchedulingService} class name with which to
   * initialize the backing {@link SchedulingService} if one is not specified
   * in the initialization configuration via the {@link
   * #SCHEDULING_SERVICE_CLASS_KEY} initialization parameter.  By default, this
   * returns the {@link #DEFAULT_SCHEDULING_SERVICE_CLASS_NAME}, but it may be
   * overridden to return something more sensible for a derived implementation.
   *
   * @return The default {@link SchedulingService} class name with which to
   *         initialize.
   *
   * @see #initSchedulingService(JsonObject)
   * @see #getDefaultSchedulingServiceConfig()
   * @see #SCHEDULING_SERVICE_CLASS_KEY
   * @see #SCHEDULING_SERVICE_CONFIG_KEY
   * @see #DEFAULT_SCHEDULING_SERVICE_CLASS_NAME
   *
   */
  public String getDefaultSchedulingServiceClassName() {
    return DEFAULT_SCHEDULING_SERVICE_CLASS_NAME;
  }

  /**
   * Gets the default {@link JsonObject} configuration with which to initialize
   * the backing {@link SchedulingService} if one is not specified in the
   * initialization configuration via the {@link #SCHEDULING_SERVICE_CONFIG_KEY}
   * initialization parameter.  By default, this returns the <code>null</code>,
   * but it may be overridden to return something more sensible for a derived
   * implementation.
   *
   * @return The default {@link JsonObject} configuration with which to
   *         initialize the backing {@link SchedulingService}.
   *
   * @see #initSchedulingService(JsonObject)
   * @see #getDefaultSchedulingServiceClassName()
   * @see #SCHEDULING_SERVICE_CLASS_KEY
   * @see #SCHEDULING_SERVICE_CONFIG_KEY
   */
  public JsonObject getDefaultSchedulingServiceConfig() {
    return null;
  }

  /**
   * Processes the message described by the specified {@link JsonObject}.
   *
   * @param message The {@link JsonObject} describing the message.
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  @Override
  public void process(JsonObject message) throws ServiceExecutionException
  {
    try {
      // check the state
      if (this.getState() != AVAILABLE) {
        throw new IllegalStateException(
            "Cannot process messages when not in the " + AVAILABLE + " state: "
                + state);
      }

      // get the scheduler
      Scheduler scheduler = this.schedulingService.createScheduler();

      // get the task group
      TaskGroup taskGroup = scheduler.getTaskGroup();
      if (taskGroup == null) {
        throw new IllegalStateException("The TaskGroup should not be null");
      }

      // schedule the tasks
      this.scheduleTasks(message, scheduler);

      // commit the scheduler tasks
      scheduler.commit();

      // wait for the tasks to be completed
      logDebug("AWAITING COMPLETION ON TASK GROUP: " + taskGroup.getTaskCount());
      taskGroup.awaitCompletion();
      logDebug("COMPLETED TASK GROUP: " + taskGroup.getTaskCount());

      // determine the state of the group
      TaskGroup.State groupState = taskGroup.getState();
      logDebug("COMPLETED TASK GROUP STATE: " + groupState);
      if (groupState == TaskGroup.State.SUCCESSFUL) return;

      // if we get here then we had a failure
      List<Task> failedTasks = taskGroup.getFailedTasks();
      if (failedTasks.size() == 1) {
        Exception failure = failedTasks.get(0).getFailure();
        if (failure instanceof ServiceExecutionException) {
          throw ((ServiceExecutionException) failure);
        } else {
          throw new ServiceExecutionException(failure);
        }
      } else {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        for (Task failedTask : failedTasks) {
          Exception failure = failedTask.getFailure();
          pw.println("----------------------------------------");
          pw.println(failedTask);
          failure.printStackTrace(pw);
          pw.println();
        }
        throw new ServiceExecutionException(pw.toString());
      }
    } catch (ServiceExecutionException e) {
      throw e;

    } catch (RuntimeException e) {
      e.printStackTrace();
      throw new ServiceExecutionException(e);
    }
  }

  /**
   * Schedules the tasks for the specified message using the specified
   * {@link Scheduler}.
   *
   * @param message The {@link JsonObject} for the message.
   * @param scheduler The {@link Scheduler} to use for the tasks.
   * @throws ServiceExecutionException If a failure occurs.
   */
  protected void scheduleTasks(JsonObject message, Scheduler scheduler)
    throws ServiceExecutionException
  {
    SzInfoMessage infoMessage = SzInfoMessage.fromRawJson(message);

    // handle the record first
    String dataSource = getString(message, RAW_DATA_SOURCE_KEY);
    String recordId = getString(message, RAW_RECORD_ID_KEY);
    this.handleRecord(dataSource, recordId, infoMessage, message, scheduler);

    // now handle the affected entities
    JsonArray jsonArray = getJsonArray(message, RAW_AFFECTED_ENTITIES_KEY);
    if (jsonArray != null) {
      for (JsonObject affected : jsonArray.getValuesAs(JsonObject.class)) {
        Long entityId = getLong(affected, RAW_ENTITY_ID_KEY);
        this.handleAffected(
            entityId, infoMessage, affected, message, scheduler);
      }
    }

    // now handle the interesting entities
    JsonObject jsonObj = getJsonObject(message, RAW_INTERESTING_ENTITIES_KEY);
    if (jsonObj != null) {
      jsonArray = getJsonArray(jsonObj, RAW_ENTITIES_KEY);
      if (jsonArray != null) {
        Iterator<SzInterestingEntity> iter
            = infoMessage.getInterestingEntities().iterator();
        for (JsonObject interesting : jsonArray.getValuesAs(JsonObject.class)) {
          SzInterestingEntity next = iter.next();
          this.handleInteresting(
              next, infoMessage, interesting, message, scheduler);
        }
      }

      // handle the notices
      jsonArray = getJsonArray(message, "NOTICES");
      if (jsonArray != null) {
        Iterator<SzNotice> noticeIter = infoMessage.getNotices().iterator();
        for (JsonObject notice : jsonArray.getValuesAs(JsonObject.class)) {
          SzNotice next = noticeIter.next();
          this.handleNotice(next, infoMessage, notice, message, scheduler);
        }
      }
    }
  }

  /**
   * Returns the {@link String} action identifier for the specified
   * {@link MessagePart}.  The default implementation of this uses the
   * {@link Map} with which this instance was constructed.
   *
   * @param messagePart The {@link MessagePart} for which the action is being
   *                    requested.
   * @return The associated action or <code>null</code> if no action should
   *         be mapped to the {@link MessagePart}.
   */
  protected String getActionForMessagePart(MessagePart messagePart) {
    return this.messagePartMap.get(messagePart);
  }

  /**
   * This method is called for the data source code and record ID found in the
   * root of the INFO message.  If {@link
   * #getActionForMessagePart(MessagePart)} returns <code>null</code> for
   * {@link MessagePart#RECORD} then this method does nothing (but
   * may be overridden), otherwise if there is an associated task for the
   * {@link MessagePart#RECORD}, then a new {@link Task} is scheduled using the
   * specified {@link Scheduler} with the associated action key and the
   * following parameters and required resources:
   * <ul>
   *   <li>Parameter: {@link #DATA_SOURCE_PARAMETER_KEY} (string)</li>
   *   <li>Parameter: {@link #RECORD_ID_PARAMETER_KEY} (string)</li>
   *   <li>Resource: {@link #RECORD_RESOURCE_KEY} (for the associated record)</li>
   * </ul>
   *
   * @param dataSource The {@link String} data source code from the info message.
   * @param recordId The {@link String} record ID from the info message.
   * @param infoMessage The {@link SzInfoMessage} describing the INFO message.
   * @param rawMessage The entire INFO message in its raw form.
   * @param scheduler The {@link Scheduler} to be used to schedule the task.
   */
  protected void handleRecord(String        dataSource,
                              String        recordId,
                              SzInfoMessage infoMessage,
                              JsonObject    rawMessage,
                              Scheduler     scheduler)
  {
    String action = this.messagePartMap.get(MessagePart.RECORD);
    if (action == null || action.trim().length() == 0) return;
    scheduler.createTaskBuilder(action)
        .parameter(DATA_SOURCE_PARAMETER_KEY, dataSource)
        .parameter(RECORD_ID_PARAMETER_KEY, recordId)
        .resource(RECORD_RESOURCE_KEY, dataSource, recordId)
        .schedule();
  }

  /**
   * This method is called for each entity ID in the
   * <code>AFFECTED_ENTITIES</code> found in an INFO message.  If
   * {@link #getActionForMessagePart(MessagePart)} returns <code>null</code> for
   * {@link MessagePart#AFFECTED_ENTITY} then this method does nothing (but
   * may be overridden), otherwise if there is an associated task for the
   * {@link MessagePart#AFFECTED_ENTITY}, then a new {@link Task} is
   * scheduled using the specified {@link Scheduler} with the associated
   * action key and the following parameters and required resources:
   * <ul>
   *   <li>Parameter: {@link #ENTITY_ID_PARAMETER_KEY} (long integer)</li>
   *   <li>Resource: {@link #ENTITY_RESOURCE_KEY} (for the associated entity)</li>
   * </ul>
   *
   * @param entityId The {@link Long} entity ID identifying the affected entity.
   * @param infoMessage The {@link SzInfoMessage} describing the INFO message.
   * @param rawMessagePart The {@link JsonObject} describing the interesting
   *                       entity in its raw JSON form.
   * @param rawMessage The entire INFO message in its raw form.
   * @param scheduler The {@link Scheduler} to be used to schedule the task.
   */
  protected void handleAffected(long          entityId,
                                SzInfoMessage infoMessage,
                                JsonObject    rawMessagePart,
                                JsonObject    rawMessage,
                                Scheduler     scheduler)
  {
    String action = this.messagePartMap.get(MessagePart.AFFECTED_ENTITY);
    if (action == null || action.trim().length() == 0) return;
    scheduler.createTaskBuilder(action)
        .parameter(ENTITY_ID_PARAMETER_KEY, entityId)
        .resource(ENTITY_RESOURCE_KEY, entityId)
        .schedule();
  }

  /**
   * This method is called for each element in the
   * <code>INTERESTING_ENTITIES</code> found in an INFO message.  If
   * {@link #getActionForMessagePart(MessagePart)} returns <code>null</code> for
   * {@link MessagePart#INTERESTING_ENTITY} then this method does nothing (but
   * may be overridden), otherwise if there is an associated task for the
   * {@link MessagePart#INTERESTING_ENTITY}, then a new {@link Task} is
   * scheduled using the specified {@link Scheduler} with the associated
   * action key and the following parameters and required resources:
   * <ul>
   *   <li>Parameter: {@link #ENTITY_ID_PARAMETER_KEY} (long integer)</li>
   *   <li>Parameter: {@link #DEGREES_PARAMETER_KEY} (integer)</li>
   *   <li>Parameter: {@link #FLAGS_PARAMETER_KEY} (array of strings)</li>
   *   <li>Parameter: {@link #SAMPLE_RECORDS_PARAMETER_KEY} (array of objects
   *       matching the format from {@link SzSampleRecord#toJsonObject()})
   *   </li>
   *   <li>Resource: {@link #ENTITY_RESOURCE_KEY} (for the associated entity)</li>
   * </ul>
   *
   * @param interestingEntity The {@link SzInterestingEntity} describing the
   *                          interesting entity to handle.
   * @param infoMessage The {@link SzInfoMessage} describing the INFO message.
   * @param rawMessagePart The {@link JsonObject} describing the interesting
   *                       entity in its raw JSON form.
   * @param rawMessage The entire INFO message in its raw form.
   * @param scheduler The {@link Scheduler} to be used to schedule the task.
   */
  protected void handleInteresting(SzInterestingEntity  interestingEntity,
                                   SzInfoMessage        infoMessage,
                                   JsonObject           rawMessagePart,
                                   JsonObject           rawMessage,
                                   Scheduler            scheduler)
  {
    String action = this.messagePartMap.get(MessagePart.INTERESTING_ENTITY);
    if (action == null || action.trim().length() == 0) return;

    // begin building the task with the basic parameters
    TaskBuilder.ListParamBuilder builder = scheduler.createTaskBuilder(action)
        .parameter(ENTITY_ID_PARAMETER_KEY, interestingEntity.getEntityId())
        .parameter(DEGREES_PARAMETER_KEY, interestingEntity.getDegrees())
        .listParameter(FLAGS_PARAMETER_KEY);

    // add the flags to the list parameter
    for (String flag : interestingEntity.getFlags()) {
      builder.add(flag);
    }

    // end the list and start the sample records parameter
    builder = builder.endList().listParameter(SAMPLE_RECORDS_PARAMETER_KEY);

    // add the sample records to the list parameter
    for (SzSampleRecord record : interestingEntity.getSampleRecords()) {
      builder.add(record.toJsonObject());
    }

    // end the list, add the required resources and schedule the task
    builder.endList()
        .resource(ENTITY_RESOURCE_KEY, interestingEntity.getEntityId())
        .schedule();
  }

  /**
   * This method is called for each element in the <code>NOTICES</code> array
   * found in an INFO message.  If {@link #getActionForMessagePart(MessagePart)}
   * returns <code>null</code> for {@link MessagePart#NOTICE} then this method
   * does nothing (but may be overridden), otherwise if there is an associated
   * task for the {@link MessagePart#NOTICE}, then a new {@link Task} is
   * scheduled using the specified {@link Scheduler} with the associated
   * action key and the following parameters and required resources:
   * <ul>
   *   <li>Parameter: {@link #CODE_PARAMETER_KEY} (string)</li>
   *   <li>Parameter: {@link #DESCRIPTION_PARAMETER_KEY} (string)</li>
   *   <li>Resources: [None]</li>
   * </ul>
   *
   * @param notice The {@link SzNotice} describing the notice.
   * @param infoMessage The {@link SzInfoMessage} describing the INFO message.
   * @param rawMessagePart The {@link JsonObject} describing the notice in its
   *                       raw JSON form.
   * @param rawMessage The entire INFO message.
   * @param scheduler The {@link Scheduler} to be used to schedule the task.
   */
  protected void handleNotice(SzNotice      notice,
                              SzInfoMessage infoMessage,
                              JsonObject    rawMessagePart,
                              JsonObject    rawMessage,
                              Scheduler     scheduler)
  {
    String action = this.getActionForMessagePart(MessagePart.NOTICE);
    if (action == null || action.trim().length() == 0) return;
    scheduler.createTaskBuilder(action)
        .parameter(CODE_PARAMETER_KEY, notice.getCode())
        .parameter(DESCRIPTION_PARAMETER_KEY, notice.getDescription())
        .schedule();
  }

  /**
   * Implemented as a synchronized method to {@linkplain #setState(State)
   * set the state} to {@link MessageConsumer.State#DESTROYING}, call {@link #doDestroy()} and
   * then perform {@link #notifyAll()} and set the state to {@link
   * MessageConsumer.State#DESTROYED}.
   */
  public void destroy() {
    synchronized (this) {
      State state = this.getState();
      if (state == DESTROYED) return;

      if (state == DESTROYING) {
        while (this.getState() != DESTROYED) {
          try {
            this.wait(1000L);
          } catch (InterruptedException e) {
            // ignore
          }
        }
        // once DESTROYED state is found, just return
        return;
      }

      // begin destruction
      this.setState(DESTROYING);
    }

    // destroy the scehduling service
    this.schedulingService.destroy();

    try {
      // now complete the destruction / cleanup
      this.doDestroy();

    } finally {
      this.setState(DESTROYED); // this should notify all as well
    }
  }

  /**
   * Gets the backing {@link SchedulingService} used by this instance.
   *
   * @return The backing {@link SchedulingService} used by this instance.
   */
  protected SchedulingService getSchedulingService() {
    return this.schedulingService;
  }
  /**
   * This is called from the {@link #destroy()} implementation and should be
   * overridden by the concrete sub-class.
   */
  protected abstract void doDestroy();
}
