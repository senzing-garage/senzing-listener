package com.senzing.listener.service.g2;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import com.senzing.g2.engine.*;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.util.AccessToken;
import com.senzing.util.AsyncWorkerPool;
import com.senzing.util.JsonUtilities;
import com.senzing.util.WorkerThreadPool;

import static com.senzing.g2.engine.G2Engine.*;
import static com.senzing.io.IOUtilities.*;
import static com.senzing.listener.service.g2.G2Service.State.*;
import static com.senzing.listener.service.ServiceUtilities.*;
import static com.senzing.util.JsonUtilities.*;
import static com.senzing.util.LoggingUtilities.*;

/**
 * This class handles communication with G2.  It sets up an instance of G2 and
 * interacts with it (get entities etc.).
 */
public class G2Service {
  /**
   * The initialization parameter for specifying the number of threads to use
   * for executing Senzing G2 operations.  If not specified, then this defaults
   * to the logical core count of the machine.
   */
  public static final String CONCURRENCY_KEY = "concurrency";

  /**
   * The initialization parameter for specifying the Senzing G2 initialization
   * config JSON for initializing the Senzing G2 engine.  This initialization
   * parameter is required.  It can be specified in several ways:
   * <ol>
   *   <li>As a file name containing INI-formatted initialization config</li>
   *   <li>As a file name containing JSON-formatted initialization config</li>
   *   <li>As JSON text that can be parsed as the configuration</li>
   *   <li>As a {@link JsonObject} representing the actual configuration</li>
   * </ol>
   */
  public static final String G2_INIT_CONFIG_KEY = "g2InitConfig";

  /**
   * The initialization parameter for specifying the module name with which
   * to initialize the Senzing G2 engine.  If not specified then {@link
   * #DEFAULT_MODULE_NAME} is used.
   */
  public static final String G2_MODULE_NAME_KEY = "g2ModuleName";

  /**
   * The initialization parameter for the <code>boolean</code> value
   * indicating if the G2 engine should be started in verbose mode.  If
   * not specified then this defaults to <code>false</code>.
   */
  public static final String G2_VERBOSE_KEY = "g2Verbose";

  /**
   * Error code when the entity ID is not found.
   */
  private static final int ENTITY_NOT_FOUND_CODE = 37;

  /**
   * Error code when a data source code is not found.
   */
  private static final int DATA_SOURCE_NOT_FOUND_CODE = 27;

  /**
   * Error code when a record ID is not found.
   */
  private static final int RECORD_NOT_FOUND_CODE = 33;

  /**
   * Enumerates the states of {@link G2Service}.
   */
  public enum State {
    /**
     * The service is uninitialized.
     */
    UNINITIALIZED,

    /**
     * The service is initialized and ready to handle requests.
     */
    READY,

    /**
     * The service has begun destroying itself.
     */
    DESTROYING,

    /**
     * The service has been destroyed.
     */
    DESTROYED;
  }

  /**
   * The {@link State} for this instance.
   */
  private State state = UNINITIALIZED;

  /**
   * The read-write lock to use to prevent simultaneous request handling with
   * initialization, reinitialization or destroying.
   */
  protected final ReadWriteLock monitor = new ReentrantReadWriteLock();

  /**
   * The object to synchronize
   */
  protected final Object reinitMonitor = new Object();

  /**
   * The {@link G2Engine} to use.
   */
  protected G2Engine engineApi;

  /**
   * The {@link G2ConfigMgr} configuration manager API.
   */
  protected G2ConfigMgr configMgrApi;

  /**
   * The set of configured data sources.
   */
  protected Set<String> dataSources;

  /**
   * The {@link Map} of FTYPE_CODE values to ATTR_CLASS values from the config.
   */
  protected Map<String, String> featureToAttrClassMap;

  /**
   * The {@link Map} of ATTR_CODE values to ATTR_CLASS values from the config.
   */
  protected Map<String, String> attrCodeToAttrClassMap;

  /**
   * The module name used to initialize the {@link G2Engine}.
   */
  private static final String DEFAULT_MODULE_NAME = "G2JNI";

  /**
   * The {@link AsyncWorkerPool} to handle
   */
  private WorkerThreadPool workerPool = null;

  /**
   * The Senzing G2 config.
   */
  private JsonObject g2Config = null;

  /**
   * The Senzing G2 config JSON text.
   */
  private String g2ConfigJson = null;

  /**
   * Default constructor.
   */
  public G2Service() {
    // do nothing
  }

  /**
   * Gets the {@link State} of this instance.
   *
   * @return The {@link State} of this instance.
   */
  public State getState() {
    this.monitor.readLock().lock();
    try {
      return this.state;
    } finally {
      this.monitor.readLock().unlock();
    }
  }

  /**
   * Provides a means to set the {@link State} for this instance.
   *
   * @param state The {@link State} for this instance.
   */
  protected void setState(State state) {
    this.monitor.writeLock().lock();
    try {
      Objects.requireNonNull(state, "State cannot be null");
      this.state = state;
    } finally {
      this.monitor.writeLock().unlock();
    }
  }

  /**
   * Gets the {@link JsonObject} describing the Senzing G2 config
   * obtained during the {@link #init(JsonObject)} call.  This returns
   * <code>null</code> if this instance has not yet been initialized.
   * 
   * @return The {@link JsonObject} describing the Senzing G2 config for this instance.
   */
  protected JsonObject getG2Config() {
    return this.g2Config;
  }

  /**
   * Gets the JSON text describing the Senzing G2 config
   * obtained during the {@link #init(JsonObject)} call.  This returns
   * <code>null</code> if this instance has not yet been initialized.
   * 
   * @return The JSON text describing the Senzing G2 config for this instance.
   */
  protected String getG2ConfigJson() {
    return this.g2ConfigJson;
  }

  /**
   * Initializes the service.  The specified {@link JsonObject} should contain
   * the following properties:
   * <ul>
   *   <li>{@link #G2_INIT_CONFIG_KEY} (required)</li>
   *   <li>{@link #G2_MODULE_NAME_KEY} (optional)</li>
   *   <li>{@link #G2_VERBOSE_KEY} (optional)</li>
   *   <li>{@link #CONCURRENCY_KEY} (optional)</li>
   * </ul>
   *
   * @param config The {@link JsonObject} with the initialization parameters
   *               defined for this class.
   *
   * @throws ServiceSetupException If failure occurs in initialization.
   */
  public void init(JsonObject config)
      throws ServiceSetupException
  {
    this.monitor.writeLock().lock();
    try {
      if (this.getState() != UNINITIALIZED) {
        throw new IllegalStateException(
            "Must be in the " + UNINITIALIZED
                + " state in order to call init(): " + this.getState());
      }

      // get the init config
      if (!config.containsKey(G2_INIT_CONFIG_KEY)) {
        throw new ServiceSetupException(
            "The " + G2_INIT_CONFIG_KEY + " initialization parameter is "
            + "required, but was not specified.  config=[ "
            + toJsonText(config));
      }

      // discover how the parameter is specified
      JsonValue initConfigValue = config.get(G2_INIT_CONFIG_KEY);
      String    initConfig      = null;
      switch (initConfigValue.getValueType()) {
        case STRING:
          initConfig = getG2IniDataAsJson(config.getString(G2_INIT_CONFIG_KEY));
          break;
        case OBJECT:
          initConfig = toJsonText(config.getJsonObject(G2_INIT_CONFIG_KEY));
          break;
        default:
          throw new ServiceSetupException(
              "The " + G2_INIT_CONFIG_KEY + " initialization parameter can "
                  + "either be a " + JsonValue.ValueType.STRING + " or an "
                  + JsonValue.ValueType.OBJECT + ", but not "
                  + initConfigValue.getValueType() + ": "
                  + toJsonText(initConfigValue));
      }

      // set the private variables
      this.g2Config     = JsonUtilities.parseJsonObject(initConfig);
      this.g2ConfigJson = initConfig; 

      // get the parameters
      boolean verbose = getConfigBoolean(config, G2_VERBOSE_KEY, Boolean.FALSE);

      String moduleName = getConfigString(
          config, G2_MODULE_NAME_KEY, DEFAULT_MODULE_NAME);

      Integer concurrency = getConfigInteger(
          config, CONCURRENCY_KEY, false, 1);

      // check if the worker pool is not yet initialized
      if (concurrency == null) {
        // initialize the worker pool with the logical core count as the size
        G2Diagnostic diagnostic = new G2DiagnosticJNI();
        diagnostic.init(DEFAULT_MODULE_NAME, initConfig, verbose);
        concurrency = diagnostic.getLogicalCores();
        diagnostic.destroy();
      }

      // initialize the config manager api
      this.configMgrApi = new G2ConfigMgrJNI();
      int result = this.configMgrApi.init(moduleName, initConfig, verbose);
      if (result != 0) {
        String errorMsg = "Failed to initialize G2ConfigMgr: "+ formatError(
            "G2ConfigMgr.init", this.configMgrApi);
        logError(errorMsg);
        throw new ServiceSetupException(errorMsg);
      }

      // now initialize the engine
      this.engineApi = new G2JNI();
      result = this.engineApi.init(moduleName, initConfig, verbose);
      if (result != 0) {
        String errorMsg = "Failed to initialize G2Engine: " + formatError(
            "G2Engine.init", this.engineApi);
        logError(errorMsg);
        throw new ServiceSetupException(errorMsg);
      }

      // get the config and cache config data
      this.initializeConfigData();

      // create the worker pool
      this.workerPool = new WorkerThreadPool(concurrency);

      // set the state to READY
      this.setState(READY);

    } catch (ServiceSetupException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw new ServiceSetupException(e);

    } finally {
      this.monitor.writeLock().unlock();
    }
  }

  /**
   * Cleans up and frees resources after processing.  This will return after
   * all worker threads have completed and have been joined and any associated
   * resources have been released.
   */
  public void destroy() {
    this.monitor.writeLock().lock();
    try {
      if (this.getState() == DESTROYED) return;
      this.setState(DESTROYING);
    } finally {
      this.monitor.writeLock().unlock();
    }

    // close the worker pool
    if (this.workerPool != null) {
      this.workerPool.close(true);
    }

    this.monitor.writeLock().lock();
    try {
      // destroy the config manager API
      if (this.configMgrApi != null) {
        this.configMgrApi.destroy();
        this.configMgrApi = null;
      }

      // destroy the engine API
      if (this.engineApi != null) {
        this.engineApi.destroy();
        this.engineApi = null;
      }
      this.setState(DESTROYED);
    } finally {
      this.monitor.writeLock().unlock();
    }
  }

   /**
   * Gets an entity for an entity id.
   *
   * @param g2EntityId The G2 id of the entity
   * @param flags bitmask flags
   *
   * @return Entity information in JSON format
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  public String getEntity(long g2EntityId, long flags)
      throws ServiceExecutionException
  {
    // obtain a read lock
    this.monitor.readLock().lock();
    try {
      // ensure we are in the ready state
      if (this.getState() != READY) {
        throw new ServiceExecutionException(
            "Can only call getEntity() in the " + READY + " state: "
                + this.getState());
      }

      // execute the operation in a worker thread
      return this.workerPool.execute(
          () -> this.g2GetEntityByEntityID(g2EntityId, flags));

    } finally {
      // release the lock
      this.monitor.readLock().unlock();
    }
  }

  /**
   * Gets an entity for an entity id.
   *
   * @param g2EntityId The G2 id of the entity
   * @param includeFullFeatures If true full features are returned. Could have
   *                            performance impact
   * @param includeFeatureStats If true, statistics for features are returned.
   *                            Could have performance impact
   *
   * @return Entity information in JSON format
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  public String getEntity(long    g2EntityId,
                          boolean includeFullFeatures,
                          boolean includeFeatureStats)
      throws ServiceExecutionException
  {
    // obtain a read lock
    this.monitor.readLock().lock();
    try {
      // ensure we are in the ready state
      if (this.getState() != READY) {
        throw new ServiceExecutionException(
            "Can only call getEntity() in the " + READY + " state: "
                + this.getState());
      }

      // execute the operation in a worker thread
      return this.workerPool.execute(() -> {
        StringBuffer response = new StringBuffer();
        long flags;
        if (!(includeFullFeatures || includeFeatureStats)) {
          flags = G2_ENTITY_DEFAULT_FLAGS;
        } else {
          flags = G2_ENTITY_INCLUDE_ALL_RELATIONS;
          flags |= G2_ENTITY_INCLUDE_RELATED_MATCHING_INFO;
          flags |= G2_ENTITY_INCLUDE_RECORD_DATA;
          if (includeFullFeatures) {
            flags |= G2_ENTITY_INCLUDE_ALL_FEATURES;
          }
          if (includeFeatureStats) {
            flags |= G2_ENTITY_OPTION_INCLUDE_FEATURE_STATS;
          }
        }

        return this.g2GetEntityByEntityID(g2EntityId, flags);
      });

    } finally {
      this.monitor.readLock().unlock();
    }
  }

  /**
   * Gets and entity having the record identified by the specified data source
   * and record id.
   *
   * @param dataSource The data source for the record.
   * @param recordId The record ID for the record.
   *
   * @return Entity information in JSON format
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  public String getEntity(String dataSource, String recordId)
      throws ServiceExecutionException
  {
    return this.getEntity(dataSource, recordId, G2_ENTITY_DEFAULT_FLAGS);
  }

  /**
   * Gets and entity having the record identified by the specified data source
   * and record id.
   *
   * @param dataSource The data source for the record.
   * @param recordId The record ID for the record.
   * @param flags The Senzing flags to use.
   *
   * @return Entity information in JSON format
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  public String getEntity(String dataSource, String recordId, long flags)
      throws ServiceExecutionException
  {
    // obtain a read lock
    this.monitor.readLock().lock();
    try {
      // ensure we are in the ready state
      if (this.getState() != READY) {
        throw new ServiceExecutionException(
            "Can only call getEntity() in the " + READY + " state: "
                + this.getState());
      }

      // execute the operation in a worker thread
      return this.workerPool.execute(() -> this.g2GetEntityByRecordID(
          dataSource, recordId, flags));

    } finally {
      this.monitor.readLock().unlock();
    }
  }

  /**
   * Gets a list of entities based on list of feature ids.  The criteria
   * parameter is formatted as:
   * <pre>
   * {
   *   "ENTITY_ID": &lt;entity id&gt;,
   *   "LIB_FEAT_IDS": [ &lt;id1&gt;, &lt;id2&gt;, ... &lt;idn&gt; ]
   * }
   * </pre>
   * The returned JSON document is formatted as:
   * <pre>
   * [
   *   {
   *     "LIB_FEAT_ID": &lt;lib feat id&gt;,
   *     "USAGE_TYPE": "&lt;usage type&gt;",
   *     "RES_ENT_ID": &lt;entity id1&gt;
   *   },
   *   ...
   * ]
   * </pre>
   *
   * @param criteria JSON document of the format
   * @param flags The flags to use for controlling the level of detail
   *              returned for each entity.
   *
   * @return JSON document of the format
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  public String searchByAttribute(String criteria, long flags)
      throws ServiceExecutionException
  {
    // obtain a read lock
    this.monitor.readLock().lock();
    try {
      // ensure we are in the ready state
      if (this.getState() != READY) {
        throw new ServiceExecutionException(
            "Can only call searchByAttribute() in the " + READY + " state: "
                + this.getState());
      }

      // execute the operation in a worker thread
      return this.workerPool.execute(() -> this.g2SearchByAttributes(criteria, flags));

    } finally {
      this.monitor.readLock().unlock();
    }
  }

  /**
   * Gets an entity for an entity id.
   *
   * @param g2EntityId1 The first G2 entity ID.
   * @param g2EntityId2 The first G2 entity ID.
   * @param maxDegrees The maximum number of degrees of separation for the path
   *                   search.
   * @param flags bitmask flags
   *
   * @return Entity path information in JSON format
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  public String findEntityPath(long g2EntityId1,
                               long g2EntityId2,
                               int  maxDegrees,
                               long flags)
      throws ServiceExecutionException
  {
    // obtain a read lock
    this.monitor.readLock().lock();
    try {
      // ensure we are in the ready state
      if (this.getState() != READY) {
        throw new ServiceExecutionException(
            "Can only call getEntity() in the " + READY + " state: "
                + this.getState());
      }

      // execute the operation in a worker thread
      return this.workerPool.execute(
          () -> this.g2FindPathByEntityID(
              g2EntityId1, g2EntityId2, maxDegrees, flags));

    } finally {
      // release the lock
      this.monitor.readLock().unlock();
    }
  }

  /**
   * Gets the current G2 configuration in JSON format.
   *
   * @return G2 configuration in JSON format
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  public String exportConfig() throws ServiceExecutionException {
    // obtain a read lock
    this.monitor.readLock().lock();
    try {
      // ensure we are in the ready state
      if (this.getState() != READY) {
        throw new ServiceExecutionException(
            "Can only call exportConfig() in the " + READY + " state: "
                + this.getState());
      }

      // execute the operation in a worker thread
      return this.workerPool.execute(() -> {
        // NOTE: no retry on exportConfig() because that would be silly
        StringBuffer response = new StringBuffer();
        int result = this.engineApi.exportConfig(response);
        if (result != 0) {
          String errorMsg = "G2Engine failed to export configuration: "
              + formatError("G2Engine.exportConfig", this.engineApi);
          logError(errorMsg);
          throw new ServiceExecutionException(errorMsg);
        }
        return response.toString();
      });

    } finally {
      this.monitor.readLock().unlock();
    }
  }

  /**
   * Gets the G2 initialization JSON from the specified initialization text
   * which is either JSON text or a path to a JSON or INI file.
   *
   * @param initConfig The JSON text or path to a JSON or INI file to converted
   *                 to JSON text.
   * @return The JSON text.
   * @throws IOException If an I/O failure occurs.
   */
  protected static String getG2IniDataAsJson(String initConfig)
      throws IOException
  {
    String trimmed = initConfig.trim();

    // check if the text specified is JSON
    if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
      return initConfig;
    }

    // check if we have a File
    File initFile = new File(initConfig);
    if (!initFile.exists()) {
      throw new IllegalArgumentException("File does not exist: " + initConfig);
    }

    // load the file
    String fileText = readTextFileAsString(initFile, UTF_8).trim();

    // check if the file contained JSON text
    if (fileText.startsWith("{") && fileText.endsWith("}")) {
      return fileText;
    }

    // if not then assume the file contains INI text
    JsonObject iniObject = JsonUtilities.iniToJson(initFile);

    // return the text version of the JSON object
    return JsonUtilities.toJsonText(iniObject);
  }

  /**
   * Returns the attribute class (<code>ATTR_CLASS</code>) associated with the
   * specified feature name (<code>FTYPE_CODE</code>).
   *
   * @param featureName The feature name from the configuration to lookup the
   *                    attribute class.
   * @return The attribute class associated with the specified f-type code.
   * @throws IllegalStateException If this instance is not in the {@link
   *                               State#READY} state.
   */
  public String getAttributeClassForFeature(String featureName)
    throws IllegalStateException
  {
    // obtain a read lock
    this.monitor.readLock().lock();
    try {
      // ensure we are in the ready state
      if (this.getState() != READY) {
        throw new IllegalStateException(
            "Can only call getAttributeClassForFeature() in the " + READY
                + " state: " + this.getState());
      }

      synchronized (this.reinitMonitor) {
        // check if not contained
        if (!this.featureToAttrClassMap.containsKey(featureName)) {
          // reinitialize the config
          this.ensureConfigCurrent(false);
        }

        return this.featureToAttrClassMap.get(featureName);
      }

    } finally {
      this.monitor.readLock().unlock();
    }
  }

  /**
   * Returns the unmodifiable {@link Set} of configured data source codes.
   *
   * @param expectedDataSources The zero or more data source codes that the
   *                            caller expects to exist.
   * @return The unmodifiable {@link Set} of configured data source codes.
   */
  public Set<String> getDataSources(String... expectedDataSources) {
    // obtain a read lock
    this.monitor.readLock().lock();
    try {
      // ensure we are in the ready state
      if (this.getState() != READY) {
        throw new IllegalStateException(
            "Can only call getDataSources() in the " + READY + " state: "
                + this.getState());
      }

      synchronized (this.reinitMonitor) {
        for (String dataSource : expectedDataSources) {
          if (!this.dataSources.contains(dataSource)) {
            this.ensureConfigCurrent(false);
            break;
          }
        }

        return this.dataSources;
      }
    } finally {
      this.monitor.readLock().unlock();
    }
  }

  /**
   * Returns the attribute class (<code>ATTR_CLASS</code>) associated with the
   * specified attribute code (<code>ATTR_CODE</code>).
   *
   * @param attrCode The attribute code from the configuration to lookup the
   *                 attribute class.
   * @return The attribute class associated with the specified attribute code.
   * @throws IllegalStateException If this instance is not in the {@link
   *                               State#READY} state.
   */
  public String getAttributeClassForAttributeCode(String attrCode)
    throws IllegalStateException
  {
    // obtain a read lock
    this.monitor.readLock().lock();
    try {
      // ensure we are in the ready state
      if (this.getState() != READY) {
        throw new IllegalStateException(
            "Can only call getAttributeClassForAttributeCode() in the " + READY
                + " state: " + this.getState());
      }

      synchronized (this.reinitMonitor) {
        if (!this.attrCodeToAttrClassMap.containsKey(attrCode)) {
          this.ensureConfigCurrent(false);
        }
      }
      return this.attrCodeToAttrClassMap.get(attrCode);

    } finally {
      this.monitor.readLock().unlock();
    }
  }

  /**
   * Checks if the engine's active config is stale and if so reinitializes
   * with the new configuration.
   *
   * @return <code>true</code> if the configuration was updated,
   *         <code>false</code> if the configuration was already current and
   *         <code>null</code> if an error occurred in attempting to ensure
   *         it is current.
   */
  public Boolean ensureConfigCurrent() {
    return this.ensureConfigCurrent(false);
  }

  /**
   * Checks if the engine's active config is stale and if so reinitializes
   * with the new configuration.
   *
   * @param pauseWorkers <code>true</code> if the worker threads should be
   *                     paused before reinitialization and <code>false</code>
   *                     if not.
   *
   * @return <code>true</code> if the configuration was updated,
   *         <code>false</code> if the configuration was already current and
   *         <code>null</code> if an error occurred in attempting to ensure
   *         it is current.
   */
  protected Boolean ensureConfigCurrent(boolean pauseWorkers) {
    // if not capable of reinitialization then return alse
    if (this.configMgrApi == null) return false;

    Long defaultConfigId = null;
    try {
      defaultConfigId = this.getNewConfigurationID();
    } catch (IllegalStateException e) {
      // if we get an exception then return null
      return null;
    }

    // if no change then return false
    if (defaultConfigId == null) return false;

    // we can pause all workers before reinitializing or just let the underlying
    // G2Engine API handle the mutual exclusion issues
    AccessToken pauseToken = null;
    if (pauseWorkers) {
      pauseToken = this.workerPool.pause();
    }

    int returnCode;
    // once we get here we just need to reinitialize
    synchronized (this.reinitMonitor) {
      try {
        // double-check on the configuration ID
        try {
          defaultConfigId = this.getNewConfigurationID();
        } catch (IllegalStateException e) {
          return null;
        }

        // check if the default config ID has already been updated
        if (defaultConfigId == null) {
          return true;
        }

        // reinitialize with the default config ID
        returnCode = this.engineApi.reinit(defaultConfigId);
        if (returnCode != 0) {
          String errorMsg = "Failed to reinitialize with config ID ("
              + defaultConfigId + "): "
              + formatError("G2Engine.reinit", this.engineApi);
          logError(errorMsg);
          return null;
        }

        // reinitialize the cached configuration data
        this.initializeConfigData();

        // return true to indicate we reinitialized
        return true;

      } finally {
        // resume the workers
        if (pauseWorkers) {
          this.workerPool.resume(pauseToken);
        }
      }
    }
  }

  /**
   * Returns the new configuration ID if the configuration ID has changed,
   * otherwise returns <code>null</code>.
   *
   * @return The new configuration ID if the configuration ID has changed,
   *         otherwise returns <code>null</code>.
   *
   * @throws IllegalStateException If an engine failure occurs.
   */
  protected Long getNewConfigurationID() throws IllegalStateException {
    // get the active configuration ID
    Result<Long> result = new Result<>();
    int returnCode = this.engineApi.getActiveConfigID(result);

    // check the return code
    if (returnCode != 0) {
      String errorMsg = "Failed to get active config ID: " + formatError(
          "G2Engine.getActiveConfigID", this.engineApi);
      logError(errorMsg);
      throw new IllegalStateException(errorMsg);
    }

    // extract the active config ID
    long activeConfigId = result.getValue();

    // synchronize since G2ConfigMgr API is not thread-safe
    synchronized (this.reinitMonitor) {
      // get the default configuration ID
      returnCode = this.configMgrApi.getDefaultConfigID(result);

      // check the return code
      if (returnCode != 0) {
        String errorMsg = "Failed to get default config ID: " + formatError(
            "G2ConfigMgr.getDefaultConfigID", this.configMgrApi);
        logError(errorMsg);
        throw new IllegalStateException(errorMsg);
      }
    }

    // extract the default config ID
    long defaultConfigId = result.getValue();

    // check if they differ
    if (activeConfigId == defaultConfigId) return null;

    // return the new default config ID
    return defaultConfigId;
  }

  /**
   * Initializes the configuration data cached by this instance. This is done
   * on startup and on reinitialization.
   *
   * @throws IllegalStateException If Senzing API has a value.
   */
  protected void initializeConfigData() throws IllegalStateException {
    synchronized (this.reinitMonitor) {
      StringBuffer sb = new StringBuffer();
      int returnCode = this.engineApi.exportConfig(sb);
      if (returnCode != 0) {
        String errorMsg = "G2Engine failed to export configuration: "
            + formatError("G2Engine.exportConfig", this.engineApi);
        logError(errorMsg);
        throw new IllegalStateException(errorMsg);
      }

      JsonObject config = JsonUtilities.parseJsonObject(sb.toString());

      Set<String>         dataSourceSet   = new LinkedHashSet<>();
      Map<String,String>  ftypeCodeMap    = new LinkedHashMap<>();
      Map<String,String>  attrCodeMap     = new LinkedHashMap<>();

      this.evaluateConfig(config,
                          dataSourceSet,
                          ftypeCodeMap,
                          attrCodeMap);

      this.dataSources            = Collections.unmodifiableSet(dataSourceSet);
      this.featureToAttrClassMap  = Collections.unmodifiableMap(ftypeCodeMap);
      this.attrCodeToAttrClassMap = Collections.unmodifiableMap(attrCodeMap);
    }
  }

  /**
   * Evaluates the configuration and populates the {@link Set} of
   * data sources and maps, mapping f-type code to attribute class and
   * attribute code to attribute class.
   *
   * @param config       The {@link JsonObject} describing the config.
   * @param dataSources  The {@link Set} of data sources to populate.
   * @param ftypeCodeMap The {@link Map} of f-type codes to attribute classes
   *                     to populate.
   * @param attrCodeMap  The {@link Map} of attribute code to attribute classes
   *                     to populate.
   */
  private static void evaluateConfig(JsonObject           config,
                                     Set<String>          dataSources,
                                     Map<String, String>  ftypeCodeMap,
                                     Map<String, String>  attrCodeMap)
  {
    // get the data sources from the config
    JsonValue jsonValue = config.getValue("/G2_CONFIG/CFG_DSRC");
    JsonArray jsonArray = jsonValue.asJsonArray();

    for (JsonValue val : jsonArray) {
      JsonObject dataSource = val.asJsonObject();
      String dsrcCode = dataSource.getString("DSRC_CODE").toUpperCase();
      dataSources.add(dsrcCode);
    }

    // get the attribute types from the config
    jsonValue = config.getValue("/G2_CONFIG/CFG_ATTR");
    jsonArray = jsonValue.asJsonArray();

    for (JsonValue val : jsonArray) {
      JsonObject cfgAttr = val.asJsonObject();
      String attrCode = cfgAttr.getString("ATTR_CODE").toUpperCase();
      String ftypeCode = cfgAttr.getString("FTYPE_CODE").toUpperCase();
      String attrClass = cfgAttr.getString("ATTR_CLASS").toUpperCase();

      String ac = attrCodeMap.get(attrCode);
      if (ac != null && !ac.equals(attrClass)) {
        logWarning("Multiple attribute classes for ATTR_CODE: "
                + attrCode + " ( " + ac + " / " + attrClass + " )");
      } else {
        attrCodeMap.put(attrCode, attrClass);
      }

      ac = ftypeCodeMap.get(ftypeCode);
      if (ac != null && !ac.equals(attrClass)) {
        logWarning("Multiple attribute classes for FTYPE_CODE: "
                + ftypeCode + " ( " + ac + " / " + attrClass + " )");
      } else {
        ftypeCodeMap.put(ftypeCode, attrClass);
      }
    }
  }

  /**
   * Handles calling {@link
   * G2Engine#getEntityByEntityID(long, long, StringBuffer)} with retry if the
   * configuration was not up-to-date.
   *
   * @param entityID The entity ID of the entity to retrieve.
   * @param flags The flags for retrieving the entity.
   * @return The JSON text for the entity, or <code>null</code> if no entity
   *         was found for the specified entity ID.
   */
  private String g2GetEntityByEntityID(long entityID, long flags)
      throws ServiceExecutionException
  {
    StringBuffer sb = new StringBuffer();
    int result = this.engineApi.getEntityByEntityID(entityID, flags, sb);
    if (result == 0) return sb.toString();

    // now handle refreshing the configuration and retrying
    sb.delete(0, sb.length());
    if (Boolean.TRUE.equals(this.ensureConfigCurrent())) {
      result = this.engineApi.getEntityByEntityID(entityID, flags, sb);
    }

    // check if not found
    if (result != 0) {
      // get the error code
      int code = this.engineApi.getLastExceptionCode();

      // check if not found
      if (code == ENTITY_NOT_FOUND_CODE) {
        return null;
      }

      // throw an exception otherwise
      String errorMsg = "G2Engine failed to retrieve an entity with error: "
          + formatError("G2Engine.getEntityByEntityID", this.engineApi);
      logError(errorMsg);
      throw new ServiceExecutionException(errorMsg);
    }

    // return the JSON text
    return sb.toString();
  }

  /**
   * Handles calling {@link
   * G2Engine#getEntityByEntityID(long, long, StringBuffer)} with retry if the
   * configuration was not up-to-date.
   *
   * @param criteria The JSON text describing the search criteria.
   * @param flags The flags for retrieving the entity.
   * @return The JSON text for the entity.
   */
  private String g2SearchByAttributes(String criteria, long flags)
      throws ServiceExecutionException
  {
    StringBuffer sb = new StringBuffer();
    int result = this.engineApi.searchByAttributes(criteria, flags, sb);
    if (result == 0) return sb.toString();

    // now handle refreshing the configuration and retrying
    sb.delete(0, sb.length());
    if (Boolean.TRUE.equals(this.ensureConfigCurrent())) {
      result = this.engineApi.searchByAttributes(criteria, flags, sb);
    }

    // check for an error and throw as an exception
    if (result != 0) {
      String errorMsg = "G2Engine failed to search entities with error: "
          + formatError("G2Engine.searchByAttributes", this.engineApi);
      logError(errorMsg);
      throw new ServiceExecutionException(errorMsg);
    }

    // return the JSON text
    return sb.toString();
  }

  /**
   * Handles calling {@link
   * G2Engine#getEntityByEntityID(long, long, StringBuffer)} with retry if the
   * configuration was not up-to-date.
   *
   * @param dataSource The data source for the record.
   * @param recordID The record ID for the record.
   * @param flags The flags for retrieving the entity.
   * @return The JSON text for the entity, or <code>null</code> if there is no
   *         record for the specified data source and record ID.
   */
  private String g2GetEntityByRecordID(String dataSource,
                                       String recordID,
                                       long   flags)
      throws ServiceExecutionException
  {
    StringBuffer sb = new StringBuffer();
    int result = this.engineApi.getEntityByRecordID(
        dataSource, recordID, flags, sb);
    if (result == 0) return sb.toString();

    // now handle refreshing the configuration and retrying
    sb.delete(0, sb.length());
    if (Boolean.TRUE.equals(this.ensureConfigCurrent())) {
      result = this.engineApi.getEntityByRecordID(
          dataSource, recordID, flags, sb);
    }

    // check if there was a failure
    if (result != 0) {
      // get the error code
      int code = this.engineApi.getLastExceptionCode();

      // check if not found
      if (code == DATA_SOURCE_NOT_FOUND_CODE || code == RECORD_NOT_FOUND_CODE) {
        return null;
      }

      // throw an exception for other errors
      String errorMsg = "G2Engine failed to retrieve an entity with error: "
          + formatError("G2Engine.getEntityByRecordID", this.engineApi);
      logError(errorMsg);
      throw new ServiceExecutionException(errorMsg);
    }

    // return the JSON text
    return sb.toString();
  }

  /**
   * Handles calling {@link
   * G2Engine#findPathByEntityID(long, long, int, StringBuffer)} with retry
   * if the configuration was not up-to-date.
   *
   * @param entityId1 The first entity ID.
   * @param entityId2 The second entity ID.
   * @param maxDegrees The maximum number of degrees of separation for the path.
   * @param flags The flags for retrieving the entity path.
   * @return The JSON text for the entity path, or <code>null</code> if either
   *         of the entities is not found.
   */
  private String g2FindPathByEntityID(long entityId1,
                                      long entityId2,
                                      int  maxDegrees,
                                      long flags)
    throws ServiceExecutionException
  {
    StringBuffer sb = new StringBuffer();

    int result = engineApi.findPathByEntityID(
        entityId1, entityId2, maxDegrees, flags, sb);

    if (result == 0) return sb.toString();

    // now handle refreshing the configuration and retrying
    sb.delete(0, sb.length());
    if (Boolean.TRUE.equals(this.ensureConfigCurrent())) {
      result = engineApi.findPathByEntityID(
          entityId1, entityId2, maxDegrees, flags, sb);
    }

    // check if not found
    if (result != 0) {
      // get the error code
      int code = this.engineApi.getLastExceptionCode();

      // check if not found
      if (code == ENTITY_NOT_FOUND_CODE) {
        return null;
      }

      // throw an exception otherwise
      String errorMsg = "G2Engine failed to find entity path with error: "
          + formatError("G2Engine.findPathByEntityId", this.engineApi);
      logError(errorMsg);
      throw new ServiceExecutionException(errorMsg);
    }

    // return the JSON text
    return sb.toString();
  }
}
