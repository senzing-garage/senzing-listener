package com.senzing.listener.service.g2;

import java.io.File;
import java.io.IOException;

import javax.json.JsonObject;

import com.senzing.g2.engine.G2Engine;
import com.senzing.g2.engine.G2JNI;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.util.JsonUtilities;

import static com.senzing.g2.engine.G2Engine.*;
import static com.senzing.io.IOUtilities.*;

/**
 * This class handles communication with G2.  It sets up an instance of G2 and
 * interacts with it (get entities etc.).
 */
public class G2Service {
  /**
   * The {@link G2Engine} to use.
   */
  protected G2Engine g2Engine;

  /**
   * The module name used to initialize the {@link G2Engine}.
   */
  private static final String DEFAULT_MODULE_NAME = "G2JNI";

  /**
   * Default constructor.
   */
  public G2Service() {
  }

  /**
   * Initializes the service.  The specified initialization config parameter
   * is either JSON to initialize with, a path to a JSON file or a path to
   * INI file.
   *
   * Gets the G2 initialization JSON from the specified initialization text
   * which is either JSON text or a path to a JSON or INI file.
   *
   * @param initConfig The JSON text or path to a JSON or INI file to converted
   *                   to JSON text.
   * @throws ServiceSetupException If failure occurs in initialization.
   */
  public synchronized void init(String initConfig)
      throws ServiceSetupException
  {
    this.init(DEFAULT_MODULE_NAME, initConfig);
  }

  /**
   * Initializes the service.  The specified initialization config parameter
   * is either JSON to initialize with, a path to a JSON file or a path to
   * INI file.
   *
   * Gets the G2 initialization JSON from the specified initialization text
   * which is either JSON text or a path to a JSON or INI file.
   *
   * @param moduleName The module name for initializing G2.
   *
   * @param initConfig The JSON text or path to a JSON or INI file to converted
   *                   to JSON text.
   * @throws ServiceSetupException If failure occurs in initialization.
   */
  public synchronized void init(String moduleName, String initConfig)
      throws ServiceSetupException
  {
    if (this.g2Engine != null) {
      throw new IllegalStateException("G2Service already initialized");
    }
    boolean verboseLogging = false;

    String configData = null;
    try {
      configData = getG2IniDataAsJson(initConfig);
    } catch (IOException | RuntimeException e) {
      throw new ServiceSetupException(e);
    }
    g2Engine = new G2JNI();
    int result = g2Engine.init(moduleName, configData, verboseLogging);
    if (result != G2ServiceDefinitions.G2_VALID_RESULT) {
      StringBuilder errorMessage
          = new StringBuilder("G2 engine failed to initialize with error: ");
      errorMessage.append(g2ErrorMessage(g2Engine));
      throw new ServiceSetupException(errorMessage.toString());
    }
  }

  /**
   * Cleans up and frees resources after processing.
   */
  public synchronized void destroy() {
    if (this.g2Engine != null) {
      this.g2Engine.destroy();
      this.g2Engine = null;
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
    StringBuffer response = new StringBuffer();
    int result = g2Engine.getEntityByEntityID(g2EntityId, flags, response);

    if (result != G2ServiceDefinitions.G2_VALID_RESULT) {
      StringBuilder errorMessage
          = new StringBuilder(
              "G2 engine failed to retrieve an entity with error: ");
      errorMessage.append(g2ErrorMessage(g2Engine));
      throw new ServiceExecutionException(errorMessage.toString());
    }
    return response.toString();
  }

  
  /**
   * Gets an entity for an entity id.
   *
   * @param g2EntiyId The G2 id of the entity
   * @param includeFullFeatures If true full features are returned. Could have
   *                            performance impact
   * @param includeFeatureStats If true, statistics for features are returned.
   *                            Could have performance impact
   *
   * @return Entity information in JSON format
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  public String getEntity(long    g2EntiyId,
                          boolean includeFullFeatures,
                          boolean includeFeatureStats)
      throws ServiceExecutionException
  {
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
    int result = g2Engine.getEntityByEntityID(g2EntiyId, flags, response);
    if (result != G2ServiceDefinitions.G2_VALID_RESULT) {
      StringBuilder errorMessage
          = new StringBuilder(
              "G2 engine failed to retrieve an entity with error: ");
      errorMessage.append(g2ErrorMessage(g2Engine));
      throw new ServiceExecutionException(errorMessage.toString());
    }
    return response.toString();

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
  public String getEntity(String dataSource, String recordId) throws ServiceExecutionException {
    StringBuffer response = new StringBuffer();
    int result = g2Engine.getEntityByRecordID(dataSource,
                                              recordId,
                                              G2_ENTITY_DEFAULT_FLAGS,
                                              response);
    if (result != G2ServiceDefinitions.G2_VALID_RESULT) {
      StringBuilder errorMessage
          = new StringBuilder(
              "G2 engine failed to retrieve an entity with error: ");
      errorMessage.append(g2ErrorMessage(g2Engine));
      throw new ServiceExecutionException(errorMessage.toString());
    }
    return response.toString();
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
   *
   * @return JSON document of the format
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  public String searchByAttribute(String criteria)
      throws ServiceExecutionException
  {
    StringBuffer response = new StringBuffer();
    int result = g2Engine.searchByAttributes(criteria,
                                             G2_ENTITY_DEFAULT_FLAGS,
                                             response);
    if (result != G2ServiceDefinitions.G2_VALID_RESULT) {
      StringBuilder errorMessage
          = new StringBuilder(
              "G2 engine failed to retrieve an entity with error: ");
      errorMessage.append(g2ErrorMessage(g2Engine));
      throw new ServiceExecutionException(errorMessage.toString());
    }
    return response.toString();
  }

  /**
   * Gets the current G2 configuration in JSON format.
   *
   * @return G2 configuration in JSON format
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  public String exportConfig() throws ServiceExecutionException {
    StringBuffer response = new StringBuffer();
    int result = g2Engine.exportConfig(response);
    if (result != G2ServiceDefinitions.G2_VALID_RESULT) {
      StringBuilder errorMessage
          = new StringBuilder(
              "G2 engine failed to export configuration with error: ");
      errorMessage.append(g2ErrorMessage(g2Engine));
      throw new ServiceExecutionException(errorMessage.toString());
    }
    return response.toString();
  }

  static protected String g2ErrorMessage(G2Engine g2Engine) {
    return g2Engine.getLastExceptionCode() + ", " + g2Engine.getLastException();
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
}
