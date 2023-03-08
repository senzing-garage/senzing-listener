package com.senzing.listener.service.g2;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonObjectBuilder;

import com.senzing.g2.engine.G2Engine;
import com.senzing.g2.engine.G2JNI;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;

import static com.senzing.g2.engine.G2Engine.*;

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
  private static final String moduleName = "G2JNI";

  /**
   * Default constructor.
   */
  public G2Service() {
  }

  /**
   * Initializes the service. It reads the information from the ini file and
   * sets up G2 using that data.
   *
   * @param iniFile The path to the INI file for initialization.
   *
   * @throws ServiceSetupException If a failure occurs.
   */
  public void init(String iniFile) throws ServiceSetupException {

    String configData = null;
    try {
      configData = getG2IniDataAsJson(iniFile);
    } catch (IOException | RuntimeException e) {
      throw new ServiceSetupException(e);
    }
    initWithG2Config(configData);
  }

  /**
   * Initializes the service. The configuration information is passed in as a json string.
   *
   * @param g2Config configuration as a json string.
   *
   * @throws ServiceSetupException If a failure occurs.
   */
  public void initWithG2Config(String g2Config) throws ServiceSetupException {
    boolean verboseLogging = false;

    g2Engine = new G2JNI();
    int result = g2Engine.init(moduleName, g2Config, verboseLogging);
    if (result != G2ServiceDefinitions.G2_VALID_RESULT) {
      StringBuilder errorMessage = new StringBuilder("G2 engine failed to initalize with error: ");
      errorMessage.append(g2ErrorMessage(g2Engine));
      throw new ServiceSetupException(errorMessage.toString());
    }
  }

  /**
   * Cleans up and frees resources after processing.
   */
  public void destroy() {
    if (g2Engine != null) {
      g2Engine.destroy();
    }
  }

   /**
   * Gets an entity for an entity id.
   *
   * @param g2EntiyId The G2 id of the entity
   * @param flags bitmask flags
   *
   * @return Entity information in JSON format
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  public String getEntity(long g2EntiyId, long flags)
      throws ServiceExecutionException
  {
    StringBuffer response = new StringBuffer();
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

  protected static String getG2IniDataAsJson(String iniFile)
      throws IOException
  {
    Pattern  iniSection  = Pattern.compile( "\\s*\\[([^]]*)\\]\\s*" );
    Pattern  iniKeyValue = Pattern.compile( "\\s*([^=]*)=(.*)" );
    JsonObjectBuilder rootObject = Json.createObjectBuilder();
    try (Scanner scanner = new Scanner(new File(iniFile))) {
      JsonObjectBuilder currentSection = null;
      String currentGroup = null;
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine().trim();
        if (line.startsWith("#")) {
          continue;
        }
        Matcher matcher = iniSection.matcher(line);
        if (matcher.matches()) {
          if (currentGroup != null) {
            rootObject.add(currentGroup, currentSection.build());
          }
          currentGroup = matcher.group(1);
          currentSection = Json.createObjectBuilder();
//          rootObject.add(matcher.group(1), currentSection);
        } else if (currentSection != null) {
          matcher = iniKeyValue.matcher(line);
          if (matcher.matches()) {
            currentSection.add(matcher.group(1), matcher.group(2));
          }
        }
      }
      if (currentGroup != null) {
        rootObject.add(currentGroup, currentSection.build());
      }
    }
    return rootObject.build().toString();
  }
}
