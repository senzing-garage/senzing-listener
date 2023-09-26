package com.senzing.listener.service;

import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.util.JsonUtilities;

import javax.json.JsonObject;

/**
 * Provides utility methods for services.
 */
public class ServiceUtilities {
  /**
   * Private default constructor.
   */
  private ServiceUtilities() {
    // do nothing
  }

  /**
   * Utility method for obtaining a {@link String} configuration parameter
   * with options to check if missing and required.  This will throw
   * a {@link ServiceSetupException} if it fails.  Any {@link String}
   * value that is obtained will be trimmed of leading and trailing whitespace
   * and if empty will be returned as <code>null</code>.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param required <code>true</code> if required, otherwise
   *                 <code>false</code>.
   * @return The {@link String} configuration value.
   * @throws ServiceSetupException If the parameter value is required but is
   *                               missing.
   */
  public static String getConfigString(JsonObject config,
                                       String     key,
                                       boolean    required)
      throws ServiceSetupException
  {
    return getConfigString(config, key, required, true);
  }

  /**
   * Utility method for obtaining a {@link String} configuration parameter
   * with options to check if missing and required.  This will throw
   * a {@link ServiceSetupException} if it fails.  Any {@link String}
   * value that is obtained will be trimmed of leading and trailing whitespace.
   * Resultant empty-string values will optionally be converted to
   * <code>null</code> if the normalization parameter is set to
   * <code>true</code> and will be returned as-is if <code>false</code>.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param required <code>true</code> if required, otherwise
   *                 <code>false</code>.
   * @param normalize <code>true</code> if empty or pure whitespace strings
   *                  should be returned as <code>null</code>, otherwise
   *                  <code>false</code> to return them as-is.
   * @return The {@link String} configuration value.
   * @throws ServiceSetupException If a failure occurs in obtaining the
   *                               parameter value.
   */
  public static String getConfigString(JsonObject config,
                                       String     key,
                                       boolean    required,
                                       boolean    normalize)
      throws ServiceSetupException
  {
    // check if required and missing
    if (required && !config.containsKey(key)) {
      throw new ServiceSetupException(
          "Following configuration parameter missing: " + key);
    }

    String result = getConfigString(config, key, null, normalize);

    // check if required and missing
    if (required && normalize && result == null) {
      throw new ServiceSetupException(
          "Following configuration parameter is specified as null "
              + "or empty string: " + key);
    }

    // return the result
    return result;
  }

  /**
   * Utility method for obtaining a {@link String} configuration parameter
   * with option to return a default value if missing.  This will throw
   * a {@link ServiceSetupException} if it fails.  Any {@link String}
   * value that is obtained will be trimmed of leading and trailing whitespace
   * and if empty will be returned as <code>null</code>.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param defaultValue The default value to return if the value is missing.
   * @return The {@link String} configuration value.
   * @throws ServiceSetupException If a failure occurs in obtaining the
   *                               parameter value.
   */
  public static String getConfigString(JsonObject config,
                                       String     key,
                                       String     defaultValue)
      throws ServiceSetupException
  {
    try {
      return getConfigString(config, key, defaultValue, true);

    } catch (Exception e) {
      throw new ServiceSetupException(
          "Failed to parse JSON configuration parameter (" + key + "): "
              + e.getMessage());
    }
  }

  /**
   * Utility method for obtaining a {@link String} configuration parameter
   * with option to return a default value if missing.  This will throw
   * a {@link ServiceSetupException} if it fails. Any {@link String}
   * value that is obtained will be trimmed of leading and trailing whitespace.
   * Resultant empty-string values will optionally be converted to
   * <code>null</code> if the normalization parameter is set to
   * <code>true</code> and will be returned as-is if <code>false</code>.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param defaultValue The default value to return if the value is missing.
   * @param normalize <code>true</code> if empty or pure whitespace strings
   *                  should be returned as <code>null</code>, otherwise
   *                  <code>false</code> to return them as-is.
   * @return The {@link String} configuration value.
   * @throws ServiceSetupException If a failure occurs in obtaining the
   *                               parameter value.
   */
  public static String getConfigString(JsonObject config,
                                       String     key,
                                       String     defaultValue,
                                       boolean    normalize)
      throws ServiceSetupException
  {
    try {
      String result = JsonUtilities.getString(config, key, defaultValue);

      // trim the whitespace (regardless of normalization)
      if (result != null) result = result.trim();

      // optionally normalize empty string to null
      if (normalize && result != null && result.length() == 0) {
        result = null;
      }

      // return the result
      return result;

    } catch (Exception e) {
      throw new ServiceSetupException(
          "Failed to parse JSON configuration parameter (" + key + "): "
              + e.getMessage());
    }
  }

  /**
   * Utility method for obtaining an {@link Integer} configuration parameter
   * with options to check if missing and required or if it is less than an
   * optional minimum value.  This will throw {@link
   * ServiceSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param required <code>true</code> if required, otherwise
   *                 <code>false</code>.
   * @param minimum The minimum integer value allowed, or <code>null</code>
   *                if no minimum is enforced.
   * @return The {@link String} configuration value.
   * @throws ServiceSetupException If the value is required and not present or
   *                               if it is present and less than the optionally
   *                               specified minimum value or could not an
   *                               integer.
   */
  public static Integer getConfigInteger(JsonObject config,
                                         String     key,
                                         boolean    required,
                                         Integer    minimum)
      throws ServiceSetupException
  {
    // check if required and missing
    if (required && !config.containsKey(key)) {
      throw new ServiceSetupException(
          "Following configuration parameter missing: " + key);
    }
    return getConfigInteger(config, key, minimum, null);
  }

  /**
   * Utility method for obtaining an {@link Integer} configuration parameter
   * with options to check if missing and required or if it is less than an
   * optional minimum value.  This will throw {@link
   * ServiceSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param minimum The minimum integer value allowed, or <code>null</code>
   *                if no minimum is enforced.
   * @param defaultValue The default value to return if the value is missing.
   * @return The {@link String} configuration value.
   * @throws ServiceSetupException If the value is present and less than the
   *                               optionally specified minimum value or could
   *                               not an integer.
   */
  public static Integer getConfigInteger(JsonObject config,
                                         String     key,
                                         Integer    minimum,
                                         Integer    defaultValue)
      throws ServiceSetupException
  {
    Integer result = null;
    try {
      result = JsonUtilities.getInteger(config, key, defaultValue);

    } catch (Exception e) {
      throw new ServiceSetupException(
          "Failed to parse JSON configuration parameter (" + key + "): "
              + e.getMessage());
    }
    // check the result
    if (result != null && minimum != null && result < minimum) {
      throw new ServiceSetupException(
          "The " + key + " configuration parameter cannot be less than "
              + minimum + ": " + result);
    }
    return result;
  }

  /**
   * Utility method for obtaining a {@link Long} configuration parameter
   * with options to check if missing and required or if it is less than an
   * optional minimum value.  This will throw {@link
   * ServiceSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param required <code>true</code> if required, otherwise
   *                 <code>false</code>.
   * @param minimum The minimum integer value allowed, or <code>null</code>
   *                if no minimum is enforced.
   * @return The {@link String} configuration value.
   * @throws ServiceSetupException If the value is required and not present or
   *                               if it is present and less than the optionally
   *                               specified minimum value or could not a long
   *                               integer.
   */
  public static Long getConfigLong(JsonObject config,
                                   String     key,
                                   boolean    required,
                                   Long       minimum)
      throws ServiceSetupException
  {
    // check if required and missing
    if (required && !config.containsKey(key)) {
      throw new ServiceSetupException(
          "Following configuration parameter missing: " + key);
    }

    return getConfigLong(config, key, minimum, null);
  }

  /**
   * Utility method for obtaining a {@link Long} configuration parameter
   * with options to check if missing and required or if it is less than an
   * optional minimum value.  This will throw {@link
   * ServiceSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param minimum The minimum integer value allowed, or <code>null</code>
   *                if no minimum is enforced.
   * @param defaultValue The default value to return if the value is missing.
   * @return The {@link String} configuration value.
   * @throws ServiceSetupException If the value is less than the optionally
   *                               specified minimum value or if it is not a
   *                               long integer.
   */
  public static Long getConfigLong(JsonObject  config,
                                   String      key,
                                   Long        minimum,
                                   Long        defaultValue)
      throws ServiceSetupException
  {
    Long result = null;
    try {
      result = JsonUtilities.getLong(config, key, defaultValue);

    } catch (Exception e) {
      throw new ServiceSetupException(
          "Failed to parse JSON configuration parameter (" + key + "): "
              + e.getMessage());
    }
    // check the result
    if (result != null && minimum != null && result < minimum) {
      throw new ServiceSetupException(
          "The " + key + " configuration parameter cannot be less than "
              + minimum + ": " + result);
    }
    return result;
  }

  /**
   * Utility method for obtaining a {@link Boolean} configuration parameter
   * with options to check if missing and required.  This will throw {@link
   * ServiceSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param required <code>true</code> if required, otherwise
   *                 <code>false</code>.
   * @return The {@link String} configuration value.
   * @throws ServiceSetupException If the value is required and not present or
   *                               if it is present and could not be interpreted
   *                               as a Boolean.
   */
  public static Boolean getConfigBoolean(JsonObject config,
                                         String     key,
                                         boolean    required)
      throws ServiceSetupException {
    // check if required and missing
    if (required && !config.containsKey(key)) {
      throw new ServiceSetupException(
          "Following configuration parameter missing: " + key);
    }

    return getConfigBoolean(config, key, null);
  }

  /**
   * Utility method for obtaining a {@link Long} configuration parameter
   * with options to check if missing and required or if it is less than an
   * optional minimum value.  This will throw {@link
   * ServiceSetupException} if it fails.
   *
   * @param config The {@link JsonObject} configuration.
   * @param key The configuration parameter key.
   * @param defaultValue The default value to return if the value is missing.
   * @return The {@link String} configuration value.
   *
   * @throws ServiceSetupException If the value is present but could not be
   *                               interpreted as a Boolean.
   */
  public static Boolean getConfigBoolean(JsonObject config,
                                         String     key,
                                         Boolean    defaultValue)
      throws ServiceSetupException
  {
    try {
      return JsonUtilities.getBoolean(config, key, defaultValue);

    } catch (Exception e) {
      throw new ServiceSetupException(
          "Failed to parse JSON configuration parameter (" + key + "): "
              + e.getMessage());
    }
  }
}
