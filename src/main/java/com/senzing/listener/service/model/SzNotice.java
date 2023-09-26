package com.senzing.listener.service.model;

import com.senzing.util.JsonUtilities;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.Serializable;
import java.util.Objects;

/**
 * Simple class to represent an element from the <code>NOTICES</code> array
 * in an INFO message.
 */
public class SzNotice implements Serializable {
  /**
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getCode() code property} for instances of this class.
   */
  public static final String CODE_KEY = "code";

  /**
   * The JSON property key for deserializing the {@linkplain
   * #setCode(String) code property} when parsing raw Senzing INFO message
   * JSON to construct instances of this class.
   */
  public static final String RAW_CODE_KEY = "CODE";

  /**
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getDescription() description property} for instances of this class.
   */
  public static final String DESCRIPTION_KEY = "description";

  /**
   * The JSON property key for deserializing the {@linkplain
   * #setDescription(String) description property} when parsing raw Senzing
   * INFO message JSON to construct instances of this class.
   */
  public static final String RAW_DESCRIPTION_KEY = "DESCRIPTION";

  /**
   * The code for the notice.
   */
  private String code;

  /**
   * The description for the notice.
   */
  private String description;

  /**
   * Default constructor.
   */
  public SzNotice() {
    this(null, null);
  }

  /**
   * Constructs with the code and the description.
   * @param code The code for the notice.
   * @param description The description for the notice.
   */
  public SzNotice(String code, String description) {
    this.code         = code;
    this.description  = description;
  }

  /**
   * Gets the code for the notice.
   *
   * @return The code for the notice.
   */
  public String getCode() {
    return this.code;
  }

  /**
   * Sets the code for the notice.
   *
   * @param code The code for the notice.
   */
  public void setCode(String code) {
    this.code = code;
  }

  /**
   * Gets the description for the notice.
   *
   * @return The description for the notice.
   */
  public String getDescription() {
    return this.description;
  }

  /**
   * Sets the description for the notice.
   *
   * @param description The description for the notice.
   */
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) return false;
    SzNotice szNotice = (SzNotice) o;
    return Objects.equals(this.getCode(), szNotice.getCode())
        && Objects.equals(this.getDescription(), szNotice.getDescription());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getCode(), this.getDescription());
  }

  /**
   * Serializes this instance as JSON to the specified {@link
   * JsonObjectBuilder}.  If the specified {@link JsonObjectBuilder} is
   * <code>null</code> then a new instance is created, populated and then
   * returned.
   *
   * @param builder The optional {@link JsonObjectBuilder} to use, or
   *                <code>null</code> if a new instance should be created.
   *
   * @return The {@link JsonObjectBuilder} that was populated.
   *
   * @see #toJsonObjectBuilder()
   * @see #toJsonObject()
   */
  public JsonObjectBuilder toJsonObjectBuilder(JsonObjectBuilder builder) {
    if (builder == null) builder = Json.createObjectBuilder();
    JsonUtilities.add(builder, CODE_KEY, this.getCode());
    JsonUtilities.add(builder, DESCRIPTION_KEY, this.getDescription());
    return builder;
  }

  /**
   * Serializes this instance as JSON to a new {@link
   * JsonObjectBuilder} and returns the {@link JsonObjectBuilder}.
   *
   * @return The {@link JsonObjectBuilder} that was populated.
   *
   * @see #toJsonObjectBuilder(JsonObjectBuilder)
   * @see #toJsonObject()
   */
  public JsonObjectBuilder toJsonObjectBuilder() {
    return this.toJsonObjectBuilder(null);
  }

  /**
   * Serializes this instance as JSON to a new {@link JsonObject} and returns
   * the created {@link JsonObject}.
   *
   * @return The {@link JsonObject} that was created to describe the serialized
   *         form of this instance.
   *
   * @see #toJsonObjectBuilder(JsonObjectBuilder)
   * @see #toJsonObjectBuilder()
   */
  public JsonObject toJsonObject() {
    return this.toJsonObjectBuilder().build();
  }

  /**
   * Serializes this instance as JSON text and returns the JSON text, optionally
   * pretty-printing the generated JSON.
   *
   * @param prettyPrint <code>true</code> if the JSON should be pretty-printed,
   *                    otherwise <code>false</code> for more efficient JSON.
   * @return The JSON text that was generated.
   */
  public String toJsonText(boolean prettyPrint) {
    return JsonUtilities.toJsonText(this.toJsonObject(), prettyPrint);
  }

  /**
   * Serializes this instance as JSON text and returns the JSON text.  The
   * returned JSON should <b>not</b> be pretty-printed.
   *
   * @return The JSON text that was generated.
   */
  public String toJsonText() {
    return this.toJsonText(false);
  }

  /**
   * Parses the specified JSON text that is formatted as if generated by
   * {@linkplain #toJsonText() serializing} an instance of this class and
   * creates a new instance of this class as described by the JSON.  If the
   * required JSON properties are not found then an exception is thrown.
   *
   * @param jsonText The JSON text to parse.
   *
   * @return The created {@link SzNotice} instance, or <code>null</code> if the
   *         specified text is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON or
   *                                  does not contain the required JSON
   *                                  properties.
   */
  public static SzNotice fromJson(String jsonText)
    throws IllegalArgumentException
  {
    if (jsonText == null) return null;
    return fromJson(JsonUtilities.parseJsonObject(jsonText));
  }

  /**
   * Parses the JSON described by the specified {@link JsonObject} where that
   * JSON is formatted as if generated by {@linkplain #toJsonObject()
   * serializing} an instance of this class and creates a new instance of this
   * class as described by the JSON.  If the required JSON properties are not
   * found then an exception is thrown.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON to parse.
   *
   * @return The created {@link SzNotice} instance, or <code>null</code> if the
   *         specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON or
   *                                  does not contain the required JSON
   *                                  properties.
   *
   */
  public static SzNotice fromJson(JsonObject jsonObject)
    throws IllegalArgumentException
  {
    return fromJson(jsonObject, CODE_KEY, DESCRIPTION_KEY);
  }

  /**
   * Parses the specified JSON text that is formatted as a raw Senzing INFO
   * message part and creates a new instance of this class as described by the
   * JSON.  If the required JSON properties are not found then an exception is
   * thrown.
   *
   * @param jsonText The JSON text to parse.
   *
   * @return The created {@link SzNotice} instance, or <code>null</code> if the
   *         specified text is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON or
   *                                  does not contain the required JSON
   *                                  properties.
   */
  public static SzNotice fromRawJson(String jsonText)
      throws IllegalArgumentException
  {
    if (jsonText == null) return null;
    return fromRawJson(JsonUtilities.parseJsonObject(jsonText));
  }

  /**
   * Parses the JSON described by the specified {@link JsonObject} where that
   * JSON is formatted as a raw Senzing INFO message part and creates a new
   * instance of this class as described by the JSON.  If the required JSON
   * properties are not found then an exception is thrown.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON to parse.
   *
   * @return The created {@link SzNotice} instance, or <code>null</code> if the
   *         specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON or
   *                                  does not contain the required JSON
   *                                  properties.
   *
   */
  public static SzNotice fromRawJson(JsonObject jsonObject)
      throws IllegalArgumentException
  {
    return fromJson(jsonObject, RAW_CODE_KEY, RAW_DESCRIPTION_KEY);
  }

  /**
   * Internal method that parses the JSON described by the specified {@link
   * JsonObject} using the specified JSON property keys for the various parts.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON to parse.
   *
   * @param codeKey The JSON property key for the code property.
   *
   * @param descriptionKey The JSON property key for the description property.
   *
   * @return The created {@link SzNotice} instance, or <code>null</code> if the
   *         specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON or
   *                                  does not contain the required JSON
   *                                  properties.
   *
   */
  private static SzNotice fromJson(JsonObject jsonObject,
                                   String     codeKey,
                                   String     descriptionKey)
      throws IllegalArgumentException
  {
    if (jsonObject == null) return null;
    if (!jsonObject.containsKey(codeKey)) {
      throw new IllegalArgumentException(
          "The specified JSON must at least contain the \"" + codeKey
              + "\" property: " + JsonUtilities.toJsonText(jsonObject));
    }
    String code = JsonUtilities.getString(jsonObject, codeKey);
    String desc = JsonUtilities.getString(jsonObject, descriptionKey);
    return new SzNotice(code, desc);
  }

  /**
   * Overridden to return the result from {@link #toJsonText()}.
   *
   * @return The result from {@link #toJsonText()}.
   */
  public String toString() {
    return this.toJsonText();
  }
}
