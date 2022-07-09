package com.senzing.listener.service.model;

import com.senzing.util.JsonUtilities;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.Serializable;
import java.util.*;

/**
 * Simple class to represent an element from the <code>SAMPLE_RECORDS</code>
 * array inside an element of the <code>INTERESTING_ENTITIES</code> array of
 * an INFO message.
 */
public class SzSampleRecord implements Serializable {
  /**
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getFlags() flags property} for instances of this class.
   */
  public static final String FLAGS_KEY = "flags";

  /**
   * The JSON property key for deserializing the {@linkplain
   * #setFlags(Collection) flags property} when parsing raw Senzing
   * INFO message JSON to construct instances of this class.
   */
  public static final String RAW_FLAGS_KEY = "FLAGS";

  /**
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getDataSource() data source property} for instances of this class.
   */
  public static final String DATA_SOURCE_KEY = "dataSource";

  /**
   * The JSON property key for deserializing the {@linkplain
   * #setDataSource(String) data source property} when parsing raw Senzing
   * INFO message JSON to construct instances of this class.
   */
  public static final String RAW_DATA_SOURCE_KEY = "DATA_SOURCE";

  /**
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getRecordId() record ID property} for instances of this class.
   */
  public static final String RECORD_ID_KEY = "recordId";

  /**
   * The JSON property key for deserializing the {@linkplain
   * #setRecordId(String) record ID property} when parsing raw Senzing
   * INFO message JSON to construct instances of this class.
   */
  public static final String RAW_RECORD_ID_KEY = "RECORD_ID";

  /**
   * The data source for the sample record.
   */
  private String dataSource;

  /**
   * The record ID for the sample record.
   */
  private String recordId;

  /**
   * The {@link Set} of flags for the sample record.
   */
  private Set<String> flags;

  /**
   * Default constructor.
   */
  public SzSampleRecord() {
    this(null, null, null);
  }

  /**
   * Constructs with the data source, record ID and flags.
   *
   * @param dataSource The data source for this instance.
   * @param recordId The record ID for this instance.
   * @param flags The {@link Collection} of flags for this instance.
   */
  public SzSampleRecord(String              dataSource,
                        String              recordId,
                        Collection<String>  flags)
  {
    this.dataSource   = dataSource;
    this.recordId     = recordId;

    this.flags = (flags == null)
        ? new LinkedHashSet<>() : new LinkedHashSet<>(flags);
    this.flags.remove(null); // remove any null entries
  }

  /**
   * Gets the data source for the sample record.
   *
   * @return The data source for the sample record.
   */
  public String getDataSource() {
    return this.dataSource;
  }

  /**
   * Sets the data source for the sample record.
   *
   * @param dataSource The data source for the sample record.
   */
  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Gets the record ID for the sample record.
   *
   * @return The record ID for the sample record.
   */
  public String getRecordId() {
    return this.recordId;
  }

  /**
   * Sets the record ID for the sample record.
   *
   * @param recordId The record ID for the sample record.
   */
  public void setRecordId(String recordId) {
    this.recordId = recordId;
  }

  /**
   * Gets the flags for the sample record as an <b>unmodifiable</b> {@link Set}.
   *
   * @return The flags for the sample record as an <b>unmodifiable</b>
   *         {@link Set}.
   */
  public Set<String> getFlags() {
    return Collections.unmodifiableSet(this.flags);
  }

  /**
   * Sets the flags for the sample record to those in the specified
   * {@link Collection}.
   *
   * @param flags The {@link Collection} of flags.
   */
  public void setFlags(Collection<String> flags) {
    this.flags.clear();
    if (flags != null) this.flags.addAll(flags);
  }

  /**
   * Adds the specified flag to the {@link Set} of flags for this sample record.
   *
   * @param flag The flag to add to the {@link Set} of flags for this sample
   *             record.
   */
  public void addFlag(String flag) {
    Objects.requireNonNull(
        flag, "The specified flag cannot be null");
    this.flags.add(flag);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) return false;
    SzSampleRecord that = (SzSampleRecord) o;
    return Objects.equals(this.getDataSource(), that.getDataSource())
        && Objects.equals(this.getRecordId(), that.getRecordId())
        && Objects.equals(this.getFlags(), that.getFlags());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getDataSource(),
                        this.getRecordId(),
                        this.getFlags());
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
    JsonUtilities.add(builder, DATA_SOURCE_KEY, this.getDataSource());
    JsonUtilities.add(builder, RECORD_ID_KEY, this.getRecordId());

    JsonArrayBuilder jab = Json.createArrayBuilder();
    for (String flag: this.getFlags()) {
      JsonUtilities.add(jab, flag);
    }
    builder.add(FLAGS_KEY, jab);

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
   * @return The created {@link SzSampleRecord} instance, or <code>null</code> if the
   *         specified text is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON or
   *                                  does not contain the required JSON
   *                                  properties.
   */
  public static SzSampleRecord fromJson(String jsonText)
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
   * @return The created {@link SzSampleRecord} instance, or <code>null</code> if the
   *         specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON or
   *                                  does not contain the required JSON
   *                                  properties.
   *
   */
  public static SzSampleRecord fromJson(JsonObject jsonObject)
    throws IllegalArgumentException
  {
    return fromJson(jsonObject, DATA_SOURCE_KEY, RECORD_ID_KEY, FLAGS_KEY);
  }

  /**
   * Parses the specified JSON text that is formatted as a raw Senzing INFO
   * message part and creates a new instance of this class as described by the
   * JSON.  If the required JSON properties are not found then an exception is
   * thrown.
   *
   * @param jsonText The JSON text to parse.
   *
   * @return The created {@link SzSampleRecord} instance, or <code>null</code> if the
   *         specified text is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON or
   *                                  does not contain the required JSON
   *                                  properties.
   */
  public static SzSampleRecord fromRawJson(String jsonText)
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
   * @return The created {@link SzSampleRecord} instance, or <code>null</code> if the
   *         specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON or
   *                                  does not contain the required JSON
   *                                  properties.
   *
   */
  public static SzSampleRecord fromRawJson(JsonObject jsonObject)
      throws IllegalArgumentException
  {
    return fromJson(jsonObject,
                    RAW_DATA_SOURCE_KEY,
                    RAW_RECORD_ID_KEY,
                    RAW_FLAGS_KEY);
  }

  /**
   * Internal method that parses the JSON described by the specified {@link
   * JsonObject} using the specified JSON property keys for the various parts.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON to parse.
   *
   * @param dataSourceKey The JSON property key for the data source property.
   *
   * @param recordIdKey The JSON property key for the record ID property.
   *
   * @param flagsKey The JSON property key for the flags property.
   *
   * @return The created {@link SzSampleRecord} instance, or <code>null</code> if the
   *         specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON or
   *                                  does not contain the required JSON
   *                                  properties.
   *
   */
  private static SzSampleRecord fromJson(JsonObject jsonObject,
                                         String     dataSourceKey,
                                         String     recordIdKey,
                                         String     flagsKey)
      throws IllegalArgumentException
  {
    if (jsonObject == null) return null;
    if (!jsonObject.containsKey(dataSourceKey)
        || !jsonObject.containsKey(flagsKey)
        || !jsonObject.containsKey(recordIdKey))
    {
      throw new IllegalArgumentException(
          "The specified JSON must contain the \"" + dataSourceKey
              + "\", \"" + recordIdKey + "\", and \"" + flagsKey
              + "\" properties: " + JsonUtilities.toJsonText(jsonObject));
    }
    String dataSource = JsonUtilities.getString(jsonObject, dataSourceKey);

    String recordId = JsonUtilities.getString(jsonObject, recordIdKey);

    List<String> flags = JsonUtilities.getStrings(jsonObject, flagsKey);

    return new SzSampleRecord(dataSource, recordId, flags);
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
