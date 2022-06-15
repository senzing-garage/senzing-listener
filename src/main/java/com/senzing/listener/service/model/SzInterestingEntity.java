package com.senzing.listener.service.model;

import com.senzing.util.JsonUtilities;

import javax.json.*;
import java.io.Serializable;
import java.util.*;

/**
 * Simple class to represent an element from the <code>SAMPLE_RECORDS</code>
 * array inside an element of the <code>INTERESTING_ENTITIES</code> array of
 * an INFO message.
 */
public class SzInterestingEntity implements Serializable {
  /**
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getEntityId() entity ID property} for instances of this class.
   */
  public static final String ENTITY_ID_KEY = "entityId";

  /**
   * The JSON property key for deserializing the {@linkplain #setEntityId(Long)
   * entity ID property} when parsing raw Senzing INFO message JSON to construct
   * instances of this class.
   */
  public static final String RAW_ENTITY_ID_KEY = "ENTITY_ID";

  /**
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getDegrees() degrees property} for instances of this class.
   */
  public static final String DEGREES_KEY = "degrees";

  /**
   * The JSON property key for deserializing the {@linkplain
   * #setDegrees(Integer) degrees property} when parsing raw Senzing
   * INFO message JSON to construct instances of this class.
   */
  public static final String RAW_DEGREES_KEY = "DEGREES";

  /**
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getFlags() flags property} for instances of this class.
   */
  public static final String FLAGS_KEY = "flags";

  /**
   * The JSON property key for deserializing the {@linkplain
   * #setFlags(Collection<String>) flags property} when parsing raw Senzing
   * INFO message JSON to construct instances of this class.
   */
  public static final String RAW_FLAGS_KEY = "FLAGS";

  /**
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getSampleRecords() sample records property} for instances of this class.
   */
  public static final String SAMPLE_RECORDS_KEY = "sampleRecords";

  /**
   * The JSON property key for deserializing the {@linkplain
   * #setSampleRecords(Collection<SzSampleRecord>) sample records property} when
   * parsing raw Senzing INFO message JSON to construct instances of this class.
   */
  public static final String RAW_SAMPLE_RECORDS_KEY = "SAMPLE_RECORDS";

  /**
   * The entity ID for the interesting entity.
   */
  private Long entityId;

  /**
   * The degrees of sepration for the interesting entity.
   */
  private Integer degrees;

  /**
   * The {@link Set} of flags for the sample record.
   */
  private Set<String> flags;

  /**
   * The {@link List} of {@link SzSampleRecord} instances describing the sample
   * records.
   */
  private List<SzSampleRecord> sampleRecords;

  /**
   * Default constructor.
   */
  public SzInterestingEntity() {
    this(null, null, null, null);
  }

  /**
   * Constructs with the entity ID, degrees of separation, flags amd sample
   * records.
   *
   * @param entityId The entity ID for this instance.
   * @param degrees The number of degrees of separation for this instance.
   * @param flags The {@link Collection} of flags for this instance.
   * @param sampleRecords The {@link Collection} of {@link SzSampleRecord}
   *                      instances describing the sample records for this
   *                      instance.
   */
  public SzInterestingEntity(Long                       entityId,
                             Integer                    degrees,
                             Collection<String>         flags,
                             Collection<SzSampleRecord> sampleRecords)
  {
    this.entityId       = entityId;
    this.degrees        = degrees;

    this.flags = (flags == null)
        ? new LinkedHashSet<>() : new LinkedHashSet<>(flags);
    this.flags.remove(null); // remove any null entries

    this.sampleRecords = (sampleRecords == null)
        ? new LinkedList<>() : new ArrayList<>(sampleRecords);
    this.sampleRecords.remove(null); // remove any null entries
  }

  /**
   * Gets the entity ID for the interesting entity.
   *
   * @return The entity ID for the interesting entity.
   */
  public Long getEntityId() {
    return this.entityId;
  }

  /**
   * Sets the entity ID for the interesting entity.
   *
   * @param entityId The entity ID for the interesting entity.
   */
  public void setEntityId(Long entityId) {
    this.entityId = entityId;
  }

  /**
   * Gets the degrees of separation for the interesting entity.
   *
   * @return The degrees of separation for the interesting entity.
   */
  public Integer getDegrees() {
    return this.degrees;
  }

  /**
   * Sets the degrees of separation for the interesting entity.
   *
   * @param degrees The degrees of separation for the interesting entity.
   */
  public void setDegrees(Integer degrees) {
    this.degrees = degrees;
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

  /**
   * Gets the sample records for the interesting entity as an
   * <b>unmodifiable</b> {@link List} of {@link SzSampleRecord}.
   *
   * @return The sample records for the interesting entity as an
   *         <b>unmodifiable</b> {@link List} of {@link SzSampleRecord}.
   */
  public List<SzSampleRecord> getSampleRecords() {
    return Collections.unmodifiableList(this.sampleRecords);
  }

  /**
   * Sets the sample records for the interesting entity to those in the
   * specified {@link Collection}.
   *
   * @param sampleRecords The {@link Collection} of {@link SzSampleRecord}
   *                      instances.
   */
  public void setSampleRecords(Collection<SzSampleRecord> sampleRecords) {
    this.sampleRecords.clear();
    if (sampleRecords != null) this.sampleRecords.addAll(sampleRecords);
  }

  /**
   * Adds the specified sample record described by the specified {@link
   * SzSampleRecord} to the {@link List} of sample records for this instance.
   *
   * @param sampleRecord The {@link SzSampleRecord} describing the sample
   *                     record to add to this interesting entity.
   */
  public void addSampleRecord(SzSampleRecord sampleRecord) {
    Objects.requireNonNull(
        sampleRecord, "The specified sample record cannot be null");
    this.sampleRecords.add(sampleRecord);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) return false;
    SzInterestingEntity that = (SzInterestingEntity) o;
    return Objects.equals(this.getEntityId(), that.getEntityId())
        && Objects.equals(this.getDegrees(), that.getDegrees())
        && Objects.equals(this.getFlags(), that.getFlags())
        && Objects.equals(this.getSampleRecords(), that.getSampleRecords());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getEntityId(),
                        this.getDegrees(),
                        this.getFlags(),
                        this.getSampleRecords());
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
    JsonUtilities.add(builder, ENTITY_ID_KEY, this.getEntityId());
    JsonUtilities.add(builder, DEGREES_KEY, this.getDegrees());

    JsonArrayBuilder jab = Json.createArrayBuilder();
    for (String flag: this.getFlags()) {
      JsonUtilities.add(jab, flag);
    }
    builder.add(FLAGS_KEY, jab);

    jab = Json.createArrayBuilder();
    for (SzSampleRecord record : this.getSampleRecords()) {
      jab.add(record.toJsonObjectBuilder());
    }
    builder.add(SAMPLE_RECORDS_KEY, jab);

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
   * @return The created {@link SzInterestingEntity} instance, or
   *         <code>null</code> if the specified text is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON
   *                                  or does not contain the required JSON
   *                                  properties.
   */
  public static SzInterestingEntity fromJson(String jsonText)
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
   * @return The created {@link SzInterestingEntity} instance, or
   *         <code>null</code> if the specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON
   *                                  or does not contain the required JSON
   *                                  properties.
   *
   */
  public static SzInterestingEntity fromJson(JsonObject jsonObject)
    throws IllegalArgumentException
  {
    return fromJson(jsonObject,
                    ENTITY_ID_KEY,
                    DEGREES_KEY,
                    FLAGS_KEY,
                    SAMPLE_RECORDS_KEY);
  }

  /**
   * Parses the specified JSON text that is formatted as a raw Senzing INFO
   * message part and creates a new instance of this class as described by the
   * JSON.  If the required JSON properties are not found then an exception is
   * thrown.
   *
   * @param jsonText The JSON text to parse.
   *
   * @return The created {@link SzInterestingEntity} instance, or
   *         <code>null</code> if the specified text is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON
   *                                  or does not contain the required JSON
   *                                  properties.
   */
  public static SzInterestingEntity fromRawJson(String jsonText)
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
   * @return The created {@link SzInterestingEntity} instance, or
   *         <code>null</code> if the specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON
   *                                  or does not contain the required JSON
   *                                  properties.
   *
   */
  public static SzInterestingEntity fromRawJson(JsonObject jsonObject)
      throws IllegalArgumentException
  {
    return fromJson(jsonObject,
                    RAW_ENTITY_ID_KEY,
                    RAW_DEGREES_KEY,
                    RAW_FLAGS_KEY,
                    RAW_SAMPLE_RECORDS_KEY);
  }

  /**
   * Internal method that parses the JSON described by the specified {@link
   * JsonObject} using the specified JSON property keys for the various parts.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON to parse.
   *
   * @param entityIdKey The JSON property key for the entity ID property.
   *
   * @param degreesKey The JSON property key for the degrees property.
   *
   * @param flagsKey The JSON property key for the flags property.
   *
   * @param sampleRecordsKey The JSON property key for the sample records
   *                         property.
   *
   * @return The created {@link SzInterestingEntity} instance, or
   *         <code>null</code> if the specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON
   *                                  or does not contain the required JSON
   *                                  properties.
   *
   */
  private static SzInterestingEntity fromJson(JsonObject  jsonObject,
                                              String      entityIdKey,
                                              String      degreesKey,
                                              String      flagsKey,
                                              String      sampleRecordsKey)
      throws IllegalArgumentException
  {
    if (jsonObject == null) return null;
    if (!jsonObject.containsKey(entityIdKey)
        || !jsonObject.containsKey(degreesKey)
        || !jsonObject.containsKey(flagsKey)
        || !jsonObject.containsKey(sampleRecordsKey))
    {
      throw new IllegalArgumentException(
          "The specified JSON must at contain the \"" + entityIdKey
              + "\", \"" + degreesKey + "\", \"" + flagsKey
              + "\" and \"" + sampleRecordsKey + "\" properties: "
              + JsonUtilities.toJsonText(jsonObject));
    }

    Long entityId = JsonUtilities.getLong(jsonObject, entityIdKey);

    Integer degrees = JsonUtilities.getInteger(jsonObject, degreesKey);

    List<String> flags = JsonUtilities.getStrings(jsonObject, flagsKey);

    JsonArray arr = JsonUtilities.getJsonArray(jsonObject, sampleRecordsKey);
    List<SzSampleRecord> sampleRecords
        = (arr == null) ? null : new ArrayList<>(arr.size());
    if (arr != null) {
      for (JsonObject obj : arr.getValuesAs(JsonObject.class)) {
        sampleRecords.add(SzSampleRecord.fromJson(obj));
      }
    }

    return new SzInterestingEntity(entityId, degrees, flags, sampleRecords);
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
