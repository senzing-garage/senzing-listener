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
public class SzInfoMessage implements Serializable {
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
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getAffectedEntities() affected entities property} for instances of this
   * class.
   */
  public static final String AFFECTED_ENTITIES_KEY = "affectedEntities";

  /**
   * The JSON property key for serializing and deserializing the entity ID
   * properties for instances of this class.
   */
  public static final String ENTITY_ID_KEY = "entityId";

  /**
   * The JSON property key for deserializing an entity ID when parsing raw
   * Senzing INFO message JSON to construct instances of this class.
   */
  public static final String RAW_ENTITY_ID_KEY = "ENTITY_ID";

  /**
   * The JSON property key for deserializing the {@linkplain
   * #setAffectedEntities(Collection) affected entities property} when
   * parsing raw Senzing INFO message JSON to construct instances of this class.
   */
  public static final String RAW_AFFECTED_ENTITIES_KEY = "AFFECTED_ENTITIES";

  /**
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getInterestingEntities() interesting entities property} for instances of
   * this class.
   */
  public static final String INTERESTING_ENTITIES_KEY = "interestingEntities";

  /**
   * The JSON property key for deserializing the {@linkplain
   * #setInterestingEntities(Collection) interesting
   * entities property} when parsing raw Senzing INFO message JSON to construct
   * instances of this class.
   */
  public static final String RAW_INTERESTING_ENTITIES_KEY
      = "INTERESTING_ENTITIES";

  /**
   * The JSON property key for serializing and deserializing the entities
   * from the {@linkplain #getInterestingEntities() interesting entities
   * property} for instances of this class.
   */
  public static final String ENTITIES_KEY = "entities";

  /**
   * The JSON property key for serializing and deserializing the entities
   * from the {@linkplain #getInterestingEntities() interesting entities
   * property} for instances of this class.
   */
  public static final String RAW_ENTITIES_KEY = "ENTITIES";

  /**
   * The JSON property key for serializing and deserializing the {@linkplain
   * #getNotices() notices property} for instances of this class.
   */
  public static final String NOTICES_KEY = "notices";

  /**
   * The JSON property key for deserializing the {@linkplain
   * #setNotices(Collection) notices property} when parsing raw
   * Senzing INFO message JSON to construct instances of this class.
   */
  public static final String RAW_NOTICES_KEY = "NOTICES";

  /**
   * The data source for the sample record.
   */
  private String dataSource;

  /**
   * The record ID for the sample record.
   */
  private String recordId;

  /**
   * The {@link Set} of {@link Long} entity ID's for the affected entities.
   */
  private Set<Long> affectedEntities;

  /**
   * The {@link List} of {@link SzInterestingEntity} instances describing the
   * associated interesting entities.
   */
  private List<SzInterestingEntity> interestingEntities;

  /**
   * The {@link List} of {@link SzNotice} instances describing the associated
   * notices.
   */
  private List<SzNotice> notices;

  /**
   * Default constructor.
   */
  public SzInfoMessage() {
    this(null, null, null, null, null);
  }

  /**
   * Constructs with the data source, record ID and flags.
   *
   * @param dataSource The data source for this instance.
   * @param recordId The record ID for this instance.
   * @param affectedEntities The {@link Collection} of {@link Long} entity ID's
   *                         identifying the affected entities.
   * @param interestingEntities The {@link Collection} of {@link
   *                            SzInterestingEntity} instances describing the
   *                            associated interesting entities.
   * @param notices The {@link Collection} of {@link SzNotice} instances
   *                descrbing the associated notices.
   */
  public SzInfoMessage(String                           dataSource,
                       String                           recordId,
                       Collection<Long>                 affectedEntities,
                       Collection<SzInterestingEntity>  interestingEntities,
                       Collection<SzNotice>             notices)
  {
    this.dataSource           = dataSource;
    this.recordId             = recordId;

    this.affectedEntities = (affectedEntities == null)
        ? new LinkedHashSet<>() : new LinkedHashSet<>(affectedEntities);
    this.affectedEntities.remove(null); // remove any null entries

    this.interestingEntities = (interestingEntities == null)
        ? new LinkedList<>() : new ArrayList<>(interestingEntities);
    this.interestingEntities.remove(null); // remove any null entries

    this.notices = (notices == null)
        ? new LinkedList<>() : new ArrayList<>(notices);
    this.notices.remove(null); // remove any null entries
  }

  /**
   * Gets the data source for the info message.
   *
   * @return The data source for the info message.
   */
  public String getDataSource() {
    return this.dataSource;
  }

  /**
   * Sets the data source for the info message.
   *
   * @param dataSource The data source for the info message.
   */
  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Gets the record ID for the info message.
   *
   * @return The record ID for the info message.
   */
  public String getRecordId() {
    return this.recordId;
  }

  /**
   * Sets the record ID for the info message.
   *
   * @param recordId The record ID for the info message.
   */
  public void setRecordId(String recordId) {
    this.recordId = recordId;
  }

  /**
   * Gets the <b>unmodifiable</b> {@link Set} of {@link Long} entity ID's
   * identifying the affected entities.
   *
   * @return The <b>unmodifiable</b> {@link Set} of {@link Long} entity ID's
   *         identifying the affected entities.
   */
  public Set<Long> getAffectedEntities() {
    return Collections.unmodifiableSet(this.affectedEntities);
  }

  /**
   * Sets the affected entities to those in the specified {@link Collection}
   * of {@link Long} entity ID's.
   *
   * @param affectedEntities The {@link Collection} of {@link Long} entity ID's.
   */
  public void setAffectedEntities(Collection<Long> affectedEntities) {
    this.affectedEntities.clear();
    if (affectedEntities != null) {
      this.affectedEntities.addAll(affectedEntities);
    }
  }

  /**
   * Adds the specified flag to the {@link Set} of flags for this sample record.
   *
   * @param entityId The entity ID to add to the {@link Set} of {@link Long}
   *                 entity ID's identifying the affected entities.
   */
  public void addAffectedEntity(Long entityId) {
    Objects.requireNonNull(
        entityId, "The specified entity ID cannot be null");
    this.affectedEntities.add(entityId);
  }

  /**
   * Gets the interesting entities for the INFO message as an
   * <b>unmodifiable</b> {@link List} of {@link SzInterestingEntity}.
   *
   * @return The interesting entities for the INFO message as an
   *         <b>unmodifiable</b> {@link List} of {@link SzInterestingEntity}.
   */
  public List<SzInterestingEntity> getInterestingEntities() {
    return Collections.unmodifiableList(this.interestingEntities);
  }

  /**
   * Sets the interesting entities for the INFO message to those in the
   * {@link Collection} of {@link SzInterestingEntity} instances.
   *
   * @param interestingEntities The {@link Collection} of {@link
   *                            SzInterestingEntity} instances.
   */
  public void setInterestingEntities(
      Collection<SzInterestingEntity> interestingEntities)
  {
    this.interestingEntities.clear();
    if (interestingEntities != null) {
      this.interestingEntities.addAll(interestingEntities);
    }
  }

  /**
   * Adds the interesting entity described by the specified {@link
   * SzInterestingEntity} to the {@link List} of interesting entities for
   * this instance.
   *
   * @param interestingEntity The {@link SzInterestingEntity} describing the
   *                          interesting entity to add to this INFO message.
   */
  public void addInterestingEntity(SzInterestingEntity interestingEntity) {
    Objects.requireNonNull(
        interestingEntity,
        "The specified interesting entity cannot be null");
    this.interestingEntities.add(interestingEntity);
  }

  /**
   * Gets the notices for the INFO message as an <b>unmodifiable</b>
   * {@link List} of {@link SzNotice} instances.
   *
   * @return The sample records for the interesting entity as an
   *         <b>unmodifiable</b> {@link List} of {@link SzInterestingEntity}.
   */
  public List<SzNotice> getNotices() {
    return Collections.unmodifiableList(this.notices);
  }

  /**
   * Sets the notices for the INFO message to those in the
   * {@link Collection} of {@link SzNotice} instances.
   *
   * @param notices The {@link Collection} of {@link SzNotice} instances.
   */
  public void setNotices(Collection<SzNotice> notices)
  {
    this.notices.clear();
    if (notices != null) {
      this.notices.addAll(notices);
    }
  }

  /**
   * Adds the notice described by the specified {@link SzNotice} to the
   * {@link List} of notices for this instance.
   *
   * @param notice The {@link SzNotice} describing the notice to add to
   *               this INFO message.
   */
  public void addNotice(SzNotice notice) {
    Objects.requireNonNull(
        notice, "The specified notice cannot be null");
    this.notices.add(notice);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) return false;
    SzInfoMessage that = (SzInfoMessage) o;
    return Objects.equals(this.getDataSource(), that.getDataSource())
        && Objects.equals(this.getRecordId(), that.getRecordId())
        && Objects.equals(
            this.getAffectedEntities(), that.getAffectedEntities())
        && Objects.equals(
            this.getInterestingEntities(), that.getInterestingEntities())
        && Objects.equals(this.getNotices(), that.getNotices());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getDataSource(),
                        this.getRecordId(),
                        this.getAffectedEntities(),
                        this.getInterestingEntities(),
                        this.getNotices());
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

    // add the affected entity ID array
    JsonArrayBuilder jab = Json.createArrayBuilder();
    for (Long entityId: this.getAffectedEntities()) {
      JsonUtilities.add(jab, entityId);
    }
    builder.add(AFFECTED_ENTITIES_KEY, jab);

    JsonObjectBuilder job = Json.createObjectBuilder();
    // add the interesting entities array
    jab = Json.createArrayBuilder();
    for (SzInterestingEntity entity : this.getInterestingEntities()) {
      jab.add(entity.toJsonObjectBuilder());
    }
    job.add(ENTITIES_KEY, jab);

    // add the notices array
    if (this.getNotices().size() > 0) {
      jab = Json.createArrayBuilder();
      for (SzNotice notice : this.getNotices()) {
        jab.add(notice.toJsonObjectBuilder());
      }
      job.add(NOTICES_KEY, jab);
    }

    builder.add(INTERESTING_ENTITIES_KEY, job);

    // return the builder
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
   * Serializes this instance as JSON text and returns the JSON text,
   * optionally pretty-printing the generated JSON.
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
   * @return The created {@link SzInfoMessage} instance, or <code>null</code>
   *         if the specified text is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON
   *                                  or does not contain the required JSON
   *                                  properties.
   */
  public static SzInfoMessage fromJson(String jsonText)
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
   * @return The created {@link SzInfoMessage} instance, or <code>null</code>
   *         if the specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON
   *                                  or does not contain the required JSON
   *                                  properties.
   *
   */
  public static SzInfoMessage fromJson(JsonObject jsonObject)
    throws IllegalArgumentException
  {
    return fromJson(jsonObject,
                    DATA_SOURCE_KEY,
                    RECORD_ID_KEY,
                    AFFECTED_ENTITIES_KEY,
                    INTERESTING_ENTITIES_KEY,
                    ENTITIES_KEY,
                    NOTICES_KEY);
  }

  /**
   * Parses the specified JSON text that is formatted as a raw Senzing INFO
   * message part and creates a new instance of this class as described by the
   * JSON.  If the required JSON properties are not found then an exception is
   * thrown.
   *
   * @param jsonText The JSON text to parse.
   *
   * @return The created {@link SzInfoMessage} instance, or <code>null</code>
   *         if the specified text is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON
   *                                  or does not contain the required JSON
   *                                  properties.
   */
  public static SzInfoMessage fromRawJson(String jsonText)
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
   * @return The created {@link SzInfoMessage} instance, or <code>null</code>
   *         if the specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON
   *                                  or does not contain the required JSON
   *                                  properties.
   *
   */
  public static SzInfoMessage fromRawJson(JsonObject jsonObject)
      throws IllegalArgumentException
  {
    return fromJson(jsonObject,
                    RAW_DATA_SOURCE_KEY,
                    RAW_RECORD_ID_KEY,
                    RAW_AFFECTED_ENTITIES_KEY,
                    RAW_INTERESTING_ENTITIES_KEY,
                    RAW_ENTITIES_KEY,
                    RAW_NOTICES_KEY);
  }

  /**
   * Internal method that parses the JSON described by the specified {@link
   * JsonObject} using the specified JSON property keys for the various parts.
   *
   * @param jsonObject The {@link JsonObject} describing the JSON.
   *
   * @param dataSourceKey The JSON property key for the data source property.
   *
   * @param recordIdKey The JSON property key for the record ID property.
   *
   * @param affectedEntitiesKey The JSON property key for the affected entities
   *                            property.
   *
   * @param interestingEntitiesKey The JSON property key for the interesting
   *                               entities property.
   *
   * @param entitiesKey The JSON property key for the entities property.
   *
   * @param noticesKey The JSON property key for the notices property.
   *
   * @return The created {@link SzInfoMessage} instance, or <code>null</code>
   *         if the specified parameter is <code>null</code>.
   *
   * @throws IllegalArgumentException If the specified text is not valid JSON
   *                                  or does not contain the required JSON
   *                                  properties.
   */
  private static SzInfoMessage fromJson(JsonObject  jsonObject,
                                        String      dataSourceKey,
                                        String      recordIdKey,
                                        String      affectedEntitiesKey,
                                        String      interestingEntitiesKey,
                                        String      entitiesKey,
                                        String      noticesKey)
      throws IllegalArgumentException
  {
    if (jsonObject == null) return null;
    if (!jsonObject.containsKey(dataSourceKey)
        || !jsonObject.containsKey(recordIdKey)
        || !jsonObject.containsKey(affectedEntitiesKey))
    {
      throw new IllegalArgumentException(
          "The specified JSON must at contain the \"" + dataSourceKey
              + "\", \"" + recordIdKey + "\", and \""
              + affectedEntitiesKey + "\" properties: "
              + JsonUtilities.toJsonText(jsonObject));
    }
    String dataSource = JsonUtilities.getString(jsonObject, dataSourceKey);

    String recordId = JsonUtilities.getString(jsonObject, recordIdKey);

    // parse the affected entity ID's
    JsonArray arr = JsonUtilities.getJsonArray(jsonObject, affectedEntitiesKey);
    List<Long> entityIds = (arr == null) ? null : new ArrayList<>(arr.size());
    if (arr != null) {
      for (int index = 0; index < arr.size(); index++) {
        JsonObject affectedObj = JsonUtilities.getJsonObject(arr, index);
        entityIds.add(JsonUtilities.getLong(affectedObj, RAW_ENTITY_ID_KEY));
      }
    }

    // parse the interesting entities
    JsonObject object = JsonUtilities.getJsonObject(jsonObject,
                                                    interestingEntitiesKey);
    List<SzInterestingEntity> entities  = null;
    List<SzNotice>            notices   = null;

    if (object != null) {
      arr = JsonUtilities.getJsonArray(object, entitiesKey);
      entities = (arr == null) ? null : new ArrayList<>(arr.size());
      if (arr != null) {
        for (JsonObject obj : arr.getValuesAs(JsonObject.class)) {
          entities.add(SzInterestingEntity.fromJson(obj));
        }
      }

      // parse the notices
      arr = JsonUtilities.getJsonArray(object, noticesKey);
      notices = (arr == null) ? null : new ArrayList<>(arr.size());

      if (arr != null) {
        for (JsonObject obj : arr.getValuesAs(JsonObject.class)) {
          notices.add(SzNotice.fromJson(obj));
        }
      }
    }

    // construct a new instance
    return new SzInfoMessage(
        dataSource, recordId, entityIds, entities, notices);
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
