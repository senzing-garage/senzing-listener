package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.locking.ResourceKey;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static java.util.Collections.*;
import static com.senzing.util.JsonUtilities.*;

/**
 * Provides a builder for a task that is tied to a {@link Scheduler}.
 * A task can be built and then scheduled to that scheduler.
 */
public class TaskBuilder {
  /**
   * The {@link Scheduler} to use for scheduling the tasks.
   */
  private Scheduler scheduler;

  /**
   * The action for the task.
   */
  private String action;

  /**
   * The sorted map of task parameters.
   */
  private SortedMap<String, Object> params;

  /**
   * The sorted set of resource keys for resources to lock for the task.
   */
  private SortedSet<ResourceKey> resourceKeys;

  /**
   * Flag indicating if tasks identical to the one being built can be collapsed
   * into a single single task with a greater mulitplicity.
   */
  private boolean allowCollapse = false;

  /**
   * Constructs with the specified {@link Scheduler} and {@link String} action
   * name.
   *
   * @param scheduler The {@link Scheduler} that created this instance.
   * @param action The {@link String} action name.
   * @throws NullPointerException If either parameter is <code>null</code>.
   */
  TaskBuilder(Scheduler scheduler, String action)
    throws NullPointerException
  {
    Objects.requireNonNull(scheduler, "Scheduler cannot be null");
    Objects.requireNonNull(action, "Action cannot be null");
    this.scheduler    = scheduler;
    this.action       = action;
    this.params       = new TreeMap<>();
    this.resourceKeys = new TreeSet<>();
  }

  /**
   * Provides a builder for a list parameter of the parent {@link TaskBuilder}.
   */
  public class ListParamBuilder {
    /**
     * The associated parameter name.
     */
    private String name;

    /**
     * The {@link List} of values to add to.
     */
    private List<Object> values;

    /**
     * Constructs with the {@link List} of values to populate.
     *
     * @param name The parameter name for this builder.
     */
    private ListParamBuilder(String name) {
      this.values = new LinkedList<>();
      this.name   = name;
    }

    /**
     * Adds a single value or a sub-list of values to the list parameter.
     *
     * @param values The one or more values to add.  If a single value then a
     *               single value is added to the list parameter, if zero or
     *               multiple values are provided then a sub-list value is added.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    public ListParamBuilder add(String... values)
        throws IllegalStateException
    {
      this.checkState();
      if (values.length == 1) {
        this.values.add(values[0]);
      } else {
        this.values.add(List.of(values));
      }
      return this;
    }

    /**
     * Adds a single value or a sub-list of values to the list parameter.
     *
     * @param values The one or more values to add.  If a single value then a
     *               single value is added to the list parameter, if zero or
     *               multiple values are provided then a sub-list value is added.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    public ListParamBuilder add(Integer... values)
        throws IllegalStateException
    {
      this.checkState();
      if (values.length == 1) {
        this.values.add(values[0]);
      } else {
        this.values.add(List.of(values));
      }
      return this;
    }

    /**
     * Adds a single value or a sub-list of values to the list parameter.
     *
     * @param values The one or more values to add.  If a single value then a
     *               single value is added to the list parameter, if zero or
     *               multiple values are provided then a sub-list value is added.
     *
     * @return A reference to this instance.
     *
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    public ListParamBuilder add(Long... values)
        throws IllegalStateException
    {
      this.checkState();
      if (values.length == 1) {
        this.values.add(values[0]);
      } else {
        this.values.add(List.of(values));
      }
      return this;
    }

    /**
     * Adds a single value or a sub-list of values to the list parameter.
     *
     * @param values The one or more values to add.  If a single value then a
     *               single value is added to the list parameter, if zero or
     *               multiple values are provided then a sub-list value is added.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    public ListParamBuilder add(Float... values)
        throws IllegalStateException
    {
      this.checkState();
      if (values.length == 1) {
        this.values.add(values[0]);
      } else {
        this.values.add(List.of(values));
      }
      return this;
    }

    /**
     * Adds a single value or a sub-list of values to the list parameter.
     *
     * @param values The one or more values to add.  If a single value then a
     *               single value is added to the list parameter, if zero or
     *               multiple values are provided then a sub-list value is added.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    public ListParamBuilder add(Double... values)
        throws IllegalStateException
    {
      this.checkState();
      if (values.length == 1) {
        this.values.add(values[0]);
      } else {
        this.values.add(List.of(values));
      }
      return this;
    }

    /**
     * Adds a single value or a sub-list of values to the list parameter.
     *
     * @param values The one or more values to add.  If a single value then a
     *               single value is added to the list parameter, if zero or
     *               multiple values are provided then a sub-list value is added.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    public ListParamBuilder add(BigInteger... values)
        throws IllegalStateException
    {
      this.checkState();
      if (values.length == 1) {
        this.values.add(values[0]);
      } else {
        this.values.add(List.of(values));
      }
      return this;
    }

    /**
     * Adds a single value or a sub-list of values to the list parameter.
     *
     * @param values The one or more values to add.  If a single value then a
     *               single value is added to the list parameter, if zero or
     *               multiple values are provided then a sub-list value is added.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    public ListParamBuilder add(BigDecimal... values)
        throws IllegalStateException
    {
      this.checkState();
      if (values.length == 1) {
        this.values.add(values[0]);
      } else {
        this.values.add(List.of(values));
      }
      return this;
    }

    /**
     * Adds a single value or a sub-list of values to the list parameter.
     *
     * @param values The one or more values to add.  If a single value then a
     *               single value is added to the list parameter, if zero or
     *               multiple values are provided then a sub-list value is added.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    public ListParamBuilder add(Boolean... values)
        throws IllegalStateException
    {
      this.checkState();
      if (values.length == 1) {
        this.values.add(values[0]);
      } else {
        this.values.add(List.of(values));
      }
      return this;
    }

    /**
     * Adds a sub-list value to this list parameter described by the specified
     * {@link JsonArray}.
     *
     * @param listValue The {@link JsonArray} describing the sub-list value to
     *                  add to the list.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    public ListParamBuilder add(JsonArray listValue)
        throws IllegalStateException
    {
      this.checkState();
      this.values.add(normalizeJsonValue(listValue));
      return this;
    }

    /**
     * Adds a map value to this list parameter described by the specified
     * {@link JsonArray}.
     *
     * @param mapValue The {@link JsonObject} describing the map value to add
     *                 to the list.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    public ListParamBuilder add(JsonObject mapValue)
        throws IllegalStateException
    {
      this.checkState();
      this.values.add(normalizeJsonValue(mapValue));
      return this;
    }

    /**
     * Checks the state of this instance and throws an {@link
     * IllegalStateException} if {@link #endList()} has already been called.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    private void checkState() throws IllegalStateException {
      if (this.values == null) {
        throw new IllegalStateException(
            "The list parameter has already been concluded: " + this.name);
      }
    }

    /**
     * Completes the list parameter value and returns the parent {@link
     * TaskBuilder}.
     *
     * @return A reference to the parent {@link TaskBuilder}.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endList()}
     */
    public TaskBuilder endList()
        throws IllegalStateException
    {
      this.checkState();
      TaskBuilder taskBuilder = TaskBuilder.this;
      taskBuilder.params.put(this.name, unmodifiableList(this.values));
      this.values = null;
      return taskBuilder;
    }
  }

  /**
   * Provides a builder for a map parameter of the parent {@link TaskBuilder}.
   */
  public class MapParamBuilder {
    /**
     * The parameter name.
     */
    private String name;

    /**
     * The {@link Map} of values to add to.
     */
    private SortedMap<String, Object> values;

    /**
     * Constructs with the parameter name.
     *
     * @param name The parameter name.
     */
    private MapParamBuilder(String name) {
      this.name   = name;
      this.values = new TreeMap<>();
    }

    /**
     * Adds a name/value pair to the map parameter with a single value or a list
     * of values as the value for the pair.  If exactly one value is provided
     * then the name/value pair will be single-valued, otherwise the value
     * will be a {@link List} of the specified values.
     *
     * @param key The named key for the value.
     * @param values The one or more values to add.  If a single value then a
     *               single value is associated with the name/value pair, if
     *               zero or multiple values are provided then a list of values
     *               is associated.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     * @throws NullPointerException If the specified key is <code>null</code>.
     * @throws IllegalArgumentException If the specified key has already been
     *                                  specified for this map parameter.
     */
    public MapParamBuilder put(String key, String... values)
        throws IllegalStateException,
               NullPointerException,
               IllegalArgumentException
    {
      this.checkKey(key);
      if (values.length == 1) {
        this.values.put(key, values[0]);
      } else {
        this.values.put(key, List.of(values));
      }
      return this;
    }

    /**
     * Adds a name/value pair to the map parameter with a single value or a list
     * of values as the value for the pair.  If exactly one value is provided
     * then the name/value pair will be single-valued, otherwise the value
     * will be a {@link List} of the specified values.
     *
     * @param key The named key for the value.
     * @param values The one or more values to add.  If a single value then a
     *               single value is associated with the name/value pair, if
     *               zero or multiple values are provided then a list of values
     *               is associated.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     * @throws NullPointerException If the specified key is <code>null</code>.
     * @throws IllegalArgumentException If the specified key has already been
     *                                  specified for this map parameter.
     */
    public MapParamBuilder put(String key, Integer... values)
        throws IllegalStateException,
               NullPointerException,
               IllegalArgumentException
    {
      this.checkKey(key);
      if (values.length == 1) {
        this.values.put(key, values[0]);
      } else {
        this.values.put(key, List.of(values));
      }
      return this;
    }

    /**
     * Adds a name/value pair to the map parameter with a single value or a list
     * of values as the value for the pair.  If exactly one value is provided
     * then the name/value pair will be single-valued, otherwise the value
     * will be a {@link List} of the specified values.
     *
     * @param key The named key for the value.
     * @param values The one or more values to add.  If a single value then a
     *               single value is associated with the name/value pair, if
     *               zero or multiple values are provided then a list of values
     *               is associated.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     * @throws NullPointerException If the specified key is <code>null</code>.
     * @throws IllegalArgumentException If the specified key has already been
     *                                  specified for this map parameter.
     */
    public MapParamBuilder put(String key, Long... values)
        throws IllegalStateException,
               NullPointerException,
               IllegalArgumentException
    {
      this.checkKey(key);
      if (values.length == 1) {
        this.values.put(key, values[0]);
      } else {
        this.values.put(key, List.of(values));
      }
      return this;
    }

    /**
     * Adds a name/value pair to the map parameter with a single value or a list
     * of values as the value for the pair.  If exactly one value is provided
     * then the name/value pair will be single-valued, otherwise the value
     * will be a {@link List} of the specified values.
     *
     * @param key The named key for the value.
     * @param values The one or more values to add.  If a single value then a
     *               single value is associated with the name/value pair, if
     *               zero or multiple values are provided then a list of values
     *               is associated.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     * @throws NullPointerException If the specified key is <code>null</code>.
     * @throws IllegalArgumentException If the specified key has already been
     *                                  specified for this map parameter.
     */
    public MapParamBuilder put(String key, Float... values)
        throws IllegalStateException,
               NullPointerException,
               IllegalArgumentException
    {
      this.checkKey(key);
      if (values.length == 1) {
        this.values.put(key, values[0]);
      } else {
        this.values.put(key, List.of(values));
      }
      return this;
    }

    /**
     * Adds a name/value pair to the map parameter with a single value or a list
     * of values as the value for the pair.  If exactly one value is provided
     * then the name/value pair will be single-valued, otherwise the value
     * will be a {@link List} of the specified values.
     *
     * @param key The named key for the value.
     * @param values The one or more values to add.  If a single value then a
     *               single value is associated with the name/value pair, if
     *               zero or multiple values are provided then a list of values
     *               is associated.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     * @throws NullPointerException If the specified key is <code>null</code>.
     * @throws IllegalArgumentException If the specified key has already been
     *                                  specified for this map parameter.
     */
    public MapParamBuilder put(String key, Double... values)
        throws IllegalStateException,
               NullPointerException,
               IllegalArgumentException
    {
      this.checkKey(key);
      if (values.length == 1) {
        this.values.put(key, values[0]);
      } else {
        this.values.put(key, List.of(values));
      }
      return this;
    }

    /**
     * Adds a name/value pair to the map parameter with a single value or a list
     * of values as the value for the pair.  If exactly one value is provided
     * then the name/value pair will be single-valued, otherwise the value
     * will be a {@link List} of the specified values.
     *
     * @param key The named key for the value.
     * @param values The one or more values to add.  If a single value then a
     *               single value is associated with the name/value pair, if
     *               zero or multiple values are provided then a list of values
     *               is associated.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     * @throws NullPointerException If the specified key is <code>null</code>.
     * @throws IllegalArgumentException If the specified key has already been
     *                                  specified for this map parameter.
     */
    public MapParamBuilder put(String key, BigInteger... values)
        throws IllegalStateException,
               NullPointerException,
               IllegalArgumentException
    {
      this.checkKey(key);
      if (values.length == 1) {
        this.values.put(key, values[0]);
      } else {
        this.values.put(key, List.of(values));
      }
      return this;
    }

    /**
     * Adds a name/value pair to the map parameter with a single value or a list
     * of values as the value for the pair.  If exactly one value is provided
     * then the name/value pair will be single-valued, otherwise the value
     * will be a {@link List} of the specified values.
     *
     * @param key The named key for the value.
     * @param values The one or more values to add.  If a single value then a
     *               single value is associated with the name/value pair, if
     *               zero or multiple values are provided then a list of values
     *               is associated.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     * @throws NullPointerException If the specified key is <code>null</code>.
     * @throws IllegalArgumentException If the specified key has already been
     *                                  specified for this map parameter.
     */
    public MapParamBuilder put(String key, BigDecimal... values)
        throws IllegalStateException,
               NullPointerException,
               IllegalArgumentException
    {
      this.checkKey(key);
      if (values.length == 1) {
        this.values.put(key, values[0]);
      } else {
        this.values.put(key, List.of(values));
      }
      return this;
    }

    /**
     * Adds a name/value pair to the map parameter with a single value or a list
     * of values as the value for the pair.  If exactly one value is provided
     * then the name/value pair will be single-valued, otherwise the value
     * will be a {@link List} of the specified values.
     *
     * @param key The named key for the value.
     * @param values The one or more values to add.  If a single value then a
     *               single value is associated with the name/value pair, if
     *               zero or multiple values are provided then a list of values
     *               is associated.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     * @throws NullPointerException If the specified key is <code>null</code>.
     * @throws IllegalArgumentException If the specified key has already been
     *                                  specified for this map parameter.
     */
    public MapParamBuilder put(String key, Boolean... values)
        throws IllegalStateException,
               NullPointerException,
               IllegalArgumentException
    {
      this.checkKey(key);
      if (values.length == 1) {
        this.values.put(key, values[0]);
      } else {
        this.values.put(key, List.of(values));
      }
      return this;
    }

    /**
     * Adds a name/value pair to the map parameter with a list of values as the
     * value for the pair.  The list of values is described by the specified
     * {@link JsonArray}.
     *
     * @param key The named key for the value.
     * @param listValue The {@link JsonArray} describing the sub-list value to
     *                  be associated.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     * @throws NullPointerException If the specified key is <code>null</code>.
     * @throws IllegalArgumentException If the specified key has already been
     *                                  specified for this map parameter.
     */
    public MapParamBuilder put(String key, JsonArray listValue)
        throws IllegalStateException,
               NullPointerException,
               IllegalArgumentException
    {
      this.checkKey(key);
      this.values.put(key, normalizeJsonValue(listValue));
      return this;
    }

    /**
     * Adds a name/value pair to the map parameter with a map of values as the
     * value for the pair.  The map of values is described by the specified
     * {@link JsonObject}.
     *
     * @param key The named key for the value.
     * @param mapValue The {@link JsonObject} describing the sub-map value to
     *                 be associated.
     *
     * @return A reference to this instance.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     * @throws NullPointerException If the specified key is <code>null</code>.
     * @throws IllegalArgumentException If the specified key has already been
     *                                  specified for this map parameter.
     */
    public MapParamBuilder put(String key, JsonObject mapValue)
      throws IllegalStateException,
             NullPointerException,
             IllegalArgumentException
    {
      this.checkKey(key);
      this.values.put(key, normalizeJsonValue(mapValue));
      return this;
    }

    /**
     * Completes the map parameter value and returns the parent {@link
     * TaskBuilder}.
     *
     * @return A reference to the parent {@link TaskBuilder}.
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     */
    public TaskBuilder endMap()
        throws IllegalStateException
    {
      this.checkState();
      TaskBuilder taskBuilder = TaskBuilder.this;
      taskBuilder.params.put(this.name, unmodifiableSortedMap(this.values));
      this.values = null;
      return taskBuilder;
    }

    /**
     * Checks if the specified key has already been added to the map parameter.
     * @param key The key to check.
     * @throws NullPointerException If the specified key is <code>null</code>.
     * @throws IllegalArgumentException If the key is already present in the map
     *                                  of values.
     */
    private void checkKey(String key)
        throws NullPointerException, IllegalArgumentException
    {
      this.checkState();
      Objects.requireNonNull(key, "The specified key cannot be null");
      if (this.values.containsKey(key)) {
        throw new IllegalArgumentException(
            "The specified key already has a name/value pair associated with "
                + "it in the map parameter value: " + key);
      }
    }

    /**
     * Checks the state of this instance and throws an {@link
     * IllegalStateException} if {@link #endMap()} has already been called.
     *
     * @throws IllegalStateException If the list parameter has already been
     *                               ended via {@link #endMap()}
     */
    private void checkState() throws IllegalStateException {
      if (this.values == null) {
        throw new IllegalStateException(
            "The list parameter has already been concluded: " + this.name);
      }
    }
  }

  /**
   * Adds a required resource that must be locked for exclusive access when
   * the associated task is performed.
   *
   * @param resourceType The type of resource being identified.
   * @param components Zero or more key components that more specifically
   *                   identify the resource.
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the resource type is <code>null</code>.
   */
  public TaskBuilder resource(String resourceType, String... components)
    throws IllegalStateException, NullPointerException
  {
    this.resourceKeys.add(new ResourceKey(resourceType, components));
    return this;
  }

  /**
   * Adds a required resource that must be locked for exclusive access when
   * the associated task is performed.  This variant converts the objects
   * in the specified components array to {@link String} instances.
   *
   * @param resourceType The type of resource being identified.
   * @param components Zero or more key components that more specifically
   *                   identify the resource.
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the resource type is <code>null</code>.
   */
  public TaskBuilder resource(String resourceType, Object... components)
      throws IllegalStateException, NullPointerException
  {
    this.resourceKeys.add(new ResourceKey(resourceType, components));
    return this;
  }

  /**
   * Adds a parameter with the specified name a single value or a list
   * of values.  If exactly one value is provided then the parameter pair will
   * be single-valued, otherwise the value will be a {@link List} of the
   * specified values.
   *
   * @param name The parameter name for the parameter.
   * @param values The one or more values for the parameter.  If a single value
   *               is provided then the parameter will have a single value, if
   *               zero or multiple values are provided then a list values are
   *               associated with the parameter.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public TaskBuilder parameter(String name, String... values)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    if (values.length == 1) {
      this.params.put(name, values[0]);
    } else {
      this.params.put(name, List.of(values));
    }
    return this;
  }

  /**
   * Adds a parameter with the specified name a single value or a list
   * of values.  If exactly one value is provided then the parameter pair will
   * be single-valued, otherwise the value will be a {@link List} of the
   * specified values.
   *
   * @param name The parameter name for the parameter.
   * @param values The one or more values for the parameter.  If a single value
   *               is provided then the parameter will have a single value, if
   *               zero or multiple values are provided then a list values are
   *               associated with the parameter.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public TaskBuilder parameter(String name, Integer... values)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    if (values.length == 1) {
      this.params.put(name, values[0]);
    } else {
      this.params.put(name, List.of(values));
    }
    return this;
  }

  /**
   * Adds a parameter with the specified name a single value or a list
   * of values.  If exactly one value is provided then the parameter pair will
   * be single-valued, otherwise the value will be a {@link List} of the
   * specified values.
   *
   * @param name The parameter name for the parameter.
   * @param values The one or more values for the parameter.  If a single value
   *               is provided then the parameter will have a single value, if
   *               zero or multiple values are provided then a list values are
   *               associated with the parameter.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public TaskBuilder parameter(String name, Long... values)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    if (values.length == 1) {
      this.params.put(name, values[0]);
    } else {
      this.params.put(name, List.of(values));
    }
    return this;
  }

  /**
   * Adds a parameter with the specified name a single value or a list
   * of values.  If exactly one value is provided then the parameter pair will
   * be single-valued, otherwise the value will be a {@link List} of the
   * specified values.
   *
   * @param name The parameter name for the parameter.
   * @param values The one or more values for the parameter.  If a single value
   *               is provided then the parameter will have a single value, if
   *               zero or multiple values are provided then a list values are
   *               associated with the parameter.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public TaskBuilder parameter(String name, Float... values)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    if (values.length == 1) {
      this.params.put(name, values[0]);
    } else {
      this.params.put(name, List.of(values));
    }
    return this;
  }

  /**
   * Adds a parameter with the specified name a single value or a list
   * of values.  If exactly one value is provided then the parameter pair will
   * be single-valued, otherwise the value will be a {@link List} of the
   * specified values.
   *
   * @param name The parameter name for the parameter.
   * @param values The one or more values for the parameter.  If a single value
   *               is provided then the parameter will have a single value, if
   *               zero or multiple values are provided then a list values are
   *               associated with the parameter.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public TaskBuilder parameter(String name, Double... values)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    if (values.length == 1) {
      this.params.put(name, values[0]);
    } else {
      this.params.put(name, List.of(values));
    }
    return this;
  }

  /**
   * Adds a parameter with the specified name a single value or a list
   * of values.  If exactly one value is provided then the parameter pair will
   * be single-valued, otherwise the value will be a {@link List} of the
   * specified values.
   *
   * @param name The parameter name for the parameter.
   * @param values The one or more values for the parameter.  If a single value
   *               is provided then the parameter will have a single value, if
   *               zero or multiple values are provided then a list values are
   *               associated with the parameter.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public TaskBuilder parameter(String name, BigInteger... values)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    if (values.length == 1) {
      this.params.put(name, values[0]);
    } else {
      this.params.put(name, List.of(values));
    }
    return this;
  }

  /**
   * Adds a parameter with the specified name a single value or a list
   * of values.  If exactly one value is provided then the parameter pair will
   * be single-valued, otherwise the value will be a {@link List} of the
   * specified values.
   *
   * @param name The parameter name for the parameter.
   * @param values The one or more values for the parameter.  If a single value
   *               is provided then the parameter will have a single value, if
   *               zero or multiple values are provided then a list values are
   *               associated with the parameter.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public TaskBuilder parameter(String name, BigDecimal... values)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    if (values.length == 1) {
      this.params.put(name, values[0]);
    } else {
      this.params.put(name, List.of(values));
    }
    return this;
  }

  /**
   * Adds a parameter with the specified name a single value or a list
   * of values.  If exactly one value is provided then the parameter pair will
   * be single-valued, otherwise the value will be a {@link List} of the
   * specified values.
   *
   * @param name The parameter name for the parameter.
   * @param values The one or more values for the parameter.  If a single value
   *               is provided then the parameter will have a single value, if
   *               zero or multiple values are provided then a list values are
   *               associated with the parameter.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public TaskBuilder parameter(String name, Boolean... values)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    if (values.length == 1) {
      this.params.put(name, values[0]);
    } else {
      this.params.put(name, List.of(values));
    }
    return this;
  }

  /**
   * Adds a parameter to the task with a list of values as its value.  The list
   * of values is described by the specified {@link JsonArray}.
   *
   * @param name The parameter name for the parameter.
   * @param listValue The {@link JsonArray} describing the sub-list value to
   *                  be associated.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public TaskBuilder parameter(String name, JsonArray listValue)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    this.params.put(name, normalizeJsonValue(listValue));
    return this;
  }

  /**
   * Adds a parameter to the task with a map of values as its value.  The map
   * of values is described by the specified {@link JsonObject}.
   *
   * @param name The parameter name for the parameter.
   * @param mapValue The {@link JsonObject} describing the sub-map value to
   *                 be associated.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public TaskBuilder parameter(String name, JsonObject mapValue)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    this.params.put(name, normalizeJsonValue(mapValue));
    return this;
  }

  /**
   * Initiates the creation of a new list parameter that will be created once
   * {@link ListParamBuilder#endList()} is called on the returned {@link
   * ListParamBuilder}.
   *
   * @param name The name for the list parameter.
   *
   * @return The {@link ListParamBuilder} for building the list parameter.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public ListParamBuilder listParameter(String name)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    ListParamBuilder paramBuilder = new ListParamBuilder(name);
    this.params.put(name, null);
    return paramBuilder;
  }

  /**
   * Initiates the creation of a new map parameter that will be created once
   * {@link MapParamBuilder#endMap()} is called on the returned {@link
   * MapParamBuilder}.
   *
   * @param name The name for the map parameter.
   *
   * @return The {@link MapParamBuilder} for building the map parameter.
   *
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   *
   * @throws NullPointerException If the specified parameter name is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter name has
   *                                  already been specified for this map
   *                                  parameter.
   */
  public MapParamBuilder mapParameter(String name)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkParamName(name);
    MapParamBuilder paramBuilder = new MapParamBuilder(name);
    this.params.put(name, null);
    return paramBuilder;
  }

  /**
   * Queues the task that has been built for scheduling (pending {@linkplain
   * Scheduler#commit()}).  The scheduled task will {@linkplain
   * Task#isAllowingCollapse() may be collapsed} with other pending identical
   * scheduled tasks.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   */
  public Scheduler schedule() throws IllegalStateException {
    return this.schedule(true);
  }

  /**
   * Queues the task that has been built for scheduling (pending {@linkplain
   * Scheduler#commit()}).  This version allows the caller to indicate
   * whether the scheduled task may be collapsed with other identical
   * tasks and performed once with a corresponding multiplicity.
   *
   * @param allowCollapse <code>true</code> if the scheduled task may be
   *                      collapsed with other identical tasks, and
   *                      <code>false</code> if it cannot be collapsed.
   *
   * @return A reference to this instance.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   */
  public Scheduler schedule(boolean allowCollapse)
      throws IllegalStateException
  {
    // check the state
    this.checkState();

    // get the task group
    TaskGroup taskGroup = this.scheduler.getTaskGroup();

    // create the task
    Task task = new Task(this.action,
                         this.params,
                         this.resourceKeys,
                         taskGroup,
                         allowCollapse);

    // add to the task group
    if (taskGroup != null) {
      taskGroup.addTask(task);
    }

    // add to the scheduler
    this.scheduler.schedule(task);

    // return the scheduler
    Scheduler result = this.scheduler;

    // set the scheduler to null
    this.scheduler = null;

    // return the result
    return result;
  }

  /**
   * Checks if this instance has already been scheduled and if so, throws an
   * {@link IllegalStateException}.
   *
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   */
  private void checkState() throws IllegalStateException {
    if (this.scheduler == null) {
      throw new IllegalStateException(
          "This TaskBuiilder instance has already created and scheduled a "
          + "task: " + this.action + " / " + this.params);
    }
  }

  /**
   * Checks if the specified parameter name has already been specified as a
   * parameter name.
   * @param name The parameter name to check.
   * @throws IllegalStateException If this instance has already created and
   *                               scheduled a task via {@link #schedule()}.
   * @throws NullPointerException If the specified key is <code>null</code>.
   * @throws IllegalArgumentException If the key is already present in the map
   *                                  of values.
   */
  private void checkParamName(String name)
    throws IllegalStateException, NullPointerException, IllegalArgumentException
  {
    this.checkState();
    Objects.requireNonNull(
        name, "The specified parameter name cannot be null");
    if (this.params.containsKey(name)) {
      throw new IllegalArgumentException(
          "The specified parameter name has already been specified: " + name);
    }
  }

}
