package com.senzing.listener.service.locking;


import com.senzing.cmdline.CommandLineUtilities;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Objects;

import static com.senzing.io.IOUtilities.UTF_8;

/**
 * Provides a key for identifying a resource that can be locked via the
 * {@link LockingService}.
 */
public final class ResourceKey implements Serializable, Comparable<ResourceKey>
{
  /**
   * The resource type.
   */
  private String resourceType = null;

  /**
   * The <b>unmodifiable</b> {@link List} of components that more specifically
   * identify the resource.
   */
  private List<String> components = null;

  /**
   * Constructs with the {@link String} key identifying the type of resource
   * (e.g.: <code>"RECORD"</code>, <code>"ENTITY"</code>, or
   * <code>"REPORT"</code>) followed by zero or more key components that more
   * specifically identify the resource within the type of resource (e.g.: an
   * entity ID or a data source code followed by a record ID).
   *
   * @param resourceType The type of resource being identified.
   * @param components Zero or more key components that more specifically
   *                   identify the resource.
   * @throws NullPointerException If the resource type is <code>null</code>.
   */
  public ResourceKey(String resourceType, String... components) {
    Objects.requireNonNull(resourceType);
    this.resourceType = resourceType;
    this.components = (components == null) ? List.of() : List.of(components);
  }

  /**
   * Returns the resource type that identifies the type of resource.  Examples
   * may be <code>"ENTITY"</code>, <code>"RECORD"</code> or
   * <code>"REPORT"</code>.
   *
   * @return The resource type that identifies the type of resource.
   */
  public String getResourceType() {
    return this.resourceType;
  }

  /**
   * Returns the <b>unmodifiable</b> {@link List} of {@link String} components
   * that more specifically identify the resource being locked within the
   * resource type.  The returned {@link List} may be empty, but will <b>not</b>
   * be <code>null</code>; however, elements in the {@link List} may be
   * <code>null</code>.
   *
   * @return The <b>unmodifiable</b> {@link List} of {@link String} components
   *         that more specifically identify the resource being locked within
   *         the resource type.
   */
  public List<String> getComponents() {
    return this.components;
  }

  /**
   * Overridden to produce a hash code for this instance based on the resource
   * type and component parts.
   *
   * @return The hash code for this instance.
   */
  @Override
  public int hashCode() {
    return this.resourceType.hashCode() ^ Objects.hash(this.components);
  }

  /**
   * Overirden to return <code>true</code> if and only if the specified
   * parameter is a non-null reference to an instance of the same class with
   * an equivalent resource type and all component parts in the same order.
   *
   * @param obj The object to compare with.
   * @return <code>true</code> if the objects are equal, otherwise
   *         <code>false</code>.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (this == obj) return true;
    if (this.getClass() != obj.getClass()) return false;
    ResourceKey key = (ResourceKey) obj;
    return Objects.equals(this.getResourceType(), key.getResourceType())
        && Objects.equals(this.getComponents(), key.getComponents());
  }

  /**
   * Overridden to return a result consistent with the {@link #equals(Object)}
   * method that orders resource keys first by their resource types and then
   * by their component identifying parts according to {@link
   * String#compareTo(String)}.
   *
   * @param key The {@link ResourceKey} to compare with.
   * @return A negative number is less-than, zero (0) if equal and a positive
   *         number if greater than the specified instance.
   */
  @Override
  public int compareTo(ResourceKey key) {
    int diff = this.getResourceType().compareTo(key.getResourceType());
    if (diff != 0) return diff;
    List<String> comp1 = this.getComponents();
    List<String> comp2 = key.getComponents();
    diff = comp1.size() - comp2.size();
    if (diff != 0) return diff;
    int count = comp1.size();
    for (int index = 0; index < count; index++) {
      String s1 = comp1.get(index);
      String s2 = comp2.get(index);
      if (Objects.equals(s1, s2)) continue;
      if (s1 == null && s2 != null) return -1;
      if (s1 != null && s2 == null) return 1;
      diff = s1.compareTo(s2);
      if (diff != 0) return diff;
    }
    return 0;
  }

  /**
   * Overridden to return a {@link String} representation of this instance.
   *
   * @return A {@link String} representation of this instance.
   */
  @Override
  public String toString() {
    try {
      StringBuilder sb = new StringBuilder();
      sb.append(URLEncoder.encode(this.getResourceType(), UTF_8));
      for (String comp : this.getComponents()) {
        sb.append(":").append(URLEncoder.encode(comp, UTF_8));
      }
      return sb.toString();
    } catch (UnsupportedEncodingException cannotHappen) {
      throw new IllegalStateException("UTF-8 Encoding is not supported");
    }
  }

  /**
   * Parses the encoded text as a {@link ResourceKey}.  The encoding is done
   * by the {@link #toString()} method.  This method returns <code>null</code>
   * if the specified parameter is <code>null</code>.
   *
   * @param text The text to parse.
   *
   * @return The {@link ResourceKey} parsed from the specified text, or
   *         <code>null</code> if the specified parameter is <code>null</code>.
   * @throws IllegalArgumentException If the specified parameter is an empty
   *                                  string after trimming leading and trailing
   *                                  white space.
   */
  public static ResourceKey parse(String text)
    throws IllegalArgumentException
  {
    // return null if parameter is null
    if (text == null) return null;

    // trim leading and trailing whitespace
    text = text.trim();
    if (text.length() == 0) {
      throw new IllegalArgumentException(
          "The specified text cannot be an empty string or only whitespace.");
    }

    String[] tokens = text.split(":");
    try {
      String resourceType = URLDecoder.decode(tokens[0], UTF_8);
      String[] components = new String[tokens.length - 1];
      for (int index = 1; index < tokens.length; index++) {
        components[index - 1] = URLDecoder.decode(tokens[index], UTF_8);
      }
      return new ResourceKey(resourceType, components);

    } catch (UnsupportedEncodingException cannotHappen) {
      throw new IllegalStateException("UTF-8 supporting is not supported.");
    }

  }

  /**
   * Simple test main that constructs a {@link ResourceKey} and converts it to
   * a {@link String}.
   * @param args The command-line arguments.
   */
  public static void main(String[] args) {
    try {
      if (args.length == 0) {
        System.err.println("Required parameters: <resource_type> <comp>*");
        System.exit(1);
      }
      String resourceType = args[0];
      String[] components = (args.length > 1)
          ? CommandLineUtilities.shiftArguments(args, 1) : new String[0];

      ResourceKey key = new ResourceKey(resourceType, components);

      System.out.println(key);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
