package com.senzing.listener.service.locking;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Identifies a lock that has been obtained.
 */
public final class LockToken implements Serializable {
  /**
   * The pattern for parsing the date values returned from the native API.
   */
  private static final String DATE_TIME_PATTERN = "yyyy-MM-dd_HH:mm:ss.SSS";

  /**
   * The time zone used for the time component of the build number.
   */
  private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

  /**
   * The {@link DateTimeFormatter} for interpreting the timestamps from the
   * native API.
   */
  private static final DateTimeFormatter DATE_TIME_FORMATTER
      = DateTimeFormatter.ofPattern(DATE_TIME_PATTERN);

  /**
   * The next lock token ID.
   */
  private static long nextTokenId = 1L;

  /**
   * The server key for this this server.
   */
  private static final String LOCAL_HOST_KEY = formatHostKey();

  /**
   * The {@link LockScope} for this instance.
   */
  private LockScope scope;

  /**
   * The key that identifies the process on the server/host on where the lock
   * was obtained.
   */
  private String processKey;

  /**
   * The key that identifies the host on which the lock was obtained.
   */
  private String hostKey;

  /**
   * The unique sequential numeric ID for this instance token instance.
   */
  private long tokenId;

  /**
   * The token key for this token which encapsulates the unique ID and the
   * timestamp when it was created.
   */
  private String tokenKey;

  /**
   * The timestamp for when this instance was constructed.
   */
  private Instant timestamp;

  /**
   * Returns the next token ID.
   */
  private synchronized long getNextTokenId() {
    return nextTokenId++;
  }

  /**
   * Constructs with the specified {@link LockScope}.
   *
   * @param scope The {@link LockScope} for the token.
   */
  public LockToken(LockScope scope) {
    Objects.requireNonNull(scope, "The scope cannot be null");
    this.scope = scope;
    ProcessHandle procHandle = ProcessHandle.current();
    long processId = procHandle.pid();
    this.processKey = String.valueOf(processId);
    ProcessHandle.Info procInfo = procHandle.info();
    Optional<Instant> startInstant = procInfo.startInstant();
    if (startInstant.isPresent()) {
      ZonedDateTime startTime = startInstant.get().atZone(UTC_ZONE);

      this.processKey = this.processKey
          + "#" + DATE_TIME_FORMATTER.format(startTime);
    }
    this.hostKey = LOCAL_HOST_KEY;

    Instant now = Instant.now();
    this.tokenId = getNextTokenId();

    this.tokenKey = "[" + this.tokenId + "#" + this.scope
        + "#" + DATE_TIME_FORMATTER.format(now.atZone(UTC_ZONE))
        + " ] @ [ " + this.processKey
        + " ] @ [ " + this.hostKey
        + " ]";

    this.timestamp = now;
  }

  /**
   * Gets the {@linkS String} representation of the MAC address for the
   * specified {@link NetworkInterface}.
   *
   * @param netInterface The {@link NetworkInterface} for which to get the
   *                     MAC address.
   * @return The {@link String} representation of the MAC address.
   */
  private static String getMacAddress(NetworkInterface netInterface)
    throws SocketException
  {
    byte[] mac = netInterface.getHardwareAddress();
    StringBuilder sb = new StringBuilder();
    String prefix = "";
    for (byte b : mac) {
      sb.append(prefix).append(String.format("%02X", b));
      prefix = "-";
    }
    return sb.toString();
  }

  /**
   * Calculates the server key using the network interfaces.  This happens
   * once when the class is loaded is reused throughout.
   *
   * @return The server key encoding the IP and MAC addresses for the server.
   */
  private static String formatHostKey() {
    try {
      LinkedHashMap<String,Integer> macAddrMap = new LinkedHashMap<>();
      LinkedList<NetworkInterface>  interfaces = new LinkedList<>();
      Enumeration<NetworkInterface> allInterfaces
          = NetworkInterface.getNetworkInterfaces();

      while (allInterfaces.hasMoreElements()) {
        NetworkInterface netInterface = allInterfaces.nextElement();
        if (netInterface.getHardwareAddress() == null) continue;
        if (!netInterface.isUp()) continue;
        if (netInterface.isVirtual()) continue;
        if (netInterface.isPointToPoint()) continue;

        Enumeration<InetAddress> addrEnum = netInterface.getInetAddresses();
        if (!addrEnum.hasMoreElements()) continue;

        String macAddr = getMacAddress(netInterface);
        Integer count = macAddrMap.get(macAddr);
        if (count == null) {
          macAddrMap.put(macAddr, 1);
        } else {
          macAddrMap.put(macAddr, count + 1);
        }
        interfaces.add(netInterface);
      }

      String prefix = "(";
      StringBuilder sb = new StringBuilder();
      for (NetworkInterface netInterface : interfaces) {
        String macAddr = getMacAddress(netInterface);
        Integer count = macAddrMap.get(macAddr);
        if (count > 1) continue;

        Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
        while (addresses.hasMoreElements()) {
          InetAddress inetAddr = addresses.nextElement();
          sb.append(prefix);
          sb.append(inetAddr.getHostAddress());
          prefix = ", ";
        }
        sb.append(")#").append(macAddr);
        prefix = " / (";
      }

      return sb.toString();
    } catch (SocketException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the {@link LockScope} describing the scope of the lock identified by
   * this lock token.
   *
   * @return The {@link LockScope} describing the scope of the lock identified
   *         by this lock token.
   */
  public LockScope getScope() {
    return this.scope;
  }

  /**
   * Gets the unique (within process) sequential token ID for this instance.
   *
   * @return The unique (within process) sequential token ID for this instance.
   */
  public long getTokenId() {
    return this.tokenId;
  }

  /**
   * Gets the {@link Instant} timestamp when this instance was constructed.
   *
   * @return The {@link Instant} timestamp when this instance was constructed.
   */
  public Instant getTimestamp() {
    return this.timestamp;
  }

  /**
   * Gets the encoded {@link String} key for the process in which the lock
   * token instance was originally constructed.
   *
   * @return The encoded {@link String} key for the process in which the lock
   *         token instance was originally constructed.
   */
  public String getProcessKey() {
    return this.processKey;
  }

  /**
   * Gets the encoded {@link String} key for the host/server on which the lock
   * token instance was originally constructed.
   *
   * @return The encoded {@link String} key for the host/server on which the
   *         lock token instance was originally constructed.
   */
  public String getHostKey() {
    return this.hostKey;
  }

  /**
   * Gets the full formatted token key which formats the elements of this
   * lock token into a unique descriptive {@link String} describing when,
   * where and how the resource is locked.  This can be used to uniquely
   * represent this {@link LockToken} as a {@link String}.
   *
   * @return The unique token key for this instance.
   */
  public String getTokenKey() {
    return this.tokenKey;
  }

  /**
   * Overridden to return <code>true</code> if and only if the specified
   * parameter is a non-null reference to an object of the same class with
   * equivalent properties.
   *
   * @param obj The object to compare with.
   * @return <code>true</code> if the objects are equal, otherwise
   *         <code>false</code>.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (this == obj) return true;
    if (obj.getClass() != this.getClass()) return false;
    LockToken that = (LockToken) obj;
    if (!Objects.equals(this.getScope(), that.getScope())) return false;
    if (!Objects.equals(this.getTokenId(), that.getTokenId())) return false;
    if (!Objects.equals(this.getTimestamp(), that.getTimestamp())) return false;
    if (!Objects.equals(this.getProcessKey(), that.getProcessKey()))
      return false;
    if (!Objects.equals(this.getHostKey(), that.getHostKey())) return false;
    if (!Objects.equals(this.getTokenKey(), that.getTokenKey())) return false;
    return true;
  }

  /**
   * Overridden to return a hash code that is consistent with the {@link
   * #equals(Object)} method.
   *
   * @return The hash code for this instance.
   */
  @Override
  public int hashCode() {
    return Objects.hash(this.getScope(),
                        this.getTokenId(),
                        this.getTimestamp(),
                        this.getProcessKey(),
                        this.getHostKey(),
                        this.getTokenKey());
  }

  /**
   * Overridden to return the result from {@link #getTokenKey()}.
   *
   * @return The result from {@link #getTokenKey()}.
   */
  public String toString() {
    return this.getTokenKey();
  }

  /**
   * Test main method to create tokens and print out their token keys.
   * @param args The command-line arguments.
   */
  public static void main(String[] args) {
    try {
      LockScope[] scopes = LockScope.values();
      for (int index = 0; index < 10; index++) {
        LockToken lockToken = new LockToken(scopes[index%3]);
        System.out.println();
        System.out.println("---------------------------------------------");
        System.out.println(lockToken);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
