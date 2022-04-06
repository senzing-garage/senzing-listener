package com.senzing.listener.service.locking;

/**
 * Enumerates the locking scopes.
 */
public enum LockScope {
  /**
   * The lock was obtained for the current process and other processes on the
   * same host or on other hosts are unaware that the lock was obtained.
   */
  PROCESS,

  /**
   * The lock was obtained on this host server and all processes on the
   * same host server using the same {@link LockingService} with an equivalent
   * configuration will be aware that the lock was obtained, but processes
   * unning on other hosts are unaware of the lock.  Such locks require some
   * level of interprocess communication within a single server such as using
   * the local file system.
   */
  LOCALHOST,

  /**
   * The lock was obtained in a manner that other processes using the same
   * {@link LockingService} with an equivalent configuration on any host server
   * will be aware of the lock.  Such locks require some level of interprocess
   * communication across servers such as a central database or messaging
   * service.
   */
  CLUSTER;
}
