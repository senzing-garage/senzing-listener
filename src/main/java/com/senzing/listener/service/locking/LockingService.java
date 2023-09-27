package com.senzing.listener.service.locking;

import com.senzing.listener.communication.MessageConsumer;
import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;

import javax.json.JsonObject;
import java.util.Set;

/**
 * Defines an interface for a locking service that will obtain locks on
 * resources identified by {@link ResourceKey} instances and
 */
public interface LockingService {
  /**
   * Enumerates the state of the {@link MessageConsumer}.
   */
  enum State {
    /**
     * The {@link MessageConsumer} has not yet been initialized.
     */
    UNINITIALIZED,

    /**
     * The {@link MessageConsumer} is initializing, but has not finished
     * initializing.
     */
    INITIALIZING,

    /**
     * The {@link MessageConsumer} has completed initialization and is ready
     * to handle providing resource locks.
     */
    INITIALIZED,

    /**
     * The {@link MessageConsumer} has begun destruction, but may still be
     * processing whatever messages were in progress.
     */
    DESTROYING,

    /**
     * The {@link MessageConsumer} is no longer processing messages and has
     * been destroyed.
     */
    DESTROYED;
  }

  /**
   * Initializes the locking service with the specified configuration.
   *
   * @param config The configuration to initialize with.
   *
   * @throws ServiceSetupException If a failure occurs.
   */
  void init(JsonObject config) throws ServiceSetupException;

  /**
   * Obtains the {@link State} of this {@link LockingService}.  Whenever the
   * state changes the implementation should perform a {@link
   * Object#notifyAll()} on this instance to notify any thread awaiting the
   * state change.
   *
   * @return The {@link State} of this {@link LockingService}.
   */
  State getState();

  /**
   * Handles any cleanup of this instance -- releasing all resources from the
   * initialization.
   */
  void destroy();

  /**
   * Returns the {@link LockScope} describing the scope of the locks that will
   * be obtained by this instance.
   *
   * @return The {@link LockScope} describing the scope of the locks that will
   *         be obtained by this instance.
   */
  LockScope getScope();

  /**
   * Acquires locks on <b>all</b> the resources identified by the
   * specified keys if all are available within the specified number of
   * milliseconds.  If the locks cannot all be obtained within the specified
   * time then <code>null</code> is returned to indicate that the locks were
   * not obtained.  Specify a wait time of zero (0) milliseconds to indicate
   * no waiting and do an immediate return if the locks are not available and
   * specify a negative number of milliseconds to wait indefinitely.
   *
   * @param resourceKeys The {@link Set} of resource keys
   * @param wait The number of milliseconds to wait for the locks.
   * @return The {@link LockToken} associated with the acquired locks, or
   *         <code>null</code> if the resources could not all be locked within
   *         the allotted time.
   * @throws ServiceExecutionException If a failure occurs in attempting to
   *                                   acquire the locks.
   * @throws NullPointerException If the specified {@link Set} is
   *                              <code>null</code> or contains
   *                              <code>null</code> values.
   * @throws IllegalArgumentException If the specified {@link Set} is empty.
   * @throws IllegalStateException If the {@link State} of this instance is
   *                               not {@link State#INITIALIZED}.
   */
  LockToken acquireLocks(Set<ResourceKey> resourceKeys, long wait)
    throws ServiceExecutionException, IllegalStateException;

  /**
   * Releases the locks on <b>all</b> the resources associated with the
   * specified {@link LockToken}.  If the specified {@link LockToken} is
   * <code>null</code> then a {@link NullPointerException} is thrown.  If the
   * specified {@link LockToken} is not recognized then an {@link
   * IllegalArgumentException} is thrown.
   *
   * @param lockToken The {@link LockToken} for which to release the locks.
   *
   * @return The number of resources whose locks were released.
   *
   * @throws NullPointerException If the specified {@link LockToken} is
   *                              <code>null</code>.
   * @throws IllegalArgumentException If the specified {@link LockToken} is not
   *                                  recognized.
   * @throws ServiceExecutionException If a failure occurs in attempting to
   *                                   acquire the locks.
   * @throws IllegalStateException If the {@link State} of this instance is
   *                               not {@link State#INITIALIZED} or {@link
   *                               State#DESTROYING}.
   */
  int releaseLocks(LockToken lockToken)
    throws ServiceExecutionException,
           NullPointerException,
           IllegalArgumentException,
           IllegalStateException;
}
