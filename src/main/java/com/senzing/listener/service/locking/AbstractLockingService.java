package com.senzing.listener.service.locking;


import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;

import javax.json.JsonObject;

import java.util.*;

import static com.senzing.listener.service.locking.LockingService.State.*;

/**
 * Provides an abstract implementation of {@link LockingService}.
 */
public abstract class AbstractLockingService implements LockingService {
  /**
   * The {@link State} for this instance.
   */
  private State state = UNINITIALIZED;

  /**
   * Gets the {@link State} for this instance.
   *
   * @return The {@link State} for this instance.
   */
  public synchronized State getState() {
    return this.state;
  }

  /**
   * Provides a means to set the {@link State} for this instance as a
   * synchronized method that will notify all upon changing the state.
   *
   * @param state The {@link State} for this instance.
   */
  protected synchronized void setState(State state) {
    Objects.requireNonNull(state,"State cannot be null");
    this.state = state;
    this.notifyAll();
  }

  /**
   * Implemented to handle any base initialziation, manage the {@link State}
   * of this instance and delegate to {@link #doInit(JsonObject)}.
   *
   * @param config The {@link JsonObject} providing the configuration.
   *
   * @throws ServiceSetupException If a failure occurs.
   */
  public void init(JsonObject config) throws ServiceSetupException {
    synchronized (this) {
      if (this.getState() != UNINITIALIZED) {
        throw new IllegalStateException(
            "Cannot initialize if not in the " + UNINITIALIZED + " state: "
                + this.getState());
      }
      this.setState(INITIALIZING);
    }

    try {
      this.doInit(config);

    } catch (ServiceSetupException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSetupException(e);
    } finally {
      this.setState(INITIALIZED);
    }
  }

  /**
   * Called by the {@link #init(JsonObject)} implementation after handling the
   * base configuration parameters and parsing the specified {@link String} as
   * a {@link JsonObject}.
   *
   * @param config The {@link JsonObject} describing the configuration.
   *
   * @throws ServiceSetupException If a failure occurs during initialization.
   */
  protected abstract void doInit(JsonObject config)
      throws ServiceSetupException;

  /**
   * Implemented as a synchronized method to {@linkplain #setState(State)
   * set the state} to {@link State#DESTROYING}, call {@link #doDestroy()} and
   * then perform {@link #notifyAll()} and set the state to {@link
   * State#DESTROYED}.
   */
  public void destroy() {
    synchronized (this) {
      this.setState(DESTROYING);
    }

    try {
      // now complete the destruction / cleanup
      this.doDestroy();

    } finally {
      this.setState(DESTROYED); // this should notify all as well
    }
  }

  /**
   * This is called from the {@link #destroy()} implementation and should be
   * overridden by the concrete sub-class.
   */
  protected abstract void doDestroy();

  /**
   * Overridden to validate the {@link State} of this instance and the specified
   * {@link Set} before delegating to {@link #doAcquireLocks(List, long)} with
   * a sorted {@link List} of {@link ResourceKey} instances.
   *
   * @param resourceKeys The {@link Set} of resource keys
   * @param wait The number of milliseconds to wait for the locks.
   * @return The {@link LockToken} associated with the acquired locks, or
   *         <code>null</code> if the resources could not all be locked within
   *         the allotted time.
   * @throws ServiceExecutionException If a failure occurs in attempting to
   *                                   acquire the locks.
   * @throws IllegalStateException If the {@link State} of this instance is
   *                               not {@link State#INITIALIZED}.
   */
  @Override
  public LockToken acquireLocks(Set<ResourceKey> resourceKeys, long wait)
      throws ServiceExecutionException, IllegalStateException
  {
    // validate the arguments
    Objects.requireNonNull(
        resourceKeys, "Set of resource keys cannot be null");
    if (resourceKeys.isEmpty()) {
      throw new IllegalArgumentException(
          "The specified set of resource keys cannot be empty.");
    }
    for (ResourceKey key: resourceKeys) {
      Objects.requireNonNull(
          key, "The specified set of resource "
              + "keys cannot contain null elements: " + resourceKeys);
    }

    synchronized (this) {
      if (this.getState() != INITIALIZED) {
        throw new IllegalStateException(
            "The LockingService must be in the " + INITIALIZED
                + " state to acquire new locks.");
      }
    }

    // sort the resource keys to ensure ordering of locks
    List<ResourceKey> sortedKeys = new ArrayList<>(resourceKeys);
    Collections.sort(sortedKeys);

    // delegate
    return this.doAcquireLocks(sortedKeys, wait);
  }

  /**
   * Handles implementation of {@link LockingService#acquireLocks(Set, long)}
   * after the {@link State} of this instance has been validated.
   *
   * @param resourceKeys The sorted {@link List} of unique resource keys
   * @param wait The number of milliseconds to wait for the locks.
   * @return The {@link LockToken} associated with the acquired locks, or
   *         <code>null</code> if the resources could not all be locked within
   *         the allotted time.
   * @throws ServiceExecutionException If a failure occurs in attempting to
   *                                   acquire the locks.
   */
  protected abstract LockToken doAcquireLocks(List<ResourceKey> resourceKeys,
                                              long              wait)
      throws ServiceExecutionException;

  /**
   * Overridden to validate the {@link State} of this instance and the
   * validate that the specified {@link LockToken} is not <code>null</code>
   * and then delegate to {@link #releaseLocks(LockToken)}.
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
  @Override
  public int releaseLocks(LockToken lockToken)
      throws ServiceExecutionException,
             NullPointerException,
             IllegalArgumentException,
             IllegalStateException
  {
    Objects.requireNonNull(lockToken, "LockToken cannot be null");
    synchronized (this) {
      State state = this.getState();
      if (state != INITIALIZED && state != DESTROYING) {
        throw new IllegalStateException(
            "The LockingService must be in either the " + INITIALIZED
                + " or " + DESTROYING + " state to release locks.");
      }
    }

    // delegate
    return this.doReleaseLocks(lockToken);
  }

  /**
   * Handles implementation of {@link LockingService#releaseLocks(LockToken)}
   * after the {@link State} of this instance has been validated.
   *
   * @param lockToken The {@link LockToken} for which to release the locks.
   *
   * @return The number of resources whose locks were released.
   *
   * @throws IllegalArgumentException If the specified {@link LockToken} is not
   *                                  recognized.
   * @throws ServiceExecutionException If a failure occurs in attempting to
   *                                   acquire the locks.
   */
  protected abstract int doReleaseLocks(LockToken lockToken)
      throws ServiceExecutionException, IllegalArgumentException;
}
