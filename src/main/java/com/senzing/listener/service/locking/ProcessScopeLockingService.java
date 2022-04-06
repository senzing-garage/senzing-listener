package com.senzing.listener.service.locking;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;

import javax.json.JsonObject;

import java.util.*;

import static com.senzing.listener.service.locking.LockScope.*;

/**
 * Provides a {@link LockingService} implementation that locks resources within
 * the scope of the current process using in-memory data constructs.
 */
public class ProcessScopeLockingService extends AbstractLockingService
    implements LockingService
{
  /**
   * The {@link Map} of {@link LockToken} instances to the {@link Set} of
   * {@link ResourceKey} instances that have locks associated with that token.
   */
  private Map<LockToken, Set<ResourceKey>> locksToResourcesMap = null;

  /**
   * The {@link Map} of {@link ResourceKey} keys to {@link LockToken} values
   * representing the lock owner for that resource.
   */
  private Map<ResourceKey, LockToken> resourceToLockMap = null;

  /**
   * Implemented to do nothing since no additional initialization is required.
   */
  @Override
  protected void doInit(JsonObject config) throws ServiceSetupException {
    this.locksToResourcesMap  = new HashMap<>();
    this.resourceToLockMap    = new HashMap<>();
  }

  /**
   * Implemented to wait for all locks that are held to be released and then
   * return once all are released.
   */
  @Override
  protected void doDestroy() {
    synchronized (this) {
      // wait for all locks to be released
      while (this.locksToResourcesMap.size() > 0) {
        try {
          this.wait(1000L);
        } catch (InterruptedException ignore) {
          // do nothing
        }
      }
    }
  }

  /**
   * Implemented to return {@link LockScope#PROCESS}.
   */
  @Override
  public LockScope getScope() {
    return PROCESS;
  }

  /**
   * Implemented to obtain the locks as in-memory locks.
   *
   * {@inheritDoc}
   */
  @Override
  protected LockToken doAcquireLocks(List<ResourceKey> resourceKeys, long wait)
      throws ServiceExecutionException
  {
    synchronized (this) {
      boolean available = true;
      long start = System.nanoTime();
      long duration = 0L;

      do {
        // mark as available and then verify
        available = true;

        // check if all resources are available to lock
        for (ResourceKey key : resourceKeys) {
          if (this.resourceToLockMap.containsKey(key)) {
            available = false;
            break;
          }
        }

        // check how long we have been waiting
        if (!available) {
          duration = (System.nanoTime() - start) / 1000000L;

          if (wait < 0L || duration < wait) {
            long timeout = (wait < 0L) ? -1L : Math.max(0L, wait-duration);
            try {
              if (timeout < 0L) {
                this.wait();
              } else {
                this.wait(timeout);
              }

            } catch (InterruptedException e) {
              // if interrupted then return null
              return null;
            }
          }
        }
      } while (!available && (wait < 0L || duration < wait));

      // check if not available
      if (!available) return null;

      // if available, then create the locks
      LockToken lockToken = new LockToken(this.getScope());
      for (ResourceKey key: resourceKeys) {
        this.resourceToLockMap.put(key,  lockToken);
      }
      this.locksToResourcesMap.put(
          lockToken, new LinkedHashSet<>(resourceKeys));

      // notify all
      this.notifyAll();

      // return the lock token
      return lockToken;
    }
  }

  /**
   * Implemented to release the in-memory locks.
   *
   * {@inheritDoc}
   */
  @Override
  protected int doReleaseLocks(LockToken lockToken)
      throws ServiceExecutionException, IllegalArgumentException
  {
    synchronized (this) {
      // verify the lock token
      if (!this.locksToResourcesMap.containsKey(lockToken)) {
        throw new IllegalArgumentException(
            "Unrecognied lock token: " + lockToken);
      }

      // get the set of locked resource keys
      Set<ResourceKey> keys = this.locksToResourcesMap.remove(lockToken);

      // iterate over the keys
      for (ResourceKey key: keys) {
        LockToken token = this.resourceToLockMap.remove(key);
        if (!token.equals(lockToken)) {
          throw new IllegalStateException(
              "Lock token associated with resource key does not match lock "
              + "token that holds the lock on the resource.  resourceKeys=[ "
              + keys + " ], lockToken=[ " + token + " ], expectedLockToken=[ "
              + lockToken + " ]");
        }
      }

      // make sure a notify-all occurs after we leave synchronized block
      this.notifyAll();

      // return the number of resource keys
      return keys.size();
    }
  }

}
