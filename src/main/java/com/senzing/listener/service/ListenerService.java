package com.senzing.listener.service;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.util.AccessToken;
import com.senzing.listener.communication.MessageConsumer;

import java.util.Set;

/**
 * Defines an interface for a {@link ListenerService} that can process
 * messages that are received.
 */
public interface ListenerService {
  /**
   * For initializing any needed resources before processing
   * 
   * @param config Configuration needed for the processing
   * 
   * @throws ServiceSetupException If a failure occurs.
   */
  void init(String config) throws ServiceSetupException;

  /**
   * This method can be overridden to provide inter-process locking of entities
   * by their entity ID's for the {@link MessageConsumer}.  By default this
   * method returns <code>null</code> to indicate that inter-process locking is
   * not provided or supported.  This method <b>should</b> be overridden if the
   * listener service is going to be run in a multi-process clustered <b>and</b>
   * concurrent processing of the same entities can lead to problematic race
   * conditions for the service.  If your {@link ListenerService} implementation
   * will be run as a single process or if concurrent processing is not a, then this method need <b>not</b> be
   * overridden and can simply return <code>null</code>.
   *
   * @param entityIds The {@link Set} of {@link Long} entity IDs to be locked.
   *
   * @return An {@link AccessToken} which can be used to unlock the entities,
   *         or <code>null</code> if inter-process cluster locking is not
   *         provided.
   */
  default AccessToken obtainClusterLocks(Set<Long> entityIds) {
    return null;
  }

  /**
   * This method can be overridden to release <b>all</b> inter-process locks on
   * entities that were previously obtained by the {@link MessageConsumer} via
   * {@link #obtainClusterLocks(Set)} with the returned {@link AccessToken} that is
   * specified.  The default implementation of this method does nothing if
   * <code>null</code> is specified and throws an {@link
   * IllegalArgumentException} if the specified token is not <code>null</code>.
   * This method <b>should</b> be overridden if the listener service is going to
   * be run in a multi-process clustered <b>and</b> concurrent processing of the
   * same entities can lead to problematic race conditions for the service.  If
   * your {@link ListenerService} implementation will be run as a single
   * process, then this method need <b>not</b> be overridden and can simply do
   * nothing.
   *
   * @param token The {@link AccessToken} identifying the {@link Set} of entity
   *              ID's that were previosly locked via {@link #obtainClusterLocks(Set)},
   *              or <code>null</code> if no locks should be released.
   *
   * @throws IllegalArgumentException If the specified {@link AccessToken} is
   *                                  <b>not</b> <code>null</code> and
   *                                  <b>not</b> recognized.
   */
  default void releaseClusterLocks(AccessToken token) {
    if (token == null) return;
    throw new IllegalArgumentException(
        "Unrecognized access token -- cluster locks are not supported.");
  }

  /**
   * This method can be overridden so that the {@link MessageConsumer} can
   * obtain the <b>unmodifiable</b> {@link Set} of {@link Long} entity IDs
   * identifying <b>all</b> entities that are locked across the cluster of
   * {@link ListenerService} instances.  By default, this returns
   * <code>null</code> to indicate that inter-process cluster locking is
   * <b>not</b> implemented.  This method <b>should</b> be overridden if the
   * listener service is going to be run in a multi-process clustered <b>and</b>
   * concurrent processing of the same entities can lead to problematic race
   * conditions for the service.  If your {@link ListenerService} implementation
   * will be run as a single process. then this method need <b>not</b> be
   * overridden and can simply do nothing.
   *
   * @return The <b>unmodifiable</b> {@link Set} of {@link Long} entity ID's
   *         identifying the entities that are locked by the {@link
   *         ListenerService} across the inter-process cluster, or
   *         <code>null</code> if inter-process locking is not implemented.
   */
  default Set<Long> getClusterLocks() {
    return null;
  }

  /**
   * This method is called by the consumer.  Processes the message passed to
   * the service from the consumer.
   * 
   * @param message The message to process.
   *
   * @throws ServiceExecutionException If a failure occurs.
   */
  void process(String message) throws ServiceExecutionException;

  /**
   * For cleaning up after processing, e.g. free up resources.
   */
  void destroy();
}
