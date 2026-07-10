/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.spi.statestore;

import io.trino.spi.statestore.listener.MapListener;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

/**
 * StateStore that stores different state collections
 *
 * @since 2019-11-29
 */
public interface StateStore
{
    /**
     * Get the state store name
     *
     * @return name of the state store
     */
    String getName();

    /**
     * Get state collection in state store
     *
     * @param name name of the state collection
     * @return the corresponding state collection
     */
    StateCollection getStateCollection(String name);

    /**
     * Remove state collection in state store
     *
     * @param name name of the state collection
     */
    void removeStateCollection(String name);

    /**
     * Get all state collections in state store
     *
     * @return Map contains all state collections
     */
    Map<String, StateCollection> getStateCollections();

    /**
     * Create state collection based on collection type
     *
     * @param name state collection name
     * @param type state collection type
     * @return created state collection
     */
    StateCollection createStateCollection(String name, StateCollection.Type type);

    /**
     * Get a state collection and it if not exist
     *
     * @param name state collection name
     * @param type state collection type
     * @return existing or created state collection
     */
    StateCollection getOrCreateStateCollection(String name, StateCollection.Type type);

    /**
     * Create state map
     *
     * @param name state map name
     * @param listeners listeners to be notified upon changes to the state map
     * @return created state map
     */
    <K, V> StateMap<K, V> createStateMap(String name, MapListener... listeners);

    /**
     * Get a lock for the state store
     *
     * @param lockKey key for the lock
     * @return a lock
     */
    Lock getLock(String lockKey);

    /**
     * get a unique Id from the state store
     *
     * @return a unique Id inform of a Long
     */
    long generateId();

    /**
     * Register node failure handler to state store
     *
     * @param nodeFailureHandler callback function to handle node failures
     */
    void registerNodeFailureHandler(Consumer nodeFailureHandler);

    /**
     * Register cluster connection failure handler to state store
     *
     * @param clusterFailureHandler callback function to handle node failures
     */
    default void registerClusterFailureHandler(Consumer clusterFailureHandler)
    {
        // no ops
    }

    /**
     * Initialize state store
     */
    void init();
}
