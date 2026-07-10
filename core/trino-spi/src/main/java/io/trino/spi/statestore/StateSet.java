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

import java.util.Set;

/**
 * StateSet is a set that stores different states
 *
 * @param <V> type of the stored states
 * @since 2019-11-29
 */
public interface StateSet<V>
        extends StateCollection
{
    /**
     * Put a state in state store with a given key
     *
     * @param value state value
     * @return previous value associated with key or null
     */
    boolean add(V value);

    /**
     * Get all the states in the StateSet
     *
     * @return Set contains all the states
     */
    Set<Object> getAll();

    /**
     * Put all key state pairs in state store
     *
     * @param set key value pairs of the states
     * @return boolean indicates if the states have been added or not
     */
    boolean addAll(Set<V> set);

    /**
     * Remove state related to the key
     *
     * @param value state value
     * @return boolean indicates if the state removed or not
     */
    boolean remove(V value);

    /**
     * Remove states related to all the keys
     *
     * @param values set of keys of the states
     * @return boolean indicates if states removed or not
     */
    boolean removeAll(Set<V> values);

    /**
     * Check if a state is in current StateSet
     *
     * @param value State value
     * @return true if state is in the StateSet, false if not in the StateSet
     */
    boolean contains(V value);
}
