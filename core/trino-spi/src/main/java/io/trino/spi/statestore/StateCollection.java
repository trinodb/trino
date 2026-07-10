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

/**
 * StateCollection that represents an abstract collection of states
 *
 * @since 2019-11-29
 */
public interface StateCollection
{
    /**
     * Type of the collection
     *
     * @since 2019-11-29
     */
    enum Type
    {
        MAP, QUEUE, SET
    }

    /**
     * Get the state store name
     *
     * @return name of the state store
     */
    String getName();

    /**
     * Get the state store type
     *
     * @return type of the state store
     */
    Type getType();

    /**
     * Remove all the states in state collection
     */
    void clear();

    /**
     * Get number of elements in the state collection
     *
     * @return number of elements
     */
    int size();

    /**
     * Check if the state collection is empty
     *
     * @return if state collection is empty
     */
    boolean isEmpty();

    /**
     * Destroy the state collection
     */
    void destroy();
}
