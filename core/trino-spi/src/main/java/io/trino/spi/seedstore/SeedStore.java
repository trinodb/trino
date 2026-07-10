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
package io.trino.spi.seedstore;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * SeedStore that contains seed nodes information
 *
 * @since 2020-03-04
 */
public interface SeedStore
{
    /**
     * Add seeds to the seed file
     *
     * @param seeds seeds added to the seed file
     * @return collection containing seeds in the seed file
     * @throws IOException exception when failed to add seeds
     */
    Collection<Seed> add(Collection<Seed> seeds)
            throws IOException;

    /**
     * Get seeds from seed file
     *
     * @return collection containing seeds in the seed file
     * @throws IOException exception when failed to get seeds
     */
    Collection<Seed> get()
            throws IOException;

    /**
     * Remove seeds from the seed file
     *
     * @param seeds seeds removed from the seed file
     * @return collection containing seeds in the seed file
     * @throws IOException exception when failed to remove seeds
     */
    Collection<Seed> remove(Collection<Seed> seeds)
            throws IOException;

    /**
     * Create seed based on seed properties
     *
     * @param properties properties used to create seed
     * @return seed object
     */
    Seed create(Map<String, String> properties);

    /**
     * Get seed store name
     *
     * @return seed store name
     */
    String getName();

    /**
     * Set seed store name
     *
     * @param name name of the seed store
     */
    void setName(String name);
}
