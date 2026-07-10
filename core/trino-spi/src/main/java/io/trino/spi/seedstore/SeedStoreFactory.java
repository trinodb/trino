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

import io.trino.spi.filesystem.HetuFileSystemClient;

import java.util.Map;

/**
 * SeedStoreFactory that creates SeedStores
 *
 * @since 2020-03-04
 */
public interface SeedStoreFactory
{
    /**
     * Get the name of the seed store factory
     *
     * @return state store factory name
     */
    String getName();

    /**
     * Create new seed store
     *
     * @param name name of the seed store
     * @param fs {@link HetuFileSystemClient} that provides access to the seed file
     * @param config seed store configurations
     * @return created seed store
     */
    SeedStore create(String name, SeedStoreSubType subType, HetuFileSystemClient fs, Map<String, String> config);
}
