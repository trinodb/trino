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

import io.trino.spi.seedstore.SeedStore;

import java.util.Map;

/**
 * StateStoreFactory responsible for creating StateStore clients
 *
 * @since 2019-11-29
 */
public interface StateStoreFactory
{
    /**
     * Get the name of the state store factory
     *
     * @return state store factory name
     */
    String getName();

    /**
     * Create new state store
     *
     * @param name name of the state store
     * @param seedStore seed store to create the state store
     * @param config state store configurations
     * @return created state store
     */
    StateStore create(String name, SeedStore seedStore, Map<String, String> config);
}
