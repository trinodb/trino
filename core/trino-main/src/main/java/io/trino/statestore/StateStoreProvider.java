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
package io.trino.statestore;

import io.trino.spi.statestore.StateStore;
import io.trino.spi.statestore.StateStoreFactory;

/**
 * StateStoreProvider that provides and manages new StateStore client
 *
 * @since 2020-03-06
 */
public interface StateStoreProvider
{
    /**
     * Add the state store factory
     *
     * @param factory state store factory
     */
    void addStateStoreFactory(StateStoreFactory factory);

    /**
     * load state store
     *
     * @throws Exception Exception during loading
     */
    void loadStateStore()
            throws Exception;

    /**
     * Get the state store object
     *
     * @return StateStore object in the provider
     */
    StateStore getStateStore();
}
