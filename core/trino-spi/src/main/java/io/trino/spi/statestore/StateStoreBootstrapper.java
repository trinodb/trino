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

import java.util.Collection;
import java.util.Map;

/**
 * StateStoreBootstrapper responsible for bootstrapping new StateStore instances
 *
 * @since 2020-03-06
 */
public interface StateStoreBootstrapper
{
    /**
     * Bootstraps a state store and initializations
     *
     * @param locations locations a collection of host:port to bootstrap the state store
     * @param config the state store configs
     * @return bootstrapped StateStore instance
     */
    StateStore bootstrap(Collection<String> locations, Map<String, String> config);
}
