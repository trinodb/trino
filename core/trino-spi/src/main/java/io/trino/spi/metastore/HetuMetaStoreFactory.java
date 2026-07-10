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
package io.trino.spi.metastore;

import io.trino.spi.filesystem.HetuFileSystemClient;
import io.trino.spi.statestore.StateStore;

import java.util.Map;

public interface HetuMetaStoreFactory
{
    /**
     * Get the name of the hetu metastore factory
     *
     * @return hetu metastore factory name
     */
    String getName();

    /**
     * Create new hetu metastore
     *
     * @param name   name of the hetu metastore
     * @param config hetu metastore configurations
     * @param client hetu file system client
     * @return created hetu metastore
     */
    HetuMetastore create(String name, Map<String, String> config, HetuFileSystemClient client, StateStore stateStore,
                         String type);
}
