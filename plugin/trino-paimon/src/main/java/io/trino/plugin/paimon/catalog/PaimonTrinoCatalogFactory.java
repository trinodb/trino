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
package io.trino.plugin.paimon.catalog;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.spi.security.ConnectorIdentity;

public class PaimonTrinoCatalogFactory
{
    private final PaimonConfig config;

    private final TrinoFileSystemFactory trinoFileSystemFactory;

    @Inject
    public PaimonTrinoCatalogFactory(
            PaimonConfig config,
            TrinoFileSystemFactory trinoFileSystemFactory)
    {
        this.config = config;
        this.trinoFileSystemFactory = trinoFileSystemFactory;
    }

    public PaimonTrinoCatalog create(ConnectorIdentity identity)
    {
        return new PaimonTrinoCatalog(config, trinoFileSystemFactory, identity);
    }
}
