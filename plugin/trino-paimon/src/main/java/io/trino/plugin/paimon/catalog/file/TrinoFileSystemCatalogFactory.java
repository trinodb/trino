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

package io.trino.plugin.paimon.catalog.file;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.plugin.paimon.catalog.TrinoCatalog;
import io.trino.plugin.paimon.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public class TrinoFileSystemCatalogFactory
        implements TrinoCatalogFactory
{
    private final PaimonConfig paimonConfig;
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public TrinoFileSystemCatalogFactory(
            PaimonConfig config,
            TrinoFileSystemFactory fileSystemFactory)
    {
        this.paimonConfig = requireNonNull(config, "config is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        return new TrinoFileSystemPaimonCatalog(
                paimonConfig,
                fileSystemFactory);
    }
}
