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
package io.trino.server;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.connector.CatalogManagerConfig;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.spi.connector.CatalogHandle;

import java.util.Optional;

import static io.trino.connector.CatalogManagerConfig.CatalogMangerKind.STATIC;
import static java.util.stream.Collectors.joining;

public class StaticCatalogHandleIds
        implements Provider<String>
{
    private final String catalogHandleIds;

    @Inject
    public StaticCatalogHandleIds(CatalogManagerConfig catalogManagerConfig, CatalogManager catalogManager)
    {
        // Only static catalog manager announces catalogs
        if (catalogManagerConfig.getCatalogMangerKind() == STATIC) {
            catalogHandleIds = catalogManager.getCatalogNames().stream()
                    .map(catalogManager::getCatalog)
                    .flatMap(Optional::stream)
                    .map(Catalog::getCatalogHandle)
                    .map(CatalogHandle::getId)
                    .distinct()
                    .sorted()
                    .collect(joining(","));
        }
        else {
            catalogHandleIds = "";
        }
    }

    @Override
    public String get()
    {
        return catalogHandleIds;
    }
}
