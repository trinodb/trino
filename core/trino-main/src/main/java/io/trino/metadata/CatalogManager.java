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
package io.trino.metadata;

import com.google.common.collect.ImmutableSet;
import io.trino.connector.CatalogProperties;
import io.trino.spi.connector.CatalogHandle;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface CatalogManager
{
    CatalogManager NO_CATALOGS = new CatalogManager()
    {
        @Override
        public Set<String> getCatalogNames()
        {
            return ImmutableSet.of();
        }

        @Override
        public Optional<Catalog> getCatalog(String catalogName)
        {
            return Optional.empty();
        }

        @Override
        public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle)
        {
            return Optional.empty();
        }

        @Override
        public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
        {
            throw new UnsupportedOperationException();
        }
    };

    Set<String> getCatalogNames();

    Optional<Catalog> getCatalog(String catalogName);

    Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle);

    void createCatalog(String catalogName, String connectorName, Map<String, String> properties);
}
