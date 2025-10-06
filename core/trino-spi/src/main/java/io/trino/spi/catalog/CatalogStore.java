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
package io.trino.spi.catalog;

import io.trino.spi.connector.ConnectorName;

import java.util.Collection;
import java.util.Map;

public interface CatalogStore
{
    /**
     * Get all catalogs. This is called at startup to load the existing catalogs.
     */
    Collection<StoredCatalog> getCatalogs();

    /**
     * Create a catalog properties from the raw properties.  This allows the
     * store to assign the initial handle for a catalog before the catalog is
     * created. This does not add the catalog to the store.
     */
    CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties);

    /**
     * Add or replace catalog properties.
     */
    void addOrReplaceCatalog(CatalogProperties catalogProperties);

    /**
     * Remove a catalog if present.
     */
    void removeCatalog(CatalogName catalogName);

    /**
     * Set the catalog manager. This allows catalog stores to trigger
     * catalog reloads when they detect external changes.
     * Default implementation does nothing for backward compatibility.
     */
    default void setCatalogManager(Object catalogManager)
    {
        // Default no-op implementation
    }

    interface StoredCatalog
    {
        CatalogName name();

        CatalogProperties loadProperties();
    }
}
