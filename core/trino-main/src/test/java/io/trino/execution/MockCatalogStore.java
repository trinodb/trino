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

package io.trino.execution;

import io.trino.connector.InMemoryCatalogStore;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.ConnectorName;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MockCatalogStore
        implements CatalogStore
{
    private final CatalogStore delegate = new InMemoryCatalogStore();
    private final AtomicBoolean failAddOrReplace = new AtomicBoolean();
    private final AtomicBoolean failRemove = new AtomicBoolean();

    @Override
    public Collection<StoredCatalog> getCatalogs()
    {
        return delegate.getCatalogs();
    }

    @Override
    public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        return delegate.createCatalogProperties(catalogName, connectorName, properties);
    }

    @Override
    public void addOrReplaceCatalog(CatalogProperties catalogProperties)
    {
        if (failAddOrReplace.get()) {
            throw new RuntimeException("Add or replace catalog failed");
        }
        delegate.addOrReplaceCatalog(catalogProperties);
    }

    @Override
    public void removeCatalog(CatalogName catalogName)
    {
        if (failRemove.get()) {
            throw new RuntimeException("Remove catalog failed");
        }
        delegate.removeCatalog(catalogName);
    }

    public void failAddOrReplaceCatalog()
    {
        failAddOrReplace.set(true);
    }

    public void failRemoveCatalog()
    {
        failRemove.set(true);
    }
}
