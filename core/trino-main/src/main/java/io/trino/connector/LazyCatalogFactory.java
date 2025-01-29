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
package io.trino.connector;

import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorName;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;

public class LazyCatalogFactory
        implements CatalogFactory
{
    private final AtomicReference<CatalogFactory> delegate = new AtomicReference<>();

    public void setCatalogFactory(CatalogFactory catalogFactory)
    {
        checkState(delegate.compareAndSet(null, catalogFactory), "catalogFactory already set");
    }

    @Override
    public void addConnectorFactory(ConnectorFactory connectorFactory)
    {
        getDelegate().addConnectorFactory(connectorFactory);
    }

    @Override
    public CatalogConnector createCatalog(CatalogProperties catalogProperties)
    {
        return getDelegate().createCatalog(catalogProperties);
    }

    @Override
    public CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector)
    {
        return getDelegate().createCatalog(catalogHandle, connectorName, connector);
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(CatalogProperties catalogProperties)
    {
        return getDelegate().getSecuritySensitivePropertyNames(catalogProperties);
    }

    private CatalogFactory getDelegate()
    {
        CatalogFactory catalogFactory = delegate.get();
        checkState(catalogFactory != null, "Catalog factory is not set");
        return catalogFactory;
    }
}
