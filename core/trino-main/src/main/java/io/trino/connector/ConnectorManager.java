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

import io.trino.metadata.CatalogManager;
import io.trino.spi.connector.Connector;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ConnectorManager
        implements ConnectorServicesProvider
{
    private final CatalogFactory catalogFactory;
    private final CatalogManager catalogManager;

    @GuardedBy("this")
    private final ConcurrentMap<String, CatalogConnector> catalogs = new ConcurrentHashMap<>();

    private final AtomicBoolean stopped = new AtomicBoolean();

    @Inject
    public ConnectorManager(CatalogFactory catalogFactory, CatalogManager catalogManager)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (stopped.getAndSet(true)) {
            return;
        }

        for (CatalogConnector connector : catalogs.values()) {
            connector.shutdown();
        }
        catalogs.clear();
    }

    @Override
    public synchronized ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        CatalogConnector catalogConnector = catalogs.get(catalogHandle.getCatalogName());
        checkArgument(catalogConnector != null, "No catalog '%s'", catalogHandle.getCatalogName());
        return catalogConnector.getMaterializedConnector(catalogHandle.getType());
    }

    public synchronized CatalogHandle createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(properties, "properties is null");

        checkState(!stopped.get(), "ConnectorManager is stopped");
        checkArgument(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        checkArgument(catalogManager.getCatalog(catalogName).isEmpty(), "Catalog '%s' already exists", catalogName);

        CatalogConnector catalog = catalogFactory.createCatalog(catalogName, connectorName, properties);
        catalogs.put(catalogName, catalog);
        catalogManager.registerCatalog(catalog.getCatalog());
        return catalog.getCatalogHandle();
    }

    public synchronized void createCatalog(CatalogHandle catalogHandle, String connectorName, Connector connector)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(connector, "connector is null");

        checkState(!stopped.get(), "ConnectorManager is stopped");
        String catalogName = catalogHandle.getCatalogName();
        checkArgument(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        checkArgument(catalogManager.getCatalog(catalogName).isEmpty(), "Catalog '%s' already exists", catalogName);

        CatalogConnector catalog = catalogFactory.createCatalog(catalogHandle, connectorName, connector);
        catalogs.put(catalogName, catalog);
        catalogManager.registerCatalog(catalog.getCatalog());
    }
}
