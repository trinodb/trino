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

import io.trino.memory.LocalMemoryManager;
import io.trino.memory.MemoryPool;
import io.trino.metadata.Catalog;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogHandleType;
import io.trino.spi.connector.ConnectorName;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.ExceededMemoryLimitException.exceededLocalUserMemoryLimit;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CatalogConnector
{
    private final CatalogHandle catalogHandle;
    private final ConnectorName connectorName;
    private final ConnectorServices catalogConnector;
    private final ConnectorServices informationSchemaConnector;
    private final ConnectorServices systemConnector;
    private final Optional<CatalogProperties> catalogProperties;
    private final Catalog catalog;
    private final LocalMemoryManager localMemoryManager;
    private final long connectorMemory;

    public CatalogConnector(
            CatalogHandle catalogHandle,
            ConnectorName connectorName,
            ConnectorServices catalogConnector,
            ConnectorServices informationSchemaConnector,
            ConnectorServices systemConnector,
            LocalMemoryManager localMemoryManager,
            Optional<CatalogProperties> catalogProperties)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.catalogConnector = requireNonNull(catalogConnector, "catalogConnector is null");
        this.informationSchemaConnector = requireNonNull(informationSchemaConnector, "informationSchemaConnector is null");
        this.systemConnector = requireNonNull(systemConnector, "systemConnector is null");
        this.localMemoryManager = requireNonNull(localMemoryManager, "localMemoryManager is null");
        this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null");
        this.connectorMemory = catalogConnector.getConnector().getInitialMemoryRequirement();

        MemoryPool memoryPool = localMemoryManager.getMemoryPool();
        boolean success = memoryPool.tryReserveConnectorMemory(connectorMemory);
        if (!success) {
            String info = format("tried to reserve %s for connector %s", succinctBytes(connectorMemory), connectorName);
            throw exceededLocalUserMemoryLimit(succinctBytes(memoryPool.getMaxBytes()), info);
        }

        this.catalog = new Catalog(
                catalogHandle.getCatalogName(),
                catalogHandle,
                connectorName,
                catalogConnector,
                informationSchemaConnector,
                systemConnector);
    }

    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    public ConnectorName getConnectorName()
    {
        return connectorName;
    }

    public Optional<CatalogProperties> getCatalogProperties()
    {
        return catalogProperties;
    }

    public Catalog getCatalog()
    {
        return catalog;
    }

    public ConnectorServices getMaterializedConnector(CatalogHandleType type)
    {
        return switch (type) {
            case NORMAL -> catalogConnector;
            case INFORMATION_SCHEMA -> informationSchemaConnector;
            case SYSTEM -> systemConnector;
        };
    }

    public void shutdown()
    {
        localMemoryManager.getMemoryPool().freeConnectorMemory(connectorMemory);

        catalogConnector.shutdown();
        informationSchemaConnector.shutdown();
        systemConnector.shutdown();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogHandle", catalogHandle)
                .add("connectorName", connectorName)
                .toString();
    }
}
