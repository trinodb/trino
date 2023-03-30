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

import io.trino.metadata.Catalog;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogHandleType;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
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

    public CatalogConnector(
            CatalogHandle catalogHandle,
            ConnectorName connectorName,
            ConnectorServices catalogConnector,
            ConnectorServices informationSchemaConnector,
            ConnectorServices systemConnector,
            Optional<CatalogProperties> catalogProperties)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.catalogConnector = requireNonNull(catalogConnector, "catalogConnector is null");
        this.informationSchemaConnector = requireNonNull(informationSchemaConnector, "informationSchemaConnector is null");
        this.systemConnector = requireNonNull(systemConnector, "systemConnector is null");
        this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null");

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
        switch (type) {
            case NORMAL:
                return catalogConnector;
            case INFORMATION_SCHEMA:
                return informationSchemaConnector;
            case SYSTEM:
                return systemConnector;
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    public void shutdown()
    {
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
