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

import io.trino.connector.ConnectorName;
import io.trino.spi.connector.CatalogHandle;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CatalogInfo
{
    private final String catalogName;
    private final CatalogHandle catalogHandle;
    private final ConnectorName connectorName;

    public CatalogInfo(String catalogName, CatalogHandle catalogHandle, ConnectorName connectorName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    public ConnectorName getConnectorName()
    {
        return connectorName;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("catalogHandle", catalogHandle)
                .add("connectorName", connectorName)
                .toString();
    }
}
