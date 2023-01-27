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
package io.trino.plugin.iceberg.catalog.jdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotEmpty;

public class IcebergJdbcCatalogConfig
{
    private String connectionUrl;
    private String catalogName;
    private String defaultWarehouseDir;

    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("iceberg.jdbc-catalog.connection-url")
    @ConfigDescription("The URI to connect to the JDBC server")
    @ConfigSecuritySensitive
    public IcebergJdbcCatalogConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @NotEmpty
    public String getCatalogName()
    {
        return catalogName;
    }

    @Config("iceberg.jdbc-catalog.catalog-name")
    @ConfigDescription("Iceberg JDBC metastore catalog name")
    public IcebergJdbcCatalogConfig setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
        return this;
    }

    @NotEmpty
    public String getDefaultWarehouseDir()
    {
        return defaultWarehouseDir;
    }

    @Config("iceberg.jdbc-catalog.default-warehouse-dir")
    @ConfigDescription("The default warehouse directory to use for JDBC")
    public IcebergJdbcCatalogConfig setDefaultWarehouseDir(String defaultWarehouseDir)
    {
        this.defaultWarehouseDir = defaultWarehouseDir;
        return this;
    }
}
