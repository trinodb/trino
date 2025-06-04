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
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class IcebergJdbcCatalogConfig
{
    public enum SchemaVersion
    {
        V0, V1
    }

    private String driverClass;
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String catalogName;
    private String defaultWarehouseDir;
    private SchemaVersion schemaVersion = SchemaVersion.V1;
    private String retryableStatusCodes;

    @NotNull
    public String getDriverClass()
    {
        return driverClass;
    }

    @Config("iceberg.jdbc-catalog.driver-class")
    @ConfigDescription("JDBC driver class name")
    public IcebergJdbcCatalogConfig setDriverClass(String driverClass)
    {
        this.driverClass = driverClass;
        return this;
    }

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

    @NotNull
    public Optional<String> getConnectionUser()
    {
        return Optional.ofNullable(connectionUser);
    }

    @Config("iceberg.jdbc-catalog.connection-user")
    @ConfigDescription("User name for JDBC client")
    public IcebergJdbcCatalogConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @NotNull
    public Optional<String> getConnectionPassword()
    {
        return Optional.ofNullable(connectionPassword);
    }

    @Config("iceberg.jdbc-catalog.connection-password")
    @ConfigDescription("Password for JDBC client")
    @ConfigSecuritySensitive
    public IcebergJdbcCatalogConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
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

    @NotNull
    public SchemaVersion getSchemaVersion()
    {
        return schemaVersion;
    }

    @Config("iceberg.jdbc-catalog.schema-version")
    @ConfigDescription("JDBC catalog schema version")
    public IcebergJdbcCatalogConfig setSchemaVersion(SchemaVersion schemaVersion)
    {
        this.schemaVersion = schemaVersion;
        return this;
    }

    @NotNull
    public Optional<String> getRetryableStatusCodes()
    {
        return Optional.ofNullable(retryableStatusCodes);
    }

    @Config("iceberg.jdbc-catalog.retryable-status-codes")
    @ConfigDescription("On connection error to JDBC metastore, retry if it is one of these JDBC status codes")
    public IcebergJdbcCatalogConfig setRetryableStatusCodes(String retryableStatusCodes)
    {
        this.retryableStatusCodes = retryableStatusCodes;
        return this;
    }
}
