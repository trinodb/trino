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
package io.trino.plugin.ducklake;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

/**
 * Configuration for Ducklake connector.
 */
public class DucklakeConfig
{
    private String catalogDatabaseUrl;
    private String catalogDatabaseUser;
    private String catalogDatabasePassword;
    private String dataPath;
    private int maxCatalogConnections = 10;

    @NotNull
    public String getCatalogDatabaseUrl()
    {
        return catalogDatabaseUrl;
    }

    @Config("ducklake.catalog.database-url")
    @ConfigDescription("JDBC URL for the Ducklake catalog database (e.g., jdbc:sqlite:/path/to/catalog.db)")
    public DucklakeConfig setCatalogDatabaseUrl(String catalogDatabaseUrl)
    {
        this.catalogDatabaseUrl = catalogDatabaseUrl;
        return this;
    }

    public String getCatalogDatabaseUser()
    {
        return catalogDatabaseUser;
    }

    @Config("ducklake.catalog.database-user")
    @ConfigDescription("Username for the catalog database (optional for SQLite)")
    public DucklakeConfig setCatalogDatabaseUser(String catalogDatabaseUser)
    {
        this.catalogDatabaseUser = catalogDatabaseUser;
        return this;
    }

    public String getCatalogDatabasePassword()
    {
        return catalogDatabasePassword;
    }

    @Config("ducklake.catalog.database-password")
    @ConfigDescription("Password for the catalog database (optional for SQLite)")
    public DucklakeConfig setCatalogDatabasePassword(String catalogDatabasePassword)
    {
        this.catalogDatabasePassword = catalogDatabasePassword;
        return this;
    }

    public String getDataPath()
    {
        return dataPath;
    }

    @Config("ducklake.data-path")
    @ConfigDescription("Base path for relative data file paths (from ducklake_metadata table)")
    public DucklakeConfig setDataPath(String dataPath)
    {
        this.dataPath = dataPath;
        return this;
    }

    public int getMaxCatalogConnections()
    {
        return maxCatalogConnections;
    }

    @Config("ducklake.catalog.max-connections")
    @ConfigDescription("Maximum number of JDBC connections to the catalog database")
    public DucklakeConfig setMaxCatalogConnections(int maxCatalogConnections)
    {
        this.maxCatalogConnections = maxCatalogConnections;
        return this;
    }
}
