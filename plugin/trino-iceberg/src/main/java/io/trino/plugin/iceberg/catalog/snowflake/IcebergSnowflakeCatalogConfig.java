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
package io.trino.plugin.iceberg.catalog.snowflake;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import net.snowflake.client.jdbc.SnowflakeDriver;

import java.net.URI;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Optional;

public class IcebergSnowflakeCatalogConfig
{
    private URI uri;
    private String user;
    private Optional<String> password;
    private Optional<String> key;
    private String database;
    private Optional<String> role = Optional.empty();

    @AssertTrue(message = "Invalid JDBC URL for Iceberg Snowflake catalog")
    public boolean isUrlValid()
            throws SQLException
    {
        Driver driver = new SnowflakeDriver();
        return driver.acceptsURL(uri.toString());
    }

    @AssertTrue(message = "Either iceberg.snowflake-catalog.password or iceberg.snowflake-catalog.key must be set, but not both")
    public boolean isAuthenticationMethodSet()
    {
        return getKey().isPresent() != getPassword().isPresent();
    }

    @NotNull
    public URI getUri()
    {
        return this.uri;
    }

    @Config("iceberg.snowflake-catalog.account-uri")
    @ConfigDescription("Snowflake JDBC URI")
    public IcebergSnowflakeCatalogConfig setUri(URI uri)
    {
        this.uri = uri;
        return this;
    }

    @NotNull
    public String getUser()
    {
        return user;
    }

    @Config("iceberg.snowflake-catalog.user")
    @ConfigDescription("Username for Snowflake")
    public IcebergSnowflakeCatalogConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    @Deprecated
    public Optional<String> getPassword()
    {
        return password;
    }

    @Deprecated
    @Config("iceberg.snowflake-catalog.password")
    @ConfigDescription("Password for Snowflake")
    @ConfigSecuritySensitive
    public IcebergSnowflakeCatalogConfig setPassword(String password)
    {
        this.password = Optional.ofNullable(password);
        return this;
    }

    public Optional<String> getKey()
    {
        return key;
    }

    @Config("iceberg.snowflake-catalog.key")
    @ConfigDescription("The base64 encoded private key for key-pair authentication")
    @ConfigSecuritySensitive
    public IcebergSnowflakeCatalogConfig setKey(String key)
    {
        this.key = Optional.ofNullable(key);
        return this;
    }

    public String getDatabase()
    {
        return database;
    }

    @Config("iceberg.snowflake-catalog.database")
    @ConfigDescription("Snowflake database")
    public IcebergSnowflakeCatalogConfig setDatabase(String database)
    {
        this.database = database;
        return this;
    }

    public Optional<String> getRole()
    {
        return role;
    }

    @Config("iceberg.snowflake-catalog.role")
    @ConfigDescription("Name of Snowflake role to use")
    public IcebergSnowflakeCatalogConfig setRole(String role)
    {
        this.role = Optional.ofNullable(role);
        return this;
    }
}
