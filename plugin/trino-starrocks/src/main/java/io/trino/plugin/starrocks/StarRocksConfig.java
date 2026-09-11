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
package io.trino.plugin.starrocks;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.Min;

import java.io.File;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class StarRocksConfig
{
    private String jdbcUrl;
    private String catalogName;
    private String username;
    private String password;
    private String flightSqlHost;
    private int flightSqlPort = 9408;
    private boolean flightSqlTlsEnabled;
    private File flightSqlTlsRootCertificate;
    private boolean flightSqlTlsSkipVerify;
    private String flightSqlTlsOverrideHostname;
    private DataSize flightSqlMaxAllocation = DataSize.of(256, MEGABYTE);

    public Optional<String> getJdbcUrl()
    {
        return Optional.ofNullable(jdbcUrl);
    }

    @Config("starrocks.jdbc-url")
    @ConfigDescription("StarRocks JDBC URL used for metadata discovery")
    public StarRocksConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public Optional<String> getCatalogName()
    {
        return Optional.ofNullable(catalogName);
    }

    @Config("starrocks.catalog-name")
    @ConfigDescription("StarRocks catalog name to expose through this Trino catalog")
    public StarRocksConfig setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
        return this;
    }

    public Optional<String> getUsername()
    {
        return Optional.ofNullable(username);
    }

    @Config("starrocks.username")
    @ConfigDescription("StarRocks username")
    public StarRocksConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("starrocks.password")
    @ConfigSecuritySensitive
    public StarRocksConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public Optional<String> getFlightSqlHost()
    {
        return Optional.ofNullable(flightSqlHost);
    }

    @Config("starrocks.flight-sql-host")
    @ConfigDescription("StarRocks FE host used for Arrow Flight SQL reads")
    public StarRocksConfig setFlightSqlHost(String flightSqlHost)
    {
        this.flightSqlHost = flightSqlHost;
        return this;
    }

    @Min(1)
    public int getFlightSqlPort()
    {
        return flightSqlPort;
    }

    @Config("starrocks.flight-sql-port")
    @ConfigDescription("StarRocks FE Arrow Flight SQL port used for reads")
    public StarRocksConfig setFlightSqlPort(int flightSqlPort)
    {
        this.flightSqlPort = flightSqlPort;
        return this;
    }

    public boolean isFlightSqlTlsEnabled()
    {
        return flightSqlTlsEnabled;
    }

    @Config("starrocks.flight-sql.tls.enabled")
    @ConfigDescription("Use TLS for Arrow Flight SQL connections")
    public StarRocksConfig setFlightSqlTlsEnabled(boolean flightSqlTlsEnabled)
    {
        this.flightSqlTlsEnabled = flightSqlTlsEnabled;
        return this;
    }

    public Optional<File> getFlightSqlTlsRootCertificate()
    {
        return Optional.ofNullable(flightSqlTlsRootCertificate);
    }

    @Config("starrocks.flight-sql.tls.root-certificate")
    @ConfigDescription("Path to a PEM root certificate file for Arrow Flight SQL TLS")
    public StarRocksConfig setFlightSqlTlsRootCertificate(File flightSqlTlsRootCertificate)
    {
        this.flightSqlTlsRootCertificate = flightSqlTlsRootCertificate;
        return this;
    }

    public boolean isFlightSqlTlsSkipVerify()
    {
        return flightSqlTlsSkipVerify;
    }

    @Config("starrocks.flight-sql.tls.skip-verify")
    @ConfigDescription("Skip Arrow Flight SQL TLS server certificate verification")
    public StarRocksConfig setFlightSqlTlsSkipVerify(boolean flightSqlTlsSkipVerify)
    {
        this.flightSqlTlsSkipVerify = flightSqlTlsSkipVerify;
        return this;
    }

    public Optional<String> getFlightSqlTlsOverrideHostname()
    {
        return Optional.ofNullable(flightSqlTlsOverrideHostname);
    }

    @Config("starrocks.flight-sql.tls.override-hostname")
    @ConfigDescription("Hostname to use for Arrow Flight SQL TLS verification")
    public StarRocksConfig setFlightSqlTlsOverrideHostname(String flightSqlTlsOverrideHostname)
    {
        this.flightSqlTlsOverrideHostname = flightSqlTlsOverrideHostname;
        return this;
    }

    @MinDataSize("1MB")
    public DataSize getFlightSqlMaxAllocation()
    {
        return flightSqlMaxAllocation;
    }

    @Config("starrocks.flight-sql.max-allocation")
    @ConfigDescription("Maximum Arrow memory allocation per StarRocks Flight SQL stream")
    public StarRocksConfig setFlightSqlMaxAllocation(DataSize flightSqlMaxAllocation)
    {
        this.flightSqlMaxAllocation = flightSqlMaxAllocation;
        return this;
    }
}
