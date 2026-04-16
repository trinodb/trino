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
package io.trino.plugin.doris;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class DorisConfig
{
    // Doris FE endpoints are the control-plane entrypoint for metadata and query planning.
    private String fenodes;
    // JDBC remains metadata-only in this design. Data reads will use Flight SQL.
    private String jdbcUrl;
    private String username;
    private String password;
    // A value of 0 means the connector will discover the BE Flight SQL port later.
    private int flightSqlPort;
    // LARGEINT defaults to VARCHAR to avoid silently truncating 128-bit values.
    private DorisLargeintMapping largeintMapping = DorisLargeintMapping.VARCHAR;
    // Maximum number of splits to generate. Helps reduce overhead for small queries.
    private int maxSplitsPerQuery = 64;
    // Minimum number of tablets per split. Helps consolidate small tables into fewer splits.
    private int minTabletsPerSplit = 1;

    public Optional<String> getFenodes()
    {
        return Optional.ofNullable(fenodes);
    }

    @Config("doris.fenodes")
    @ConfigDescription("Comma-separated Doris FE host:port list")
    public DorisConfig setFenodes(String fenodes)
    {
        this.fenodes = fenodes;
        return this;
    }

    public Optional<String> getJdbcUrl()
    {
        return Optional.ofNullable(jdbcUrl);
    }

    @Config("doris.jdbc-url")
    @ConfigDescription("Doris FE JDBC URL for metadata access")
    public DorisConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public Optional<String> getUsername()
    {
        return Optional.ofNullable(username);
    }

    @Config("doris.username")
    public DorisConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("doris.password")
    @ConfigSecuritySensitive
    public DorisConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @Min(0)
    @Max(65535)
    public int getFlightSqlPort()
    {
        return flightSqlPort;
    }

    @Config("doris.flight-sql-port")
    @ConfigDescription("Explicit Doris Flight SQL port. Use 0 to auto-detect later")
    public DorisConfig setFlightSqlPort(int flightSqlPort)
    {
        this.flightSqlPort = flightSqlPort;
        return this;
    }

    @NotNull
    public DorisLargeintMapping getLargeintMapping()
    {
        return largeintMapping;
    }

    @Config("doris.largeint-mapping")
    @ConfigDescription("How Doris LARGEINT should map into Trino")
    public DorisConfig setLargeintMapping(DorisLargeintMapping largeintMapping)
    {
        this.largeintMapping = largeintMapping;
        return this;
    }

    @Min(1)
    public int getMaxSplitsPerQuery()
    {
        return maxSplitsPerQuery;
    }

    @Config("doris.max-splits-per-query")
    @ConfigDescription("Maximum number of splits to generate per query. Reduces overhead for small queries")
    public DorisConfig setMaxSplitsPerQuery(int maxSplitsPerQuery)
    {
        this.maxSplitsPerQuery = maxSplitsPerQuery;
        return this;
    }

    @Min(1)
    public int getMinTabletsPerSplit()
    {
        return minTabletsPerSplit;
    }

    @Config("doris.min-tablets-per-split")
    @ConfigDescription("Minimum number of tablets per split. Helps consolidate small tables")
    public DorisConfig setMinTabletsPerSplit(int minTabletsPerSplit)
    {
        this.minTabletsPerSplit = minTabletsPerSplit;
        return this;
    }
}
