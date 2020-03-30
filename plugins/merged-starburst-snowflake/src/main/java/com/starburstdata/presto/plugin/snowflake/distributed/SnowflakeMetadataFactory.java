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
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.airlift.units.Duration;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcMetadataConfig;
import io.prestosql.plugin.jdbc.caching.BaseJdbcCachingConfig;
import io.prestosql.plugin.jdbc.caching.CachingJdbcClient;

import javax.inject.Inject;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class SnowflakeMetadataFactory
{
    private final SnowflakeConnectionManager connectionManager;
    private final JdbcClient jdbcClient;
    private final boolean allowDropTable;

    @Inject
    public SnowflakeMetadataFactory(
            SnowflakeConnectionManager connectionManager,
            JdbcClient jdbcClient,
            JdbcMetadataConfig config,
            BaseJdbcCachingConfig cachingConfig)
    {
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
        this.jdbcClient = new CachingJdbcClient(requireNonNull(jdbcClient, "jdbcClient is null"), cachingConfig);
        requireNonNull(config, "config is null");
        this.allowDropTable = config.isAllowDropTable();
    }

    SnowflakeMetadata create()
    {
        return new SnowflakeMetadata(
                connectionManager,
                new CachingJdbcClient(jdbcClient, new Duration(1, TimeUnit.DAYS), true),
                allowDropTable);
    }
}
