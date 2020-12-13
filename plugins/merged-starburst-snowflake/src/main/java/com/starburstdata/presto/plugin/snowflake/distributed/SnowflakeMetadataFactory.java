/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.airlift.units.Duration;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.CachingJdbcClient;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcMetadataConfig;

import javax.inject.Inject;

import java.util.Set;
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
            BaseJdbcConfig cachingConfig)
    {
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
        this.jdbcClient = new CachingJdbcClient(requireNonNull(jdbcClient, "jdbcClient is null"), Set.of(), cachingConfig);
        requireNonNull(config, "config is null");
        this.allowDropTable = config.isAllowDropTable();
    }

    SnowflakeMetadata create()
    {
        return new SnowflakeMetadata(
                connectionManager,
                new CachingJdbcClient(jdbcClient, Set.of(), new Duration(1, TimeUnit.DAYS), true),
                allowDropTable);
    }
}
