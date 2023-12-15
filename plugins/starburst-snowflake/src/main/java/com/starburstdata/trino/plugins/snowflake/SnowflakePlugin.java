/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import com.google.common.collect.ImmutableList;
import com.starburstdata.trino.plugins.snowflake.distributed.SnowflakeDistributedConnectorFactory;
import com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcClientModule;
import com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelConnectorFactory;
import io.trino.plugin.jdbc.JdbcConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeConnectorFlavour.DISTRIBUTED;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeConnectorFlavour.JDBC;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeConnectorFlavour.PARALLEL;

public class SnowflakePlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(
                new JdbcConnectorFactory(
                        JDBC.getName(),
                        new SnowflakeJdbcClientModule(JDBC)),
                new SnowflakeDistributedConnectorFactory(DISTRIBUTED.getName()),
                new SnowflakeParallelConnectorFactory(PARALLEL.getName()));
    }
}
