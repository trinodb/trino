/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeDistributedConnectorFactory;
import com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeJdbcClientModule;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

public class TestingSnowflakePlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(
                new JdbcConnectorFactory(
                        SnowflakePlugin.SNOWFLAKE_JDBC,
                        catalogName -> {
                            return new SnowflakeJdbcClientModule(catalogName, false);
                        }),
                new SnowflakeDistributedConnectorFactory(SnowflakePlugin.SNOWFLAKE_DISTRIBUTED));
    }
}
