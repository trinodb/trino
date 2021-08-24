/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.jdbc;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.jdbc.JdbcConnectionPoolConfig;
import com.starburstdata.presto.plugin.jdbc.PoolingConnectionFactory;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Driver;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeJdbcSessionProperties.getWarehouse;

public class WarehouseAwarePoolingConnectionFactory
        extends PoolingConnectionFactory
{
    public WarehouseAwarePoolingConnectionFactory(
            String catalogName,
            Class<? extends Driver> driverClass,
            Properties connectionProperties,
            BaseJdbcConfig config,
            JdbcConnectionPoolConfig poolConfig,
            CredentialPropertiesProvider<String, String> credentialPropertiesProvider)
    {
        super(catalogName, driverClass, connectionProperties, config, poolConfig, credentialPropertiesProvider);
    }

    @Override
    protected Map<String, String> getConnectionProperties(ConnectorSession session)
    {
        Map<String, String> properties = new HashMap<>(super.getConnectionProperties(session));
        getWarehouse(session).ifPresent(warehouse -> properties.put("warehouse", warehouse));
        return ImmutableMap.copyOf(properties);
    }
}
