/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.jdbc;

import com.google.common.collect.ImmutableMap;
import com.snowflake.client.jdbc.SnowflakeDriver;
import com.starburstdata.trino.plugins.jdbc.JdbcConnectionPoolConfig;
import com.starburstdata.trino.plugins.jdbc.PoolingConnectionFactory;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Properties;

import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcSessionProperties.WAREHOUSE;
import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcSessionProperties.getWarehouse;

public class WarehouseAwareDriverPoolingConnectionFactory
        extends PoolingConnectionFactory
{
    public WarehouseAwareDriverPoolingConnectionFactory(
            String catalogName,
            Properties connectionProperties, BaseJdbcConfig config,
            JdbcConnectionPoolConfig poolConfig,
            CredentialProvider credentialProvider,
            IdentityCacheMapping identityCacheMapping)
    {
        super(
                catalogName,
                SnowflakeDriver.class,
                connectionProperties,
                config,
                poolConfig,
                new DefaultCredentialPropertiesProvider(credentialProvider),
                identityCacheMapping);
    }

    @Override
    protected Map<String, String> getConnectionProperties(ConnectorSession session)
    {
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder().putAll(super.getConnectionProperties(session));
        getWarehouse(session).ifPresent(warehouse -> propertiesBuilder.put(WAREHOUSE, warehouse));
        return propertiesBuilder.buildKeepingLast();
    }
}
