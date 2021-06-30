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

import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.plugin.snowflake.SnowflakeConfig;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public class SnowflakeJdbcSessionProperties
        implements SessionPropertiesProvider
{
    private static final String WAREHOUSE = "warehouse";

    private final SnowflakeConfig snowflakeConfig;

    @Inject
    public SnowflakeJdbcSessionProperties(SnowflakeConfig snowflakeConfig)
    {
        this.snowflakeConfig = requireNonNull(snowflakeConfig, "snowflakeConfig is null");
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return ImmutableList.of(
                stringProperty(
                        WAREHOUSE,
                        "Warehouse to connect to while executing queries",
                        snowflakeConfig.getWarehouse().orElse(null),
                        false));
    }

    public static Optional<String> getWarehouse(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(WAREHOUSE, String.class));
    }
}
