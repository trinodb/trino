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
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static java.util.Objects.requireNonNull;

public class SnowflakeSessionProperties
        implements SessionPropertiesProvider
{
    public static final String EXPERIMENTAL_PUSHDOWN_ENABLED = "experimental_pushdown_enabled";

    private final SnowflakeConfig snowflakeConfig;

    @Inject
    public SnowflakeSessionProperties(SnowflakeConfig snowflakeConfig)
    {
        this.snowflakeConfig = requireNonNull(snowflakeConfig, "snowflakeConfig is null");
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return ImmutableList.of(
                booleanProperty(
                        EXPERIMENTAL_PUSHDOWN_ENABLED,
                        "Enable some experimental pushdowns",
                        snowflakeConfig.isExperimentalPushdownEnabled(),
                        false));
    }

    public static boolean getExperimentalPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(EXPERIMENTAL_PUSHDOWN_ENABLED, Boolean.class);
    }
}
