/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.distributed;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CachingJdbcClient;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcTransactionHandle;
import io.trino.plugin.jdbc.SingletonIdentityCacheMapping;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class SnowflakeMetadataFactory
        implements JdbcMetadataFactory
{
    private final JdbcClient jdbcClient;
    private final Set<JdbcQueryEventListener> jdbcQueryEventListeners;

    @Inject
    public SnowflakeMetadataFactory(
            JdbcClient jdbcClient,
            IdentityCacheMapping identityMapping,
            BaseJdbcConfig cachingConfig,
            Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        this.jdbcClient = new CachingJdbcClient(requireNonNull(jdbcClient, "jdbcClient is null"), Set.of(), identityMapping, cachingConfig);
        this.jdbcQueryEventListeners = ImmutableSet.copyOf(requireNonNull(jdbcQueryEventListeners, "jdbcQueryEventListeners is null"));
    }

    @Override
    public SnowflakeMetadata create(JdbcTransactionHandle handle)
    {
        return new SnowflakeMetadata(
                new CachingJdbcClient(jdbcClient, Set.of(), new SingletonIdentityCacheMapping(), new Duration(1, TimeUnit.DAYS), true, Integer.MAX_VALUE),
                jdbcQueryEventListeners);
    }
}
