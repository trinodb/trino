/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.stargate;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultJdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.SyntheticColumnHandleBuilder;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StargateMetadataFactory
        extends DefaultJdbcMetadataFactory
{
    private final Set<JdbcQueryEventListener> jdbcQueryEventListeners;
    private final SyntheticColumnHandleBuilder syntheticColumnBuilder;

    @Inject
    public StargateMetadataFactory(JdbcClient jdbcClient, Set<JdbcQueryEventListener> jdbcQueryEventListeners, SyntheticColumnHandleBuilder syntheticColumnBuilder)
    {
        super(jdbcClient, jdbcQueryEventListeners, syntheticColumnBuilder);
        this.jdbcQueryEventListeners = ImmutableSet.copyOf(requireNonNull(jdbcQueryEventListeners, "jdbcQueryEventListeners is null"));
        this.syntheticColumnBuilder = requireNonNull(syntheticColumnBuilder, "syntheticColumnBuilder is null");
    }

    @Override
    protected JdbcMetadata create(JdbcClient transactionCachingJdbcClient)
    {
        return new StargateMetadata(transactionCachingJdbcClient, jdbcQueryEventListeners, syntheticColumnBuilder);
    }
}
