/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultJdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StargateMetadataFactory
        extends DefaultJdbcMetadataFactory
{
    private final StargateCatalogIdentityFactory catalogIdentityFactory;
    private final TimestampTimeZoneDomain timestampTimeZoneDomain;
    private final Set<JdbcQueryEventListener> jdbcQueryEventListeners;

    @Inject
    public StargateMetadataFactory(StargateCatalogIdentityFactory catalogIdentityFactory, JdbcClient jdbcClient, TimestampTimeZoneDomain timestampTimeZoneDomain, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(jdbcClient, timestampTimeZoneDomain, jdbcQueryEventListeners);
        this.catalogIdentityFactory = requireNonNull(catalogIdentityFactory, "catalogIdentityFactory is null");
        this.timestampTimeZoneDomain = requireNonNull(timestampTimeZoneDomain, "timestampTimeZoneDomain is null");
        this.jdbcQueryEventListeners = ImmutableSet.copyOf(requireNonNull(jdbcQueryEventListeners, "jdbcQueryEventListeners is null"));
    }

    @Override
    protected JdbcMetadata create(JdbcClient transactionCachingJdbcClient)
    {
        return new StargateMetadata(catalogIdentityFactory, transactionCachingJdbcClient, timestampTimeZoneDomain, jdbcQueryEventListeners);
    }
}
