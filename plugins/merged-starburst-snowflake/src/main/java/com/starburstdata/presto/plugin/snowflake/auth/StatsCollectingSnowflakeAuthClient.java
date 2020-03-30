/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.auth;

import io.prestosql.plugin.jdbc.JdbcIdentity;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import static java.util.Objects.requireNonNull;

public class StatsCollectingSnowflakeAuthClient
        implements SnowflakeAuthClient
{
    private final SnowflakeAuthClient delegate;
    private final SnowflakeAuthClientStats stats = new SnowflakeAuthClientStats();

    public StatsCollectingSnowflakeAuthClient(SnowflakeAuthClient delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Managed
    @Flatten
    public SnowflakeAuthClientStats getStats()
    {
        return stats;
    }

    @Override
    public SamlRequest generateSamlRequest(JdbcIdentity identity)
    {
        return stats.getGenerateSamlRequest().call(() -> delegate.generateSamlRequest(identity));
    }

    @Override
    public OauthCredential requestOauthToken(SamlResponse samlResponse)
    {
        return stats.getRequestOauthToken().call(() -> delegate.requestOauthToken(samlResponse));
    }

    @Override
    public OauthCredential refreshCredential(OauthCredential credential)
    {
        return stats.getRefreshCredential().call(() -> delegate.refreshCredential(credential));
    }
}
