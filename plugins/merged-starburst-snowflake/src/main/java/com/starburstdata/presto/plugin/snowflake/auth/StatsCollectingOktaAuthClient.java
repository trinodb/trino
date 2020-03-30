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

import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import static java.util.Objects.requireNonNull;

public class StatsCollectingOktaAuthClient
        implements OktaAuthClient
{
    private final OktaAuthClient delegate;
    private final OktaAuthClientStats stats = new OktaAuthClientStats();

    public StatsCollectingOktaAuthClient(OktaAuthClient delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Managed
    @Flatten
    public OktaAuthClientStats getStats()
    {
        return stats;
    }

    @Override
    public OktaAuthenticationResult authenticate(String user, String password)
    {
        return stats.getAuthenticate().call(() -> delegate.authenticate(user, password));
    }

    @Override
    public SamlResponse obtainSamlAssertion(SamlRequest samlRequest, OktaAuthenticationResult authenticationResult)
    {
        return stats.getObtainSamlAssertion().call(() -> delegate.obtainSamlAssertion(samlRequest, authenticationResult));
    }
}
