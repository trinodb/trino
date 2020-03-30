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

import io.prestosql.plugin.base.jmx.InvocationStats;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class SnowflakeAuthClientStats
{
    private final InvocationStats requestOauthToken = new InvocationStats();
    private final InvocationStats refreshCredential = new InvocationStats();
    private final InvocationStats generateSamlRequest = new InvocationStats();

    @Managed
    @Nested
    public InvocationStats getRequestOauthToken()
    {
        return requestOauthToken;
    }

    @Managed
    @Nested
    public InvocationStats getRefreshCredential()
    {
        return refreshCredential;
    }

    @Managed
    @Nested
    public InvocationStats getGenerateSamlRequest()
    {
        return generateSamlRequest;
    }
}
