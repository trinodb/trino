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

import com.starburstdata.presto.plugin.toolkit.InvocationStats;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class OktaAuthClientStats
{
    private final InvocationStats authenticate = new InvocationStats();
    private final InvocationStats obtainSamlAssertion = new InvocationStats();

    @Managed
    @Nested
    public InvocationStats getAuthenticate()
    {
        return authenticate;
    }

    @Managed
    @Nested
    public InvocationStats getObtainSamlAssertion()
    {
        return obtainSamlAssertion;
    }
}
