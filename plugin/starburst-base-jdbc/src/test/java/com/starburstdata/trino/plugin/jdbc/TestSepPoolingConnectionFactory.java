/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package com.starburstdata.trino.plugin.jdbc;

import com.starburstdata.trino.plugin.jdbc.auth.AuthenticationBasedIdentityCacheMapping;
import io.trino.plugin.jdbc.IdentityCacheMapping;

public class TestSepPoolingConnectionFactory
        extends BasePoolingConnectionFactoryTest
{
    @Override
    protected IdentityCacheMapping getIdentityCacheMapping()
    {
        return new AuthenticationBasedIdentityCacheMapping();
    }
}
