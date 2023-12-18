/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.jdbc;

import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMapping;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.credential.ExtraCredentialConfig;

public class TestGalaxyPoolingConnectionFactory
        extends BasePoolingConnectionFactoryTest
{
    @Override
    protected IdentityCacheMapping getIdentityCacheMapping()
    {
        return new ExtraCredentialsBasedIdentityCacheMapping(new ExtraCredentialConfig().setUserCredentialName("user").setPasswordCredentialName("password"));
    }
}
