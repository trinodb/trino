/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.jdbc;

import com.starburstdata.presto.plugin.snowflake.auth.OauthCredential;
import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeOauthService;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.credential.CredentialPropertiesProvider;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class SnowflakeOauthPropertiesProvider
        implements CredentialPropertiesProvider
{
    private final SnowflakeOauthService snowflakeOauthService;

    public SnowflakeOauthPropertiesProvider(SnowflakeOauthService snowflakeOauthService)
    {
        this.snowflakeOauthService = requireNonNull(snowflakeOauthService, "snowflakeOauthService is null");
    }

    @Override
    public Properties getCredentialProperties(JdbcIdentity identity)
    {
        OauthCredential cred = snowflakeOauthService.getCredential(identity);

        Properties props = new Properties();
        props.put("authenticator", "oauth");
        props.put("token", cred.getAccessToken());

        return props;
    }
}
