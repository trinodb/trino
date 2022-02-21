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

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.snowflake.auth.OauthCredential;
import com.starburstdata.presto.plugin.snowflake.auth.SnowflakeOauthService;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SnowflakeOauthPropertiesProvider
        implements CredentialPropertiesProvider<String, String>
{
    private final SnowflakeOauthService snowflakeOauthService;

    public SnowflakeOauthPropertiesProvider(SnowflakeOauthService snowflakeOauthService)
    {
        this.snowflakeOauthService = requireNonNull(snowflakeOauthService, "snowflakeOauthService is null");
    }

    @Override
    public Map<String, String> getCredentialProperties(ConnectorIdentity identity)
    {
        OauthCredential cred = snowflakeOauthService.getCredential(identity);

        return ImmutableMap.<String, String>builder()
                .put("authenticator", "oauth")
                .put("token", cred.getAccessToken())
                .buildOrThrow();
    }
}
