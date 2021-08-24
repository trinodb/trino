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
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;

import static com.starburstdata.presto.plugin.toolkit.OAuth2TokenPassThrough.OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class SnowflakeOAuth2TokenPassthroughProvider
        implements CredentialPropertiesProvider<String, String>
{
    @Override
    public Map<String, String> getCredentialProperties(ConnectorIdentity identity)
    {
        String token = identity.getExtraCredentials().getOrDefault(OAUTH2_ACCESS_TOKEN_PASSTHROUGH_CREDENTIAL, "");
        if (token.isEmpty()) {
            throw new TrinoException(GENERIC_USER_ERROR, "Token pass-through authentication requires a valid token");
        }
        return ImmutableMap.<String, String>builder()
                .put("authenticator", "oauth")
                .put("token", token)
                .build();
    }
}
