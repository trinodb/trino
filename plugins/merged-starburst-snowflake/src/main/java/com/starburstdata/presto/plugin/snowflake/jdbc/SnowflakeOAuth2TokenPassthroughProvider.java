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
import com.starburstdata.presto.plugin.toolkit.security.multiple.tokens.IdPName;
import com.starburstdata.presto.plugin.toolkit.security.multiple.tokens.TokenPassThroughConfig;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.presto.plugin.toolkit.security.passthrough.TokenPassThrough.getToken;
import static java.util.Objects.requireNonNull;

public class SnowflakeOAuth2TokenPassthroughProvider
        implements CredentialPropertiesProvider<String, String>
{
    private final Optional<IdPName> name;

    public SnowflakeOAuth2TokenPassthroughProvider(TokenPassThroughConfig config)
    {
        this.name = requireNonNull(config, "config is null").getIdpName();
    }

    @Override
    public Map<String, String> getCredentialProperties(ConnectorIdentity identity)
    {
        return ImmutableMap.<String, String>builder()
                .put("authenticator", "oauth")
                .put("token", getToken(identity, name))
                .build();
    }
}
