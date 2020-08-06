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

import com.starburstdata.presto.okta.OktaAuthenticationResult;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DefaultSnowflakeOauthService
        implements SnowflakeOauthService
{
    private final SnowflakeAuthClient snowflakeAuthClient;
    private final OktaAuthClient oktaAuthClient;
    private final CredentialProvider credentialProvider;

    public DefaultSnowflakeOauthService(SnowflakeAuthClient snowflakeAuthClient, OktaAuthClient oktaAuthClient, CredentialProvider credentialProvider)
    {
        this.snowflakeAuthClient = requireNonNull(snowflakeAuthClient, "snowflakeAuthClient is null");
        this.oktaAuthClient = requireNonNull(oktaAuthClient, "oktaAuthClient is null");
        this.credentialProvider = requireNonNull(credentialProvider, "credentialProvider is null");
    }

    @Override
    public OauthCredential getCredential(JdbcIdentity identity)
    {
        SamlRequest samlRequest = snowflakeAuthClient.generateSamlRequest(identity);
        String user = credentialProvider.getConnectionUser(Optional.of(identity))
                .orElseThrow(() -> new IllegalStateException("Cannot determine user name"));
        String password = credentialProvider.getConnectionPassword(Optional.of(identity))
                .orElseThrow(() -> new IllegalStateException("Cannot determine password"));
        OktaAuthenticationResult authResult = oktaAuthClient.authenticate(user, password);
        SamlResponse samlResponse = oktaAuthClient.obtainSamlAssertion(samlRequest, authResult);

        return snowflakeAuthClient.requestOauthToken(samlResponse);
    }

    @Override
    public OauthCredential refreshCredential(OauthCredential credential)
    {
        return snowflakeAuthClient.refreshCredential(credential);
    }
}
