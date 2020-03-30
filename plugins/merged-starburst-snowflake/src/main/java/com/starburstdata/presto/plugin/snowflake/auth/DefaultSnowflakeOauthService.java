/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starburstdata.presto.plugin.snowflake.auth;

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
