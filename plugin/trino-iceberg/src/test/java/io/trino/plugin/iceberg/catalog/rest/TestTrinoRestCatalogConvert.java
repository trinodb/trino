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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType.USER;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.aws.AwsProperties.REST_ACCESS_KEY_ID;
import static org.apache.iceberg.aws.AwsProperties.REST_SECRET_ACCESS_KEY;
import static org.apache.iceberg.aws.AwsProperties.REST_SESSION_TOKEN;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;
import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoRestCatalogConvert
{
    @Test
    void testConvertWithStsExchanger_callsExchanger()
            throws IOException
    {
        TrackingStsExchanger stsExchanger = new TrackingStsExchanger();
        UserTokenProvider tokenProvider = providerWithNoExchange();
        TrinoRestCatalog catalog = createCatalog(Optional.of(stsExchanger), tokenProvider);

        ConnectorSession session = sessionWithToken("test-oidc-jwt");
        SessionCatalog.SessionContext ctx = catalog.convert(session);

        assertThat(stsExchanger.callCount).isEqualTo(1);
        assertThat(ctx.credentials())
                .containsEntry(REST_ACCESS_KEY_ID, "access-key")
                .containsEntry(REST_SECRET_ACCESS_KEY, "secret-key")
                .containsEntry(REST_SESSION_TOKEN, "session-token")
                .doesNotContainKey(TOKEN);
    }

    @Test
    void testConvertWithStsExchanger_noToken_skipsExchanger()
            throws IOException
    {
        TrackingStsExchanger stsExchanger = new TrackingStsExchanger();
        UserTokenProvider tokenProvider = providerWithNoExchange();
        TrinoRestCatalog catalog = createCatalog(Optional.of(stsExchanger), tokenProvider);

        ConnectorSession session = sessionWithoutToken();
        SessionCatalog.SessionContext ctx = catalog.convert(session);

        assertThat(stsExchanger.callCount).isEqualTo(0);
        assertThat(ctx.credentials()).doesNotContainKey(REST_ACCESS_KEY_ID);
    }

    @Test
    void testConvertWithoutStsExchanger_forwardsTokenDirectly()
            throws IOException
    {
        UserTokenProvider tokenProvider = providerWithNoExchange();
        TrinoRestCatalog catalog = createCatalog(Optional.empty(), tokenProvider);

        ConnectorSession session = sessionWithToken("test-oidc-jwt");
        SessionCatalog.SessionContext ctx = catalog.convert(session);

        assertThat(ctx.credentials())
                .doesNotContainKey(REST_ACCESS_KEY_ID)
                .containsEntry(TOKEN, "test-oidc-jwt");
    }

    @Test
    void testConvertWithTokenExchange_exchangedTokenPassedToSts()
            throws IOException
    {
        TrackingStsExchanger stsExchanger = new TrackingStsExchanger();
        UserTokenProvider tokenProvider = providerWithExchange("exchanged-token");
        TrinoRestCatalog catalog = createCatalog(Optional.of(stsExchanger), tokenProvider);

        ConnectorSession session = sessionWithToken("original-token");
        catalog.convert(session);

        assertThat(stsExchanger.lastReceivedToken).isEqualTo("exchanged-token");
    }

    @Test
    void testConvertWithTokenExchange_noSts_forwardsExchangedToken()
            throws IOException
    {
        UserTokenProvider tokenProvider = providerWithExchange("exchanged-token");
        TrinoRestCatalog catalog = createCatalog(Optional.empty(), tokenProvider);

        ConnectorSession session = sessionWithToken("original-token");
        SessionCatalog.SessionContext ctx = catalog.convert(session);

        assertThat(ctx.credentials()).containsEntry(TOKEN, "exchanged-token");
    }

    private static UserTokenProvider providerWithNoExchange()
    {
        return new UserTokenProvider(Optional.empty());
    }

    private static UserTokenProvider providerWithExchange(String exchangedToken)
    {
        return new UserTokenProvider(Optional.of(new FixedTokenExchanger(exchangedToken)));
    }

    private static TrinoRestCatalog createCatalog(Optional<OidcStsCredentialExchanger> stsExchanger, UserTokenProvider tokenProvider)
            throws IOException
    {
        Path warehouseLocation = Files.createTempDirectory(null);
        warehouseLocation.toFile().deleteOnExit();

        String catalogName = "iceberg_rest";
        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog
                .builder()
                .delegate(backendCatalog(warehouseLocation))
                .build();
        restSessionCatalog.initialize(catalogName, ImmutableMap.of());

        return new TrinoRestCatalog(
                restSessionCatalog,
                new CatalogName(catalogName),
                USER,
                ImmutableMap.of(),
                stsExchanger,
                tokenProvider,
                false,
                "test",
                TESTING_TYPE_MANAGER,
                false,
                false,
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
                true);
    }

    private static ConnectorSession sessionWithToken(String token)
    {
        return TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.forUser("testuser")
                        .withExtraCredentials(Map.of(TOKEN, token))
                        .build())
                .build();
    }

    private static ConnectorSession sessionWithoutToken()
    {
        return TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.forUser("testuser")
                        .withExtraCredentials(ImmutableMap.of())
                        .build())
                .build();
    }

    private static final class TrackingStsExchanger
            extends OidcStsCredentialExchanger
    {
        int callCount;
        String lastReceivedToken;

        TrackingStsExchanger()
        {
            super(null, "test-role");
        }

        @Override
        public AwsSessionCredentials getCredentials(String catalogToken)
        {
            callCount++;
            lastReceivedToken = catalogToken;
            return AwsSessionCredentials.create("access-key", "secret-key", "session-token");
        }

        @Override
        CachedCredentials exchange(String catalogToken, String sub)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final class FixedTokenExchanger
            extends OidcTokenExchanger
    {
        private final String token;

        FixedTokenExchanger(String token)
        {
            super(URI.create("http://fake"), "client", "secret", "scope");
            this.token = token;
        }

        @Override
        public String getToken(String oidcToken)
        {
            return token;
        }
    }
}
