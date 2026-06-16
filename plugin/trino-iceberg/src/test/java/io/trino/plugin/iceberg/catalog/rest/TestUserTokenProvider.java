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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;
import static org.assertj.core.api.Assertions.assertThat;

class TestUserTokenProvider
{
    @Test
    void testCatalogToken_noExchanger_returnsOriginalToken()
    {
        UserTokenProvider provider = new UserTokenProvider(Optional.empty());
        ConnectorSession session = sessionWithToken("original-token");

        assertThat(provider.catalogToken(session)).hasValue("original-token");
    }

    @Test
    void testCatalogToken_withExchanger_returnsExchangedToken()
    {
        FakeExchanger exchanger = new FakeExchanger("exchanged-token");
        UserTokenProvider provider = new UserTokenProvider(Optional.of(exchanger));
        ConnectorSession session = sessionWithToken("original-token");

        assertThat(provider.catalogToken(session)).hasValue("exchanged-token");
        assertThat(exchanger.receivedToken).isEqualTo("original-token");
    }

    @Test
    void testCatalogToken_noToken_returnsEmpty()
    {
        UserTokenProvider provider = new UserTokenProvider(Optional.empty());
        ConnectorSession session = sessionWithoutToken();

        assertThat(provider.catalogToken(session)).isEmpty();
    }

    @Test
    void testCatalogToken_noToken_exchangerNotCalled()
    {
        FakeExchanger exchanger = new FakeExchanger("exchanged-token");
        UserTokenProvider provider = new UserTokenProvider(Optional.of(exchanger));
        ConnectorSession session = sessionWithoutToken();

        assertThat(provider.catalogToken(session)).isEmpty();
        assertThat(exchanger.receivedToken).isNull();
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
                        .withExtraCredentials(Map.of())
                        .build())
                .build();
    }

    private static final class FakeExchanger
            extends OidcTokenExchanger
    {
        String receivedToken;
        private final String returnToken;

        FakeExchanger(String returnToken)
        {
            super(URI.create("http://fake"), "client", "secret", "scope");
            this.returnToken = returnToken;
        }

        @Override
        public String getToken(String oidcToken)
        {
            this.receivedToken = oidcToken;
            return returnToken;
        }
    }
}
