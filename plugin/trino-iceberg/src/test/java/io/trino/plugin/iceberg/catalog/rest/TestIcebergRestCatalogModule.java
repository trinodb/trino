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

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestIcebergRestCatalogModule
{
    @Test
    void testTokenDelegationRequiresUserSession()
    {
        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setTokenDelegation(true)
                .setSessionType(IcebergRestCatalogConfig.SessionType.NONE);

        assertThatThrownBy(() -> validateConfig(config, new IcebergRestCatalogSigV4Config(), new IcebergRestCatalogTokenExchangeConfig()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("iceberg.rest-catalog.token-delegation requires iceberg.rest-catalog.session=user");
    }

    @Test
    void testStsWebIdentityRequiresUserSession()
    {
        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setSessionType(IcebergRestCatalogConfig.SessionType.NONE);

        IcebergRestCatalogSigV4Config sigV4Config = new IcebergRestCatalogSigV4Config()
                .setStsWebIdentity(true);

        assertThatThrownBy(() -> validateConfig(config, sigV4Config, new IcebergRestCatalogTokenExchangeConfig()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("iceberg.rest-catalog.sts-web-identity requires iceberg.rest-catalog.session=user");
    }

    @Test
    void testTokenExchangeRequiresUserSession()
    {
        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setSessionType(IcebergRestCatalogConfig.SessionType.NONE);

        IcebergRestCatalogTokenExchangeConfig tokenExchangeConfig = enabledTokenExchangeConfig();

        assertThatThrownBy(() -> validateConfig(config, new IcebergRestCatalogSigV4Config(), tokenExchangeConfig))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("iceberg.rest-catalog.token-exchange-enabled requires iceberg.rest-catalog.session=user");
    }

    @Test
    void testTokenDelegationWithUserSessionIsValid()
    {
        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setTokenDelegation(true)
                .setSessionType(IcebergRestCatalogConfig.SessionType.USER);

        validateConfig(config, new IcebergRestCatalogSigV4Config(), new IcebergRestCatalogTokenExchangeConfig());
    }

    @Test
    void testStsWebIdentityWithUserSessionIsValid()
    {
        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setSessionType(IcebergRestCatalogConfig.SessionType.USER);

        IcebergRestCatalogSigV4Config sigV4Config = new IcebergRestCatalogSigV4Config()
                .setStsWebIdentity(true);

        validateConfig(config, sigV4Config, new IcebergRestCatalogTokenExchangeConfig());
    }

    @Test
    void testTokenExchangeWithUserSessionIsValid()
    {
        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setSessionType(IcebergRestCatalogConfig.SessionType.USER);

        validateConfig(config, new IcebergRestCatalogSigV4Config(), enabledTokenExchangeConfig());
    }

    private static IcebergRestCatalogTokenExchangeConfig enabledTokenExchangeConfig()
    {
        return new IcebergRestCatalogTokenExchangeConfig()
                .setEnabled(true)
                .setEndpoint(URI.create("https://login.microsoftonline.com/tenant/oauth2/v2.0/token"))
                .setClientId("client-id")
                .setClientSecret("client-secret")
                .setScope("api://minio/storage.access");
    }

    // Mirrors the validation logic in IcebergRestCatalogModule.setup()
    private static void validateConfig(
            IcebergRestCatalogConfig config,
            IcebergRestCatalogSigV4Config sigV4Config,
            IcebergRestCatalogTokenExchangeConfig tokenExchangeConfig)
    {
        if (config.isTokenDelegation() && config.getSessionType() != IcebergRestCatalogConfig.SessionType.USER) {
            throw new TrinoException(
                    io.trino.spi.StandardErrorCode.NOT_SUPPORTED,
                    "iceberg.rest-catalog.token-delegation requires iceberg.rest-catalog.session=user");
        }
        if (sigV4Config.isStsWebIdentity() && config.getSessionType() != IcebergRestCatalogConfig.SessionType.USER) {
            throw new TrinoException(
                    io.trino.spi.StandardErrorCode.NOT_SUPPORTED,
                    "iceberg.rest-catalog.sts-web-identity requires iceberg.rest-catalog.session=user");
        }
        if (tokenExchangeConfig.isEnabled() && config.getSessionType() != IcebergRestCatalogConfig.SessionType.USER) {
            throw new TrinoException(
                    io.trino.spi.StandardErrorCode.NOT_SUPPORTED,
                    "iceberg.rest-catalog.token-exchange-enabled requires iceberg.rest-catalog.session=user");
        }
    }
}
