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
package io.trino.tests.product.jdbc;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;

/**
 * OAuth2 environment with refresh token support.
 * <p>
 * This environment is used for OAuth2 authentication tests where
 * tokens can be refreshed without requiring user re-authentication.
 */
public class JdbcOAuth2RefreshEnvironment
        extends JdbcOAuth2Environment
{
    // The timeout for internally issued tokens (Trino's internal token)
    private static final int ISSUED_TOKEN_TIMEOUT_IN_SECONDS = 30;

    @Override
    protected void configureTrino(GenericContainer<?> container)
    {
        // Write config.properties with refresh token support
        String config = getBaseConfigProperties() +
                "http-server.authentication.oauth2.scopes=openid,offline\n" +
                "http-server.authentication.oauth2.refresh-tokens=true\n" +
                "http-server.authentication.oauth2.refresh-tokens.issued-token.timeout=" + ISSUED_TOKEN_TIMEOUT_IN_SECONDS + "s\n" +
                "http-server.log.enabled=false\n";
        container.withCopyToContainer(
                Transferable.of(config),
                "/etc/trino/config.properties");

        // Write log.properties (minimal logging config)
        container.withCopyToContainer(
                Transferable.of("io.trino=INFO\n"),
                "/etc/trino/log.properties");
    }

    /**
     * Returns the timeout in seconds for the internally issued token.
     */
    public int getIssuedTokenTimeoutSeconds()
    {
        return ISSUED_TOKEN_TIMEOUT_IN_SECONDS;
    }
}
