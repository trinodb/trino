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
 * OAuth2 environment without refresh token support.
 * <p>
 * This environment is used for basic OAuth2 authentication tests where
 * the user must re-authenticate when the access token expires.
 */
public class JdbcOAuth2BasicEnvironment
        extends JdbcOAuth2Environment
{
    @Override
    protected void configureTrino(GenericContainer<?> container)
    {
        // Write config.properties
        container.withCopyToContainer(
                Transferable.of(getBaseConfigProperties()),
                "/etc/trino/config.properties");

        // Write log.properties (minimal logging config)
        container.withCopyToContainer(
                Transferable.of("io.trino=INFO\n"),
                "/etc/trino/log.properties");
    }
}
