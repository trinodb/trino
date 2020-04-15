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
package io.prestosql.plugin.jdbc.credential;

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.Bootstrap;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestExtraCredentialProvider
{
    @Test
    public void testUserNameOverwritten()
    {
        Map<String, String> properties = ImmutableMap.of(
                "connection-user", "default_user",
                "connection-password", "default_password",
                "user-credential-name", "user");

        CredentialProvider credentialProvider = getCredentialProvider(properties);
        Optional<JdbcIdentity> jdbcIdentity = Optional.of(new JdbcIdentity("user", Optional.empty(), ImmutableMap.of("user", "overwritten_user")));
        assertEquals(credentialProvider.getConnectionUser(jdbcIdentity).get(), "overwritten_user");
        assertEquals(credentialProvider.getConnectionPassword(jdbcIdentity).get(), "default_password");
    }

    @Test
    public void testPasswordOverwritten()
    {
        Map<String, String> properties = ImmutableMap.of(
                "connection-user", "default_user",
                "connection-password", "default_password",
                "password-credential-name", "password");

        CredentialProvider credentialProvider = getCredentialProvider(properties);
        Optional<JdbcIdentity> jdbcIdentity = Optional.of(new JdbcIdentity("user", Optional.empty(), ImmutableMap.of("password", "overwritten_password")));
        assertEquals(credentialProvider.getConnectionUser(jdbcIdentity).get(), "default_user");
        assertEquals(credentialProvider.getConnectionPassword(jdbcIdentity).get(), "overwritten_password");
    }

    @Test
    public void testCredentialsOverwritten()
    {
        Map<String, String> properties = ImmutableMap.of(
                "connection-user", "default_user",
                "connection-password", "default_password",
                "user-credential-name", "user",
                "password-credential-name", "password");

        CredentialProvider credentialProvider = getCredentialProvider(properties);
        Optional<JdbcIdentity> jdbcIdentity = Optional.of(new JdbcIdentity("user", Optional.empty(), ImmutableMap.of("user", "overwritten_user", "password", "overwritten_password")));
        assertEquals(credentialProvider.getConnectionUser(jdbcIdentity).get(), "overwritten_user");
        assertEquals(credentialProvider.getConnectionPassword(jdbcIdentity).get(), "overwritten_password");
    }

    @Test
    public void testCredentialsNotOverwritten()
    {
        Map<String, String> properties = ImmutableMap.of(
                "connection-user", "default_user",
                "connection-password", "default_password",
                "user-credential-name", "user",
                "password-credential-name", "password");

        CredentialProvider credentialProvider = getCredentialProvider(properties);
        Optional<JdbcIdentity> jdbcIdentity = Optional.of(new JdbcIdentity("user", Optional.empty(), ImmutableMap.of()));
        assertEquals(credentialProvider.getConnectionUser(jdbcIdentity).get(), "default_user");
        assertEquals(credentialProvider.getConnectionPassword(jdbcIdentity).get(), "default_password");

        jdbcIdentity = Optional.of(new JdbcIdentity("user", Optional.empty(), ImmutableMap.of("connection_user", "overwritten_user", "connection_password", "overwritten_password")));
        assertEquals(credentialProvider.getConnectionUser(jdbcIdentity).get(), "default_user");
        assertEquals(credentialProvider.getConnectionPassword(jdbcIdentity).get(), "default_password");
    }

    private static CredentialProvider getCredentialProvider(Map<String, String> properties)
    {
        return new Bootstrap(new CredentialProviderModule())
                .strictConfig()
                .doNotInitializeLogging()
                .quiet()
                .setRequiredConfigurationProperties(properties)
                .initialize()
                .getInstance(CredentialProvider.class);
    }
}
