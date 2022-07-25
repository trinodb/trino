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
package io.trino.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTrinoDriverImpersonateUser
{
    private static final String TEST_USER = "test_user";
    private static final String PASSWORD = "password";

    private TestingTrinoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Path passwordConfigDummy = Files.createTempFile("passwordConfigDummy", null);
        passwordConfigDummy.toFile().deleteOnExit();
        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("password-authenticator.config-files", passwordConfigDummy.toString())
                        .put("http-server.authentication.type", "password")
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", new File(getResource("localhost.keystore").toURI()).getPath())
                        .put("http-server.https.keystore.key", "changeit")
                        .buildOrThrow())
                .build();

        server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestTrinoDriverImpersonateUser::authenticate);
    }

    private static Principal authenticate(String user, String password)
    {
        if ((TEST_USER.equals(user) && PASSWORD.equals(password))) {
            return new BasicPrincipal(user);
        }
        throw new AccessDeniedException("Invalid credentials");
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        server.close();
    }

    @Test
    public void testInvalidCredentials()
    {
        assertThatThrownBy(() -> trySelectCurrentUser(ImmutableMap.of()));
        assertThatThrownBy(() -> trySelectCurrentUser(ImmutableMap.of("user", "invalidUser", "password", PASSWORD)));
        assertThatThrownBy(() -> trySelectCurrentUser(ImmutableMap.of("user", TEST_USER, "password", "invalidPassword")));
        assertThatThrownBy(() -> trySelectCurrentUser(ImmutableMap.of("user", "invalidUser", "password", PASSWORD, "sessionUser", TEST_USER)));
    }

    @Test
    public void testQueryUserNotSpecified()
            throws Exception
    {
        assertEquals(trySelectCurrentUser(ImmutableMap.of("user", TEST_USER, "password", PASSWORD)), TEST_USER);
    }

    @Test
    public void testImpersonateUser()
            throws Exception
    {
        assertEquals(trySelectCurrentUser(ImmutableMap.of("user", TEST_USER, "password", PASSWORD, "sessionUser", "differentUser")), "differentUser");
    }

    private String trySelectCurrentUser(Map<String, String> additionalProperties)
            throws Exception
    {
        try (Connection connection = createConnection(additionalProperties);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT current_user")) {
            assertTrue(resultSet.next());
            return resultSet.getString(1);
        }
    }

    private Connection createConnection(Map<String, String> additionalProperties)
            throws Exception
    {
        String url = format("jdbc:trino://localhost:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLTrustStorePath", new File(getResource("localhost.truststore").toURI()).getPath());
        properties.setProperty("SSLTrustStorePassword", "changeit");
        additionalProperties.forEach(properties::setProperty);
        return DriverManager.getConnection(url, properties);
    }
}
