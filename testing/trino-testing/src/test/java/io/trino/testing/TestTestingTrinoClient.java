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
package io.trino.testing;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.trino.Session;
import io.trino.client.OkHttpUtil;
import io.trino.execution.QueryIdGenerator;
import io.trino.metadata.SessionPropertyManager;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestTestingTrinoClient
{
    private static final String TEST_USER = "test_user";
    private static final String PASSWORD = "password";
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
    private static final SessionPropertyManager sessionManager = new SessionPropertyManager();
    private static final Session session = Session.builder(sessionManager)
            .setIdentity(Identity.forUser(TEST_USER).build())
            .setOriginalIdentity(Identity.forUser(TEST_USER).build())
            .setQueryId(queryIdGenerator.createNextQueryId())
            .build();

    private TestingTrinoServer server;

    @BeforeClass
    public void setup()
            throws IOException
    {
        Path passwordConfigDummy = Files.createTempFile("passwordConfigDummy", "");
        passwordConfigDummy.toFile().deleteOnExit();
        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("password-authenticator.config-files", passwordConfigDummy.toString())
                        .put("http-server.authentication.type", "password")
                        .put("http-server.authentication.allow-insecure-over-http", "false")
                        .put("http-server.process-forwarded", "true")
                        .buildOrThrow())
                .build();

        server.getInstance(Key.get(PasswordAuthenticatorManager.class)).setAuthenticators(TestTestingTrinoClient::authenticate);
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
        server = null;
    }

    @Test
    public void testAuthenticationWithForwarding()
    {
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .addInterceptor(OkHttpUtil.basicAuth(TEST_USER, PASSWORD))
                .addInterceptor(httpsForwarded())
                .build();

        try (TestingTrinoClient client = new TestingTrinoClient(server, session, httpClient)) {
            MaterializedResult result = client.execute("SELECT 123").getResult();
            assertEquals(result.getOnlyValue(), 123);
        }
    }

    @Test
    public void testAuthenticationWithoutForwarding()
    {
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .addInterceptor(OkHttpUtil.basicAuth(TEST_USER, PASSWORD))
                .build();

        try (TestingTrinoClient client = new TestingTrinoClient(server, session, httpClient)) {
            assertThatThrownBy(() -> client.execute("SELECT 123"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Error 403 Forbidden");
        }
    }

    private static Interceptor httpsForwarded()
    {
        return chain -> {
            Request request = chain.request();
            return chain.proceed(request.newBuilder()
                    .url(request.url().newBuilder().scheme("http").build())
                    .addHeader("X-Forwarded-Proto", "https")
                    .build());
        };
    }
}
