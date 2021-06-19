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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logging;
import io.trino.client.ClientException;
import io.trino.client.auth.external.DesktopBrowserRedirectHandler;
import io.trino.client.auth.external.RedirectException;
import io.trino.client.auth.external.RedirectHandler;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.Authenticator;
import io.trino.server.security.ResourceSecurity;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.Identity;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;

import static com.google.common.io.Resources.getResource;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.jdbc.TestJdbcExternalAuthentication.RedirectHandlerFixture.withHandler;
import static io.trino.jdbc.TestJdbcExternalAuthentication.TokenPollingErrorFixture.withPollingError;
import static io.trino.jdbc.TestJdbcExternalAuthentication.WwwAuthenticateHeaderFixture.withWwwAuthenticate;
import static io.trino.jdbc.TrinoDriverUri.setRedirectHandler;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.server.security.ServerSecurityModule.authenticatorModule;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestJdbcExternalAuthentication
{
    private static final String TEST_CATALOG = "test_catalog";
    private TestingTrinoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();

        server = TestingTrinoServer.builder()
                .setAdditionalModule(new DummyExternalAuthModule(() -> server.getAddress().getPort()))
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.authentication.type", "dummy-external")
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", getResource("localhost.keystore").getPath())
                        .put("http-server.https.keystore.key", "changeit")
                        .put("web-ui.enabled", "false")
                        .build())
                .build();
        server.installPlugin(new TpchPlugin());
        server.createCatalog(TEST_CATALOG, "tpch");
        server.waitForNodeRefresh(Duration.ofSeconds(10));
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        closeAll(server);
        server = null;
    }

    @BeforeMethod(alwaysRun = true)
    public void clearUpLoggingSessions()
    {
        invalidateAllTokens();
    }

    @Test
    public void testSuccessfulAuthenticationWithHttpGetOnlyRedirectHandler()
            throws Exception
    {
        try (RedirectHandlerFixture ignore = withHandler(new HttpGetOnlyRedirectHandler());
                Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 123")).isTrue();
        }
    }

    /**
     * Ignored due to lack of ui environment with web-browser on CI servers.
     * Still this test is useful for local environments.
     */
    @Test(enabled = false)
    public void testSuccessfulAuthenticationWithDefaultBrowserRedirect()
            throws Exception
    {
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 123")).isTrue();
        }
    }

    @Test
    public void testAuthenticationFailsAfterUnfinishedRedirect()
            throws Exception
    {
        try (RedirectHandlerFixture ignore = withHandler(new NoOpRedirectHandler());
                Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("SELECT 123"))
                    .isInstanceOf(SQLException.class);
        }
    }

    @Test
    public void testAuthenticationFailsAfterRedirectException()
            throws Exception
    {
        try (RedirectHandlerFixture ignore = withHandler(new FailingRedirectHandler());
                Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("SELECT 123"))
                    .isInstanceOf(SQLException.class)
                    .hasCauseExactlyInstanceOf(RedirectException.class);
        }
    }

    @Test
    public void testAuthenticationFailsAfterServerAuthenticationFailure()
            throws Exception
    {
        try (RedirectHandlerFixture ignore = withHandler(new HttpGetOnlyRedirectHandler());
                AutoCloseable ignore2 = withPollingError("error occurred during token polling");
                Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("SELECT 123"))
                    .isInstanceOf(SQLException.class)
                    .hasMessage("error occurred during token polling");
        }
    }

    @Test
    public void testAuthenticationFailsAfterReceivingMalformedHeaderFromServer()
            throws Exception
    {
        try (RedirectHandlerFixture ignore = withHandler(new HttpGetOnlyRedirectHandler());
                AutoCloseable ignored = withWwwAuthenticate("Bearer no-valid-fields");
                Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("SELECT 123"))
                    .isInstanceOf(SQLException.class)
                    .hasCauseInstanceOf(ClientException.class)
                    .hasMessage("Authentication failed: Authentication required");
        }
    }

    @Test
    public void testAuthenticationReusesObtainedTokenPerConnection()
            throws Exception
    {
        try (RedirectHandlerFixture ignore = withHandler(new HttpGetOnlyRedirectHandler());
                Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SELECT 123");
            statement.execute("SELECT 123");
            statement.execute("SELECT 123");

            assertThat(countIssuedTokens()).isEqualTo(1);
        }
    }

    @Test
    public void testAuthenticationAfterInitialTokenHasBeenInvalidated()
            throws Exception
    {
        try (RedirectHandlerFixture ignore = withHandler(new HttpGetOnlyRedirectHandler());
                Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SELECT 123");

            invalidateAllTokens();
            assertThat(countIssuedTokens()).isEqualTo(0);

            assertThat(statement.execute("SELECT 123")).isTrue();
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:trino://localhost:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLTrustStorePath", getResource("localhost.truststore").getPath());
        properties.setProperty("SSLTrustStorePassword", "changeit");
        properties.setProperty("externalAuthentication", "true");
        properties.setProperty("externalAuthenticationTimeout", "2s");
        return DriverManager.getConnection(url, properties);
    }

    private static class DummyExternalAuthModule
            extends AbstractConfigurationAwareModule
    {
        private final IntSupplier port;

        public DummyExternalAuthModule(IntSupplier port)
        {
            this.port = requireNonNull(port, "port is null");
        }

        @Override
        protected void setup(Binder ignored)
        {
            install(authenticatorModule("dummy-external", DummyAuthenticator.class, binder -> {
                binder.bind(Authentications.class).in(SINGLETON);
                binder.bind(IntSupplier.class).toInstance(port);
                jaxrsBinder(binder).bind(DummyExternalAuthResources.class);
            }));
        }
    }

    private static class Authentications
    {
        private final Map<String, String> logginSessions = new ConcurrentHashMap<>();
        private final Set<String> validTokens = ConcurrentHashMap.newKeySet();

        public String startAuthentication()
        {
            String sessionId = UUID.randomUUID().toString();
            logginSessions.put(sessionId, "");
            return sessionId;
        }

        public void logIn(String sessionId)
        {
            String token = sessionId + "_token";
            validTokens.add(token);
            logginSessions.put(sessionId, token);
        }

        public Optional<String> getToken(String sessionId)
                throws IllegalArgumentException
        {
            return Optional.ofNullable(logginSessions.get(sessionId))
                    .filter(s -> !s.isEmpty());
        }

        public boolean verifyToken(String token)
        {
            return validTokens.contains(token);
        }

        public void invalidateAllTokens()
        {
            validTokens.clear();
        }

        public int countValidTokens()
        {
            return validTokens.size();
        }
    }

    private void invalidateAllTokens()
    {
        Authentications authentications = server.getInstance(Key.get(Authentications.class));
        authentications.invalidateAllTokens();
    }

    private int countIssuedTokens()
    {
        Authentications authentications = server.getInstance(Key.get(Authentications.class));
        return authentications.countValidTokens();
    }

    public static class DummyAuthenticator
            implements Authenticator
    {
        private final IntSupplier port;
        private final Authentications authentications;

        @Inject
        public DummyAuthenticator(IntSupplier port, Authentications authentications)
        {
            this.port = requireNonNull(port, "port is null");
            this.authentications = requireNonNull(authentications, "authentications is null");
        }

        @Override
        public Identity authenticate(ContainerRequestContext request)
                throws AuthenticationException
        {
            List<String> bearerHeaders = request.getHeaders().getOrDefault(AUTHORIZATION, ImmutableList.of());
            if (bearerHeaders.stream()
                    .filter(header -> header.startsWith("Bearer "))
                    .anyMatch(header -> authentications.verifyToken(header.substring("Bearer ".length())))) {
                return Identity.ofUser("user");
            }

            String sessionId = authentications.startAuthentication();

            throw Optional.ofNullable(WwwAuthenticateHeaderFixture.HEADER.get())
                    .map(header -> new AuthenticationException("Authentication required", header))
                    .orElseGet(() -> new AuthenticationException(
                            "Authentication required",
                            format("Bearer x_redirect_server=\"http://localhost:%s/v1/authentications/dummy/logins/%s\", " +
                                            "x_token_server=\"http://localhost:%s/v1/authentications/dummy/%s\"",
                                    port.getAsInt(), sessionId, port.getAsInt(), sessionId)));
        }
    }

    @Path("/v1/authentications/dummy")
    public static class DummyExternalAuthResources
    {
        private final Authentications authentications;

        @Inject
        public DummyExternalAuthResources(Authentications authentications)
        {
            this.authentications = authentications;
        }

        @GET
        @Produces(TEXT_PLAIN)
        @ResourceSecurity(PUBLIC)
        @Path("logins/{sessionId}")
        public String logInUser(@PathParam("sessionId") String sessionId)
        {
            authentications.logIn(sessionId);
            return "User has been successfully logged in during " + sessionId + " session";
        }

        @GET
        @ResourceSecurity(PUBLIC)
        @Path("{sessionId}")
        public Response getToken(@PathParam("sessionId") String sessionId, @Context HttpServletRequest request)
        {
            try {
                return Optional.ofNullable(TokenPollingErrorFixture.ERROR.get())
                        .map(error -> Response.ok(format("{ \"error\" : \"%s\"}", error), APPLICATION_JSON_TYPE).build())
                        .orElseGet(() -> authentications.getToken(sessionId)
                                .map(token -> Response.ok(format("{ \"token\" : \"%s\"}", token), APPLICATION_JSON_TYPE).build())
                                .orElseGet(() -> Response.ok(format("{ \"nextUri\" : \"%s\" }", request.getRequestURI()), APPLICATION_JSON_TYPE).build()));
            }
            catch (IllegalArgumentException ex) {
                return Response.status(NOT_FOUND).build();
            }
        }
    }

    public static class HttpGetOnlyRedirectHandler
            implements RedirectHandler
    {
        @Override
        public void redirectTo(URI uri)
                throws RedirectException
        {
            OkHttpClient client = new OkHttpClient();

            Request request = new Request.Builder()
                    .url(HttpUrl.get(uri.toString()))
                    .build();

            try (okhttp3.Response response = client.newCall(request).execute()) {
                if (response.code() != HTTP_OK) {
                    throw new RedirectException("HTTP GET failed with status " + response.code());
                }
            }
            catch (IOException e) {
                throw new RedirectException("Redirection failed", e);
            }
        }
    }

    public static class NoOpRedirectHandler
            implements RedirectHandler
    {
        @Override
        public void redirectTo(URI uri)
                throws RedirectException
        {}
    }

    public static class FailingRedirectHandler
            implements RedirectHandler
    {
        @Override
        public void redirectTo(URI uri)
                throws RedirectException
        {
            throw new RedirectException("Redirect to uri has failed " + uri);
        }
    }

    static class RedirectHandlerFixture
            implements AutoCloseable
    {
        private static final RedirectHandlerFixture INSTANCE = new RedirectHandlerFixture();

        private RedirectHandlerFixture() {}

        public static RedirectHandlerFixture withHandler(RedirectHandler handler)
        {
            setRedirectHandler(handler);
            return INSTANCE;
        }

        @Override
        public void close()
        {
            setRedirectHandler(new DesktopBrowserRedirectHandler());
        }
    }

    static class TokenPollingErrorFixture
            implements AutoCloseable
    {
        private static final AtomicReference<String> ERROR = new AtomicReference<>(null);

        public static AutoCloseable withPollingError(String error)
        {
            if (ERROR.compareAndSet(null, error)) {
                return new TokenPollingErrorFixture();
            }
            throw new ConcurrentModificationException("polling errors can't be invoked in parallel");
        }

        @Override
        public void close()
        {
            ERROR.set(null);
        }
    }

    static class WwwAuthenticateHeaderFixture
            implements AutoCloseable
    {
        private static final AtomicReference<String> HEADER = new AtomicReference<>(null);

        public static AutoCloseable withWwwAuthenticate(String header)
        {
            if (HEADER.compareAndSet(null, header)) {
                return new WwwAuthenticateHeaderFixture();
            }
            throw new ConcurrentModificationException("with WWW-Authenticate header can't be invoked in parallel");
        }

        @Override
        public void close()
        {
            HEADER.set(null);
        }
    }
}
