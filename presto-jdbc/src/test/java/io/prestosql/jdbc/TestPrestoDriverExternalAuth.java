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
package io.prestosql.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.client.auth.external.DesktopBrowserRedirectHandler;
import io.prestosql.client.auth.external.RedirectException;
import io.prestosql.client.auth.external.RedirectHandler;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.security.AuthenticationException;
import io.prestosql.server.security.Authenticator;
import io.prestosql.server.security.ResourceSecurity;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.security.Identity;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static com.google.common.base.Predicates.not;
import static com.google.common.io.Resources.getResource;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.prestosql.jdbc.RedirectHandlerFactory.setHandlerFactory;
import static io.prestosql.jdbc.TestPrestoDriver.waitForNodeRefresh;
import static io.prestosql.jdbc.TestPrestoDriverExternalAuth.Authentications.VALID_TOKEN;
import static io.prestosql.jdbc.TestPrestoDriverExternalAuth.RedirectHandlerFixture.withHandler;
import static io.prestosql.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.prestosql.server.security.ServerSecurityModule.authenticatorModule;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPrestoDriverExternalAuth
{
    private static final String TEST_CATALOG = "test_catalog";
    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();

        server = TestingPrestoServer.builder()
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
        waitForNodeRefresh(server);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        server.close();
    }

    @Test(singleThreaded = true)
    public void testSuccessfulAuthenticationWithHTTPGetOnlyRedirectHandler()
            throws Exception
    {
        try (RedirectHandlerFixture ignore = withHandler(HTTPGetOnlyRedirectHandler::new);
                Connection connection = createConnection(ImmutableMap.of(
                        "externalAuthentication", "true"));
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 123")).isTrue();
        }
    }

    /**
     * Ignored due to lack of ui environment with web-browser on CI servers.
     * Still this test is useful for local environments.
     */
    @Test(singleThreaded = true, enabled = false)
    public void testSuccessfulAuthenticationWithDefaultBrowserRedirect()
            throws Exception
    {
        try (Connection connection = createConnection(ImmutableMap.of("externalAuthentication", "true"));
                Statement statement = connection.createStatement()) {
            assertThat(statement.execute("SELECT 123")).isTrue();
        }
    }

    @Test(singleThreaded = true)
    public void testAuthenticationFailsAfterUnfinishedRedirect()
            throws Exception
    {
        try (RedirectHandlerFixture ignore = withHandler(NoOpRedirectHandler::new);
                Connection connection = createConnection(ImmutableMap.of(
                        "externalAuthentication", "true",
                        "externalAuthenticationTimeout", "2"));
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("SELECT 123"))
                    .isInstanceOf(SQLException.class);
        }
    }

    @Test(singleThreaded = true)
    public void testAuthenticationFailsAfterRedirectException()
            throws Exception
    {
        try (RedirectHandlerFixture ignore = withHandler(FailingRedirectHandler::new);
                Connection connection = createConnection(ImmutableMap.of(
                        "externalAuthentication", "true",
                        "externalAuthenticationTimeout", "2"));
                Statement statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("SELECT 123"))
                    .isInstanceOf(SQLException.class)
                    .hasCauseExactlyInstanceOf(RedirectException.class);
        }
    }

    private Connection createConnection(Map<String, String> additionalProperties)
            throws SQLException
    {
        String url = format("jdbc:presto://localhost:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLTrustStorePath", getResource("localhost.truststore").getPath());
        properties.setProperty("SSLTrustStorePassword", "changeit");
        additionalProperties.forEach(properties::setProperty);
        return DriverManager.getConnection(url, properties);
    }

    static class DummyExternalAuthModule
            extends AbstractConfigurationAwareModule
    {
        private final IntSupplier port;

        DummyExternalAuthModule(IntSupplier port)
        {
            this.port = port;
        }

        @Override
        protected void setup(Binder binder)
        {
            install(authenticatorModule("dummy-external", DummyAuthenticator.class, subBinder -> {
                subBinder.bind(Authentications.class).in(SINGLETON);
                subBinder.bind(IntSupplier.class).toInstance(port);
                jaxrsBinder(subBinder).bind(DummyExternalAuthResources.class);
            }));
        }
    }

    static class Authentications
    {
        public static final String VALID_TOKEN = "VALID_TOKEN";
        private final ConcurrentHashMap<String, String> logginSessions = new ConcurrentHashMap<>();

        String startAuthentication()
        {
            String sessionId = UUID.randomUUID().toString();
            logginSessions.put(sessionId, "");
            return sessionId;
        }

        public void logIn(String sessionId)
        {
            logginSessions.put(sessionId, VALID_TOKEN);
        }

        public Optional<String> getToken(String sessionId)
                throws IllegalArgumentException
        {
            if (logginSessions.containsKey(sessionId)) {
                return Optional.of(logginSessions.get(sessionId))
                        .filter(not(String::isEmpty));
            }
            throw new IllegalArgumentException("No login session exists for " + sessionId);
        }
    }

    static class DummyAuthenticator
            implements Authenticator
    {
        private IntSupplier port;
        private Authentications authentications;

        @Inject
        DummyAuthenticator(IntSupplier port, Authentications authentications)
        {
            this.port = requireNonNull(port, "port is null");
            this.authentications = requireNonNull(authentications, "authentications is null");
        }

        @Override
        public Identity authenticate(ContainerRequestContext request)
                throws AuthenticationException
        {
            List<String> bearerHeaders = request.getHeaders().get(AUTHORIZATION);
            if (bearerHeaders != null
                    && !bearerHeaders.isEmpty()
                    && bearerHeaders.get(0).startsWith("Bearer ")
                    && bearerHeaders.get(0).contains(VALID_TOKEN)) {
                return Identity.ofUser("user");
            }
            String sessionId = authentications.startAuthentication();
            throw new AuthenticationException(
                    "Authentication required",
                    /**
                     * - redirectUrl should take relative path as well.
                     * - presto server can communicate with IdP directly (to exchange tokens)
                     * - User's browser can talk to IdP and presto server
                     */
                    format("External-Bearer redirectUrl=\"http://localhost:%s/v1/authentications/dummy/logins/%s\", " +
                                    "tokenPath=\"/v1/authentications/dummy/%s\"",
                            port.getAsInt(), sessionId, sessionId));
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
        public Response getToken(@PathParam("sessionId") String sessionId)
        {
            try {
                return authentications.getToken(sessionId)
                        .map(token -> Response.ok(token, TEXT_PLAIN_TYPE))
                        .orElseGet(Response::accepted)
                        .build();
            }
            catch (IllegalArgumentException ex) {
                return Response.status(NOT_FOUND).build();
            }
        }
    }

    public static class HTTPGetOnlyRedirectHandler
            implements RedirectHandler
    {
        private final Logger log = Logger.get(HTTPGetOnlyRedirectHandler.class);

        @Override
        public void redirectTo(String uri)
                throws RedirectException
        {
            try {
                InputStream content = (InputStream) new URI(uri).toURL()
                        .openConnection()
                        .getContent();

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(content))) {
                    log.info("redirected to %s and obtained response %s",
                            uri,
                            reader.lines()
                                    .collect(joining("/n")));
                }
            }
            catch (IOException | URISyntaxException e) {
                throw new RedirectException("Redirection failed", e);
            }
        }
    }

    public static class NoOpRedirectHandler
            implements RedirectHandler
    {
        @Override
        public void redirectTo(String uri)
                throws RedirectException
        {
        }
    }

    public static class FailingRedirectHandler
            implements RedirectHandler
    {
        @Override
        public void redirectTo(String uri)
                throws RedirectException
        {
            throw new RedirectException("Redirect to uri has failed " + uri);
        }
    }

    static class RedirectHandlerFixture
            implements AutoCloseable
    {
        private static final RedirectHandlerFixture INSTANCE = new RedirectHandlerFixture();

        public static RedirectHandlerFixture withHandler(Supplier<RedirectHandler> handler)
        {
            setHandlerFactory(handler);
            return INSTANCE;
        }

        @Override
        public void close()
        {
            setHandlerFactory(DesktopBrowserRedirectHandler::new);
        }
    }
}
