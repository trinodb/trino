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
package io.trino.plugin.openpolicyagent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.openpolicyagent.FunctionalHelpers.Pair;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OpaAccessControlSystemTest
{
    private URI opaServerUri;
    private DistributedQueryRunner runner;

    private static final int OPA_PORT = 8181;
    @Container
    public static GenericContainer<?> opaContainer = new GenericContainer<>(DockerImageName.parse("openpolicyagent/opa:latest-rootless"))
            .withCommand("run", "--server", "--addr", ":%d".formatted(OPA_PORT))
            .withExposedPorts(OPA_PORT);

    private void ensureOpaUp()
            throws IOException, InterruptedException
    {
        assertTrue(opaContainer.isRunning());
        InetSocketAddress opaSocket = new InetSocketAddress(opaContainer.getHost(), opaContainer.getMappedPort(OPA_PORT));
        String opaEndpoint = String.format("%s:%d", opaSocket.getHostString(), opaSocket.getPort());
        awaitSocketOpen(opaSocket, 100, 200);
        this.opaServerUri = URI.create(String.format("http://%s/", opaEndpoint));
    }

    private void setupTrinoWithOpa(String basePolicyRelativeUri, Optional<String> batchPolicyRelativeUri)
            throws Exception
    {
        ensureOpaUp();
        ImmutableMap.Builder<String, String> opaConfigBuilder = ImmutableMap.builder();
        opaConfigBuilder.put("opa.policy.uri", opaServerUri.resolve(basePolicyRelativeUri).toString());
        batchPolicyRelativeUri.ifPresent(s -> opaConfigBuilder.put("opa.policy.batched-uri", opaServerUri.resolve(s).toString()));
        this.runner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .setSystemAccessControl(new OpaAccessControlFactory().create(opaConfigBuilder.buildOrThrow()))
                .setNodeCount(1)
                .build();
        runner.installPlugin(new BlackHolePlugin());
        runner.createCatalog("catalogOne", "blackhole");
        runner.createCatalog("catalogTwo", "blackhole");
    }

    private static void awaitSocketOpen(InetSocketAddress addr, int attempts, int timeoutMs)
            throws IOException, InterruptedException
    {
        for (int i = 0; i < attempts; ++i) {
            try (Socket socket = new Socket()) {
                socket.connect(addr, timeoutMs);
                return;
            }
            catch (SocketTimeoutException e) {
                // ignored
            }
            catch (IOException e) {
                Thread.sleep(timeoutMs);
            }
        }
        throw new SocketTimeoutException("Timed out waiting for addr %s to be available (%d attempts made with a %d ms wait)".formatted(addr, attempts, timeoutMs));
    }

    private static String stringOfLines(String... lines)
    {
        StringBuilder out = new StringBuilder();
        for (String line : lines) {
            out.append(line);
            out.append("\r\n");
        }
        return out.toString();
    }

    private void submitPolicy(String... policyLines)
            throws IOException, InterruptedException
    {
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpResponse<String> policyResponse =
                httpClient.send(
                        HttpRequest.newBuilder(opaServerUri.resolve("v1/policies/trino"))
                                .PUT(HttpRequest.BodyPublishers.ofString(stringOfLines(policyLines)))
                                .header("Content-Type", "text/plain").build(),
                        HttpResponse.BodyHandlers.ofString());
        assertEquals(policyResponse.statusCode(), 200, "Failed to submit policy: " + policyResponse.body());
    }

    private Session user(String user)
    {
        return testSessionBuilder().setIdentity(Identity.ofUser(user)).build();
    }

    private Set<String> querySetOfStrings(Session session, String query)
    {
        return runner.execute(session, query)
                .getMaterializedRows()
                .stream()
                .map(row -> row.getField(0).toString())
                .collect(toImmutableSet());
    }

    private static Stream<Arguments> filterSchemaTests()
    {
        Stream<Pair<String, Set<String>>> userAndExpectedCatalogs = Stream.of(
                Pair.of("bob", ImmutableSet.of("catalogOne")),
                Pair.of("admin", ImmutableSet.of("catalogOne", "catalogTwo", "system")));
        return userAndExpectedCatalogs.map(testCase -> Arguments.of(Named.of(testCase.getFirst(), testCase.getFirst()), testCase.getSecond()));
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @DisplayName("Unbatched Authorizer Tests")
    class UnbatchedAuthorizerTests
    {
        @BeforeAll
        public void setupTrino()
                throws Exception
        {
            setupTrinoWithOpa("v1/data/trino/allow", Optional.empty());
        }

        @AfterAll
        public void teardown()
        {
            if (runner != null) {
                runner.close();
            }
        }

        @ParameterizedTest(name = "{index}: {0}")
        @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlSystemTest#filterSchemaTests")
        public void testAllowsQueryAndFilters(String userName, Set<String> expectedCatalogs)
                throws IOException, InterruptedException
        {
            submitPolicy("""
                    package trino
                    import future.keywords.in
                    import future.keywords.if

                    default allow = false
                    allow {
                      is_bob
                      can_be_accessed_by_bob
                    }
                    allow if is_admin

                    is_admin {
                      input.context.identity.user == "admin"
                    }
                    is_bob {
                      input.context.identity.user == "bob"
                    }
                    can_be_accessed_by_bob {
                      input.action.operation in ["ImpersonateUser", "ExecuteQuery"]
                    }
                    can_be_accessed_by_bob {
                      input.action.operation in ["FilterCatalogs", "AccessCatalog"]
                      input.action.resource.catalog.name == "catalogOne"
                    }
                    """);
            Set<String> catalogs = querySetOfStrings(user(userName), "SHOW CATALOGS");
            assertEquals(expectedCatalogs, catalogs);
        }

        @Test
        public void testShouldDenyQueryIfDirected()
                throws IOException, InterruptedException
        {
            submitPolicy("""
                    package trino
                    import future.keywords.in
                    default allow = false

                    allow {
                        input.context.identity.user in ["someone", "admin"]
                    }
                    """);
            RuntimeException error = assertThrows(RuntimeException.class, () -> {
                runner.execute(user("bob"), "SHOW CATALOGS");
            });
            assertTrue(error.getMessage().contains("Access Denied"),
                    "Error must mention 'Access Denied': " + error.getMessage());
            // smoke test: we can still query if we are the right user
            runner.execute(user("admin"), "SHOW CATALOGS");
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @DisplayName("Batched Authorizer Tests")
    class BatchedAuthorizerTests
    {
        @BeforeAll
        public void setupTrino()
                throws Exception
        {
            setupTrinoWithOpa("v1/data/trino/allow", Optional.of("v1/data/trino/batchAllow"));
        }

        @AfterAll
        public void teardown()
        {
            if (runner != null) {
                runner.close();
            }
        }

        @ParameterizedTest(name = "{index}: {0}")
        @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlSystemTest#filterSchemaTests")
        public void testFilterOutItemsBatch(String userName, Set<String> expectedCatalogs)
                throws IOException, InterruptedException
        {
            submitPolicy("""
                    package trino
                    import future.keywords.in
                    import future.keywords.if
                    default allow = false

                    allow if is_admin

                    allow {
                        is_bob
                        input.action.operation in ["AccessCatalog", "ExecuteQuery", "ImpersonateUser", "ShowSchemas", "SelectFromColumns"]
                    }

                    is_bob {
                        input.context.identity.user == "bob"
                    }

                    is_admin {
                        input.context.identity.user == "admin"
                    }

                    batchAllow[i] {
                        some i
                        is_bob
                        input.action.operation == "FilterCatalogs"
                        input.action.filterResources[i].catalog.name == "catalogOne"
                    }

                    batchAllow[i] {
                        some i
                        input.action.filterResources[i]
                        is_admin
                    }
                    """);
            Set<String> catalogs = querySetOfStrings(user(userName), "SHOW CATALOGS");
            assertEquals(expectedCatalogs, catalogs);
        }

        @Test
        public void testDenyUnbatchedQuery()
                throws IOException, InterruptedException
        {
            submitPolicy("""
                    package trino
                    import future.keywords.in
                    default allow = false
                    """);
            RuntimeException error = assertThrows(RuntimeException.class, () -> {
                runner.execute(user("bob"), "SELECT version()");
            });
            assertTrue(error.getMessage().contains("Access Denied"),
                    "Error must mention 'Access Denied': " + error.getMessage());
        }

        @Test
        public void testAllowUnbatchedQuery()
                throws IOException, InterruptedException
        {
            submitPolicy("""
                    package trino
                    import future.keywords.in
                    default allow = false
                    allow {
                        input.context.identity.user == "bob"
                        input.action.operation in ["ImpersonateUser", "ExecuteFunction", "AccessCatalog", "ExecuteQuery"]
                    }
                    """);
            Set<String> version = querySetOfStrings(user("bob"), "SELECT version()");
            assertFalse(version.isEmpty());
        }
    }
}
