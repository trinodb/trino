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
package io.trino.plugin.opa;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
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

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Testcontainers
@TestInstance(PER_CLASS)
public class TestOpaAccessControlSystem
{
    private URI opaServerUri;
    private DistributedQueryRunner runner;

    private static final int OPA_PORT = 8181;
    @Container
    private static final GenericContainer<?> OPA_CONTAINER = new GenericContainer<>(DockerImageName.parse("openpolicyagent/opa:latest-rootless"))
            .withCommand("run", "--server", "--addr", ":%d".formatted(OPA_PORT))
            .withExposedPorts(OPA_PORT);

    @Nested
    @TestInstance(PER_CLASS)
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

        @Test
        public void testAllowsQueryAndFilters()
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
                      input.action.resource.catalog.name == "catalog_one"
                    }
                    """);
            Set<String> catalogsForBob = querySetOfStrings(user("bob"), "SHOW CATALOGS");
            assertThat(catalogsForBob).containsExactlyInAnyOrder("catalog_one");
            Set<String> catalogsForAdmin = querySetOfStrings(user("admin"), "SHOW CATALOGS");
            assertThat(catalogsForAdmin).containsExactlyInAnyOrder("catalog_one", "catalog_two", "system");
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
            assertThatThrownBy(() -> runner.execute(user("bob"), "SHOW CATALOGS"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Access Denied");
            // smoke test: we can still query if we are the right user
            runner.execute(user("admin"), "SHOW CATALOGS");
        }
    }

    @Nested
    @TestInstance(PER_CLASS)
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

        @Test
        public void testFilterOutItemsBatch()
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
                        input.action.filterResources[i].catalog.name == "catalog_one"
                    }

                    batchAllow[i] {
                        some i
                        input.action.filterResources[i]
                        is_admin
                    }
                    """);
            Set<String> catalogsForBob = querySetOfStrings(user("bob"), "SHOW CATALOGS");
            assertThat(catalogsForBob).containsExactlyInAnyOrder("catalog_one");
            Set<String> catalogsForAdmin = querySetOfStrings(user("admin"), "SHOW CATALOGS");
            assertThat(catalogsForAdmin).containsExactlyInAnyOrder("catalog_one", "catalog_two", "system");
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
            assertThatThrownBy(() -> runner.execute(user("bob"), "SELECT version()"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Access Denied");
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
            assertThat(version).isNotEmpty();
        }
    }

    private void ensureOpaUp()
            throws IOException, InterruptedException
    {
        assertThat(OPA_CONTAINER.isRunning()).isTrue();
        InetSocketAddress opaSocket = new InetSocketAddress(OPA_CONTAINER.getHost(), OPA_CONTAINER.getMappedPort(OPA_PORT));
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
        batchPolicyRelativeUri.ifPresent(relativeUri -> opaConfigBuilder.put("opa.policy.batched-uri", opaServerUri.resolve(relativeUri).toString()));
        this.runner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .setSystemAccessControl(new OpaAccessControlFactory().create(opaConfigBuilder.buildOrThrow()))
                .setNodeCount(1)
                .build();
        runner.installPlugin(new BlackHolePlugin());
        runner.createCatalog("catalog_one", "blackhole");
        runner.createCatalog("catalog_two", "blackhole");
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

    private void submitPolicy(String policyString)
            throws IOException, InterruptedException
    {
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpResponse<String> policyResponse =
                httpClient.send(
                        HttpRequest.newBuilder(opaServerUri.resolve("v1/policies/trino"))
                                .PUT(HttpRequest.BodyPublishers.ofString(policyString))
                                .header("Content-Type", "text/plain").build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(policyResponse.statusCode()).withFailMessage("Failed to submit policy: %s", policyResponse.body()).isEqualTo(200);
    }

    private Set<String> querySetOfStrings(Session session, String query)
    {
        return runner.execute(session, query).getMaterializedRows().stream().map(row -> row.getField(0).toString()).collect(toImmutableSet());
    }

    private static Session user(String user)
    {
        return testSessionBuilder().setIdentity(Identity.ofUser(user)).build();
    }
}
