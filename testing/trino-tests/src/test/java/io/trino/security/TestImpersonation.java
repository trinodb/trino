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
package io.trino.security;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logging;
import io.trino.jdbc.TrinoConnection;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.TestingAccessControlManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.jdbc.BaseTrinoDriverTest.getCurrentUser;
import static io.trino.spi.security.PrincipalType.USER;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestImpersonation
{
    private TestingTrinoServer server;
    private final TestingSystemSecurityMetadata securityMetadata = new TestingSystemSecurityMetadata();
    private TestingAccessControlManager accessControl;

    @BeforeAll
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = TestingTrinoServer.builder()
                .setAdditionalModule(binder -> {
                    newOptionalBinder(binder, SystemSecurityMetadata.class)
                            .setBinding()
                            .toInstance(securityMetadata);
                }).build();
        server.installPlugin(new MemoryPlugin());
        server.createCatalog("memory", "memory");
        accessControl = server.getAccessControl();
    }

    @ParameterizedTest
    @MethodSource("roles")
    @Timeout(10)
    public void testImpersonationAllowedByRole(String roleName)
            throws Exception
    {
        securityMetadata.reset();
        accessControl.reset();
        try (TrinoConnection connection = createConnection("memory", "default", "alice").unwrap(TrinoConnection.class);
                Statement statement = connection.createStatement()) {
            assertThat(getCurrentUser(connection)).isEqualTo("alice");
            securityMetadata.createRole(null, "invalid_role", Optional.empty());
            securityMetadata.grantRoles(
                    null,
                    ImmutableSet.of("invalid_role"),
                    ImmutableSet.of(new TrinoPrincipal(USER, "alice")),
                    false,
                    Optional.empty());
            denyImpersonation();
            statement.execute("SET ROLE invalid_role");
            assertThatThrownBy(() -> statement.execute("SET SESSION AUTHORIZATION john"))
                    .hasMessageContaining("User alice cannot impersonate user john");

            securityMetadata.createRole(null, "alice_role", Optional.empty());
            securityMetadata.grantRoles(
                    null,
                    ImmutableSet.of("alice_role"),
                    ImmutableSet.of(new TrinoPrincipal(USER, "alice")),
                    false,
                    Optional.empty());
            assertThatThrownBy(() -> statement.execute("SET SESSION AUTHORIZATION john"))
                    .hasMessageContaining("User alice cannot impersonate user john");

            statement.execute("SET ROLE " + roleName);
            statement.execute("SET SESSION AUTHORIZATION john");

            // This would fail if roles were not correctly propagated
            statement.execute("SHOW SCHEMAS IN memory");
            // Call more than once to make sure everything is propagated correctly
            // to subsequent calls
            statement.execute("SHOW SCHEMAS IN memory");
        }
    }

    @Test
    @Timeout(10)
    public void testImpersonationDisallowedWhenRoleIsNone()
            throws Exception
    {
        securityMetadata.reset();
        accessControl.reset();
        try (TrinoConnection connection = createConnection("memory", "default", "alice").unwrap(TrinoConnection.class);
                Statement statement = connection.createStatement()) {
            assertThat(getCurrentUser(connection)).isEqualTo("alice");
            securityMetadata.createRole(null, "alice_role", Optional.empty());
            denyImpersonation();
            securityMetadata.grantRoles(
                    null,
                    ImmutableSet.of("alice_role"),
                    ImmutableSet.of(new TrinoPrincipal(USER, "alice")),
                    false,
                    Optional.empty());
            statement.execute("SET ROLE NONE");

            assertThatThrownBy(() -> statement.execute("SET SESSION AUTHORIZATION john"))
                    .hasMessageContaining("User alice cannot impersonate user john");
        }
    }

    private Connection createConnection(String catalog, String schema, String user)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s/%s", server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, user, null);
    }

    private Stream<String> roles()
    {
        return Stream.of("alice_role", "ALL");
    }

    private void denyImpersonation()
    {
        accessControl.denyImpersonation((identity, _) ->
                identity.getEnabledRoles()
                        .stream()
                        .anyMatch(role -> role.equalsIgnoreCase("alice_role")));
    }
}
