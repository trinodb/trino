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
import com.google.inject.Key;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.spi.security.PrincipalType.USER;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestImpersonation
{
    private TestingTrinoServer server;
    private final TestingSystemSecurityMetadata securityMetadata = new TestingSystemSecurityMetadata();
    private TestingSystemSecurityMetadata systemSecurityMetadata;
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
        systemSecurityMetadata = (TestingSystemSecurityMetadata) server.getInstance(Key.get(SystemSecurityMetadata.class));
        accessControl = server.getAccessControl();
    }

    @Test
    @Timeout(1000)
    public void testImpersonate()
            throws Exception
    {
        systemSecurityMetadata.reset();

        accessControl.enableImpersonationControl();
        try (TrinoConnection connection = createConnection("memory", "default", "alice").unwrap(TrinoConnection.class);
                Statement statement = connection.createStatement()) {
            assertThat(getCurrentUser(connection)).isEqualTo("alice");
            systemSecurityMetadata.createRole(null, "alice_role", Optional.empty());
            systemSecurityMetadata.grantRoles(
                    null,
                    ImmutableSet.of("alice_role"),
                    ImmutableSet.of(new TrinoPrincipal(USER, "alice")),
                    false,
                    Optional.empty());
            accessControl.allowImpersonation("alice_role");
            assertThatThrownBy(() -> statement.execute("SET SESSION AUTHORIZATION john"))
                    .hasMessageContaining("User alice cannot impersonate user john");
            statement.execute("SET ROLE alice_role");
            statement.execute("SET SESSION AUTHORIZATION john");

            assertThatThrownBy(() -> statement.execute("SHOW SCHEMAS IN memory"))
                    .hasMessageContaining("User alice cannot impersonate user john");
        }
    }

    private Connection createConnection(String catalog, String schema, String user)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s/%s", server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, user, null);
    }

    private static String getCurrentUser(Connection connection)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT current_user")) {
            while (rs.next()) {
                return rs.getString(1);
            }
        }

        throw new RuntimeException("Failed to get CURRENT_USER");
    }
}
