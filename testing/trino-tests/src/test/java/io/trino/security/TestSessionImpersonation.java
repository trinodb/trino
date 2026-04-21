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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.jdbc.TrinoConnection;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.server.BasicQueryInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.testing.TestingGroupProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static io.trino.jdbc.BaseTrinoDriverTest.getCurrentUser;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestSessionImpersonation
{
    private TestingTrinoServer server;

    @BeforeAll
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = TestingTrinoServer.builder()
                .setSystemAccessControl(new AllowAllSystemAccessControl())
                .build();
        server.installPlugin(new MemoryPlugin());
        server.createCatalog("memory", "memory");
    }

    @Test
    @Timeout(10)
    public void testSessionRepresentationReturnsCorrectGroupsDuringImpersonation()
    {
        Set<String> aliceGroups = ImmutableSet.of("alice_group");
        Set<String> johnGroups = ImmutableSet.of("john_group");
        Identity alice = Identity.forUser("alice").withGroups(aliceGroups).build();
        Identity john = Identity.forUser("john").withGroups(johnGroups).build();

        Session aliceImpersonationSession = testSessionBuilder()
                .setOriginalIdentity(alice)
                .setIdentity(john)
                .build();

        Set<String> originalUserGroups = aliceImpersonationSession.toSessionRepresentation()
                .getOriginalUserGroups();
        Set<String> userGroups = aliceImpersonationSession.toSessionRepresentation()
                .getGroups();
        assertThat(originalUserGroups).isEqualTo(aliceGroups);
        assertThat(userGroups).isEqualTo(johnGroups);
    }

    @Test
    @Timeout(60)
    public void testSessionReturnsCorrectGroupsForImpersonatedQueries()
            throws Exception
    {
        Set<String> johnGroups = ImmutableSet.of("john_group");
        Set<String> aliceGroups = ImmutableSet.of("alice_group");
        String alice = "alice";
        String john = "john";

        TestingGroupProvider testingGroupProvider = new TestingGroupProvider();
        testingGroupProvider.setUserGroups(ImmutableMap.of(
                john, johnGroups,
                alice, aliceGroups));
        server.getGroupProvider().setConfiguredGroupProvider(testingGroupProvider);

        try (TrinoConnection connection = createConnection("memory", "default", "alice").unwrap(TrinoConnection.class);
                Statement statement = connection.createStatement()) {
            assertThat(getCurrentUser(connection)).isEqualTo("alice");

            statement.execute("SET SESSION AUTHORIZATION john");

            String showCatalogsQuery = "SHOW CATALOGS";
            String showSchemasQuery = "SHOW SCHEMAS FROM memory";
            String showTablesQuery = "SHOW TABLES FROM memory.default";

            statement.execute(showCatalogsQuery);
            statement.execute(showSchemasQuery);
            statement.execute(showTablesQuery);

            BasicQueryInfo showCatalogsQueryInfo = getQueryInfo(showCatalogsQuery);
            BasicQueryInfo showSchemasQueryInfo = getQueryInfo(showSchemasQuery);
            BasicQueryInfo showTablesQueryInfo = getQueryInfo(showTablesQuery);

            assertSessionUsersAndGroups(showCatalogsQueryInfo, alice, aliceGroups, john, johnGroups);
            assertSessionUsersAndGroups(showSchemasQueryInfo, alice, aliceGroups, john, johnGroups);
            assertSessionUsersAndGroups(showTablesQueryInfo, alice, aliceGroups, john, johnGroups);
        }
    }

    private void assertSessionUsersAndGroups(
            BasicQueryInfo queryInfo,
            String expectedOriginalUser,
            Set<String> expectedOriginalUserGroups,
            String expectedUser,
            Set<String> expectedUserGroups)
    {
        assertThat(queryInfo.getSession().getOriginalUser()).isEqualTo(expectedOriginalUser);
        assertThat(queryInfo.getSession().getOriginalUserGroups()).isEqualTo(expectedOriginalUserGroups);
        assertThat(queryInfo.getSession().getUser()).isEqualTo(expectedUser);
        assertThat(queryInfo.getSession().getGroups()).isEqualTo(expectedUserGroups);
    }

    private BasicQueryInfo getQueryInfo(String query)
    {
        QueryId queryId = null;
        for (BasicQueryInfo basicQueryInfo : server.getDispatchManager().getQueries()) {
            if (basicQueryInfo.getQuery().equals(query)) {
                queryId = basicQueryInfo.getQueryId();
            }
        }
        return server.getDispatchManager().getQueryInfo(queryId);
    }

    private Connection createConnection(String catalog, String schema, String user)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s/%s", server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, user, null);
    }
}
