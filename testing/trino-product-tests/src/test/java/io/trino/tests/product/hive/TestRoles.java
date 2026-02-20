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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.ProductTestEnvironment.TrinoSession;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Hive role management.
 */
@ProductTest
@RequiresEnvironment(HiveKerberosImpersonationEnvironment.class)
@TestGroup.Authorization
class TestRoles
{
    private static final String ROLE1 = "role1";
    private static final String ROLE2 = "role2";
    private static final String ROLE3 = "role3";
    private static final Set<String> TEST_ROLES = ImmutableSet.of(ROLE1, ROLE2, ROLE3);

    // The default JDBC user for Kerberos Hive environments
    private static final String DEFAULT_USER = "hdfs";

    @BeforeEach
    void setUp(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("SET ROLE admin IN hive");
        cleanup(env);
    }

    @AfterEach
    void tearDown(HiveKerberosImpersonationEnvironment env)
    {
        cleanup(env);
    }

    private void cleanup(HiveKerberosImpersonationEnvironment env)
    {
        Set<String> existentRoles = listRoles(env);
        for (String role : TEST_ROLES) {
            if (existentRoles.contains(role)) {
                env.executeTrinoUpdate(format("DROP ROLE %s IN hive", role));
            }
        }
        revokeAdminRoleIfGranted(env, "alice");
        revokeAdminRoleIfGranted(env, "bob");
        revokeAdminRoleIfGranted(env, "charlie");
    }

    private void revokeAdminRoleIfGranted(HiveKerberosImpersonationEnvironment env, String user)
    {
        try {
            env.executeTrinoUpdate(format("REVOKE admin FROM %s IN hive", user));
        }
        catch (RuntimeException ignored) {
            // Ignore "not granted" cleanup path to keep tests isolated across random order.
        }
    }

    private Set<String> listRoles(HiveKerberosImpersonationEnvironment env)
    {
        return env.executeTrino("SELECT * FROM hive.information_schema.roles").rows().stream()
                .map(row -> row.get(0))
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    @Test
    void testCreateRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate(format("CREATE ROLE %s IN hive", ROLE1));
        assertThat(listRoles(env)).contains(ROLE1);
    }

    @Test
    void testDropRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate(format("CREATE ROLE %s IN hive", ROLE1));
        assertThat(listRoles(env)).contains(ROLE1);
        env.executeTrinoUpdate(format("DROP ROLE %s IN hive", ROLE1));
        assertThat(listRoles(env)).doesNotContain(ROLE1);
    }

    @Test
    void testListRoles(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate(format("CREATE ROLE %s IN hive", ROLE1));
        QueryResult expected = env.executeHive("SHOW ROLES");
        QueryResult actual = env.executeTrino("SELECT * FROM hive.information_schema.roles");
        assertThat(actual).containsOnly(expected.getRows().toArray(Row[]::new));
    }

    @Test
    void testCreateDuplicateRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate(format("CREATE ROLE %s IN hive", ROLE1));
        assertThatThrownBy(() -> env.executeTrinoUpdate(format("CREATE ROLE %s IN hive", ROLE1)))
                .hasStackTraceContaining(format("Role '%s' already exists", ROLE1));
    }

    @Test
    void testDropNonExistentRole(HiveKerberosImpersonationEnvironment env)
    {
        assertThatThrownBy(() -> env.executeTrinoUpdate(format("DROP ROLE %s IN hive", ROLE3)))
                .hasStackTraceContaining(format("Role '%s' does not exist", ROLE3));
    }

    @Test
    void testCreateDropRoleAccessControl(HiveKerberosImpersonationEnvironment env)
    {
        // Only users that are granted with "admin" role can create, drop and list roles
        // Alice is not granted with "admin" role
        assertThatThrownBy(() -> env.executeTrinoUpdate(format("CREATE ROLE %s IN hive", ROLE3), "alice"))
                .hasStackTraceContaining(format("Cannot create role %s", ROLE3));
        assertThatThrownBy(() -> env.executeTrinoUpdate(format("DROP ROLE %s IN hive", ROLE3), "alice"))
                .hasStackTraceContaining(format("Cannot drop role %s", ROLE3));
        assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.information_schema.roles", "alice"))
                .hasStackTraceContaining("Cannot select from table information_schema.roles");
    }

    @Test
    void testPublicRoleIsGrantedToEveryone(HiveKerberosImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .contains(row("alice", "USER", "public", "NO"));
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "bob"))
                .contains(row("bob", "USER", "public", "NO"));
    }

    @Test
    void testAdminRoleIsGrantedToHdfs(HiveKerberosImpersonationEnvironment env)
    {
        QueryResult result = env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles");
        // Admin role is granted to hdfs, with either YES or NO for is_grantable
        List<List<Object>> rows = result.rows();
        boolean hasAdminRole = rows.stream()
                .anyMatch(row -> DEFAULT_USER.equals(row.get(0))
                        && "USER".equals(row.get(1))
                        && "admin".equals(row.get(2)));
        assertThat(hasAdminRole).as("hdfs user should have admin role").isTrue();
    }

    @Test
    void testGrantRoleToUser(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test
    void testGrantRoleToRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));
    }

    @Test
    void testGrantRoleWithAdminOption(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));
    }

    @Test
    void testGrantRoleMultipleTimes(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));
    }

    @Test
    void testRevokeRoleFromUser(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));

        env.executeTrinoUpdate("REVOKE role1 FROM USER alice IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test
    void testRevokeRoleFromRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        env.executeTrinoUpdate("REVOKE role2 FROM ROLE role1 IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test
    void testRevokeRoleFromOwner(HiveKerberosImpersonationEnvironment env)
    {
        try {
            env.executeTrinoUpdate("CREATE TABLE hive.default.test_table (foo BIGINT)", "alice");
            assertThat(env.executeTrino("SHOW GRANTS ON hive.default.test_table", "alice"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "INSERT", "YES", null)));

            env.executeTrinoUpdate("REVOKE SELECT ON hive.default.test_table FROM USER alice");

            // now there should be no SELECT privileges shown even though alice has OWNERSHIP
            assertThat(env.executeTrino("SHOW GRANTS ON hive.default.test_table", "alice"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "INSERT", "YES", null)));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.test_table", "alice");
        }
    }

    @Test
    void testDropGrantedRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));

        env.executeTrinoUpdate("DROP ROLE role1 IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test
    void testRevokeTransitiveRoleFromUser(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        env.executeTrinoUpdate("REVOKE role1 FROM USER alice IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test
    void testRevokeTransitiveRoleFromRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role3 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 IN hive");
        env.executeTrinoUpdate("GRANT role3 TO ROLE role2 IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"),
                        row("role2", "ROLE", "role3", "NO"));

        env.executeTrinoUpdate("REVOKE role2 FROM ROLE role1 IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test
    void testDropTransitiveRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role3 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 IN hive");
        env.executeTrinoUpdate("GRANT role3 TO ROLE role2 IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"),
                        row("role2", "ROLE", "role3", "NO"));

        env.executeTrinoUpdate("DROP ROLE role2 IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test
    void testRevokeAdminOption(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));

        env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");
        env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1 IN hive");

        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));
    }

    @Test
    void testRevokeMultipleTimes(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));

        env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");
        env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");
        env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1 IN hive");
        env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1 IN hive");

        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        env.executeTrinoUpdate("REVOKE role1 FROM USER alice IN hive");
        env.executeTrinoUpdate("REVOKE role1 FROM USER alice IN hive");
        env.executeTrinoUpdate("REVOKE role2 FROM ROLE role1 IN hive");
        env.executeTrinoUpdate("REVOKE role2 FROM ROLE role1 IN hive");

        assertThat(env.executeTrino("SELECT * FROM hive.information_schema.applicable_roles", "alice"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test
    void testGrantRevokeRoleAccessControl(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");

        assertThatThrownBy(() -> env.executeTrinoUpdate("GRANT role1 TO USER bob IN hive", "alice"))
                .hasStackTraceContaining("Cannot grant roles [role1] to [USER bob]");
        assertThatThrownBy(() -> env.executeTrinoUpdate("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive", "alice"))
                .hasStackTraceContaining("Cannot grant roles [role1] to [USER bob]");
        assertThatThrownBy(() -> env.executeTrinoUpdate("REVOKE role1 FROM USER bob IN hive", "alice"))
                .hasStackTraceContaining("Cannot revoke roles [role1] from [USER bob]");
        assertThatThrownBy(() -> env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive", "alice"))
                .hasStackTraceContaining("Cannot revoke roles [role1] from [USER bob]");

        env.executeTrinoUpdate("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");

        env.executeTrinoUpdate("GRANT role1 TO USER bob IN hive", "alice");
        env.executeTrinoUpdate("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive", "alice");
        env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive", "alice");
        env.executeTrinoUpdate("REVOKE role1 FROM USER bob IN hive", "alice");

        env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");

        assertThatThrownBy(() -> env.executeTrinoUpdate("GRANT role1 TO USER bob IN hive", "alice"))
                .hasStackTraceContaining("Cannot grant roles [role1] to [USER bob]");
        assertThatThrownBy(() -> env.executeTrinoUpdate("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive", "alice"))
                .hasStackTraceContaining("Cannot grant roles [role1] to [USER bob]");
        assertThatThrownBy(() -> env.executeTrinoUpdate("REVOKE role1 FROM USER bob IN hive", "alice"))
                .hasStackTraceContaining("Cannot revoke roles [role1] from [USER bob]");
        assertThatThrownBy(() -> env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive", "alice"))
                .hasStackTraceContaining("Cannot revoke roles [role1] from [USER bob]");

        env.executeTrinoUpdate("GRANT role2 TO USER alice IN hive");
        env.executeTrinoUpdate("GRANT role1 TO ROLE role2 WITH ADMIN OPTION IN hive");

        env.executeTrinoUpdate("GRANT role1 TO USER bob IN hive", "alice");
        env.executeTrinoUpdate("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive", "alice");
        env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive", "alice");
        env.executeTrinoUpdate("REVOKE role1 FROM USER bob IN hive", "alice");

        env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role1 FROM ROLE role2 IN hive", "alice");

        assertThatThrownBy(() -> env.executeTrinoUpdate("GRANT role1 TO USER bob IN hive", "alice"))
                .hasStackTraceContaining("Cannot grant roles [role1] to [USER bob]");
        assertThatThrownBy(() -> env.executeTrinoUpdate("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive", "alice"))
                .hasStackTraceContaining("Cannot grant roles [role1] to [USER bob]");
        assertThatThrownBy(() -> env.executeTrinoUpdate("REVOKE role1 FROM USER bob IN hive", "alice"))
                .hasStackTraceContaining("Cannot revoke roles [role1] from [USER bob]");
        assertThatThrownBy(() -> env.executeTrinoUpdate("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive", "alice"))
                .hasStackTraceContaining("Cannot revoke roles [role1] from [USER bob]");
    }

    @Test
    void testSetRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role3 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER alice IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 IN hive");
        env.executeTrinoUpdate("GRANT role3 TO ROLE role2 IN hive");

        env.executeTrinoInSession("alice", session -> {
            session.executeUpdate("SET ROLE ALL IN hive");
            assertThat(session.executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                    .containsOnly(
                            row("public"),
                            row("role1"),
                            row("role2"),
                            row("role3"));

            session.executeUpdate("SET ROLE NONE IN hive");
            assertThat(session.executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                    .containsOnly(
                            row("public"));

            session.executeUpdate("SET ROLE role1 IN hive");
            assertThat(session.executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                    .containsOnly(
                            row("public"),
                            row("role1"),
                            row("role2"),
                            row("role3"));

            session.executeUpdate("SET ROLE role2 IN hive");
            assertThat(session.executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                    .containsOnly(
                            row("public"),
                            row("role2"),
                            row("role3"));

            session.executeUpdate("SET ROLE role3 IN hive");
            assertThat(session.executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                    .containsOnly(
                            row("public"),
                            row("role3"));
        });
    }

    @Test
    void testSetAdminRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET ROLE NONE IN hive");
            assertThat(session.executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                    .containsOnly(
                            row("public"));
            session.executeUpdate("SET ROLE admin IN hive");
            assertThat(session.executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                    .containsOnly(
                            row("public"),
                            row("admin"));
        });
    }

    @Test
    void testShowRoles(HiveKerberosImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SHOW ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        assertThat(env.executeTrino("SHOW ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"),
                        row("role1"));
        assertThatThrownBy(() -> env.executeTrino("SHOW ROLES FROM hive", "alice"))
                .hasStackTraceContaining("Cannot show roles");
        env.executeTrinoUpdate("GRANT admin TO alice IN hive");

        env.executeTrinoInSession("alice", session -> {
            session.executeUpdate("SET ROLE admin IN hive");
            assertThat(session.executeQuery("SHOW ROLES FROM hive"))
                    .containsOnly(
                            row("public"),
                            row("admin"),
                            row("role1"));
        });
    }

    @Test
    void testShowCurrentRoles(HiveKerberosImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CURRENT ROLES FROM hive", "alice"))
                .containsOnly(
                        row("public"));
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO alice IN hive");
        env.executeTrinoUpdate("GRANT role2 TO alice IN hive");
        assertThat(env.executeTrino("SHOW CURRENT ROLES FROM hive", "alice"))
                .containsOnly(
                        row("public"),
                        row("role1"),
                        row("role2"));

        env.executeTrinoInSession("alice", session -> {
            session.executeUpdate("SET ROLE role2 IN hive");
            assertThat(session.executeQuery("SHOW CURRENT ROLES FROM hive"))
                    .containsOnly(
                            row("public"),
                            row("role2"));
        });
    }

    @Test
    void testShowRoleGrants(HiveKerberosImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SHOW ROLE GRANTS FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        assertThat(env.executeTrino("SHOW ROLE GRANTS FROM hive", "alice"))
                .containsOnly(
                        row("public"));
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO alice IN hive");
        env.executeTrinoUpdate("GRANT role2 TO role1 IN hive");
        assertThat(env.executeTrino("SHOW ROLE GRANTS FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        assertThat(env.executeTrino("SHOW ROLE GRANTS FROM hive", "alice"))
                .containsOnly(
                        row("public"),
                        row("role1"));
    }

    @Test
    void testSetRoleCreateDropSchema(HiveKerberosImpersonationEnvironment env)
    {
        assertAdminExecute(env, "CREATE SCHEMA hive.test_admin_schema");
        env.executeTrinoUpdate("DROP SCHEMA hive.test_admin_schema");
    }

    @Test
    void testAdminCanDropAnyTable(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE TABLE hive.default.test_table (foo BIGINT)", "alice");
        assertAdminExecute(env, "DROP TABLE hive.default.test_table");
    }

    @Test
    void testAdminCanRenameAnyTable(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE TABLE hive.default.test_table (foo BIGINT)", "alice");
        assertAdminExecute(env, "ALTER TABLE hive.default.test_table RENAME TO hive.default.test_table_1");
        env.executeTrinoUpdate("DROP TABLE hive.default.test_table_1", "alice");
    }

    @Test
    void testAdminCanAddColumnToAnyTable(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE TABLE hive.default.test_table (foo BIGINT)", "alice");
        assertAdminExecute(env, "ALTER TABLE hive.default.test_table ADD COLUMN bar DATE");
        env.executeTrinoUpdate("DROP TABLE hive.default.test_table", "alice");
    }

    @Test
    void testAdminCanRenameColumnInAnyTable(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE TABLE hive.default.test_table (foo BIGINT)", "alice");
        assertAdminExecute(env, "ALTER TABLE hive.default.test_table RENAME COLUMN foo TO bar");
        env.executeTrinoUpdate("DROP TABLE hive.default.test_table", "alice");
    }

    @Test
    void testAdminCanShowAllGrants(HiveKerberosImpersonationEnvironment env)
    {
        try {
            env.executeTrinoUpdate("CREATE TABLE hive.default.test_table_bob (foo BIGINT)", "bob");
            env.executeTrinoUpdate("CREATE TABLE hive.default.test_table_alice (foo BIGINT)", "alice");
            env.executeTrinoUpdate("GRANT admin TO alice IN hive");

            env.executeTrinoInSession("alice", session -> {
                session.executeUpdate("SET ROLE ADMIN IN hive");

                assertThat(session.executeQuery("SHOW GRANTS ON hive.default.test_table_alice"))
                        .containsOnly(ImmutableList.of(
                                row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "DELETE", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "UPDATE", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "INSERT", "YES", null)));

                assertThat(session.executeQuery("SHOW GRANTS ON hive.default.test_table_bob"))
                        .containsOnly(ImmutableList.of(
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "SELECT", "YES", null),
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "DELETE", "YES", null),
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "UPDATE", "YES", null),
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "INSERT", "YES", null)));

                session.executeUpdate("GRANT SELECT ON hive.default.test_table_alice  TO bob WITH GRANT OPTION");
                session.executeUpdate("GRANT INSERT ON hive.default.test_table_alice  TO bob");

                assertThat(session.executeQuery("SHOW GRANTS ON hive.default.test_table_alice"))
                        .containsOnly(ImmutableList.of(
                                row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "DELETE", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "UPDATE", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "INSERT", "YES", null),
                                row("alice", "USER", "bob", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                                row("alice", "USER", "bob", "USER", "hive", "default", "test_table_alice", "INSERT", "NO", null)));
            });
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.test_table_alice", "alice");
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.test_table_bob", "bob");
            env.executeTrinoUpdate("REVOKE admin FROM alice IN hive");
        }
    }

    @Test
    void testAdminCanShowGrantsOnlyFromCurrentSchema(HiveKerberosImpersonationEnvironment env)
    {
        try {
            env.executeTrinoUpdate("CREATE TABLE hive.default.test_table_bob (foo BIGINT)", "bob");
            env.executeTrinoUpdate("CREATE SCHEMA hive.test");
            env.executeTrinoUpdate("GRANT admin TO alice IN hive");

            env.executeTrinoInSession("alice", session -> {
                session.executeUpdate("SET ROLE ADMIN IN hive");
                session.executeUpdate("CREATE TABLE hive.test.test_table_bob (foo BIGINT) with (external_location='/tmp')");

                assertThat(session.executeQuery("SHOW GRANTS ON hive.default.test_table_bob"))
                        .containsOnly(ImmutableList.of(
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "SELECT", "YES", null),
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "DELETE", "YES", null),
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "UPDATE", "YES", null),
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "INSERT", "YES", null)));

                assertThat(session.executeQuery("SHOW GRANTS ON hive.test.test_table_bob"))
                        .containsOnly(ImmutableList.of(
                                row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "SELECT", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "DELETE", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "UPDATE", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "INSERT", "YES", null)));

                assertThat(session.executeQuery("SELECT * FROM hive.information_schema.table_privileges where table_name = 'test_table_bob'"))
                        .containsOnly(ImmutableList.of(
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "SELECT", "YES", null),
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "DELETE", "YES", null),
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "UPDATE", "YES", null),
                                row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "INSERT", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "SELECT", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "DELETE", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "UPDATE", "YES", null),
                                row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "INSERT", "YES", null)));
            });
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.test_table_bob", "bob");
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.test.test_table_bob", "alice");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS hive.test");
            env.executeTrinoUpdate("REVOKE admin FROM alice IN hive");
        }
    }

    @Test
    void testSetRoleTablePermissions(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");

        env.executeTrinoUpdate("GRANT role1 TO USER bob IN hive");
        env.executeTrinoUpdate("GRANT role2 TO USER bob IN hive");

        env.executeTrinoUpdate("CREATE TABLE hive.default.test_table (foo BIGINT)", "alice");
        env.executeTrinoUpdate("GRANT SELECT ON hive.default.test_table TO ROLE role1", "alice");
        env.executeTrinoUpdate("GRANT INSERT ON hive.default.test_table TO ROLE role2", "alice");

        String select = "SELECT * FROM hive.default.test_table";
        String insert = "INSERT INTO hive.default.test_table (foo) VALUES (1)";

        assertAdminExecute(env, select);
        assertAdminExecute(env, insert);

        env.executeTrino(select, "bob");
        env.executeTrinoUpdate(insert, "bob");
        assertThat(env.executeTrino("SHOW GRANTS ON hive.default.test_table", "bob"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null),
                        row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));

        env.executeTrinoInSession("bob", session -> {
            session.executeUpdate("SET ROLE ALL IN hive");
            session.executeQuery(select);
            session.executeUpdate(insert);
            assertThat(session.executeQuery("SHOW GRANTS ON hive.default.test_table"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null),
                            row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));

            session.executeUpdate("SET ROLE NONE IN hive");
            assertThatThrownBy(() -> session.executeQuery(select))
                    .hasMessageContaining("Access Denied");
            assertThatThrownBy(() -> session.executeUpdate(insert))
                    .hasMessageContaining("Access Denied");
            assertThat(session.executeQuery("SHOW GRANTS ON hive.default.test_table"))
                    .containsOnly(ImmutableList.of());

            session.executeUpdate("SET ROLE role1 IN hive");
            session.executeQuery(select);
            assertThatThrownBy(() -> session.executeUpdate(insert))
                    .hasMessageContaining("Access Denied");
            assertThat(session.executeQuery("SHOW GRANTS ON hive.default.test_table"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null)));

            session.executeUpdate("SET ROLE role2 IN hive");
            assertThatThrownBy(() -> session.executeQuery(select))
                    .hasMessageContaining("Access Denied");
            session.executeUpdate(insert);
            assertThat(session.executeQuery("SHOW GRANTS ON hive.default.test_table"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));
        });

        env.executeTrinoUpdate("DROP TABLE hive.default.test_table", "alice");
    }

    private void assertAdminExecute(HiveKerberosImpersonationEnvironment env, String query)
    {
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET ROLE NONE IN hive");
            assertThatThrownBy(() -> executeStatement(session, query))
                    .hasMessageContaining("Access Denied");

            session.executeUpdate("SET ROLE ALL IN hive");
            assertThatThrownBy(() -> executeStatement(session, query))
                    .hasMessageContaining("Access Denied");

            session.executeUpdate("SET ROLE admin IN hive");
            executeStatement(session, query);
        });
    }

    private static void executeStatement(TrinoSession session, String query)
            throws SQLException
    {
        if (isQueryStatement(query)) {
            session.executeQuery(query);
            return;
        }
        session.executeUpdate(query);
    }

    private static boolean isQueryStatement(String query)
    {
        String normalized = query.stripLeading().toUpperCase(Locale.ROOT);
        return normalized.startsWith("SELECT")
                || normalized.startsWith("SHOW")
                || normalized.startsWith("WITH");
    }
}
