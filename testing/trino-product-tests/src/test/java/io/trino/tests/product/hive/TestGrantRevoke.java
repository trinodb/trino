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
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for GRANT and REVOKE functionality with SQL-standard security.
 * <p>
 * Pre-requisites for this test:
 * <ul>
 *   <li>hive.security=sql-standard is configured in the Hive catalog</li>
 *   <li>HDFS and HMS impersonation are enabled</li>
 *   <li>Multiple users (alice, bob, charlie) can connect to Trino</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveKerberosImpersonationEnvironment.class)
@TestGroup.Authorization
class TestGrantRevoke
{
    private static final Set<String> PREDEFINED_ROLES = ImmutableSet.of("admin", "public");
    private static final String DEFAULT_TRINO_USER = "hdfs";
    private static final String TABLE_NAME = "alice_owned_table";
    private static final String VIEW_NAME = "alice_view";

    @BeforeEach
    void setup(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdateAs("alice", format("DROP TABLE IF EXISTS hive.default.%s", TABLE_NAME));
        env.executeTrinoUpdateAs("alice", format("CREATE TABLE hive.default.%s(month bigint, day bigint)", TABLE_NAME));

        env.executeTrinoUpdate("SET ROLE admin IN hive");
        env.executeHiveUpdate("SET ROLE admin");
        assertAccessDeniedOnAllOperationsOnTable(env, "bob", TABLE_NAME);
    }

    @AfterEach
    void cleanup(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdateAs("alice", format("DROP TABLE IF EXISTS hive.default.%s", TABLE_NAME));
        env.executeTrinoUpdateAs("alice", format("DROP VIEW IF EXISTS hive.default.%s", VIEW_NAME));
        cleanupRoles(env);
    }

    private void cleanupRoles(HiveKerberosImpersonationEnvironment env)
    {
        for (String role : listRoles(env)) {
            if (!PREDEFINED_ROLES.contains(role)) {
                env.executeTrinoUpdate(format("DROP ROLE %s IN hive", role));
            }
        }
    }

    private Set<String> listRoles(HiveKerberosImpersonationEnvironment env)
    {
        return env.executeTrino("SHOW ROLES FROM hive")
                .rows()
                .stream()
                .map(row -> row.get(0).toString())
                .collect(toImmutableSet());
    }

    @Test
    void testCleanupRoles(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");

        cleanupRoles(env);

        assertThat(listRoles(env)).containsOnlyElementsOf(PREDEFINED_ROLES);
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testGrantRevoke(HiveKerberosImpersonationEnvironment env)
    {
        // test GRANT
        env.executeTrinoUpdateAs("alice", format("GRANT SELECT ON hive.default.%s TO bob", TABLE_NAME));
        assertThat(env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();
        env.executeTrinoUpdateAs("alice", format("GRANT INSERT, SELECT ON hive.default.%s TO bob", TABLE_NAME));
        assertThat(env.executeTrinoUpdateAs("bob", format("INSERT INTO hive.default.%s VALUES (3, 22)", TABLE_NAME)))
                .isEqualTo(1);
        assertThat(env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasRowsCount(1);
        assertThatThrownBy(() -> env.executeTrinoUpdateAs("bob", format("DELETE FROM hive.default.%s WHERE day=3", TABLE_NAME)))
                .hasMessageContaining("Access Denied: Cannot delete from table default." + TABLE_NAME);

        // test REVOKE
        env.executeTrinoUpdateAs("alice", format("REVOKE INSERT ON hive.default.%s FROM bob", TABLE_NAME));
        assertThatThrownBy(() -> env.executeTrinoUpdateAs("bob", format("INSERT INTO hive.default.%s VALUES ('y', 5)", TABLE_NAME)))
                .hasMessageContaining("Access Denied: Cannot insert into table default." + TABLE_NAME);
        assertThat(env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasRowsCount(1);
        env.executeTrinoUpdateAs("alice", format("REVOKE INSERT, SELECT ON hive.default.%s FROM bob", TABLE_NAME));
        assertThatThrownBy(() -> env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME)))
                .hasMessageContaining("Access Denied: Cannot select from table default." + TABLE_NAME);
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testGrantOptionsOnGrantedPrivilege(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdateAs("alice", format("GRANT SELECT ON hive.default.%s TO bob", TABLE_NAME));
        assertThatThrownBy(() -> env.executeTrinoUpdateAs("alice", format("GRANT SELECT ON hive.default.%s TO bob WITH GRANT OPTION", TABLE_NAME)))
                // Updating a privilege with GRANT OPTION is not supported by Hive. https://issues.apache.org/jira/browse/HIVE-15689
                .hasMessageContaining("Granting SELECT WITH GRANT OPTION is not supported while USER bob possesses SELECT");
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testGrantRevokeWithGrantOption(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER charlie IN hive");

        // test GRANT WITH GRANT OPTION
        env.executeTrinoUpdateAs("alice", format("GRANT SELECT ON hive.default.%s TO bob WITH GRANT OPTION", TABLE_NAME));
        assertThatThrownBy(() -> env.executeTrinoAs("charlie", format("SELECT * FROM hive.default.%s", TABLE_NAME)))
                .hasMessageContaining("Access Denied: Cannot select from table default." + TABLE_NAME);
        env.executeTrinoUpdateAs("bob", format("GRANT SELECT ON hive.default.%s TO ROLE role1", TABLE_NAME));
        assertThat(env.executeTrinoAs("charlie", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();

        // test REVOKE WITH GRANT OPTION
        env.executeTrinoUpdateAs("alice", format("REVOKE GRANT OPTION FOR SELECT ON hive.default.%s FROM bob", TABLE_NAME));
        assertThatThrownBy(() -> env.executeTrinoUpdateAs("bob", format("GRANT SELECT ON hive.default.%s TO ROLE role1 ", TABLE_NAME)))
                .hasMessageContaining("Access Denied: Cannot grant privilege SELECT on table default." + TABLE_NAME);
        assertThat(env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();
        // Since Hive doesn't support REVOKE with CASCADE, charlie would still have access to table
        assertThat(env.executeTrinoAs("charlie", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();

        // test GRANT WITH GRANT OPTION post revoke
        assertThatThrownBy(() -> env.executeTrinoUpdateAs("alice", format("GRANT SELECT ON hive.default.%s TO bob WITH GRANT OPTION", TABLE_NAME)))
                // Updating a privilege with GRANT OPTION is not supported by Hive. https://issues.apache.org/jira/browse/HIVE-15689
                .hasMessageContaining("Granting SELECT WITH GRANT OPTION is not supported while USER bob possesses SELECT");
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testShowGrants(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate(format("GRANT SELECT ON hive.default.%s TO ROLE role1", TABLE_NAME));
        env.executeTrinoUpdate("GRANT role1 TO USER bob IN hive");
        env.executeTrinoUpdateAs("alice", format("GRANT SELECT ON hive.default.%s TO bob WITH GRANT OPTION", TABLE_NAME));
        env.executeTrinoUpdateAs("alice", format("GRANT INSERT ON hive.default.%s TO bob", TABLE_NAME));

        assertThat(env.executeTrinoAs("bob", format("SHOW GRANTS ON hive.default.%s", TABLE_NAME)))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "bob", "USER", "hive", "default", "alice_owned_table", "SELECT", "YES", null),
                        row("alice", "USER", "bob", "USER", "hive", "default", "alice_owned_table", "INSERT", "NO", null),
                        row(DEFAULT_TRINO_USER, "USER", "role1", "ROLE", "hive", "default", "alice_owned_table", "SELECT", "NO", null)));
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testAll(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdateAs("alice", format("GRANT ALL PRIVILEGES ON hive.default.%s TO USER bob", TABLE_NAME));
        assertThat(env.executeTrinoUpdateAs("bob", format("INSERT INTO hive.default.%s VALUES (4, 13)", TABLE_NAME)))
                .isEqualTo(1);
        assertThat(env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasRowsCount(1);
        env.executeTrinoUpdateAs("bob", format("DELETE FROM hive.default.%s", TABLE_NAME));
        assertThat(env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();

        env.executeTrinoUpdateAs("alice", format("REVOKE ALL PRIVILEGES ON hive.default.%s FROM USER bob", TABLE_NAME));
        assertAccessDeniedOnAllOperationsOnTable(env, "bob", TABLE_NAME);

        assertThat(env.executeTrinoAs("bob", format("SHOW GRANTS ON hive.default.%s", TABLE_NAME))).hasNoRows();
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testPublic(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdateAs("alice", format("GRANT SELECT ON hive.default.%s TO ROLE PUBLIC", TABLE_NAME));
        assertThat(env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();
        env.executeTrinoUpdateAs("alice", format("REVOKE SELECT ON hive.default.%s FROM ROLE PUBLIC", TABLE_NAME));
        assertThatThrownBy(() -> env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME)))
                .hasMessageContaining("Access Denied: Cannot select from table default." + TABLE_NAME);
        assertThat(env.executeTrinoAs("alice", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testCustomRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER bob IN hive");
        env.executeTrinoUpdateAs("alice", format("GRANT SELECT ON hive.default.%s TO ROLE role1", TABLE_NAME));
        assertThat(env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();
        env.executeTrinoUpdateAs("alice", format("REVOKE SELECT ON hive.default.%s FROM ROLE role1", TABLE_NAME));
        assertThatThrownBy(() -> env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME)))
                .hasMessageContaining("Access Denied: Cannot select from table default." + TABLE_NAME);
        assertThat(env.executeTrinoAs("alice", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testTransitiveRole(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("CREATE ROLE role2 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER bob IN hive");
        env.executeTrinoUpdate("GRANT role2 TO ROLE role1 IN hive");
        env.executeTrinoUpdateAs("alice", format("GRANT SELECT ON hive.default.%s TO ROLE role2", TABLE_NAME));
        assertThat(env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();
        env.executeTrinoUpdateAs("alice", format("REVOKE SELECT ON hive.default.%s FROM ROLE role2", TABLE_NAME));
        assertThatThrownBy(() -> env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME)))
                .hasMessageContaining("Access Denied: Cannot select from table default." + TABLE_NAME);
        assertThat(env.executeTrinoAs("alice", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testDropRoleWithPermissionsGranted(HiveKerberosImpersonationEnvironment env)
    {
        env.executeTrinoUpdate("CREATE ROLE role1 IN hive");
        env.executeTrinoUpdate("GRANT role1 TO USER bob IN hive");
        env.executeTrinoUpdateAs("alice", format("GRANT SELECT ON hive.default.%s TO ROLE role1", TABLE_NAME));
        assertThat(env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();
        env.executeTrinoUpdate("DROP ROLE role1 IN hive");
        assertThatThrownBy(() -> env.executeTrinoAs("bob", format("SELECT * FROM hive.default.%s", TABLE_NAME)))
                .hasMessageContaining("Access Denied: Cannot select from table default." + TABLE_NAME);
        assertThat(env.executeTrinoAs("alice", format("SELECT * FROM hive.default.%s", TABLE_NAME))).hasNoRows();
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testTableOwnerPrivileges(HiveKerberosImpersonationEnvironment env)
    {
        assertThat(env.executeTrino(format("SHOW GRANTS ON hive.default.%s", TABLE_NAME))
                .project(2, 3, 7, 8))
                .contains(
                        row("USER", "alice", TABLE_NAME, "SELECT"),
                        row("USER", "alice", TABLE_NAME, "INSERT"),
                        row("USER", "alice", TABLE_NAME, "UPDATE"),
                        row("USER", "alice", TABLE_NAME, "DELETE"));
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testViewOwnerPrivileges(HiveKerberosImpersonationEnvironment env)
    {
        // Create view as alice
        env.executeTrinoUpdateAs("alice", format("CREATE VIEW hive.default.%s AS SELECT * FROM hive.default.%s", VIEW_NAME, TABLE_NAME));
        try {
            assertThat(env.executeTrino(format("SHOW GRANTS ON hive.default.%s", VIEW_NAME))
                    .project(2, 3, 7, 8))
                    .contains(
                            row("USER", "alice", VIEW_NAME, "SELECT"),
                            row("USER", "alice", VIEW_NAME, "INSERT"),
                            row("USER", "alice", VIEW_NAME, "UPDATE"),
                            row("USER", "alice", VIEW_NAME, "DELETE"));
        }
        finally {
            env.executeTrinoUpdateAs("alice", format("DROP VIEW IF EXISTS hive.default.%s", VIEW_NAME));
        }
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/XXXXX", match = ".*")
    void testTablePrivilegesWithHiveOnlyViews(HiveKerberosImpersonationEnvironment env)
    {
        String hiveOnlyViewName = "hive_only_view";
        // Create view using Hive
        env.executeHiveUpdate(format("CREATE VIEW %s AS SELECT * FROM %s", hiveOnlyViewName, TABLE_NAME));
        try {
            assertThat(env.executeTrino("SELECT DISTINCT table_name FROM information_schema.table_privileges"))
                    .contains(row(TABLE_NAME))
                    .contains(row(hiveOnlyViewName));
            assertThat(env.executeTrino("SHOW GRANTS").project(7))
                    .contains(row(TABLE_NAME))
                    .contains(row(hiveOnlyViewName));
        }
        finally {
            env.executeHiveUpdate(format("DROP VIEW IF EXISTS %s", hiveOnlyViewName));
        }
    }

    private static void assertAccessDeniedOnAllOperationsOnTable(HiveKerberosImpersonationEnvironment env, String user, String tableName)
    {
        assertThatThrownBy(() -> env.executeTrinoAs(user, format("SELECT * FROM hive.default.%s", tableName)))
                .hasMessageContaining("Access Denied: Cannot select from table default." + tableName);
        assertThatThrownBy(() -> env.executeTrinoUpdateAs(user, format("INSERT INTO hive.default.%s VALUES (3, 22)", tableName)))
                .hasMessageContaining("Access Denied: Cannot insert into table default." + tableName);
        assertThatThrownBy(() -> env.executeTrinoUpdateAs(user, format("DELETE FROM hive.default.%s WHERE day=3", tableName)))
                .hasMessageContaining("Access Denied: Cannot delete from table default." + tableName);
    }
}
