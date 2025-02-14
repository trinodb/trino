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
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.anyOf;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.TestGroups.AUTHORIZATION;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.connectToTrino;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRoles
        extends HiveProductTest
{
    private static final String ROLE1 = "role1";
    private static final String ROLE2 = "role2";
    private static final String ROLE3 = "role3";
    private static final Set<String> TEST_ROLES = ImmutableSet.of(ROLE1, ROLE2, ROLE3);

    @Inject
    @Named("databases.trino.jdbc_user")
    private String userName;

    @BeforeMethodWithContext
    public void setUp()
    {
        onTrino().executeQuery("SET ROLE admin IN hive");
        onHive().executeQuery("SET ROLE admin");
        cleanup();
    }

    @AfterMethodWithContext
    public void tearDown()
    {
        cleanup();
    }

    private void cleanup()
    {
        Set<String> existentRoles = listRoles();
        for (String role : TEST_ROLES) {
            if (existentRoles.contains(role)) {
                onHive().executeQuery(format("DROP ROLE %s", role));
            }
        }
    }

    private Set<String> listRoles()
    {
        return onHive().executeQuery("SHOW ROLES").rows().stream()
                .map(Iterables::getOnlyElement)
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateRole()
    {
        onTrino().executeQuery(format("CREATE ROLE %s IN hive", ROLE1));
        assertThat(listRoles()).contains(ROLE1);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropRole()
    {
        onHive().executeQuery(format("CREATE ROLE %s", ROLE1));
        assertThat(listRoles()).contains(ROLE1);
        onTrino().executeQuery(format("DROP ROLE %s IN hive", ROLE1));
        assertThat(listRoles()).doesNotContain(ROLE1);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testListRoles()
    {
        onTrino().executeQuery(format("CREATE ROLE %s IN hive", ROLE1));
        QueryResult expected = onHive().executeQuery("SHOW ROLES");
        QueryResult actual = onTrino().executeQuery("SELECT * FROM hive.information_schema.roles");
        assertThat(actual.rows()).containsOnly(expected.rows().toArray(new List[] {}));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateDuplicateRole()
    {
        onTrino().executeQuery(format("CREATE ROLE %s IN hive", ROLE1));
        assertQueryFailure(() -> onTrino().executeQuery(format("CREATE ROLE %s IN hive", ROLE1)))
                .hasMessageContaining("Role '%s' already exists", ROLE1);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropNonExistentRole()
    {
        assertQueryFailure(() -> onTrino().executeQuery(format("DROP ROLE %s IN hive", ROLE3)))
                .hasMessageContaining("Role '%s' does not exist", ROLE3);
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateDropRoleAccessControl()
    {
        // Only users that are granted with "admin" role can create, drop and list roles
        // Alice is not granted with "admin" role
        assertQueryFailure(() -> onTrinoAlice().executeQuery(format("CREATE ROLE %s IN hive", ROLE3)))
                .hasMessageContaining("Cannot create role %s", ROLE3);
        assertQueryFailure(() -> onTrinoAlice().executeQuery(format("DROP ROLE %s IN hive", ROLE3)))
                .hasMessageContaining("Cannot drop role %s", ROLE3);
        assertQueryFailure(() -> onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.roles"))
                .hasMessageContaining("Cannot select from table information_schema.roles");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testPublicRoleIsGrantedToEveryone()
    {
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row("alice", "USER", "public", "NO"));
        assertThat(onTrinoBob().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row("bob", "USER", "public", "NO"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminRoleIsGrantedToHdfs()
    {
        assertThat(onTrino().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row(userName, "USER", "admin", anyOf("YES", "NO")));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleToUser()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleToRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleWithAdminOption()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleMultipleTimes()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeRoleFromUser()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));

        onTrino().executeQuery("REVOKE role1 FROM USER alice IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeRoleFromRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        onTrino().executeQuery("REVOKE role2 FROM ROLE role1 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeRoleFromOwner()
    {
        try {
            onTrinoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
            assertThat(onTrinoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "INSERT", "YES", null)));

            onTrino().executeQuery("REVOKE SELECT ON hive.default.test_table FROM USER alice");

            // now there should be no SELECT privileges shown even though alice has OWNERSHIP
            assertThat(onTrinoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "INSERT", "YES", null)));
        }
        finally {
            onTrinoAlice().executeQuery("DROP TABLE IF EXISTS hive.default.test_table");
        }
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropGrantedRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));

        onTrino().executeQuery("DROP ROLE role1 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeTransitiveRoleFromUser()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        onTrino().executeQuery("REVOKE role1 FROM USER alice IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeTransitiveRoleFromRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("CREATE ROLE role3 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role3 TO ROLE role2 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"),
                        row("role2", "ROLE", "role3", "NO"));

        onTrino().executeQuery("REVOKE role2 FROM ROLE role1 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropTransitiveRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("CREATE ROLE role3 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role3 TO ROLE role2 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"),
                        row("role2", "ROLE", "role3", "NO"));

        onTrino().executeQuery("DROP ROLE role2 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeAdminOption()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));

        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");
        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1 IN hive");

        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeMultipleTimes()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));

        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");
        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");
        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1 IN hive");
        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1 IN hive");

        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        onTrino().executeQuery("REVOKE role1 FROM USER alice IN hive");
        onTrino().executeQuery("REVOKE role1 FROM USER alice IN hive");
        onTrino().executeQuery("REVOKE role2 FROM ROLE role1 IN hive");
        onTrino().executeQuery("REVOKE role2 FROM ROLE role1 IN hive");

        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevokeRoleAccessControl()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");

        assertQueryFailure(() -> onTrinoAlice().executeQuery("GRANT role1 TO USER bob IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        assertQueryFailure(() -> onTrinoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        assertQueryFailure(() -> onTrinoAlice().executeQuery("REVOKE role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");
        assertQueryFailure(() -> onTrinoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");

        onTrino().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");

        onTrinoAlice().executeQuery("GRANT role1 TO USER bob IN hive");
        onTrinoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive");
        onTrinoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive");
        onTrinoAlice().executeQuery("REVOKE role1 FROM USER bob IN hive");

        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");

        assertQueryFailure(() -> onTrinoAlice().executeQuery("GRANT role1 TO USER bob IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        assertQueryFailure(() -> onTrinoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        assertQueryFailure(() -> onTrinoAlice().executeQuery("REVOKE role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");
        assertQueryFailure(() -> onTrinoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");

        onTrino().executeQuery("GRANT role2 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role1 TO ROLE role2 WITH ADMIN OPTION IN hive");

        onTrinoAlice().executeQuery("GRANT role1 TO USER bob IN hive");
        onTrinoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive");
        onTrinoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive");
        onTrinoAlice().executeQuery("REVOKE role1 FROM USER bob IN hive");

        onTrinoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM ROLE role2 IN hive");

        assertQueryFailure(() -> onTrinoAlice().executeQuery("GRANT role1 TO USER bob IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        assertQueryFailure(() -> onTrinoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        assertQueryFailure(() -> onTrinoAlice().executeQuery("REVOKE role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");
        assertQueryFailure(() -> onTrinoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("CREATE ROLE role3 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role3 TO ROLE role2 IN hive");

        onTrinoAlice().executeQuery("SET ROLE ALL IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role1"),
                        row("role2"),
                        row("role3"));

        onTrinoAlice().executeQuery("SET ROLE NONE IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"));

        onTrinoAlice().executeQuery("SET ROLE role1 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role1"),
                        row("role2"),
                        row("role3"));

        onTrinoAlice().executeQuery("SET ROLE role2 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role2"),
                        row("role3"));

        onTrinoAlice().executeQuery("SET ROLE role3 IN hive");
        assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role3"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetAdminRole()
    {
        onTrino().executeQuery("SET ROLE NONE IN hive");
        assertThat(onTrino().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"));
        onTrino().executeQuery("SET ROLE admin IN hive");
        assertThat(onTrino().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("admin"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowRoles()
    {
        assertThat(onTrino().executeQuery("SHOW ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        assertThat(onTrino().executeQuery("SHOW ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"),
                        row("role1"));
        assertQueryFailure(() -> onTrinoAlice().executeQuery("SHOW ROLES FROM hive"))
                .hasMessageContaining("Cannot show roles");
        onTrino().executeQuery("GRANT admin TO alice IN hive");
        onTrinoAlice().executeQuery("SET ROLE admin IN hive");
        assertThat(onTrinoAlice().executeQuery("SHOW ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"),
                        row("role1"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowCurrentRoles()
    {
        assertThat(onTrinoAlice().executeQuery("SHOW CURRENT ROLES FROM hive"))
                .containsOnly(
                        row("public"));
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO alice IN hive");
        onTrino().executeQuery("GRANT role2 TO alice IN hive");
        assertThat(onTrinoAlice().executeQuery("SHOW CURRENT ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("role1"),
                        row("role2"));
        onTrinoAlice().executeQuery("SET ROLE role2 IN hive");
        assertThat(onTrinoAlice().executeQuery("SHOW CURRENT ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("role2"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowRoleGrants()
    {
        assertThat(onTrino().executeQuery("SHOW ROLE GRANTS FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        assertThat(onTrinoAlice().executeQuery("SHOW ROLE GRANTS FROM hive"))
                .containsOnly(
                        row("public"));
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO alice IN hive");
        onTrino().executeQuery("GRANT role2 TO role1 IN hive");
        assertThat(onTrino().executeQuery("SHOW ROLE GRANTS FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        assertThat(onTrinoAlice().executeQuery("SHOW ROLE GRANTS FROM hive"))
                .containsOnly(
                        row("public"),
                        row("role1"));
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetRoleCreateDropSchema()
    {
        assertAdminExecute("CREATE SCHEMA hive.test_admin_schema");
        onTrino().executeQuery("DROP SCHEMA hive.test_admin_schema");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanDropAnyTable()
    {
        onTrinoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("DROP TABLE hive.default.test_table");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanRenameAnyTable()
    {
        onTrinoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("ALTER TABLE hive.default.test_table RENAME TO hive.default.test_table_1");
        onTrinoAlice().executeQuery("DROP TABLE hive.default.test_table_1");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanAddColumnToAnyTable()
    {
        onTrinoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("ALTER TABLE hive.default.test_table ADD COLUMN bar DATE");
        onTrinoAlice().executeQuery("DROP TABLE hive.default.test_table");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanRenameColumnInAnyTable()
    {
        onTrinoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("ALTER TABLE hive.default.test_table RENAME COLUMN foo TO bar");
        onTrinoAlice().executeQuery("DROP TABLE hive.default.test_table");
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanShowAllGrants()
    {
        try {
            onTrinoBob().executeQuery("CREATE TABLE hive.default.test_table_bob (foo BIGINT)");
            onTrinoAlice().executeQuery("CREATE TABLE hive.default.test_table_alice (foo BIGINT)");
            onTrino().executeQuery("GRANT admin TO alice IN hive");
            onTrinoAlice().executeQuery("SET ROLE ADMIN IN hive");

            assertThat(onTrinoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_alice"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "INSERT", "YES", null)));

            assertThat(onTrinoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_bob"))
                    .containsOnly(ImmutableList.of(
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "SELECT", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "DELETE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "UPDATE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "INSERT", "YES", null)));

            onTrinoAlice().executeQuery("GRANT SELECT ON hive.default.test_table_alice  TO bob WITH GRANT OPTION");
            onTrinoAlice().executeQuery("GRANT INSERT ON hive.default.test_table_alice  TO bob");

            assertThat(onTrinoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_alice"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "INSERT", "YES", null),
                            row("alice", "USER", "bob", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                            row("alice", "USER", "bob", "USER", "hive", "default", "test_table_alice", "INSERT", "NO", null)));
        }
        finally {
            onTrinoAlice().executeQuery("DROP TABLE IF EXISTS hive.default.test_table_alice");
            onTrinoBob().executeQuery("DROP TABLE IF EXISTS hive.default.test_table_bob");
            onTrino().executeQuery("REVOKE admin FROM alice IN hive");
        }
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanShowGrantsOnlyFromCurrentSchema()
    {
        try {
            onTrinoBob().executeQuery("CREATE TABLE hive.default.test_table_bob (foo BIGINT)");
            onTrino().executeQuery("CREATE SCHEMA hive.test");
            onTrino().executeQuery("GRANT admin TO alice IN hive");
            onTrinoAlice().executeQuery("SET ROLE ADMIN IN hive");
            onTrinoAlice().executeQuery("CREATE TABLE hive.test.test_table_bob (foo BIGINT) with (external_location='/tmp')");

            assertThat(onTrinoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_bob"))
                    .containsOnly(ImmutableList.of(
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "SELECT", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "DELETE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "UPDATE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "INSERT", "YES", null)));

            assertThat(onTrinoAlice().executeQuery("SHOW GRANTS ON hive.test.test_table_bob"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "INSERT", "YES", null)));
            assertThat(onTrinoAlice().executeQuery("SELECT * FROM hive.information_schema.table_privileges where table_name = 'test_table_bob'"))
                    .containsOnly(ImmutableList.of(
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "SELECT", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "DELETE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "UPDATE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "INSERT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "INSERT", "YES", null)));
        }
        finally {
            onTrinoBob().executeQuery("DROP TABLE IF EXISTS hive.default.test_table_bob");
            onTrinoAlice().executeQuery("DROP TABLE IF EXISTS hive.test.test_table_bob");
            onTrino().executeQuery("DROP SCHEMA IF EXISTS hive.test");
            onTrino().executeQuery("REVOKE admin FROM alice IN hive");
        }
    }

    @Test(groups = {AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetRoleTablePermissions()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");

        onTrino().executeQuery("GRANT role1 TO USER bob IN hive");
        onTrino().executeQuery("GRANT role2 TO USER bob IN hive");

        onTrinoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        onTrinoAlice().executeQuery("GRANT SELECT ON hive.default.test_table TO ROLE role1");
        onTrinoAlice().executeQuery("GRANT INSERT ON hive.default.test_table TO ROLE role2");

        String select = "SELECT * FROM hive.default.test_table";
        String insert = "INSERT INTO hive.default.test_table (foo) VALUES (1)";

        assertAdminExecute(select);
        assertAdminExecute(insert);

        onTrinoBob().executeQuery(select);
        onTrinoBob().executeQuery(insert);
        assertThat(onTrinoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null),
                        row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));

        onTrinoBob().executeQuery("SET ROLE ALL IN hive");
        onTrinoBob().executeQuery(select);
        onTrinoBob().executeQuery(insert);
        assertThat(onTrinoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null),
                        row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));

        onTrinoBob().executeQuery("SET ROLE NONE IN hive");
        assertQueryFailure(() -> onTrinoBob().executeQuery(select))
                .hasMessageContaining("Access Denied");
        assertQueryFailure(() -> onTrinoBob().executeQuery(insert))
                .hasMessageContaining("Access Denied");
        assertThat(onTrinoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of());

        onTrinoBob().executeQuery("SET ROLE role1 IN hive");
        onTrinoBob().executeQuery(select);
        assertQueryFailure(() -> onTrinoBob().executeQuery(insert))
                .hasMessageContaining("Access Denied");
        assertThat(onTrinoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null)));

        onTrinoBob().executeQuery("SET ROLE role2 IN hive");
        assertQueryFailure(() -> onTrinoBob().executeQuery(select))
                .hasMessageContaining("Access Denied");
        onTrinoBob().executeQuery(insert);
        assertThat(onTrinoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));

        onTrinoAlice().executeQuery("DROP TABLE hive.default.test_table");
    }

    private static void assertAdminExecute(String query)
    {
        onTrino().executeQuery("SET ROLE NONE IN hive");
        assertQueryFailure(() -> onTrino().executeQuery(query))
                .hasMessageContaining("Access Denied");

        onTrino().executeQuery("SET ROLE ALL IN hive");
        assertQueryFailure(() -> onTrino().executeQuery(query))
                .hasMessageContaining("Access Denied");

        onTrino().executeQuery("SET ROLE admin IN hive");
        onTrino().executeQuery(query);
    }

    private static QueryExecutor onTrinoAlice()
    {
        return connectToTrino("alice@trino");
    }

    private static QueryExecutor onTrinoBob()
    {
        return connectToTrino("bob@trino");
    }
}
