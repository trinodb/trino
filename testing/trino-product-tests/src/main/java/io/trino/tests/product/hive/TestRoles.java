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
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.anyOf;
import static io.trino.tests.product.TestGroups.AUTHORIZATION;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.ROLES;
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
    @Named("databases.presto.jdbc_user")
    private String userName;

    @BeforeTestWithContext
    public void setUp()
    {
        onTrino().executeQuery("SET ROLE admin IN hive");
        onHive().executeQuery("SET ROLE admin");
        cleanup();
    }

    @AfterTestWithContext
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

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateRole()
    {
        onTrino().executeQuery(format("CREATE ROLE %s IN hive", ROLE1));
        assertThat(listRoles()).contains(ROLE1);
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropRole()
    {
        onHive().executeQuery(format("CREATE ROLE %s", ROLE1));
        assertThat(listRoles()).contains(ROLE1);
        onTrino().executeQuery(format("DROP ROLE %s IN hive", ROLE1));
        assertThat(listRoles()).doesNotContain(ROLE1);
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testListRoles()
    {
        onTrino().executeQuery(format("CREATE ROLE %s IN hive", ROLE1));
        QueryResult expected = onHive().executeQuery("SHOW ROLES");
        QueryResult actual = onTrino().executeQuery("SELECT * FROM hive.information_schema.roles");
        assertThat(actual.rows()).containsOnly(expected.rows().toArray(new List[] {}));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testListGrants()
    {
        if (getHiveVersionMajor() >= 3) {
            throw new SkipException(""); // TODO (https://github.com/trinodb/trino/issues/1218) this currently fails on HDP 3
        }

        onTrino().executeQuery("SHOW GRANTS"); // must not fail
        onTrino().executeQuery("SELECT * FROM information_schema.table_privileges"); // must not fail

        onTrino().executeQuery("CREATE TABLE test_list_grants(c int)");

        QueryAssert.assertThat(onTrino().executeQuery("SHOW GRANTS"))
                .contains(
                        row(userName, "USER", userName, "USER", "hive", "default", "test_list_grants", "SELECT", "YES", null),
                        row(userName, "USER", userName, "USER", "hive", "default", "test_list_grants", "INSERT", "YES", null),
                        row(userName, "USER", userName, "USER", "hive", "default", "test_list_grants", "UPDATE", "YES", null),
                        row(userName, "USER", userName, "USER", "hive", "default", "test_list_grants", "DELETE", "YES", null));

        QueryAssert.assertThat(onTrino().executeQuery("SELECT * FROM information_schema.table_privileges"))
                .contains(
                        row(userName, "USER", userName, "USER", "hive", "default", "test_list_grants", "SELECT", "YES", null),
                        row(userName, "USER", userName, "USER", "hive", "default", "test_list_grants", "INSERT", "YES", null),
                        row(userName, "USER", userName, "USER", "hive", "default", "test_list_grants", "UPDATE", "YES", null),
                        row(userName, "USER", userName, "USER", "hive", "default", "test_list_grants", "DELETE", "YES", null));

        onTrino().executeQuery("DROP TABLE test_list_grants");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateDuplicateRole()
    {
        onTrino().executeQuery(format("CREATE ROLE %s IN hive", ROLE1));
        QueryAssert.assertQueryFailure(() -> onTrino().executeQuery(format("CREATE ROLE %s IN hive", ROLE1)))
                .hasMessageContaining("Role '%s' already exists", ROLE1);
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropNonExistentRole()
    {
        QueryAssert.assertQueryFailure(() -> onTrino().executeQuery(format("DROP ROLE %s IN hive", ROLE3)))
                .hasMessageContaining("Role '%s' does not exist", ROLE3);
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testCreateDropRoleAccessControl()
    {
        // Only users that are granted with "admin" role can create, drop and list roles
        // Alice is not granted with "admin" role
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery(format("CREATE ROLE %s IN hive", ROLE3)))
                .hasMessageContaining("Cannot create role %s", ROLE3);
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery(format("DROP ROLE %s IN hive", ROLE3)))
                .hasMessageContaining("Cannot drop role %s", ROLE3);
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.roles"))
                .hasMessageContaining("Cannot select from table information_schema.roles");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testPublicRoleIsGrantedToEveryone()
    {
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row("alice", "USER", "public", "NO"));
        QueryAssert.assertThat(onPrestoBob().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row("bob", "USER", "public", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminRoleIsGrantedToHdfs()
    {
        QueryAssert.assertThat(onTrino().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .contains(row(userName, "USER", "admin", anyOf("YES", "NO")));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleToUser()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleToRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRoleWithAdminOption()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
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
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeRoleFromUser()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));

        onTrino().executeQuery("REVOKE role1 FROM USER alice IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeRoleFromRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        onTrino().executeQuery("REVOKE role2 FROM ROLE role1 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeRoleFromOwner()
    {
        try {
            onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "INSERT", "YES", null)));

            onTrino().executeQuery("REVOKE SELECT ON hive.default.test_table FROM USER alice");

            // now there should be no SELECT privileges shown even though alice has OWNERSHIP
            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table", "INSERT", "YES", null)));
        }
        finally {
            onPrestoAlice().executeQuery("DROP TABLE IF EXISTS hive.default.test_table");
        }
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropGrantedRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));

        onTrino().executeQuery("DROP ROLE role1 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeTransitiveRoleFromUser()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        onTrino().executeQuery("REVOKE role1 FROM USER alice IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeTransitiveRoleFromRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("CREATE ROLE role3 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role3 TO ROLE role2 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"),
                        row("role2", "ROLE", "role3", "NO"));

        onTrino().executeQuery("REVOKE role2 FROM ROLE role1 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testDropTransitiveRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("CREATE ROLE role3 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role3 TO ROLE role2 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"),
                        row("role2", "ROLE", "role3", "NO"));

        onTrino().executeQuery("DROP ROLE role2 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeAdminOption()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));

        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");
        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1 IN hive");

        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testRevokeMultipleTimes()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 WITH ADMIN OPTION IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "YES"),
                        row("role1", "ROLE", "role2", "YES"));

        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");
        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");
        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1 IN hive");
        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role2 FROM ROLE role1 IN hive");

        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"),
                        row("alice", "USER", "role1", "NO"),
                        row("role1", "ROLE", "role2", "NO"));

        onTrino().executeQuery("REVOKE role1 FROM USER alice IN hive");
        onTrino().executeQuery("REVOKE role1 FROM USER alice IN hive");
        onTrino().executeQuery("REVOKE role2 FROM ROLE role1 IN hive");
        onTrino().executeQuery("REVOKE role2 FROM ROLE role1 IN hive");

        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.applicable_roles"))
                .containsOnly(
                        row("alice", "USER", "public", "NO"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testGrantRevokeRoleAccessControl()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");

        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("REVOKE role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");

        onTrino().executeQuery("GRANT role1 TO USER alice WITH ADMIN OPTION IN hive");

        onPrestoAlice().executeQuery("GRANT role1 TO USER bob IN hive");
        onPrestoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive");
        onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive");
        onPrestoAlice().executeQuery("REVOKE role1 FROM USER bob IN hive");

        onTrino().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER alice IN hive");

        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("REVOKE role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");

        onTrino().executeQuery("GRANT role2 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role1 TO ROLE role2 WITH ADMIN OPTION IN hive");

        onPrestoAlice().executeQuery("GRANT role1 TO USER bob IN hive");
        onPrestoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive");
        onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive");
        onPrestoAlice().executeQuery("REVOKE role1 FROM USER bob IN hive");

        onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM ROLE role2 IN hive");

        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("GRANT role1 TO USER bob WITH ADMIN OPTION IN hive"))
                .hasMessageContaining("Cannot grant roles [role1] to [USER bob]");
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("REVOKE role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("REVOKE ADMIN OPTION FOR role1 FROM USER bob IN hive"))
                .hasMessageContaining("Cannot revoke roles [role1] from [USER bob]");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetRole()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("CREATE ROLE role3 IN hive");
        onTrino().executeQuery("GRANT role1 TO USER alice IN hive");
        onTrino().executeQuery("GRANT role2 TO ROLE role1 IN hive");
        onTrino().executeQuery("GRANT role3 TO ROLE role2 IN hive");

        onPrestoAlice().executeQuery("SET ROLE ALL IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role1"),
                        row("role2"),
                        row("role3"));

        onPrestoAlice().executeQuery("SET ROLE NONE IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"));

        onPrestoAlice().executeQuery("SET ROLE role1 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role1"),
                        row("role2"),
                        row("role3"));

        onPrestoAlice().executeQuery("SET ROLE role2 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role2"),
                        row("role3"));

        onPrestoAlice().executeQuery("SET ROLE role3 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("role3"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetAdminRole()
    {
        onTrino().executeQuery("SET ROLE NONE IN hive");
        QueryAssert.assertThat(onTrino().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"));
        onTrino().executeQuery("SET ROLE admin IN hive");
        QueryAssert.assertThat(onTrino().executeQuery("SELECT * FROM hive.information_schema.enabled_roles"))
                .containsOnly(
                        row("public"),
                        row("admin"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowRoles()
    {
        QueryAssert.assertThat(onTrino().executeQuery("SHOW ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        QueryAssert.assertThat(onTrino().executeQuery("SHOW ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"),
                        row("role1"));
        QueryAssert.assertQueryFailure(() -> onPrestoAlice().executeQuery("SHOW ROLES FROM hive"))
                .hasMessageContaining("Cannot show roles");
        onTrino().executeQuery("GRANT admin TO alice IN hive");
        onPrestoAlice().executeQuery("SET ROLE admin IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"),
                        row("role1"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowCurrentRoles()
    {
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW CURRENT ROLES FROM hive"))
                .containsOnly(
                        row("public"));
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO alice IN hive");
        onTrino().executeQuery("GRANT role2 TO alice IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW CURRENT ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("role1"),
                        row("role2"));
        onPrestoAlice().executeQuery("SET ROLE role2 IN hive");
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW CURRENT ROLES FROM hive"))
                .containsOnly(
                        row("public"),
                        row("role2"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testShowRoleGrants()
    {
        QueryAssert.assertThat(onTrino().executeQuery("SHOW ROLE GRANTS FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW ROLE GRANTS FROM hive"))
                .containsOnly(
                        row("public"));
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");
        onTrino().executeQuery("GRANT role1 TO alice IN hive");
        onTrino().executeQuery("GRANT role2 TO role1 IN hive");
        QueryAssert.assertThat(onTrino().executeQuery("SHOW ROLE GRANTS FROM hive"))
                .containsOnly(
                        row("public"),
                        row("admin"));
        QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW ROLE GRANTS FROM hive"))
                .containsOnly(
                        row("public"),
                        row("role1"));
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetRoleCreateDropSchema()
    {
        assertAdminExecute("CREATE SCHEMA hive.test_admin_schema");
        onTrino().executeQuery("DROP SCHEMA hive.test_admin_schema");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanDropAnyTable()
    {
        onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("DROP TABLE hive.default.test_table");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanRenameAnyTable()
    {
        onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("ALTER TABLE hive.default.test_table RENAME TO hive.default.test_table_1");
        onPrestoAlice().executeQuery("DROP TABLE hive.default.test_table_1");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanAddColumnToAnyTable()
    {
        onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("ALTER TABLE hive.default.test_table ADD COLUMN bar DATE");
        onPrestoAlice().executeQuery("DROP TABLE hive.default.test_table");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanRenameColumnInAnyTable()
    {
        onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        assertAdminExecute("ALTER TABLE hive.default.test_table RENAME COLUMN foo TO bar");
        onPrestoAlice().executeQuery("DROP TABLE hive.default.test_table");
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanShowAllGrants()
    {
        try {
            onPrestoBob().executeQuery("CREATE TABLE hive.default.test_table_bob (foo BIGINT)");
            onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table_alice (foo BIGINT)");
            onTrino().executeQuery("GRANT admin TO alice IN hive");
            onPrestoAlice().executeQuery("SET ROLE ADMIN IN hive");

            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_alice"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "INSERT", "YES", null)));

            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_bob"))
                    .containsOnly(ImmutableList.of(
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "SELECT", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "DELETE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "UPDATE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "INSERT", "YES", null)));

            onPrestoAlice().executeQuery("GRANT SELECT ON hive.default.test_table_alice  TO bob WITH GRANT OPTION");
            onPrestoAlice().executeQuery("GRANT INSERT ON hive.default.test_table_alice  TO bob");

            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_alice"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "default", "test_table_alice", "INSERT", "YES", null),
                            row("alice", "USER", "bob", "USER", "hive", "default", "test_table_alice", "SELECT", "YES", null),
                            row("alice", "USER", "bob", "USER", "hive", "default", "test_table_alice", "INSERT", "NO", null)));
        }
        finally {
            onPrestoAlice().executeQuery("DROP TABLE IF EXISTS hive.default.test_table_alice");
            onPrestoBob().executeQuery("DROP TABLE IF EXISTS hive.default.test_table_bob");
            onTrino().executeQuery("REVOKE admin FROM alice IN hive");
        }
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testAdminCanShowGrantsOnlyFromCurrentSchema()
    {
        try {
            onPrestoBob().executeQuery("CREATE TABLE hive.default.test_table_bob (foo BIGINT)");
            onTrino().executeQuery("CREATE SCHEMA hive.test");
            onTrino().executeQuery("GRANT admin TO alice IN hive");
            onPrestoAlice().executeQuery("SET ROLE ADMIN IN hive");
            onPrestoAlice().executeQuery("CREATE TABLE hive.test.test_table_bob (foo BIGINT) with (external_location='/tmp')");

            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.default.test_table_bob"))
                    .containsOnly(ImmutableList.of(
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "SELECT", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "DELETE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "UPDATE", "YES", null),
                            row("bob", "USER", "bob", "USER", "hive", "default", "test_table_bob", "INSERT", "YES", null)));

            QueryAssert.assertThat(onPrestoAlice().executeQuery("SHOW GRANTS ON hive.test.test_table_bob"))
                    .containsOnly(ImmutableList.of(
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "SELECT", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "DELETE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "UPDATE", "YES", null),
                            row("alice", "USER", "alice", "USER", "hive", "test", "test_table_bob", "INSERT", "YES", null)));
            QueryAssert.assertThat(onPrestoAlice().executeQuery("SELECT * FROM hive.information_schema.table_privileges where table_name = 'test_table_bob'"))
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
            onPrestoBob().executeQuery("DROP TABLE IF EXISTS hive.default.test_table_bob");
            onPrestoAlice().executeQuery("DROP TABLE IF EXISTS hive.test.test_table_bob");
            onTrino().executeQuery("DROP SCHEMA IF EXISTS hive.test");
            onTrino().executeQuery("REVOKE admin FROM alice IN hive");
        }
    }

    @Test(groups = {ROLES, AUTHORIZATION, PROFILE_SPECIFIC_TESTS})
    public void testSetRoleTablePermissions()
    {
        onTrino().executeQuery("CREATE ROLE role1 IN hive");
        onTrino().executeQuery("CREATE ROLE role2 IN hive");

        onTrino().executeQuery("GRANT role1 TO USER bob IN hive");
        onTrino().executeQuery("GRANT role2 TO USER bob IN hive");

        onPrestoAlice().executeQuery("CREATE TABLE hive.default.test_table (foo BIGINT)");
        onPrestoAlice().executeQuery("GRANT SELECT ON hive.default.test_table TO ROLE role1");
        onPrestoAlice().executeQuery("GRANT INSERT ON hive.default.test_table TO ROLE role2");

        String select = "SELECT * FROM hive.default.test_table";
        String insert = "INSERT INTO hive.default.test_table (foo) VALUES (1)";

        assertAdminExecute(select);
        assertAdminExecute(insert);

        onPrestoBob().executeQuery(select);
        onPrestoBob().executeQuery(insert);
        QueryAssert.assertThat(onPrestoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null),
                        row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));

        onPrestoBob().executeQuery("SET ROLE ALL IN hive");
        onPrestoBob().executeQuery(select);
        onPrestoBob().executeQuery(insert);
        QueryAssert.assertThat(onPrestoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null),
                        row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));

        onPrestoBob().executeQuery("SET ROLE NONE IN hive");
        QueryAssert.assertQueryFailure(() -> onPrestoBob().executeQuery(select))
                .hasMessageContaining("Access Denied");
        QueryAssert.assertQueryFailure(() -> onPrestoBob().executeQuery(insert))
                .hasMessageContaining("Access Denied");
        QueryAssert.assertThat(onPrestoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of());

        onPrestoBob().executeQuery("SET ROLE role1 IN hive");
        onPrestoBob().executeQuery(select);
        QueryAssert.assertQueryFailure(() -> onPrestoBob().executeQuery(insert))
                .hasMessageContaining("Access Denied");
        QueryAssert.assertThat(onPrestoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role1", "ROLE", "hive", "default", "test_table", "SELECT", "NO", null)));

        onPrestoBob().executeQuery("SET ROLE role2 IN hive");
        QueryAssert.assertQueryFailure(() -> onPrestoBob().executeQuery(select))
                .hasMessageContaining("Access Denied");
        onPrestoBob().executeQuery(insert);
        QueryAssert.assertThat(onPrestoBob().executeQuery("SHOW GRANTS ON hive.default.test_table"))
                .containsOnly(ImmutableList.of(
                        row("alice", "USER", "role2", "ROLE", "hive", "default", "test_table", "INSERT", "NO", null)));

        onPrestoAlice().executeQuery("DROP TABLE hive.default.test_table");
    }

    private static void assertAdminExecute(String query)
    {
        onTrino().executeQuery("SET ROLE NONE IN hive");
        QueryAssert.assertQueryFailure(() -> onTrino().executeQuery(query))
                .hasMessageContaining("Access Denied");

        onTrino().executeQuery("SET ROLE ALL IN hive");
        QueryAssert.assertQueryFailure(() -> onTrino().executeQuery(query))
                .hasMessageContaining("Access Denied");

        onTrino().executeQuery("SET ROLE admin IN hive");
        onTrino().executeQuery(query);
    }

    private static QueryExecutor onPrestoAlice()
    {
        return connectToTrino("alice@presto");
    }

    private static QueryExecutor onPrestoBob()
    {
        return connectToTrino("bob@presto");
    }
}
