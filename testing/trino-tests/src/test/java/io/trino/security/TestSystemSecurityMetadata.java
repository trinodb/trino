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
import io.trino.Session;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.testing.TestingSession.testSessionBuilder;

@Test(singleThreaded = true) // singleThreaded is because TestingSystemSecurityMetadata is stateful and is shared between tests
public class TestSystemSecurityMetadata
        extends AbstractTestQueryFramework
{
    private final TestingSystemSecurityMetadata securityMetadata = new TestingSystemSecurityMetadata();

    @BeforeMethod
    public void reset()
    {
        securityMetadata.reset();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("blackhole")
                .setSchema("default")
                .build();
        return DistributedQueryRunner.builder(session)
                .setAdditionalModule(binder -> {
                    newOptionalBinder(binder, SystemSecurityMetadata.class)
                            .setBinding()
                            .toInstance(securityMetadata);
                })
                .setNodeCount(1)
                .build();
    }

    @Test
    public void testNoSystemRoles()
    {
        assertQueryReturnsEmptyResult("SHOW ROLES");
        assertQueryReturnsEmptyResult("SHOW CURRENT ROLES");
        assertQueryReturnsEmptyResult("SHOW ROLE GRANTS");
        assertQueryReturnsEmptyResult("SELECT * FROM system.information_schema.applicable_roles");
    }

    @Test
    public void testRoleCreationAndDeletion()
    {
        assertQueryReturnsEmptyResult("SHOW ROLES");

        assertQuerySucceeds("CREATE ROLE role1");
        assertQuery("SHOW ROLES", "VALUES 'role1'");

        assertQuerySucceeds("DROP ROLE role1");
        assertQueryReturnsEmptyResult("SHOW ROLES");
    }

    @Test
    public void testRoleGrant()
    {
        Session alice = user("alice");
        Session aliceWithRole = user("alice", "role1");

        assertQuerySucceeds("CREATE ROLE role1");

        assertQueryFails(alice, "SET ROLE role1", "Access Denied: Cannot set role role1");
        assertQuery(alice, "SHOW ROLES", "VALUES 'role1'");
        assertQueryReturnsEmptyResult(alice, "SHOW CURRENT ROLES");
        assertQueryReturnsEmptyResult(alice, "SHOW ROLE GRANTS");
        assertQueryReturnsEmptyResult(alice, "SELECT * FROM system.information_schema.applicable_roles");
        assertQueryFails(aliceWithRole, "SHOW ROLES", "Access Denied: Cannot set role role1");
        assertQueryFails(aliceWithRole, "SHOW CURRENT ROLES", "Access Denied: Cannot set role role1");
        assertQueryFails(aliceWithRole, "SHOW ROLE GRANTS", "Access Denied: Cannot set role role1");
        assertQueryFails(aliceWithRole, "SELECT * FROM system.information_schema.applicable_roles", "Access Denied: Cannot set role role1");

        assertQuerySucceeds("GRANT role1 TO USER alice");
        assertQuerySucceeds(alice, "SET ROLE role1");
        assertQuery(alice, "SHOW ROLES", "VALUES 'role1'");
        assertQuery(alice, "SHOW CURRENT ROLES", "VALUES 'role1'");
        assertQuery(alice, "SHOW ROLE GRANTS", "VALUES 'role1'");
        assertQuery(alice, "SELECT * FROM system.information_schema.applicable_roles", "SELECT 'alice', 'USER', 'role1', 'NO'");
        assertQuery(aliceWithRole, "SHOW ROLES", "VALUES 'role1'");
        assertQuery(aliceWithRole, "SHOW CURRENT ROLES", "VALUES 'role1'");
        assertQuery(aliceWithRole, "SHOW ROLE GRANTS", "VALUES 'role1'");
        assertQuery(aliceWithRole, "SELECT * FROM system.information_schema.applicable_roles", "SELECT 'alice', 'USER', 'role1', 'NO'");

        assertQuerySucceeds("REVOKE role1 FROM USER alice");
        assertQuery(alice, "SHOW ROLES", "VALUES 'role1'");
        assertQueryReturnsEmptyResult(alice, "SHOW CURRENT ROLES");
        assertQueryReturnsEmptyResult(alice, "SHOW ROLE GRANTS");
        assertQueryReturnsEmptyResult(alice, "SELECT * FROM system.information_schema.applicable_roles");
        assertQueryFails(aliceWithRole, "SHOW ROLES", "Access Denied: Cannot set role role1");
        assertQueryFails(aliceWithRole, "SHOW CURRENT ROLES", "Access Denied: Cannot set role role1");
        assertQueryFails(aliceWithRole, "SHOW ROLE GRANTS", "Access Denied: Cannot set role role1");
        assertQueryFails(aliceWithRole, "SELECT * FROM system.information_schema.applicable_roles", "Access Denied: Cannot set role role1");

        assertQuerySucceeds("DROP ROLE role1");
    }

    @Test
    public void testTransitiveRoleGrant()
    {
        Session alice = user("alice");
        Session aliceWithRole = user("alice", "role2");

        assertQuerySucceeds("CREATE ROLE role1");
        assertQuerySucceeds("CREATE ROLE role2");
        assertQuerySucceeds("GRANT role1 TO USER alice");

        String roleNotApplicableErrorMessage = "Access Denied: Cannot set role role2";
        assertQueryFails(aliceWithRole, "SHOW ROLES", roleNotApplicableErrorMessage);

        assertQuerySucceeds("GRANT role2 TO ROLE role1");
        assertQuery(alice, "SHOW ROLES", "VALUES 'role1', 'role2'");
        assertQuery(alice, "SHOW CURRENT ROLES", "VALUES 'role1', 'role2'");
        assertQuery(alice, "SHOW ROLE GRANTS", "VALUES 'role1'");
        assertQuery(
                aliceWithRole,
                "SELECT * FROM system.information_schema.applicable_roles",
                "VALUES "
                        + "('alice', 'USER', 'role1', 'NO'),"
                        + "('role1', 'ROLE', 'role2', 'NO')");

        assertQuerySucceeds("REVOKE role2 FROM ROLE role1");
        assertQueryFails(aliceWithRole, "SHOW ROLES", roleNotApplicableErrorMessage);

        assertQuerySucceeds("REVOKE role1 FROM USER alice");
        assertQuerySucceeds("DROP ROLE role1");
    }

    private static Session user(String alice, String... roles)
    {
        return testSessionBuilder()
                .setIdentity(Identity.forUser(alice)
                        .withEnabledRoles(ImmutableSet.copyOf(roles))
                        .build())
                .build();
    }
}
