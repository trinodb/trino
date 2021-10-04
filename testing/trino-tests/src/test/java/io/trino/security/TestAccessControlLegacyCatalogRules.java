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
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.security.Identity;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestAccessControlLegacyCatalogRules
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("default")
                .setIdentity(Identity.forUser("alice")
                        .withConnectorRoles(ImmutableMap.of("mock", new SelectedRole(ROLE, Optional.of("alice_role"))))
                        .build())
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setCoordinatorProperties(ImmutableMap.of("deprecated.legacy-catalog-roles", "true"))
                .setNodeCount(1)
                .build();
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withListRoleGrants((connectorSession, roles, grantees, limit) -> ImmutableSet.of(new RoleGrant(new TrinoPrincipal(USER, "alice"), "alice_role", false)))
                .build()));
        queryRunner.createCatalog("mock", "mock");
        return queryRunner;
    }

    @Test
    public void testNoCatalogIsNeededInSessionForShowRoles()
    {
        assertQuery("SHOW ROLES", "VALUES 'alice_role'");
        assertQuery("SHOW ROLE GRANTS", "VALUES 'alice_role'");
        assertQuery("SHOW CURRENT ROLES", "VALUES 'alice_role'");
        assertQuery("SELECT * FROM mock.information_schema.applicable_roles", "SELECT 'alice', 'USER', 'alice_role', 'NO'");
    }
}
