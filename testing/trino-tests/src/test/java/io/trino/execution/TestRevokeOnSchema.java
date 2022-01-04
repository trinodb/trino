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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.Grants;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MutableGrants;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.EnumSet;

import static io.trino.common.Randoms.randomUsername;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRevokeOnSchema
{
    private static final Session admin = sessionOf("admin");
    private static final Session userWithAllPrivileges = sessionOf(randomUsername());
    private static final Session userWithSelect = sessionOf(randomUsername());
    private DistributedQueryRunner queryRunner;
    private QueryAssertions assertions;

    @BeforeClass
    public void initClass()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(userWithAllPrivileges).build();
        Grants<String> schemaGrants = new MutableGrants<>();
        schemaGrants.grant(new TrinoPrincipal(USER, admin.getUser()), "default", EnumSet.allOf(Privilege.class), true);
        schemaGrants.grant(new TrinoPrincipal(USER, userWithAllPrivileges.getUser()), "default", EnumSet.allOf(Privilege.class), true);
        schemaGrants.grant(new TrinoPrincipal(USER, userWithSelect.getUser()), "default", ImmutableSet.of(Privilege.SELECT), true);
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(session -> ImmutableList.of("information_schema", "default"))
                .withSchemaGrants(schemaGrants)
                .build();
        queryRunner.installPlugin(new MockConnectorPlugin(connectorFactory));
        queryRunner.createCatalog("local", "mock");
        assertions = new QueryAssertions(queryRunner);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test(dataProvider = "privilegesAndUsers")
    public void testRevokeOnSchema(String privilege, Session user)
    {
        assertThat(assertions.query(user, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema'), (VARCHAR 'default')");

        queryRunner.execute(admin, format("REVOKE %s ON SCHEMA default FROM %s", privilege, user.getUser()));

        assertThat(assertions.query(user, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema')");
    }

    @Test(dataProvider = "privilegesAndUsers")
    public void testRevokeOnNonExistingCatalog(String privilege, Session user)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE %s ON SCHEMA missing_catalog.missing_schema FROM %s", privilege, user.getUser())))
                .hasMessageContaining("Schema 'missing_catalog.missing_schema' does not exist");
    }

    @Test(dataProvider = "privilegesAndUsers")
    public void testRevokeOnNonExistingSchema(String privilege, Session user)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE %s ON SCHEMA missing_schema FROM %s", privilege, user.getUser())))
                .hasMessageContaining("Schema 'local.missing_schema' does not exist");
    }

    @Test(dataProvider = "privileges")
    public void testAccessDenied(String privilege)
    {
        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE %s ON SCHEMA default FROM %s", privilege, randomUsername())))
                .hasMessageContaining(
                        "Access Denied: Cannot revoke privilege %s on schema default",
                        privilege.equals("ALL PRIVILEGES") ? "CREATE" : privilege);
    }

    @DataProvider(name = "privilegesAndUsers")
    public static Object[][] privilegesAndUsers()
    {
        return new Object[][] {
                {"SELECT", userWithSelect},
                {"ALL PRIVILEGES", userWithAllPrivileges}
        };
    }

    @DataProvider(name = "privileges")
    public static Object[][] privileges()
    {
        return new Object[][] {
                {"CREATE"},
                {"SELECT"},
                {"ALL PRIVILEGES"}
        };
    }

    private static Session sessionOf(String username)
    {
        return testSessionBuilder()
                .setIdentity(Identity.ofUser(username))
                .setCatalog("local")
                .setSchema("default")
                .build();
    }
}
