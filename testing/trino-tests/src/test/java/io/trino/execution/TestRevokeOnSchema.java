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
        queryRunner = null; // closed by assertions.close
    }

    @Test
    public void testRevokeOnSchema()
    {
        testRevokeOnSchema("SELECT", userWithSelect);
        testRevokeOnSchema("ALL PRIVILEGES", userWithAllPrivileges);
    }

    private void testRevokeOnSchema(String privilege, Session user)
    {
        assertThat(assertions.query(user, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema'), (VARCHAR 'default')");

        queryRunner.execute(admin, format("REVOKE %s ON SCHEMA default FROM %s", privilege, user.getUser()));

        assertThat(assertions.query(user, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema')");
    }

    @Test
    public void testRevokeOnNonExistingCatalog()
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE SELECT ON SCHEMA missing_catalog.missing_schema FROM %s", userWithSelect.getUser())))
                .hasMessageContaining("Schema 'missing_catalog.missing_schema' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE ALL PRIVILEGES ON SCHEMA missing_catalog.missing_schema FROM %s", userWithAllPrivileges.getUser())))
                .hasMessageContaining("Schema 'missing_catalog.missing_schema' does not exist");
    }

    @Test
    public void testRevokeOnNonExistingSchema()
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE SELECT ON SCHEMA missing_schema FROM %s", userWithSelect.getUser())))
                .hasMessageContaining("Schema 'local.missing_schema' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE ALL PRIVILEGES ON SCHEMA missing_schema FROM %s", userWithAllPrivileges.getUser())))
                .hasMessageContaining("Schema 'local.missing_schema' does not exist");
    }

    @Test
    public void testAccessDenied()
    {
        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE CREATE ON SCHEMA default FROM %s", randomUsername())))
                .hasMessageContaining("Access Denied: Cannot revoke privilege CREATE on schema default");
        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE SELECT ON SCHEMA default FROM %s", randomUsername())))
                .hasMessageContaining("Access Denied: Cannot revoke privilege SELECT on schema default");
        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE ALL PRIVILEGES ON SCHEMA default FROM %s", randomUsername())))
                .hasMessageContaining("Access Denied: Cannot revoke privilege CREATE on schema default");
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
