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
import io.trino.connector.MockConnectorTableHandle;
import io.trino.connector.MutableGrants;
import io.trino.metadata.DisabledSystemSecurityMetadata;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.SchemaTableName;
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
import java.util.Set;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.common.Randoms.randomUsername;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ROOT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestDenyOnSchema
{
    private final SchemaTableName table = new SchemaTableName("default", "table_one");
    private final Session admin = sessionOf("admin");
    private final Grants<SchemaTableName> tableGrants = new MutableGrants<>();
    private DistributedQueryRunner queryRunner;
    private QueryAssertions assertions;

    private CatalogSchemaName expectedSchemaName;
    private Set<Privilege> expectedPrivileges;
    private TrinoPrincipal expectedGrantee;
    private boolean denyCalled;

    @BeforeClass
    public void initClass()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(admin)
                .setAdditionalModule(binder -> {
                    newOptionalBinder(binder, SystemSecurityMetadata.class)
                            .setBinding()
                            .toInstance(new DisabledSystemSecurityMetadata()
                            {
                                @Override
                                public void denySchemaPrivileges(Session session,
                                        CatalogSchemaName schemaName,
                                        Set<Privilege> privileges,
                                        TrinoPrincipal grantee)
                                {
                                    assertThat(expectedSchemaName).isEqualTo(schemaName);
                                    assertThat(expectedPrivileges).isEqualTo(privileges);
                                    assertThat(expectedGrantee).isEqualTo(grantee);
                                    assertThat(denyCalled).isFalse();
                                    denyCalled = true;
                                }
                            });
                })
                .build();
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(session -> ImmutableList.of("default"))
                .withListTables((session, schemaName) -> "default".equalsIgnoreCase(schemaName) ? ImmutableList.of(table.getSchemaName()) : ImmutableList.of())
                .withGetTableHandle((session, tableName) -> tableName.equals(table) ? new MockConnectorTableHandle(tableName) : null)
                .build();
        queryRunner.installPlugin(new MockConnectorPlugin(connectorFactory));
        queryRunner.createCatalog("local", "mock");
        assertions = new QueryAssertions(queryRunner);
        tableGrants.grant(new TrinoPrincipal(USER, "admin"), table, EnumSet.allOf(Privilege.class), true);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
        queryRunner = null; // closed by assertions.close
    }

    @Test(dataProvider = "privileges")
    public void testValidDenySchema(String privilege)
    {
        String username = randomUsername();

        denyCalled = false;
        expectedSchemaName = new CatalogSchemaName("local", "default");
        if (privilege.equalsIgnoreCase("all privileges")) {
            expectedPrivileges = ImmutableSet.copyOf(Privilege.values());
        }
        else {
            expectedPrivileges = ImmutableSet.of(Privilege.valueOf(privilege.toUpperCase(ROOT)));
        }
        expectedGrantee = new TrinoPrincipal(USER, username);

        queryRunner.execute(admin, format("DENY %s ON SCHEMA default TO %s", privilege, username));
        assertThat(denyCalled).isTrue();
    }

    @Test(dataProvider = "privileges")
    public void testDenyOnNonExistingCatalog(String privilege)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY %s ON SCHEMA missing_catalog.missing_schema TO %s", privilege, randomUsername())))
                .hasMessageContaining("Schema 'missing_catalog.missing_schema' does not exist");
    }

    @Test(dataProvider = "privileges")
    public void testDenyOnNonExistingSchema(String privilege)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY %s ON SCHEMA missing_schema TO %s", privilege, randomUsername())))
                .hasMessageContaining("Schema 'local.missing_schema' does not exist");
    }

    @DataProvider(name = "privileges")
    public static Object[][] privileges()
    {
        return new Object[][] {
                {"CREATE"},
                {"SELECT"},
                {"INSERT"},
                {"UPDATE"},
                {"DELETE"},
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
