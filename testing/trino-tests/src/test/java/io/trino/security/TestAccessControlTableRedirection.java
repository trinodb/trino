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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.DisabledSystemSecurityMetadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.ADD_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.COMMENT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.COMMENT_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * This class aims to ensure that the access control is working as expected in the context of working with table redirections.
 * This class makes use of a mock connector as an alternative of working with a full-fledged connector supporting table redirects in order
 * to have lightweight requirements for the testing environment.
 *
 * @see TestAccessControl
 */
public class TestAccessControlTableRedirection
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "test_catalog";
    private static final String SCHEMA_NAME = "test_schema";

    private static final String REDIRECTION_SOURCE_TABLE_NAME = "redirection_source";
    private static final String REDIRECTION_TARGET_TABLE_NAME = "redirection_target";
    private static final SchemaTableName REDIRECTION_TARGET_SCHEMA_TABLE_NAME = new SchemaTableName(SCHEMA_NAME, REDIRECTION_TARGET_TABLE_NAME);

    private static final String DATA_COLUMN_NAME = "data_column";
    private static final String ID_COLUMN_NAME = "id_column";

    private static final Map<String, Set<String>> SCHEMA_TABLE_MAPPING = ImmutableMap.of(
            SCHEMA_NAME,
            ImmutableSet.of(REDIRECTION_SOURCE_TABLE_NAME, REDIRECTION_TARGET_TABLE_NAME));

    private static final Map<SchemaTableName, SchemaTableName> TABLE_REDIRECTIONS = ImmutableMap.<SchemaTableName, SchemaTableName>builder()
            .put(schemaTableName(SCHEMA_NAME, REDIRECTION_SOURCE_TABLE_NAME), schemaTableName(SCHEMA_NAME, REDIRECTION_TARGET_TABLE_NAME))
            .buildOrThrow();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA_NAME)
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setAdditionalModule(binder -> {
                    newOptionalBinder(binder, SystemSecurityMetadata.class)
                            .setBinding()
                            .toInstance(new DisabledSystemSecurityMetadata()
                            {
                                @Override
                                public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
                                {
                                }

                                @Override
                                public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
                                {
                                }

                                @Override
                                public boolean roleExists(Session session, String role)
                                {
                                    return true;
                                }

                                @Override
                                public void setTableOwner(Session session, CatalogSchemaTableName table, TrinoPrincipal principal)
                                {
                                }

                                @Override
                                public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
                                {
                                }
                            });
                })
                .build();
        queryRunner.installPlugin(new MockConnectorPlugin(createMockConnectorFactory()));
        queryRunner.createCatalog(CATALOG_NAME, "mock", ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testSelect()
    {
        assertAccessAllowed("SELECT * FROM redirection_source");
        assertAccessDenied(
                "SELECT * FROM redirection_source",
                "Cannot select from columns \\[data_column, id_column] in table or view test_catalog.test_schema.redirection_target",
                privilege("redirection_target.data_column", SELECT_COLUMN));
    }

    @Test
    public void testDropTable()
    {
        assertAccessAllowed("DROP TABLE redirection_source");
        assertAccessDenied(
                "DROP TABLE redirection_source",
                "Cannot drop table test_catalog.test_schema.redirection_target",
                privilege(REDIRECTION_TARGET_TABLE_NAME, DROP_TABLE));
    }

    @Test
    public void testCommentTable()
    {
        assertAccessAllowed("COMMENT ON TABLE redirection_source IS 'This is my redirection target table'");
        assertAccessDenied(
                "COMMENT ON TABLE redirection_source IS 'This is my redirection target table'",
                "Cannot comment table to test_catalog.test_schema.redirection_target",
                privilege(REDIRECTION_TARGET_TABLE_NAME, COMMENT_TABLE));
    }

    @Test
    public void testCommentColumn()
    {
        assertAccessAllowed("COMMENT ON COLUMN redirection_source.data_column IS 'Data is the new oil'");
        assertAccessDenied(
                "COMMENT ON COLUMN redirection_source.data_column IS 'Data is the new oil'",
                "Cannot comment column to test_catalog.test_schema.redirection_target",
                privilege(REDIRECTION_TARGET_TABLE_NAME, COMMENT_COLUMN));
    }

    @Test
    public void testShowGrants()
    {
        assertAccessAllowed("SHOW GRANTS ON redirection_target");
        assertQueryFails(
                "SHOW GRANTS ON redirection_source",
                ".* Table redirection_source is redirected to test_catalog.test_schema.redirection_target and SHOW GRANTS is not supported with table redirections");
    }

    @Test
    public void testGrant()
    {
        assertAccessAllowed("GRANT SELECT ON redirection_target TO ROLE PUBLIC");
        assertQueryFails(
                "GRANT SELECT ON redirection_source TO ROLE PUBLIC",
                ".* Table test_catalog.test_schema.redirection_source is redirected to test_catalog.test_schema.redirection_target and GRANT is not supported with table redirections");
    }

    @Test
    public void testRevoke()
    {
        assertAccessAllowed("REVOKE SELECT ON redirection_target FROM ROLE PUBLIC");
        assertQueryFails(
                "REVOKE SELECT ON redirection_source FROM ROLE PUBLIC",
                ".* Table test_catalog.test_schema.redirection_source is redirected to test_catalog.test_schema.redirection_target and REVOKE is not supported with table redirections");
    }

    @Test
    public void testSetTableAuthorization()
    {
        assertAccessAllowed("ALTER TABLE redirection_target SET AUTHORIZATION ROLE PUBLIC");
        assertQueryFails(
                "ALTER TABLE redirection_source SET AUTHORIZATION ROLE PUBLIC",
                ".* Table test_catalog.test_schema.redirection_source is redirected to test_catalog.test_schema.redirection_target and SET TABLE AUTHORIZATION is not supported with table redirections");
    }

    @Test
    public void testDeny()
    {
        assertAccessAllowed("DENY DELETE ON redirection_target TO ROLE PUBLIC");
        assertQueryFails(
                "DENY DELETE ON redirection_source TO ROLE PUBLIC",
                ".* Table test_catalog.test_schema.redirection_source is redirected to test_catalog.test_schema.redirection_target and DENY is not supported with table redirections");
    }

    @Test
    public void testAddColumn()
    {
        assertAccessAllowed("ALTER TABLE redirection_source ADD COLUMN a_new_column integer");
        assertAccessDenied(
                "ALTER TABLE redirection_source ADD COLUMN a_new_column integer",
                "Cannot add a column to table test_catalog.test_schema.redirection_target",
                privilege(REDIRECTION_TARGET_TABLE_NAME, ADD_COLUMN));
    }

    @Test
    public void testDropColumn()
    {
        assertAccessAllowed("ALTER TABLE redirection_source DROP COLUMN " + DATA_COLUMN_NAME);
        assertAccessDenied(
                "ALTER TABLE redirection_source DROP COLUMN " + DATA_COLUMN_NAME,
                "Cannot drop a column from table test_catalog.test_schema.redirection_target",
                privilege(REDIRECTION_TARGET_TABLE_NAME, DROP_COLUMN));
    }

    @Test
    public void testRenameColumn()
    {
        assertAccessAllowed("ALTER TABLE redirection_source RENAME COLUMN data_column TO new_oil_column");
        assertAccessDenied(
                "ALTER TABLE redirection_source RENAME COLUMN data_column TO new_oil_column",
                "Cannot rename a column in table test_catalog.test_schema.redirection_target",
                privilege(REDIRECTION_TARGET_TABLE_NAME, RENAME_COLUMN));
    }

    @Test
    public void testRenameTable()
    {
        assertAccessAllowed("ALTER TABLE redirection_source RENAME TO renamed_table");
        assertAccessDenied(
                "ALTER TABLE redirection_source RENAME TO renamed_table",
                "Cannot rename table from test_catalog.test_schema.redirection_target to test_catalog.test_schema.renamed_table",
                privilege(REDIRECTION_TARGET_TABLE_NAME, RENAME_TABLE));
    }

    private static MockConnectorFactory createMockConnectorFactory()
    {
        return MockConnectorFactory.builder()
                .withListTables((session, schemaName) -> SCHEMA_TABLE_MAPPING.getOrDefault(schemaName, ImmutableSet.of()).stream()
                        .map(name -> new SchemaTableName(schemaName, name))
                        .collect(toImmutableList()))
                .withGetTableHandle((session, tableName) -> {
                    if (SCHEMA_TABLE_MAPPING.getOrDefault(tableName.getSchemaName(), ImmutableSet.of()).contains(tableName.getTableName())
                            && !TABLE_REDIRECTIONS.containsKey(tableName)) {
                        return new MockConnectorTableHandle(tableName);
                    }
                    return null;
                })
                .withGetViews(((connectorSession, prefix) -> ImmutableMap.of()))
                .withRedirectTable(((connectorSession, schemaTableName) -> Optional.ofNullable(TABLE_REDIRECTIONS.get(schemaTableName))
                        .map(target -> new CatalogSchemaTableName(CATALOG_NAME, target))))
                .withGetColumns(schemaTableName -> {
                    if (REDIRECTION_TARGET_SCHEMA_TABLE_NAME.equals(schemaTableName)) {
                        return ImmutableList.of(new ColumnMetadata(ID_COLUMN_NAME, INTEGER), new ColumnMetadata(DATA_COLUMN_NAME, VARCHAR));
                    }
                    throw new RuntimeException("Columns do not exist for: " + schemaTableName);
                })
                .build();
    }
}
