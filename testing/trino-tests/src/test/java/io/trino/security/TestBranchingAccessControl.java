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
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_DATA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_SCHEMA;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_COLUMNS;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.UPDATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.allExceptBranchPrivilege;
import static io.trino.testing.TestingAccessControlManager.branchPrivilege;
import static io.trino.testing.TestingAccessControlManager.privilege;

final class TestBranchingAccessControl
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION);

        ConnectorViewDefinition view = new ConnectorViewDefinition(
                "SELECT nationkey FROM mock.tiny.nation FOR VERSION AS OF 'dev'",
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorViewDefinition.ViewColumn("nationkey", BigintType.BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of("owner"),
                false,
                ImmutableList.of());

        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withGetTableHandle((_, schemaTableName) -> new MockConnectorTableHandle(schemaTableName))
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    throw new UnsupportedOperationException();
                })
                .withBranches(ImmutableList.of("main", "dev"))
                .withData(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_DATA;
                    }
                    throw new UnsupportedOperationException();
                })
                .withGetViews((_, _) -> ImmutableMap.of(new SchemaTableName("tiny", "nation_view"), view))
                .build()));
        queryRunner.createCatalog("mock", "mock", ImmutableMap.of());

        return queryRunner;
    }

    @Test
    void testSelectFromBranchDeniedByBranchPrivilege()
    {
        assertAccessDenied(
                "SELECT nationkey FROM mock.tiny.nation FOR VERSION AS OF 'dev'",
                "Cannot select from columns \\[nationkey] in branch dev in table mock.tiny.nation",
                branchPrivilege("nation", "dev", SELECT_COLUMN));

        // SELECT without branch is still allowed
        assertAccessAllowed(
                "SELECT nationkey FROM mock.tiny.nation",
                branchPrivilege("nation", "dev", SELECT_COLUMN));

        // SELECT from a different branch is still allowed
        assertAccessAllowed(
                "SELECT nationkey FROM mock.tiny.nation FOR VERSION AS OF 'main'",
                branchPrivilege("nation", "dev", SELECT_COLUMN));

        // only grant on branch allows access
        assertAccessAllowed(
                "SELECT nationkey FROM mock.tiny.nation FOR VERSION AS OF 'main'",
                allExceptBranchPrivilege("nation", "main", SELECT_COLUMN));

        // corner case: SELECT * creates a synthetic scope:
        // if branch information is not copied correctly between scopes, it will try to access table with no branch
        assertAccessAllowed(
                "SELECT * FROM mock.tiny.nation FOR VERSION AS OF 'main'",
                allExceptBranchPrivilege("nation", "main", SELECT_COLUMN));
    }

    @Test
    void testSelectFromBranchDeniedByTablePrivilege()
    {
        // Denying without branch blocks all access, including branch access
        // (This actually depends on specific semantics of the access control, but at least we'll test the error messages)
        assertAccessDenied(
                "SELECT nationkey FROM mock.tiny.nation FOR VERSION AS OF 'dev'",
                "Cannot select from columns \\[nationkey] in branch dev in table mock.tiny.nation",
                privilege("nation", SELECT_COLUMN));
    }

    @Test
    void testInsertIntoBranchDeniedByBranchPrivilege()
    {
        assertAccessDenied(
                "INSERT INTO mock.tiny.nation @ dev VALUES (101, 'POLAND', 0, 'No comment')",
                "Cannot insert into branch dev in table mock.tiny.nation",
                branchPrivilege("nation", "dev", INSERT_TABLE));

        // INSERT without branch is still allowed
        assertAccessAllowed(
                "INSERT INTO mock.tiny.nation VALUES (101, 'POLAND', 0, 'No comment')",
                branchPrivilege("nation", "dev", INSERT_TABLE));

        // INSERT into a different branch is still allowed
        assertAccessAllowed(
                "INSERT INTO mock.tiny.nation @ main VALUES (101, 'POLAND', 0, 'No comment')",
                branchPrivilege("nation", "dev", INSERT_TABLE));

        // only grant on branch allows access
        assertAccessAllowed(
                "INSERT INTO mock.tiny.nation @ main VALUES (101, 'POLAND', 0, 'No comment')",
                allExceptBranchPrivilege("nation", "main", INSERT_TABLE));
    }

    @Test
    void testInsertIntoBranchDeniedByTablePrivilege()
    {
        // Denying without branch blocks all access, including branch access
        // (This actually depends on specific semantics of the access control, but at least we'll test the error messages)
        assertAccessDenied(
                "INSERT INTO mock.tiny.nation @ dev VALUES (101, 'POLAND', 0, 'No comment')",
                "Cannot insert into branch dev in table mock.tiny.nation",
                privilege("nation", INSERT_TABLE));
    }

    @Test
    void testDeleteFromBranchDeniedByBranchPrivilege()
    {
        assertAccessDenied(
                "DELETE FROM mock.tiny.nation @ dev WHERE nationkey < 0",
                "Cannot delete from branch dev in table mock.tiny.nation",
                branchPrivilege("nation", "dev", DELETE_TABLE));

        // DELETE without branch is still allowed
        assertAccessAllowed(
                "DELETE FROM mock.tiny.nation WHERE nationkey < 0",
                branchPrivilege("nation", "dev", DELETE_TABLE));

        // DELETE from a different branch is still allowed
        assertAccessAllowed(
                "DELETE FROM mock.tiny.nation @ main WHERE nationkey < 0",
                branchPrivilege("nation", "dev", DELETE_TABLE));

        // only grant on branch allows access
        assertAccessAllowed(
                "DELETE FROM mock.tiny.nation @ main WHERE nationkey < 0",
                allExceptBranchPrivilege("nation", "main", DELETE_TABLE));
    }

    @Test
    void testDeleteFromBranchDeniedByTablePrivilege()
    {
        // Denying without branch blocks all access, including branch access
        // (This actually depends on specific semantics of the access control, but at least we'll test the error messages)
        assertAccessDenied(
                "DELETE FROM mock.tiny.nation @ dev WHERE nationkey < 0",
                "Cannot delete from branch dev in table mock.tiny.nation",
                privilege("nation", DELETE_TABLE));
    }

    @Test
    void testUpdateBranchDeniedByBranchPrivilege()
    {
        assertAccessDenied(
                "UPDATE mock.tiny.nation @ dev SET regionkey = 0 WHERE nationkey < 0",
                "Cannot update columns \\[regionkey] in branch dev in table mock.tiny.nation",
                branchPrivilege("nation", "dev", UPDATE_TABLE));

        // UPDATE without branch is still allowed
        assertAccessAllowed(
                "UPDATE mock.tiny.nation SET regionkey = 0 WHERE nationkey < 0",
                branchPrivilege("nation", "dev", UPDATE_TABLE));

        // UPDATE on a different branch is still allowed
        assertAccessAllowed(
                "UPDATE mock.tiny.nation @ main SET regionkey = 0 WHERE nationkey < 0",
                branchPrivilege("nation", "dev", UPDATE_TABLE));

        // only grant on branch allows access
        assertAccessAllowed(
                "UPDATE mock.tiny.nation @ main SET regionkey = 0 WHERE nationkey < 0",
                allExceptBranchPrivilege("nation", "main", UPDATE_TABLE));
    }

    @Test
    void testUpdateBranchDeniedByTablePrivilege()
    {
        // Denying without branch blocks all access, including branch access
        // (This actually depends on specific semantics of the access control, but at least we'll test the error messages)
        assertAccessDenied(
                "UPDATE mock.tiny.nation @ dev SET regionkey = 0 WHERE nationkey < 0",
                "Cannot update columns \\[regionkey] in branch dev in table mock.tiny.nation",
                privilege("nation", UPDATE_TABLE));
    }

    @Test
    void testSelectFromViewReferencingBranchDeniedByBranchPrivilege()
    {
        assertAccessDenied(
                "SELECT nationkey FROM mock.tiny.nation_view",
                "View owner does not have sufficient privileges: View owner 'owner' cannot create view that selects from branch dev in mock.tiny.nation",
                branchPrivilege("owner", "nation", "dev", CREATE_VIEW_WITH_SELECT_COLUMNS));
    }

    @Test
    void testSelectFromViewReferencingBranchDeniedByTablePrivilege()
    {
        // Denying without branch blocks all access, including branch access
        // (This actually depends on specific semantics of the access control, but at least we'll test the error messages)
        assertAccessDenied(
                "SELECT nationkey FROM mock.tiny.nation_view",
                "View owner does not have sufficient privileges: View owner 'owner' cannot create view that selects from branch dev in mock.tiny.nation",
                privilege("owner", "nation", CREATE_VIEW_WITH_SELECT_COLUMNS));
    }
}
