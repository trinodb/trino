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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.tree.DropBranch;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.BRANCH_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDropBranchTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    void testDropBranch()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        TableHandle table = metadata.getTableHandle(testSession, tableName).orElseThrow();
        metadata.createBranch(testSession, table, "dev", Optional.empty(), FAIL, Map.of());
        assertBranches(tableName, "main", "dev");

        getFutureValue(executeDropBranch(asQualifiedName(tableName), false, "dev"));
        assertBranches(tableName, "main");
    }

    @Test
    void testDropBranchIfExists()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        assertBranches(tableName, "main");

        getFutureValue(executeDropBranch(asQualifiedName(tableName), true, "non-existing"));
        assertBranches(tableName, "main");
    }

    @Test
    void testDropNotExistingBranch()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropBranch(asQualifiedName(tableName), false, "not_found")))
                .hasErrorCode(BRANCH_NOT_FOUND)
                .hasMessage("line 1:1: Branch 'not_found' does not exist");
    }

    @Test
    void testDropBranchOnNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropBranch(asQualifiedName(tableName), false, "not_found")))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("line 1:1: Table 'test_catalog.schema.existing_table' does not exist");
    }

    @Test
    void testDropBranchOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropBranch(asQualifiedName(viewName), false, "main")))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Dropping branch from view is not supported");
    }

    @Test
    void testDropBranchOnMaterializedView()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeDropBranch(viewName, false, "main")))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Dropping branch from materialized view is not supported");
    }

    private ListenableFuture<Void> executeDropBranch(QualifiedName tableName, boolean exists, String branchName)
    {
        return new DropBranchTask(metadata, new AllowAllAccessControl())
                .execute(new DropBranch(new NodeLocation(1, 1), tableName, exists, new Identifier(branchName)), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }

    private void assertBranches(QualifiedObjectName tableName, String... expectedBranches)
    {
        assertThat(metadata.listBranches(testSession, tableName))
                .containsExactlyInAnyOrder(expectedBranches);
    }
}
