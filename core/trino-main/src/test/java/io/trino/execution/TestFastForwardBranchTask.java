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
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.tree.FastForwardBranch;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.BRANCH_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

final class TestFastForwardBranchTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    void testFastForwardNotExistingBranch()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        assertBranches(tableName, "main");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeFastForwardBranch(asQualifiedName(tableName), "main", "not_found")))
                .hasErrorCode(BRANCH_NOT_FOUND)
                .hasMessage("line 1:1: Branch 'not_found' does not exist");
        assertTrinoExceptionThrownBy(() -> getFutureValue(executeFastForwardBranch(asQualifiedName(tableName), "not_found", "main")))
                .hasErrorCode(BRANCH_NOT_FOUND)
                .hasMessage("line 1:1: Branch 'not_found' does not exist");
        assertBranches(tableName, "main");
    }

    @Test
    void testFastForwardSameBranch()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        assertBranches(tableName, "main");

        getFutureValue(executeFastForwardBranch(asQualifiedName(tableName), "main", "main"));
        assertBranches(tableName, "main");
    }

    @Test
    void testFastForwardBranchOnNotExistingTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeFastForwardBranch(asQualifiedName(tableName), "main", "main")))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessage("line 1:1: Table 'test_catalog.schema.existing_table' does not exist");
    }

    @Test
    void testFastForwardBranchOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeFastForwardBranch(asQualifiedName(viewName), "main", "main")))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Fast forwarding branch on view is not supported");
    }

    @Test
    void testFastForwardBranchOnMaterializedView()
    {
        QualifiedName viewName = qualifiedName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(viewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(executeFastForwardBranch(viewName, "main", "main")))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Fast forwarding branch on materialized view is not supported");
    }

    private ListenableFuture<Void> executeFastForwardBranch(QualifiedName tableName, String fromBranch, String toBranch)
    {
        return new FastForwardBranchTask(metadata, new AllowAllAccessControl())
                .execute(new FastForwardBranch(new NodeLocation(1, 1), tableName, new Identifier(fromBranch), new Identifier(toBranch)), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }

    private void assertBranches(QualifiedObjectName tableName, String... expectedBranches)
    {
        assertThat(metadata.listBranches(testSession, tableName))
                .containsExactlyInAnyOrder(expectedBranches);
    }
}
