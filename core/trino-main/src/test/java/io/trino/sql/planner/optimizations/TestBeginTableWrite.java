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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Function;

import static io.trino.sql.planner.TypeProvider.empty;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBeginTableWrite
{
    @Test
    public void testValidDelete()
    {
        assertThatCode(() -> applyOptimization(
                p -> p.tableDelete(
                        new SchemaTableName("sch", "tab"),
                        p.tableScan(ImmutableList.of(p.symbol("rowId")), true),
                        p.symbol("rowId", BigintType.BIGINT))))
                .doesNotThrowAnyException();
    }

    @Test
    public void testValidUpdate()
    {
        assertThatCode(() -> applyOptimization(
                p -> p.tableUpdate(
                        new SchemaTableName("sch", "tab"),
                        p.tableScan(ImmutableList.of(p.symbol("columnToBeUpdated")), true),
                        p.symbol("rowId", BigintType.BIGINT),
                        ImmutableList.of(p.symbol("columnToBeUpdated")))))
                .doesNotThrowAnyException();
    }

    @Test
    public void testDeleteWithNonDeletableTableScan()
    {
        assertThatThrownBy(() -> applyOptimization(
                p -> p.tableDelete(
                        new SchemaTableName("sch", "tab"),
                        p.join(
                                INNER,
                                p.tableScan(ImmutableList.of(), false),
                                p.limit(
                                        1,
                                        p.tableScan(ImmutableList.of(p.symbol("rowId")), true))),
                        p.symbol("rowId", BigintType.BIGINT))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("TableScanNode should be an updatable target");
    }

    @Test
    public void testUpdateWithNonUpdatableTableScan()
    {
        assertThatThrownBy(() -> applyOptimization(
                p -> p.tableUpdate(
                        new SchemaTableName("sch", "tab"),
                        p.join(
                                INNER,
                                p.tableScan(ImmutableList.of(), false),
                                p.limit(
                                        1,
                                        p.tableScan(ImmutableList.of(p.symbol("columnToBeUpdated"), p.symbol("rowId")), true))),
                        p.symbol("rowId", BigintType.BIGINT),
                        ImmutableList.of(p.symbol("columnToBeUpdated")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("TableScanNode should be an updatable target");
    }

    @Test
    public void testDeleteWithInvalidNode()
    {
        assertThatThrownBy(() -> applyOptimization(
                p -> p.tableDelete(
                        new SchemaTableName("sch", "tab"),
                        p.distinctLimit(
                                10,
                                ImmutableList.of(p.symbol("rowId")),
                                p.tableScan(ImmutableList.of(p.symbol("a")), true)),
                        p.symbol("rowId", BigintType.BIGINT))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid descendant for DeleteNode or UpdateNode: io.trino.sql.planner.plan.DistinctLimitNode");
    }

    @Test
    public void testUpdateWithInvalidNode()
    {
        assertThatThrownBy(() -> applyOptimization(
                p -> p.tableUpdate(
                        new SchemaTableName("sch", "tab"),
                        p.distinctLimit(
                                10,
                                ImmutableList.of(p.symbol("a"), p.symbol("rowId")),
                                p.tableScan(ImmutableList.of(p.symbol("a")), true)),
                        p.symbol("rowId", BigintType.BIGINT),
                        ImmutableList.of(p.symbol("columnToBeUpdated")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid descendant for DeleteNode or UpdateNode: io.trino.sql.planner.plan.DistinctLimitNode");
    }

    private void applyOptimization(Function<PlanBuilder, PlanNode> planProvider)
    {
        Metadata metadata = new MockMetadata();
        new BeginTableWrite(metadata)
                .optimize(
                        planProvider.apply(new PlanBuilder(new PlanNodeIdAllocator(), metadata)),
                        testSessionBuilder().build(),
                        empty(),
                        new SymbolAllocator(),
                        new PlanNodeIdAllocator(),
                        WarningCollector.NOOP);
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        @Override
        public TableHandle beginDelete(Session session, TableHandle tableHandle)
        {
            return tableHandle;
        }

        @Override
        public TableHandle beginUpdate(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
        {
            return tableHandle;
        }
    }
}
