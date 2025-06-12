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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.connector.system.SystemTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.RefreshType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.RefreshType.FULL;
import static io.trino.spi.RefreshType.INCREMENTAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestIncrementalRefreshVisitor
{
    @Test
    public void testIncrementalRefreshForScanFilterProject()
    {
        PlanNode root = getBasePlan();

        RefreshType refreshType = IncrementalRefreshVisitor.canIncrementallyRefresh(root);
        assertThat(refreshType).isEqualTo(INCREMENTAL);
    }

    @Test
    public void testFullRefreshForLimitNode()
    {
        PlanNode base = getBasePlan();
        PlanNode root = new LimitNode(
                new PlanNodeId("limitNode"),
                base,
                10,
                false);

        RefreshType refreshType = IncrementalRefreshVisitor.canIncrementallyRefresh(root);
        assertThat(refreshType).isEqualTo(FULL);
    }

    @Test
    public void testFullRefreshForJoinNode()
    {
        PlanNode left = getBasePlan();
        PlanNode right = getBasePlan();
        PlanNode root = new JoinNode(
                new PlanNodeId("joinNode"),
                JoinType.INNER,
                left,
                right,
                List.of(),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        RefreshType refreshType = IncrementalRefreshVisitor.canIncrementallyRefresh(root);
        assertThat(refreshType).isEqualTo(FULL);
    }

    @Test
    public void testFullRefreshForIntermediateApplyNode()
    {
        TableScanNode tableScanNode = getTableScanNode();
        ApplyNode applyNode = new ApplyNode(
                new PlanNodeId("applyNode"),
                tableScanNode,
                tableScanNode,
                Map.of(),
                List.of(),
                new io.trino.sql.tree.NullLiteral());
        PlanNode root = withFilterAndProject(applyNode);

        RefreshType refreshType = IncrementalRefreshVisitor.canIncrementallyRefresh(root);
        assertThat(refreshType).isEqualTo(FULL);
    }

    private PlanNode getBasePlan()
    {
        return withFilterAndProject(getTableScanNode());
    }

    private PlanNode withFilterAndProject(PlanNode baseNode)
    {
        FilterNode filterNode = new FilterNode(
                new PlanNodeId("filterNode"),
                baseNode,
                new IsNull(new Reference(VARCHAR, "name")));
        return new ProjectNode(
                new PlanNodeId("projectNode"),
                filterNode,
                Assignments.of());
    }

    private TableScanNode getTableScanNode()
    {
        TableHandle tableHandle = new TableHandle(
                GlobalSystemConnector.CATALOG_HANDLE,
                new SystemTableHandle("jdbc", "tables", TupleDomain.all()),
                TestingTransactionHandle.create());
        return new TableScanNode(
                new PlanNodeId("tableScan"),
                tableHandle,
                ImmutableList.of(),
                ImmutableMap.of(),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
    }
}
