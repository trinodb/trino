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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.Input;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.SecureExpression;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInputExtractor
{
    private static final Symbol X = new Symbol(BIGINT, "x");
    private static final Symbol Y = new Symbol(BIGINT, "y");
    private static final Object CONNECTOR_INFO = "partitions=[secret]";

    @Test
    public void testConnectorInfoIsWithheldForSecureScans()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);
        TableScanNode secureScan = planBuilder.tableScan(ImmutableList.of(X), ImmutableMap.of(X, new TestingColumnHandle("x")));
        TableScanNode plainScan = planBuilder.tableScan(ImmutableList.of(Y), ImmutableMap.of(Y, new TestingColumnHandle("y")));
        PlanNode root = planBuilder.join(
                INNER,
                planBuilder.filter(new SecureExpression(comparison(EQUAL, X.toSymbolReference(), new Constant(BIGINT, 1L))), secureScan),
                plainScan);

        List<Input> inputs = new InputExtractor(new TestingMetadata(), TEST_SESSION).extractInputs(new SubPlan(fragment(root), ImmutableList.of()));

        assertThat(inputs).hasSize(2);
        // connector metadata such as pruned partition ids would reveal what the secure filter enforced
        assertThat(input(inputs, secureScan.getId()).connectorInfo()).isEmpty();
        assertThat(input(inputs, secureScan.getId()).columns()).isEmpty();
        assertThat(input(inputs, plainScan.getId()).connectorInfo()).contains(CONNECTOR_INFO);
        assertThat(input(inputs, plainScan.getId()).columns()).hasSize(1);
    }

    private static Input input(List<Input> inputs, PlanNodeId planNodeId)
    {
        return inputs.stream()
                .filter(input -> input.planNodeId().equals(planNodeId))
                .collect(onlyElement());
    }

    private static PlanFragment fragment(PlanNode root)
    {
        return new PlanFragment(
                new PlanFragmentId("0"),
                root,
                ImmutableSet.copyOf(root.getOutputSymbols()),
                SINGLE_DISTRIBUTION,
                OptionalInt.empty(),
                ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), root.getOutputSymbols()),
                OptionalInt.empty(),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private static class TestingMetadata
            extends AbstractMockMetadata
    {
        @Override
        public CatalogSchemaTableName getTableName(Session session, TableHandle tableHandle)
        {
            return new CatalogSchemaTableName("test_catalog", "test_schema", "test_table");
        }

        @Override
        public Optional<Object> getInfo(Session session, TableHandle handle)
        {
            return Optional.of(CONNECTOR_INFO);
        }

        @Override
        public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return new ColumnMetadata(((TestingColumnHandle) columnHandle).getName(), BIGINT);
        }

        @Override
        public Optional<CatalogInfo> getCatalogInfo(Session session, String catalogName)
        {
            return Optional.empty();
        }
    }
}
