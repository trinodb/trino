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
import io.trino.sql.ir.SensitiveExpression;
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
    public void testConnectorInfoIsWithheldForSensitiveScans()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);
        TableScanNode sensitiveScan = planBuilder.tableScan(ImmutableList.of(X), ImmutableMap.of(X, new TestingColumnHandle("x")));
        TableScanNode plainScan = planBuilder.tableScan(ImmutableList.of(Y), ImmutableMap.of(Y, new TestingColumnHandle("y")));
        PlanNode root = planBuilder.join(
                INNER,
                planBuilder.filter(new SensitiveExpression(comparison(EQUAL, X.toSymbolReference(), new Constant(BIGINT, 1L))), sensitiveScan),
                plainScan);

        SubPlan plan = new SubPlan(fragment(root), ImmutableList.of());
        InputExtractor extractor = new InputExtractor(new TestingMetadata(), TEST_SESSION);
        List<Input> inputs = extractor.extractInputs(plan);
        List<Input> redacted = extractor.redactInputs(plan, inputs);

        // event listeners get every input
        assertThat(inputs).hasSize(2);
        assertThat(input(inputs, sensitiveScan.getId()).connectorInfo()).contains(CONNECTOR_INFO);
        assertThat(input(inputs, sensitiveScan.getId()).columns()).hasSize(1);
        // connector metadata such as pruned partition ids would reveal what the sensitive filter enforced
        assertThat(redacted).hasSize(2);
        assertThat(input(redacted, sensitiveScan.getId()).connectorInfo()).isEmpty();
        assertThat(input(redacted, sensitiveScan.getId()).columns()).isEmpty();
        assertThat(input(redacted, plainScan.getId())).isEqualTo(input(inputs, plainScan.getId()));
        // without sensitive expressions, the inputs are reported as they are
        SubPlan plain = new SubPlan(fragment(plainScan), ImmutableList.of());
        List<Input> plainInputs = extractor.extractInputs(plain);
        assertThat(extractor.redactInputs(plain, plainInputs)).isSameAs(plainInputs);
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
