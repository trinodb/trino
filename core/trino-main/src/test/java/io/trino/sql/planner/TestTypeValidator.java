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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.sanity.TypeValidator;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTypeValidator
{
    private static final TypeValidator TYPE_VALIDATOR = new TypeValidator();

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final SymbolAllocator symbolAllocator = new SymbolAllocator();
    private final Symbol columnA = symbolAllocator.newSymbol("a", BIGINT);
    private final Symbol columnB = symbolAllocator.newSymbol("b", INTEGER);
    private final Symbol columnC = symbolAllocator.newSymbol("c", DOUBLE);
    private final Symbol columnD = symbolAllocator.newSymbol("d", DATE);
    // varchar(3), to test type only coercion
    private final Symbol columnE = symbolAllocator.newSymbol("e", VarcharType.createVarcharType(3));

    private final TableScanNode baseTableScan = new TableScanNode(
            newId(),
            TEST_TABLE_HANDLE,
            ImmutableList.copyOf(((Map<Symbol, ColumnHandle>) ImmutableMap.<Symbol, ColumnHandle>builder()
                    .put(columnA, new TestingColumnHandle("a"))
                    .put(columnB, new TestingColumnHandle("b"))
                    .put(columnC, new TestingColumnHandle("c"))
                    .put(columnD, new TestingColumnHandle("d"))
                    .put(columnE, new TestingColumnHandle("e"))
                    .buildOrThrow()).keySet()),
            ImmutableMap.<Symbol, ColumnHandle>builder()
                    .put(columnA, new TestingColumnHandle("a"))
                    .put(columnB, new TestingColumnHandle("b"))
                    .put(columnC, new TestingColumnHandle("c"))
                    .put(columnD, new TestingColumnHandle("d"))
                    .put(columnE, new TestingColumnHandle("e"))
                    .buildOrThrow(),
            TupleDomain.all(),
            Optional.empty(),
            false,
            Optional.empty());

    @Test
    public void testValidProject()
    {
        Expression expression1 = new Cast(columnB.toSymbolReference(), BIGINT);
        Expression expression2 = new Cast(columnC.toSymbolReference(), BIGINT);
        Assignments assignments = Assignments.builder()
                .put(symbolAllocator.newSymbol(expression1), expression1)
                .put(symbolAllocator.newSymbol(expression2), expression2)
                .build();
        PlanNode node = new ProjectNode(
                newId(),
                baseTableScan,
                assignments);

        assertTypesValid(node);
    }

    @Test
    public void testValidUnion()
    {
        Symbol outputSymbol = symbolAllocator.newSymbol("output", DATE);
        ListMultimap<Symbol, Symbol> mappings = ImmutableListMultimap.<Symbol, Symbol>builder()
                .put(outputSymbol, columnD)
                .put(outputSymbol, columnD)
                .build();

        PlanNode node = new UnionNode(
                newId(),
                ImmutableList.of(baseTableScan, baseTableScan),
                mappings,
                ImmutableList.copyOf(mappings.keySet()));

        assertTypesValid(node);
    }

    @Test
    public void testValidWindow()
    {
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", DOUBLE);
        ResolvedFunction resolvedFunction = functionResolution.resolveFunction("sum", fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(resolvedFunction, ImmutableList.of(columnC.toSymbolReference()), frame, false);

        DataOrganizationSpecification specification = new DataOrganizationSpecification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowSymbol, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertTypesValid(node);
    }

    @Test
    public void testValidAggregation()
    {
        Symbol aggregationSymbol = symbolAllocator.newSymbol("sum", DOUBLE);

        PlanNode node = singleAggregation(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        functionResolution.resolveFunction("sum", fromTypes(DOUBLE)),
                        ImmutableList.of(columnC.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)));

        assertTypesValid(node);
    }

    @Test
    public void testInvalidAggregationFunctionCall()
    {
        Symbol aggregationSymbol = symbolAllocator.newSymbol("sum", DOUBLE);

        PlanNode node = singleAggregation(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        functionResolution.resolveFunction("sum", fromTypes(DOUBLE)),
                        ImmutableList.of(columnA.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)));

        assertThatThrownBy(() -> assertTypesValid(node))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("type of symbol 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint");
    }

    @Test
    public void testInvalidAggregationFunctionSignature()
    {
        Symbol aggregationSymbol = symbolAllocator.newSymbol("sum", BIGINT);

        PlanNode node = singleAggregation(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        functionResolution.resolveFunction("sum", fromTypes(DOUBLE)),
                        ImmutableList.of(columnC.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)));

        assertThatThrownBy(() -> assertTypesValid(node))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("type of symbol 'sum(_[0-9]+)?' is expected to be bigint, but the actual type is double");
    }

    @Test
    public void testInvalidWindowFunctionCall()
    {
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", DOUBLE);
        ResolvedFunction resolvedFunction = functionResolution.resolveFunction("sum", fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(resolvedFunction, ImmutableList.of(columnA.toSymbolReference()), frame, false);

        DataOrganizationSpecification specification = new DataOrganizationSpecification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowSymbol, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertThatThrownBy(() -> assertTypesValid(node))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("type of symbol 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint");
    }

    @Test
    public void testInvalidWindowFunctionSignature()
    {
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", BIGINT);
        ResolvedFunction resolvedFunction = functionResolution.resolveFunction("sum", fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(resolvedFunction, ImmutableList.of(columnC.toSymbolReference()), frame, false);

        DataOrganizationSpecification specification = new DataOrganizationSpecification(ImmutableList.of(), Optional.empty());

        PlanNode node = new WindowNode(
                newId(),
                baseTableScan,
                specification,
                ImmutableMap.of(windowSymbol, function),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        assertThatThrownBy(() -> assertTypesValid(node))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("type of symbol 'sum(_[0-9]+)?' is expected to be bigint, but the actual type is double");
    }

    @Test
    public void testInvalidUnion()
    {
        Symbol outputSymbol = symbolAllocator.newSymbol("output", DATE);
        ListMultimap<Symbol, Symbol> mappings = ImmutableListMultimap.<Symbol, Symbol>builder()
                .put(outputSymbol, columnD)
                .put(outputSymbol, columnA) // should be a symbol with DATE type
                .build();

        PlanNode node = new UnionNode(
                newId(),
                ImmutableList.of(baseTableScan, baseTableScan),
                mappings,
                ImmutableList.copyOf(mappings.keySet()));

        assertThatThrownBy(() -> assertTypesValid(node))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("type of symbol 'output(_[0-9]+)?' is expected to be date, but the actual type is bigint");
    }

    private void assertTypesValid(PlanNode node)
    {
        TYPE_VALIDATOR.validate(node, TEST_SESSION, PLANNER_CONTEXT, WarningCollector.NOOP);
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }
}
