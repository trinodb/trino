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
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.sanity.TypeValidator;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.WindowFrame;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestTypeValidator
{
    private static final TypeValidator TYPE_VALIDATOR = new TypeValidator();

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private SymbolAllocator symbolAllocator;
    private TableScanNode baseTableScan;
    private Symbol columnA;
    private Symbol columnB;
    private Symbol columnC;
    private Symbol columnD;
    private Symbol columnE;

    @BeforeMethod
    public void setUp()
    {
        symbolAllocator = new SymbolAllocator();
        columnA = symbolAllocator.newSymbol("a", BIGINT);
        columnB = symbolAllocator.newSymbol("b", INTEGER);
        columnC = symbolAllocator.newSymbol("c", DOUBLE);
        columnD = symbolAllocator.newSymbol("d", DATE);
        columnE = symbolAllocator.newSymbol("e", VarcharType.createVarcharType(3));  // varchar(3), to test type only coercion

        Map<Symbol, ColumnHandle> assignments = ImmutableMap.<Symbol, ColumnHandle>builder()
                .put(columnA, new TestingColumnHandle("a"))
                .put(columnB, new TestingColumnHandle("b"))
                .put(columnC, new TestingColumnHandle("c"))
                .put(columnD, new TestingColumnHandle("d"))
                .put(columnE, new TestingColumnHandle("e"))
                .buildOrThrow();

        baseTableScan = new TableScanNode(
                newId(),
                TEST_TABLE_HANDLE,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    @Test
    public void testValidProject()
    {
        Expression expression1 = new Cast(columnB.toSymbolReference(), toSqlType(BIGINT));
        Expression expression2 = new Cast(columnC.toSymbolReference(), toSqlType(BIGINT));
        Assignments assignments = Assignments.builder()
                .put(symbolAllocator.newSymbol(expression1, BIGINT), expression1)
                .put(symbolAllocator.newSymbol(expression2, BIGINT), expression2)
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
        ResolvedFunction resolvedFunction = functionResolution.resolveFunction(QualifiedName.of("sum"), fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                FrameBound.Type.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(resolvedFunction, ImmutableList.of(columnC.toSymbolReference()), frame, false);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

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

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        functionResolution.resolveFunction(QualifiedName.of("sum"), fromTypes(DOUBLE)),
                        ImmutableList.of(columnC.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());

        assertTypesValid(node);
    }

    @Test
    public void testValidTypeOnlyCoercion()
    {
        Expression expression = new Cast(columnB.toSymbolReference(), toSqlType(BIGINT));
        Assignments assignments = Assignments.builder()
                .put(symbolAllocator.newSymbol(expression, BIGINT), expression)
                .put(symbolAllocator.newSymbol(columnE.toSymbolReference(), VARCHAR), columnE.toSymbolReference()) // implicit coercion from varchar(3) to varchar
                .build();
        PlanNode node = new ProjectNode(newId(), baseTableScan, assignments);

        assertTypesValid(node);
    }

    @Test
    public void testInvalidProject()
    {
        Expression expression1 = new Cast(columnB.toSymbolReference(), toSqlType(INTEGER));
        Expression expression2 = new Cast(columnA.toSymbolReference(), toSqlType(INTEGER));
        Assignments assignments = Assignments.builder()
                .put(symbolAllocator.newSymbol(expression1, BIGINT), expression1) // should be INTEGER
                .put(symbolAllocator.newSymbol(expression1, INTEGER), expression2)
                .build();
        PlanNode node = new ProjectNode(
                newId(),
                baseTableScan,
                assignments);

        assertThatThrownBy(() -> assertTypesValid(node))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("type of symbol 'expr(_[0-9]+)?' is expected to be bigint, but the actual type is integer");
    }

    @Test
    public void testInvalidAggregationFunctionCall()
    {
        Symbol aggregationSymbol = symbolAllocator.newSymbol("sum", DOUBLE);

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        functionResolution.resolveFunction(QualifiedName.of("sum"), fromTypes(DOUBLE)),
                        ImmutableList.of(columnA.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());

        assertThatThrownBy(() -> assertTypesValid(node))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("type of symbol 'sum(_[0-9]+)?' is expected to be double, but the actual type is bigint");
    }

    @Test
    public void testInvalidAggregationFunctionSignature()
    {
        Symbol aggregationSymbol = symbolAllocator.newSymbol("sum", BIGINT);

        PlanNode node = new AggregationNode(
                newId(),
                baseTableScan,
                ImmutableMap.of(aggregationSymbol, new Aggregation(
                        functionResolution.resolveFunction(QualifiedName.of("sum"), fromTypes(DOUBLE)),
                        ImmutableList.of(columnC.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(ImmutableList.of(columnA, columnB)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());

        assertThatThrownBy(() -> assertTypesValid(node))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("type of symbol 'sum(_[0-9]+)?' is expected to be bigint, but the actual type is double");
    }

    @Test
    public void testInvalidWindowFunctionCall()
    {
        Symbol windowSymbol = symbolAllocator.newSymbol("sum", DOUBLE);
        ResolvedFunction resolvedFunction = functionResolution.resolveFunction(QualifiedName.of("sum"), fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                FrameBound.Type.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(resolvedFunction, ImmutableList.of(columnA.toSymbolReference()), frame, false);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

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
        ResolvedFunction resolvedFunction = functionResolution.resolveFunction(QualifiedName.of("sum"), fromTypes(DOUBLE));

        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                FrameBound.Type.UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function function = new WindowNode.Function(resolvedFunction, ImmutableList.of(columnC.toSymbolReference()), frame, false);

        WindowNode.Specification specification = new WindowNode.Specification(ImmutableList.of(), Optional.empty());

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
        TYPE_VALIDATOR.validate(node, TEST_SESSION, PLANNER_CONTEXT, createTestingTypeAnalyzer(PLANNER_CONTEXT), symbolAllocator.getTypes(), WarningCollector.NOOP);
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }
}
