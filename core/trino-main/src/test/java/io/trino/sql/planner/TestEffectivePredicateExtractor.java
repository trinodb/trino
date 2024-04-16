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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingTransactionHandle;
import io.trino.transaction.TestingTransactionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.function.FunctionId.toFunctionId;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.planner.plan.AggregationNode.globalAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.tests.BogusType.BOGUS;
import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestEffectivePredicateExtractor
{
    private static final Session SESSION = TestingSession.testSessionBuilder().build();

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final Metadata metadata = new AbstractMockMetadata()
    {
        private final Metadata delegate = functionResolution.getMetadata();

        @Override
        public ResolvedFunction resolveBuiltinFunction(String name, List<TypeSignatureProvider> parameterTypes)
        {
            return delegate.resolveBuiltinFunction(name, parameterTypes);
        }

        @Override
        public ResolvedFunction getCoercion(Type fromType, Type toType)
        {
            return delegate.getCoercion(fromType, toType);
        }

        @Override
        public TableProperties getTableProperties(Session session, TableHandle handle)
        {
            return new TableProperties(
                    TEST_CATALOG_HANDLE,
                    TestingConnectorTransactionHandle.INSTANCE,
                    new ConnectorTableProperties(
                            ((PredicatedTableHandle) handle.connectorHandle()).getPredicate(),
                            Optional.empty(),
                            Optional.empty(),
                            ImmutableList.of()));
        }
    };
    private final PlannerContext plannerContext = plannerContextBuilder().withMetadata(metadata).build();

    private final EffectivePredicateExtractor effectivePredicateExtractor = new EffectivePredicateExtractor(plannerContext, true);
    private final EffectivePredicateExtractor effectivePredicateExtractorWithoutTableProperties = new EffectivePredicateExtractor(plannerContext, false);

    private Map<Symbol, ColumnHandle> scanAssignments;
    private TableScanNode baseTableScan;
    private ExpressionIdentityNormalizer expressionNormalizer;

    @BeforeEach
    public void setUp()
    {
        scanAssignments = ImmutableMap.<Symbol, ColumnHandle>builder()
                .put(new Symbol(BIGINT, "a"), new TestingColumnHandle("a"))
                .put(new Symbol(BIGINT, "b"), new TestingColumnHandle("b"))
                .put(new Symbol(BIGINT, "c"), new TestingColumnHandle("c"))
                .put(new Symbol(BIGINT, "d"), new TestingColumnHandle("d"))
                .put(new Symbol(BIGINT, "e"), new TestingColumnHandle("e"))
                .put(new Symbol(BIGINT, "f"), new TestingColumnHandle("f"))
                .put(new Symbol(RowType.anonymous(ImmutableList.of(BIGINT, BIGINT)), "r"), new TestingColumnHandle("r"))
                .buildOrThrow();

        Map<Symbol, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c"), new Symbol(BIGINT, "d"), new Symbol(BIGINT, "e"), new Symbol(BIGINT, "f"))));
        baseTableScan = TableScanNode.newInstance(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                false,
                Optional.empty());

        expressionNormalizer = new ExpressionIdentityNormalizer();
    }

    @Test
    public void testAggregation()
    {
        PlanNode node = singleAggregation(
                newId(),
                filter(
                        baseTableScan,
                        and(
                                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "d")),
                                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "e")),
                                equals(new Reference(BIGINT, "c"), new Reference(BIGINT, "f")),
                                lessThan(new Reference(BIGINT, "d"), bigintLiteral(10)),
                                lessThan(new Reference(BIGINT, "c"), new Reference(BIGINT, "d")),
                                greaterThan(new Reference(BIGINT, "a"), bigintLiteral(2)),
                                equals(new Reference(BIGINT, "e"), new Reference(BIGINT, "f")))),
                ImmutableMap.of(
                        new Symbol(BIGINT, "c"), new Aggregation(
                                fakeFunction("test"),
                                ImmutableList.of(),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        new Symbol(BIGINT, "d"), new Aggregation(
                                fakeFunction("test"),
                                ImmutableList.of(),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty())),
                singleGroupingSet(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c"))));

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // Rewrite in terms of group by symbols
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(
                lessThan(new Reference(BIGINT, "a"), bigintLiteral(10)),
                lessThan(new Reference(BIGINT, "b"), new Reference(BIGINT, "a")),
                greaterThan(new Reference(BIGINT, "a"), bigintLiteral(2)),
                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "c"))));
    }

    @Test
    public void testGroupByEmpty()
    {
        PlanNode node = new AggregationNode(
                newId(),
                filter(baseTableScan, FALSE),
                ImmutableMap.of(),
                globalAggregation(),
                ImmutableList.of(),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        assertThat(effectivePredicate).isEqualTo(TRUE);
    }

    @Test
    public void testFilter()
    {
        PlanNode node = filter(
                baseTableScan,
                and(
                        greaterThan(
                                new Reference(DOUBLE, "a"),
                                functionResolution
                                        .functionCallBuilder("rand")
                                        .build()),
                        lessThan(new Reference(BIGINT, "b"), bigintLiteral(10))));

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // Non-deterministic functions should be purged
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(lessThan(new Reference(BIGINT, "b"), bigintLiteral(10))));
    }

    @Test
    public void testProject()
    {
        PlanNode node = new ProjectNode(
                newId(),
                filter(
                        baseTableScan,
                        and(
                                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "c")),
                                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)))),
                Assignments.of(new Symbol(BIGINT, "d"), new Reference(BIGINT, "a"), new Symbol(BIGINT, "e"), new Reference(BIGINT, "c")));

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // Rewrite in terms of project output symbols
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(
                lessThan(new Reference(BIGINT, "d"), bigintLiteral(10)),
                equals(new Reference(BIGINT, "d"), new Reference(BIGINT, "e"))));
    }

    @Test
    public void testProjectWithSymbolReuse()
    {
        // symbol B is reused so underlying predicates involving BE are invalid after Projection
        // and will not be included in the resulting predicate
        PlanNode projectReusingB = new ProjectNode(
                newId(),
                filter(
                        baseTableScan,
                        and(
                                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "c")),
                                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)))),
                Assignments.of(new Symbol(BIGINT, "d"), new Reference(BIGINT, "a"), new Symbol(BIGINT, "b"), new Reference(BIGINT, "c")));

        Expression effectivePredicateWhenBReused = effectivePredicateExtractor.extract(SESSION, projectReusingB);

        assertThat(normalizeConjuncts(effectivePredicateWhenBReused)).isEqualTo(normalizeConjuncts(lessThan(new Reference(BIGINT, "b"), bigintLiteral(10))));

        // symbol C is reused so underlying predicates involving CE are invalid after Projection
        // and will not be included in the resulting predicate
        // also, Projection assignments containing C in the assigned expression will not be used to derive equalities
        PlanNode projectReusingC = new ProjectNode(
                newId(),
                filter(
                        baseTableScan,
                        and(
                                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "c")),
                                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)))),
                Assignments.builder()
                        .put(new Symbol(BIGINT, "c"), new Reference(BIGINT, "a"))
                        .put(new Symbol(BIGINT, "e"), new Reference(BIGINT, "c"))
                        .put(new Symbol(BIGINT, "f"), new Reference(BIGINT, "b"))
                        .build());

        Expression effectivePredicateWhenCReused = effectivePredicateExtractor.extract(SESSION, projectReusingC);

        assertThat(normalizeConjuncts(effectivePredicateWhenCReused)).isEqualTo(normalizeConjuncts(normalizeConjuncts(equals(new Reference(BIGINT, "c"), new Reference(BIGINT, "f")))));
    }

    @Test
    public void testTopN()
    {
        PlanNode node = new TopNNode(
                newId(),
                filter(
                        baseTableScan,
                        and(
                                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                                equals(new Reference(DOUBLE, "b"), new Reference(DOUBLE, "c")),
                                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)))),
                1,
                new OrderingScheme(ImmutableList.of(new Symbol(BIGINT, "a")), ImmutableMap.of(new Symbol(BIGINT, "a"), SortOrder.ASC_NULLS_LAST)), TopNNode.Step.PARTIAL);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // Pass through
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(
                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                equals(new Reference(DOUBLE, "b"), new Reference(DOUBLE, "c")),
                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10))));
    }

    @Test
    public void testLimit()
    {
        PlanNode node = new LimitNode(
                newId(),
                filter(
                        baseTableScan,
                        and(
                                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "c")),
                                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)))),
                1,
                false);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // Pass through
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(
                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "c")),
                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10))));
    }

    @Test
    public void testSort()
    {
        PlanNode node = new SortNode(
                newId(),
                filter(
                        baseTableScan,
                        and(
                                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "c")),
                                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)))),
                new OrderingScheme(ImmutableList.of(new Symbol(BIGINT, "a")), ImmutableMap.of(new Symbol(BIGINT, "a"), SortOrder.ASC_NULLS_LAST)),
                false);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // Pass through
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(
                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "c")),
                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10))));
    }

    @Test
    public void testWindow()
    {
        PlanNode node = new WindowNode(
                newId(),
                filter(
                        baseTableScan,
                        and(
                                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "c")),
                                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)))),
                new DataOrganizationSpecification(
                        ImmutableList.of(new Symbol(BIGINT, "a")),
                        Optional.of(new OrderingScheme(
                                ImmutableList.of(new Symbol(BIGINT, "a")),
                                ImmutableMap.of(new Symbol(BIGINT, "a"), SortOrder.ASC_NULLS_LAST)))),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // Pass through
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(
                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "c")),
                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10))));
    }

    @Test
    public void testTableScan()
    {
        // Effective predicate is True if there is no effective predicate
        Map<Symbol, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(DOUBLE, "c"), new Symbol(REAL, "d"))));
        PlanNode node = TableScanNode.newInstance(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                false,
                Optional.empty());
        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);
        assertThat(effectivePredicate).isEqualTo(TRUE);

        node = new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.none()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.none(),
                Optional.empty(),
                false,
                Optional.empty());
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);
        assertThat(effectivePredicate).isEqualTo(FALSE);

        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(scanAssignments.get(new Symbol(BIGINT, "a")), Domain.singleValue(BIGINT, 1L)));
        node = new TableScanNode(
                newId(),
                makeTableHandle(predicate),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                predicate,
                Optional.empty(),
                false,
                Optional.empty());
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(equals(bigintLiteral(1L), new Reference(BIGINT, "a"))));

        predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                scanAssignments.get(new Symbol(BIGINT, "a")), Domain.singleValue(BIGINT, 1L),
                scanAssignments.get(new Symbol(BIGINT, "b")), Domain.singleValue(BIGINT, 2L)));
        node = new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.withColumnDomains(ImmutableMap.of(scanAssignments.get(new Symbol(BIGINT, "a")), Domain.singleValue(BIGINT, 1L)))),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                predicate,
                Optional.empty(),
                false,
                Optional.empty());
        effectivePredicate = effectivePredicateExtractorWithoutTableProperties.extract(SESSION, node);
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(equals(bigintLiteral(2L), new Reference(BIGINT, "b")), equals(bigintLiteral(1L), new Reference(BIGINT, "a"))));

        node = new TableScanNode(
                newId(),
                makeTableHandle(predicate),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);
        assertThat(effectivePredicate).isEqualTo(and(equals(new Reference(BIGINT, "a"), bigintLiteral(1)), equals(new Reference(BIGINT, "b"), bigintLiteral(2))));

        node = new TableScanNode(
                newId(),
                makeTableHandle(predicate),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        scanAssignments.get(new Symbol(BIGINT, "a")), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)),
                        scanAssignments.get(new Symbol(BIGINT, "b")), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)))),
                Optional.empty(),
                false,
                Optional.empty());
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(equals(bigintLiteral(2L), new Reference(BIGINT, "b")), equals(bigintLiteral(1L), new Reference(BIGINT, "a"))));

        node = new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);
        assertThat(effectivePredicate).isEqualTo(TRUE);
    }

    @Test
    public void testValues()
    {
        // one column
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(BIGINT, "a")),
                        ImmutableList.of(
                                new Row(ImmutableList.of(bigintLiteral(1))),
                                new Row(ImmutableList.of(bigintLiteral(2)))))
        )).isEqualTo(new In(new Reference(BIGINT, "a"), ImmutableList.of(bigintLiteral(1), bigintLiteral(2))));

        // one column with null
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(BIGINT, "a")),
                        ImmutableList.of(
                                new Row(ImmutableList.of(bigintLiteral(1))),
                                new Row(ImmutableList.of(bigintLiteral(2))),
                                new Row(ImmutableList.of(new Constant(BIGINT, null)))))))
                .isEqualTo(or(
                        new IsNull(new Reference(BIGINT, "a")),
                        new In(new Reference(BIGINT, "a"), ImmutableList.of(bigintLiteral(1), bigintLiteral(2)))));

        // all nulls
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(BIGINT, "a")),
                        ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, null)))))))
                .isEqualTo(new IsNull(new Reference(BIGINT, "a")));

        // nested row
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(RowType.anonymous(ImmutableList.of(BIGINT, BIGINT)), "r")),
                        ImmutableList.of(new Row(ImmutableList.of(new Row(ImmutableList.of(bigintLiteral(1), new Constant(UNKNOWN, null)))))))))
                .isEqualTo(TRUE);

        // many rows
        List<Expression> rows = IntStream.range(0, 500)
                .mapToObj(TestEffectivePredicateExtractor::bigintLiteral)
                .map(ImmutableList::of)
                .map(Row::new)
                .collect(toImmutableList());
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(BIGINT, "a")),
                        rows)
        )).isEqualTo(new Between(new Reference(BIGINT, "a"), bigintLiteral(0), bigintLiteral(499)));

        // NaN
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(DOUBLE, "c")),
                        ImmutableList.of(new Row(ImmutableList.of(doubleLiteral(Double.NaN)))))
        )).isEqualTo(new Not(new IsNull(new Reference(DOUBLE, "c"))));

        // NaN and NULL
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(DOUBLE, "c")),
                        ImmutableList.of(
                                new Row(ImmutableList.of(new Constant(DOUBLE, null))),
                                new Row(ImmutableList.of(doubleLiteral(Double.NaN)))))
        )).isEqualTo(TRUE);

        // NaN and value
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(DOUBLE, "x")),
                        ImmutableList.of(
                                new Row(ImmutableList.of(doubleLiteral(42.))),
                                new Row(ImmutableList.of(doubleLiteral(Double.NaN)))))
        )).isEqualTo(new Not(new IsNull(new Reference(DOUBLE, "x"))));

        // Real NaN
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(REAL, "d")),
                        ImmutableList.of(new Row(ImmutableList.of(new Cast(doubleLiteral(Double.NaN), REAL)))))))
                .isEqualTo(new Not(new IsNull(new Reference(REAL, "d"))));

        // multiple columns
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b")),
                        ImmutableList.of(
                                new Row(ImmutableList.of(bigintLiteral(1), bigintLiteral(100))),
                                new Row(ImmutableList.of(bigintLiteral(2), bigintLiteral(200)))))))
                .isEqualTo(and(
                        new In(new Reference(BIGINT, "a"), ImmutableList.of(bigintLiteral(1), bigintLiteral(2))),
                        new In(new Reference(BIGINT, "b"), ImmutableList.of(bigintLiteral(100), bigintLiteral(200)))));

        // multiple columns with null
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b")),
                        ImmutableList.of(
                                new Row(ImmutableList.of(bigintLiteral(1), new Constant(BIGINT, null))),
                                new Row(ImmutableList.of(new Constant(BIGINT, null), bigintLiteral(200)))))
        )).isEqualTo(and(
                or(new IsNull(new Reference(BIGINT, "a")), new Comparison(EQUAL, new Reference(BIGINT, "a"), bigintLiteral(1))),
                or(new IsNull(new Reference(BIGINT, "b")), new Comparison(EQUAL, new Reference(BIGINT, "b"), bigintLiteral(200)))));

        // non-deterministic
        ResolvedFunction rand = functionResolution.resolveFunction("rand", ImmutableList.of());
        ValuesNode node = new ValuesNode(
                newId(),
                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b")),
                ImmutableList.of(new Row(ImmutableList.of(bigintLiteral(1), new Call(rand, ImmutableList.of())))));
        assertThat(extract(node)).isEqualTo(new Comparison(EQUAL, new Reference(BIGINT, "a"), bigintLiteral(1)));

        // non-constant
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(BIGINT, "a")),
                        ImmutableList.of(
                                new Row(ImmutableList.of(bigintLiteral(1))),
                                new Row(ImmutableList.of(new Reference(BIGINT, "b")))))
        )).isEqualTo(TRUE);

        // non-comparable and non-orderable
        assertThat(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(new Symbol(BOGUS, "g")),
                        ImmutableList.of(
                                new Row(ImmutableList.of(bigintLiteral(1))),
                                new Row(ImmutableList.of(bigintLiteral(2)))))
        )).isEqualTo(TRUE);
    }

    private Expression extract(PlanNode node)
    {
        return transaction(new TestingTransactionManager(), metadata, new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, transactionSession -> {
                    return effectivePredicateExtractor.extract(transactionSession, node);
                });
    }

    @Test
    public void testUnion()
    {
        ImmutableListMultimap<Symbol, Symbol> symbolMapping = ImmutableListMultimap.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "a"), new Symbol(BIGINT, "c"), new Symbol(BIGINT, "a"), new Symbol(BIGINT, "e"));
        PlanNode node = new UnionNode(
                newId(),
                ImmutableList.of(
                        filter(baseTableScan, greaterThan(new Reference(BIGINT, "a"), bigintLiteral(10))),
                        filter(baseTableScan, and(greaterThan(new Reference(BIGINT, "a"), bigintLiteral(10)), lessThan(new Reference(BIGINT, "a"), bigintLiteral(100)))),
                        filter(baseTableScan, and(greaterThan(new Reference(BIGINT, "a"), bigintLiteral(10)), lessThan(new Reference(BIGINT, "a"), bigintLiteral(100))))),
                symbolMapping,
                ImmutableList.copyOf(symbolMapping.keySet()));

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // Only the common conjuncts can be inferred through a Union
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(greaterThan(new Reference(BIGINT, "a"), bigintLiteral(10))));
    }

    @Test
    public void testInnerJoin()
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "d")));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(new Symbol(BIGINT, "b"), new Symbol(BIGINT, "e")));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c"))));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BIGINT, "e"), new Symbol(BIGINT, "f"))));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(
                leftScan,
                and(
                        lessThan(new Reference(BIGINT, "b"), new Reference(BIGINT, "a")),
                        lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)),
                        equals(new Reference(BIGINT, "g"), bigintLiteral(10))));
        FilterNode right = filter(
                rightScan,
                and(
                        equals(new Reference(BIGINT, "d"), new Reference(BIGINT, "e")),
                        lessThan(new Reference(BIGINT, "f"), bigintLiteral(100))));

        PlanNode node = new JoinNode(
                newId(),
                JoinType.INNER,
                left,
                right,
                criteria,
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.of(lessThanOrEqual(new Reference(BIGINT, "b"), new Reference(BIGINT, "e"))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // All predicates having output symbol should be carried through
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(
                lessThan(new Reference(BIGINT, "b"), new Reference(BIGINT, "a")),
                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)),
                equals(new Reference(BIGINT, "d"), new Reference(BIGINT, "e")),
                lessThan(new Reference(BIGINT, "f"), bigintLiteral(100)),
                equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "d")),
                equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "e")),
                lessThanOrEqual(new Reference(BIGINT, "b"), new Reference(BIGINT, "e"))));
    }

    @Test
    public void testInnerJoinPropagatesPredicatesViaEquiConditions()
    {
        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c"))));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BIGINT, "e"), new Symbol(BIGINT, "f"))));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan, equals(new Reference(BIGINT, "a"), bigintLiteral(10)));

        // predicates on "a" column should be propagated to output symbols via join equi conditions
        PlanNode node = new JoinNode(
                newId(),
                JoinType.INNER,
                left,
                rightScan,
                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "d"))),
                ImmutableList.of(),
                rightScan.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(equals(new Reference(BIGINT, "d"), bigintLiteral(10))));
    }

    @Test
    public void testInnerJoinWithFalseFilter()
    {
        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c"))));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BIGINT, "e"), new Symbol(BIGINT, "f"))));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        PlanNode node = new JoinNode(
                newId(),
                JoinType.INNER,
                leftScan,
                rightScan,
                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "d"))),
                leftScan.getOutputSymbols(),
                rightScan.getOutputSymbols(),
                false,
                Optional.of(FALSE),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        assertThat(effectivePredicate).isEqualTo(FALSE);
    }

    @Test
    public void testLeftJoin()
    {
        FilterNode left = filter(
                tableScanNode(Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c"))))),
                and(
                        lessThan(new Reference(BIGINT, "b"), new Reference(BIGINT, "a")),
                        lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)),
                        equals(new Reference(BIGINT, "g"), bigintLiteral(10))));
        FilterNode right = filter(
                tableScanNode(Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BIGINT, "e"), new Symbol(BIGINT, "f"))))),
                and(
                        equals(new Reference(BIGINT, "d"), new Reference(BIGINT, "e")),
                        lessThan(new Reference(BIGINT, "f"), bigintLiteral(100))));
        PlanNode node = new JoinNode(
                newId(),
                JoinType.LEFT,
                left,
                right,
                ImmutableList.of(
                        new JoinNode.EquiJoinClause(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "d")),
                        new JoinNode.EquiJoinClause(new Symbol(BIGINT, "b"), new Symbol(BIGINT, "e"))),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // All right side symbols having output symbols should be checked against NULL
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(
                lessThan(new Reference(BIGINT, "b"), new Reference(BIGINT, "a")),
                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)),
                or(equals(new Reference(BIGINT, "d"), new Reference(BIGINT, "e")), and(isNull(new Reference(BIGINT, "d")), isNull(new Reference(BIGINT, "e")))),
                or(lessThan(new Reference(BIGINT, "f"), bigintLiteral(100)), isNull(new Reference(BIGINT, "f"))),
                or(equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "d")), isNull(new Reference(BIGINT, "d"))),
                or(equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "e")), isNull(new Reference(BIGINT, "e")))));
    }

    @Test
    public void testLeftJoinWithFalseInner()
    {
        List<JoinNode.EquiJoinClause> criteria = ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "d")));

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c"))));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BIGINT, "e"), new Symbol(BIGINT, "f"))));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(
                leftScan,
                and(
                        lessThan(new Reference(BIGINT, "b"), new Reference(BIGINT, "a")),
                        lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)),
                        equals(new Reference(BIGINT, "g"), bigintLiteral(10))));
        FilterNode right = filter(rightScan, FALSE);
        PlanNode node = new JoinNode(
                newId(),
                JoinType.LEFT,
                left,
                right,
                criteria,
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // False literal on the right side should be ignored
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(
                lessThan(new Reference(BIGINT, "b"), new Reference(BIGINT, "a")),
                lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)),
                or(equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "d")), isNull(new Reference(BIGINT, "d")))));
    }

    @Test
    public void testRightJoin()
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "d")));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(new Symbol(BIGINT, "b"), new Symbol(BIGINT, "e")));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c"))));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BIGINT, "e"), new Symbol(BIGINT, "f"))));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(
                leftScan,
                and(
                        lessThan(new Reference(BIGINT, "b"), new Reference(BIGINT, "a")),
                        lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)),
                        equals(new Reference(BIGINT, "g"), bigintLiteral(10))));
        FilterNode right = filter(
                rightScan,
                and(
                        equals(new Reference(BIGINT, "d"), new Reference(BIGINT, "e")),
                        lessThan(new Reference(BIGINT, "f"), bigintLiteral(100))));
        PlanNode node = new JoinNode(
                newId(),
                JoinType.RIGHT,
                left,
                right,
                criteria,
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // All left side symbols should be checked against NULL
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(
                or(lessThan(new Reference(BIGINT, "b"), new Reference(BIGINT, "a")), and(isNull(new Reference(BIGINT, "b")), isNull(new Reference(BIGINT, "a")))),
                or(lessThan(new Reference(BIGINT, "c"), bigintLiteral(10)), isNull(new Reference(BIGINT, "c"))),
                equals(new Reference(BIGINT, "d"), new Reference(BIGINT, "e")),
                lessThan(new Reference(BIGINT, "f"), bigintLiteral(100)),
                or(equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "d")), isNull(new Reference(BIGINT, "a"))),
                or(equals(new Reference(BIGINT, "b"), new Reference(BIGINT, "e")), isNull(new Reference(BIGINT, "b")))));
    }

    @Test
    public void testRightJoinWithFalseInner()
    {
        List<JoinNode.EquiJoinClause> criteria = ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "d")));

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c"))));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BIGINT, "e"), new Symbol(BIGINT, "f"))));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan, FALSE);
        FilterNode right = filter(
                rightScan,
                and(
                        equals(new Reference(BIGINT, "d"), new Reference(BIGINT, "e")),
                        lessThan(new Reference(BIGINT, "f"), bigintLiteral(100))));
        PlanNode node = new JoinNode(
                newId(),
                JoinType.RIGHT,
                left,
                right,
                criteria,
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // False literal on the left side should be ignored
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(
                equals(new Reference(BIGINT, "d"), new Reference(BIGINT, "e")),
                lessThan(new Reference(BIGINT, "f"), bigintLiteral(100)),
                or(equals(new Reference(BIGINT, "a"), new Reference(BIGINT, "d")), isNull(new Reference(BIGINT, "a")))));
    }

    @Test
    public void testSemiJoin()
    {
        PlanNode node = new SemiJoinNode(
                newId(),
                filter(baseTableScan, and(greaterThan(new Reference(BIGINT, "a"), bigintLiteral(10)), lessThan(new Reference(BIGINT, "a"), bigintLiteral(100)))),
                filter(baseTableScan, greaterThan(new Reference(BIGINT, "a"), bigintLiteral(5))),
                new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(DOUBLE, "c"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node);

        // Currently, only pull predicates through the source plan
        assertThat(normalizeConjuncts(effectivePredicate)).isEqualTo(normalizeConjuncts(and(greaterThan(new Reference(BIGINT, "a"), bigintLiteral(10)), lessThan(new Reference(BIGINT, "a"), bigintLiteral(100)))));
    }

    private static TableScanNode tableScanNode(Map<Symbol, ColumnHandle> scanAssignments)
    {
        return new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(scanAssignments.keySet()),
                scanAssignments,
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }

    private static FilterNode filter(PlanNode source, Expression predicate)
    {
        return new FilterNode(newId(), source, predicate);
    }

    private static Expression bigintLiteral(long number)
    {
        return new Constant(BIGINT, number);
    }

    private static Expression doubleLiteral(double value)
    {
        return new Constant(DOUBLE, value);
    }

    private static Comparison equals(Expression expression1, Expression expression2)
    {
        return new Comparison(EQUAL, expression1, expression2);
    }

    private static Comparison lessThan(Expression expression1, Expression expression2)
    {
        return new Comparison(Comparison.Operator.LESS_THAN, expression1, expression2);
    }

    private static Comparison lessThanOrEqual(Expression expression1, Expression expression2)
    {
        return new Comparison(Comparison.Operator.LESS_THAN_OR_EQUAL, expression1, expression2);
    }

    private static Comparison greaterThan(Expression expression1, Expression expression2)
    {
        return new Comparison(Comparison.Operator.GREATER_THAN, expression1, expression2);
    }

    private static IsNull isNull(Expression expression)
    {
        return new IsNull(expression);
    }

    private static ResolvedFunction fakeFunction(String name)
    {
        BoundSignature boundSignature = new BoundSignature(builtinFunctionName(name), UNKNOWN, ImmutableList.of());
        return new ResolvedFunction(
                boundSignature,
                GlobalSystemConnector.CATALOG_HANDLE,
                toFunctionId(name, boundSignature.toSignature()),
                SCALAR,
                true,
                new FunctionNullability(false, ImmutableList.of()),
                ImmutableMap.of(),
                ImmutableSet.of());
    }

    private Set<Expression> normalizeConjuncts(Expression... conjuncts)
    {
        return normalizeConjuncts(Arrays.asList(conjuncts));
    }

    private Set<Expression> normalizeConjuncts(Collection<Expression> conjuncts)
    {
        return normalizeConjuncts(combineConjuncts(conjuncts));
    }

    private Set<Expression> normalizeConjuncts(Expression predicate)
    {
        // Normalize the predicate by identity so that the EqualityInference will produce stable rewrites in this test
        // and thereby produce comparable Sets of conjuncts from this method.
        predicate = expressionNormalizer.normalize(predicate);

        // Equality inference rewrites and equality generation will always be stable across multiple runs in the same JVM
        EqualityInference inference = new EqualityInference(predicate);

        Set<Symbol> scope = SymbolsExtractor.extractUnique(predicate);
        Set<Expression> rewrittenSet = EqualityInference.nonInferrableConjuncts(predicate)
                .map(expression -> inference.rewrite(expression, scope))
                .peek(rewritten -> checkState(rewritten != null, "Rewrite with full symbol scope should always be possible"))
                .collect(Collectors.toSet());
        rewrittenSet.addAll(inference.generateEqualitiesPartitionedBy(scope).getScopeEqualities());

        return rewrittenSet;
    }

    private static TableHandle makeTableHandle(TupleDomain<ColumnHandle> predicate)
    {
        return new TableHandle(
                TEST_CATALOG_HANDLE,
                new PredicatedTableHandle(predicate),
                TestingTransactionHandle.create());
    }

    /**
     * Normalizes Expression nodes (and all sub-expressions) by identity.
     * <p>
     * Identity equality of Expression nodes is necessary for EqualityInference to generate stable rewrites
     * (as specified by Ordering.arbitrary())
     */
    private static class ExpressionIdentityNormalizer
    {
        private final Map<Expression, Expression> expressionCache = new HashMap<>();

        private Expression normalize(Expression expression)
        {
            Expression identityNormalizedExpression = expressionCache.get(expression);
            if (identityNormalizedExpression == null) {
                // Make sure all sub-expressions are normalized first
                IrUtils.preOrder(expression)
                        .filter(e -> !e.equals(expression))
                        .forEach(this::normalize);

                // Since we have not seen this expression before, rewrite it entirely in terms of the normalized sub-expressions
                identityNormalizedExpression = ExpressionTreeRewriter.rewriteWith(new ExpressionNodeInliner(expressionCache), expression);
                expressionCache.put(identityNormalizedExpression, identityNormalizedExpression);
            }
            return identityNormalizedExpression;
        }
    }

    private static class PredicatedTableHandle
            implements ConnectorTableHandle
    {
        private final TupleDomain<ColumnHandle> predicate;

        public PredicatedTableHandle(TupleDomain<ColumnHandle> predicate)
        {
            this.predicate = predicate;
        }

        public TupleDomain<ColumnHandle> getPredicate()
        {
            return predicate;
        }
    }
}
