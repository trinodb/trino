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
package io.prestosql.sql.planner;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.AbstractMockMetadata;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.TableProperties;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.testing.TestingMetadata.TestingColumnHandle;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.TestingTransactionHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.FunctionId.toFunctionId;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.ExpressionUtils.and;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ExpressionUtils.or;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.prestosql.sql.planner.plan.AggregationNode.globalAggregation;
import static io.prestosql.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestEffectivePredicateExtractor
{
    private static final Symbol A = new Symbol("a");
    private static final Symbol B = new Symbol("b");
    private static final Symbol C = new Symbol("c");
    private static final Symbol D = new Symbol("d");
    private static final Symbol E = new Symbol("e");
    private static final Symbol F = new Symbol("f");
    private static final Symbol G = new Symbol("g");
    private static final Expression AE = A.toSymbolReference();
    private static final Expression BE = B.toSymbolReference();
    private static final Expression CE = C.toSymbolReference();
    private static final Expression DE = D.toSymbolReference();
    private static final Expression EE = E.toSymbolReference();
    private static final Expression FE = F.toSymbolReference();
    private static final Expression GE = G.toSymbolReference();
    private static final Session SESSION = TestingSession.testSessionBuilder().build();

    private final Metadata metadata = new AbstractMockMetadata()
    {
        private final Metadata delegate = createTestMetadataManager();

        @Override
        public BlockEncodingSerde getBlockEncodingSerde()
        {
            return delegate.getBlockEncodingSerde();
        }

        @Override
        public Type getType(TypeSignature signature)
        {
            return delegate.getType(signature);
        }

        public Type fromSqlType(String sqlType)
        {
            return delegate.fromSqlType(sqlType);
        }

        @Override
        public Type getType(TypeId id)
        {
            return delegate.getType(id);
        }

        @Override
        public ResolvedFunction resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
        {
            return delegate.resolveFunction(name, parameterTypes);
        }

        @Override
        public FunctionMetadata getFunctionMetadata(ResolvedFunction resolvedFunction)
        {
            return delegate.getFunctionMetadata(resolvedFunction);
        }

        @Override
        public ResolvedFunction getCoercion(Type fromType, Type toType)
        {
            return delegate.getCoercion(fromType, toType);
        }

        @Override
        public ScalarFunctionImplementation getScalarFunctionImplementation(ResolvedFunction resolvedFunction)
        {
            return delegate.getScalarFunctionImplementation(resolvedFunction);
        }

        @Override
        public TableProperties getTableProperties(Session session, TableHandle handle)
        {
            return new TableProperties(new CatalogName("test"), TestingConnectorTransactionHandle.INSTANCE, new ConnectorTableProperties(
                    ((PredicatedTableHandle) handle.getConnectorHandle()).getPredicate(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableList.of()));
        }
    };

    private final TypeAnalyzer typeAnalyzer = new TypeAnalyzer(new SqlParser(), metadata);
    private final EffectivePredicateExtractor effectivePredicateExtractor = new EffectivePredicateExtractor(new DomainTranslator(metadata), metadata, true);
    private final EffectivePredicateExtractor effectivePredicateExtractorWithoutTableProperties = new EffectivePredicateExtractor(new DomainTranslator(metadata), metadata, false);

    private Map<Symbol, ColumnHandle> scanAssignments;
    private TableScanNode baseTableScan;
    private ExpressionIdentityNormalizer expressionNormalizer;

    @BeforeMethod
    public void setUp()
    {
        scanAssignments = ImmutableMap.<Symbol, ColumnHandle>builder()
                .put(A, new TestingColumnHandle("a"))
                .put(B, new TestingColumnHandle("b"))
                .put(C, new TestingColumnHandle("c"))
                .put(D, new TestingColumnHandle("d"))
                .put(E, new TestingColumnHandle("e"))
                .put(F, new TestingColumnHandle("f"))
                .build();

        Map<Symbol, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C, D, E, F)));
        baseTableScan = TableScanNode.newInstance(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments);

        expressionNormalizer = new ExpressionIdentityNormalizer();
    }

    @Test
    public void testAggregation()
    {
        PlanNode node = new AggregationNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, DE),
                                equals(BE, EE),
                                equals(CE, FE),
                                lessThan(DE, bigintLiteral(10)),
                                lessThan(CE, DE),
                                greaterThan(AE, bigintLiteral(2)),
                                equals(EE, FE))),
                ImmutableMap.of(
                        C, new Aggregation(
                                fakeFunction("test", AGGREGATE),
                                ImmutableList.of(),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        D, new Aggregation(
                                fakeFunction("test", AGGREGATE),
                                ImmutableList.of(),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty())),
                singleGroupingSet(ImmutableList.of(A, B, C)),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Rewrite in terms of group by symbols
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        lessThan(AE, bigintLiteral(10)),
                        lessThan(BE, AE),
                        greaterThan(AE, bigintLiteral(2)),
                        equals(BE, CE)));
    }

    @Test
    public void testGroupByEmpty()
    {
        PlanNode node = new AggregationNode(
                newId(),
                filter(baseTableScan, FALSE_LITERAL),
                ImmutableMap.of(),
                globalAggregation(),
                ImmutableList.of(),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        assertEquals(effectivePredicate, TRUE_LITERAL);
    }

    @Test
    public void testFilter()
    {
        PlanNode node = filter(baseTableScan,
                and(
                        greaterThan(AE, new FunctionCallBuilder(metadata)
                                .setName(QualifiedName.of("rand"))
                                .build()),
                        lessThan(BE, bigintLiteral(10))));

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Non-deterministic functions should be purged
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, bigintLiteral(10))));
    }

    @Test
    public void testProject()
    {
        PlanNode node = new ProjectNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                Assignments.of(D, AE, E, CE));

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Rewrite in terms of project output symbols
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        lessThan(DE, bigintLiteral(10)),
                        equals(DE, EE)));
    }

    @Test
    public void testTopN()
    {
        PlanNode node = new TopNNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                1, new OrderingScheme(ImmutableList.of(A), ImmutableMap.of(A, SortOrder.ASC_NULLS_LAST)), TopNNode.Step.PARTIAL);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testLimit()
    {
        PlanNode node = new LimitNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                1,
                false);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testSort()
    {
        PlanNode node = new SortNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                new OrderingScheme(ImmutableList.of(A), ImmutableMap.of(A, SortOrder.ASC_NULLS_LAST)),
                false);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testWindow()
    {
        PlanNode node = new WindowNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                new WindowNode.Specification(
                        ImmutableList.of(A),
                        Optional.of(new OrderingScheme(
                                ImmutableList.of(A),
                                ImmutableMap.of(A, SortOrder.ASC_NULLS_LAST)))),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testTableScan()
    {
        // Effective predicate is True if there is no effective predicate
        Map<Symbol, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C, D)));
        PlanNode node = TableScanNode.newInstance(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments);
        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(effectivePredicate, BooleanLiteral.TRUE_LITERAL);

        node = new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.none()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.none());
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(effectivePredicate, FALSE_LITERAL);

        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(scanAssignments.get(A), Domain.singleValue(BIGINT, 1L)));
        node = new TableScanNode(
                newId(),
                makeTableHandle(predicate),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                predicate);
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(bigintLiteral(1L), AE)));

        predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                scanAssignments.get(A), Domain.singleValue(BIGINT, 1L),
                scanAssignments.get(B), Domain.singleValue(BIGINT, 2L)));
        node = new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.withColumnDomains(ImmutableMap.of(scanAssignments.get(A), Domain.singleValue(BIGINT, 1L)))),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                predicate);
        effectivePredicate = effectivePredicateExtractorWithoutTableProperties.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(bigintLiteral(2L), BE), equals(bigintLiteral(1L), AE)));

        node = new TableScanNode(
                newId(),
                makeTableHandle(predicate),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all());
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(effectivePredicate, BooleanLiteral.TRUE_LITERAL);

        node = new TableScanNode(
                newId(),
                makeTableHandle(predicate),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        scanAssignments.get(A), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)),
                        scanAssignments.get(B), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)))));
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(bigintLiteral(2L), BE), equals(bigintLiteral(1L), AE)));

        node = new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all());
        effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);
        assertEquals(effectivePredicate, BooleanLiteral.TRUE_LITERAL);
    }

    @Test
    public void testValues()
    {
        TypeProvider types = TypeProvider.copyOf(ImmutableMap.<Symbol, Type>builder()
                .put(A, BIGINT)
                .put(B, BIGINT)
                .build());

        // one column
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteral(1)),
                                ImmutableList.of(bigintLiteral(2)))),
                types,
                typeAnalyzer),
                new InPredicate(AE, new InListExpression(ImmutableList.of(bigintLiteral(1), bigintLiteral(2)))));

        // one column with null
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteral(1)),
                                ImmutableList.of(bigintLiteral(2)),
                                ImmutableList.of(new Cast(new NullLiteral(), toSqlType(BIGINT))))),
                types,
                typeAnalyzer),
                or(
                        new InPredicate(AE, new InListExpression(ImmutableList.of(bigintLiteral(1), bigintLiteral(2)))),
                        new IsNullPredicate(AE)));

        // all nulls
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A),
                        ImmutableList.of(
                                ImmutableList.of(new Cast(new NullLiteral(), toSqlType(BIGINT))))),
                types,
                typeAnalyzer),
                new IsNullPredicate(AE));

        // many rows
        List<List<Expression>> rows = IntStream.range(0, 500)
                .mapToObj(TestEffectivePredicateExtractor::bigintLiteral)
                .map(ImmutableList::of)
                .collect(toImmutableList());
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A),
                        rows),
                types,
                typeAnalyzer),
                new BetweenPredicate(AE, bigintLiteral(0), bigintLiteral(499)));

        // multiple columns
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A, B),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteral(1), bigintLiteral(100)),
                                ImmutableList.of(bigintLiteral(2), bigintLiteral(200)))),
                types,
                typeAnalyzer),
                and(
                        new InPredicate(AE, new InListExpression(ImmutableList.of(bigintLiteral(1), bigintLiteral(2)))),
                        new InPredicate(BE, new InListExpression(ImmutableList.of(bigintLiteral(100), bigintLiteral(200))))));

        // multiple columns with null
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A, B),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteral(1), new Cast(new NullLiteral(), toSqlType(BIGINT))),
                                ImmutableList.of(new Cast(new NullLiteral(), toSqlType(BIGINT)), bigintLiteral(200)))),
                types,
                typeAnalyzer),
                and(
                        or(new ComparisonExpression(EQUAL, AE, bigintLiteral(1)), new IsNullPredicate(AE)),
                        or(new ComparisonExpression(EQUAL, BE, bigintLiteral(200)), new IsNullPredicate(BE))));

        // non-deterministic
        ResolvedFunction rand = metadata.resolveFunction(QualifiedName.of("rand"), ImmutableList.of());
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A, B),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteral(1), new FunctionCall(rand.toQualifiedName(), ImmutableList.of())))),
                types,
                typeAnalyzer),
                new ComparisonExpression(EQUAL, AE, bigintLiteral(1)));

        // non-constant
        assertEquals(effectivePredicateExtractor.extract(
                SESSION,
                new ValuesNode(
                        newId(),
                        ImmutableList.of(A),
                        ImmutableList.of(
                                ImmutableList.of(bigintLiteral(1)),
                                ImmutableList.of(BE))),
                types,
                typeAnalyzer),
                TRUE_LITERAL);
    }

    @Test
    public void testUnion()
    {
        ImmutableListMultimap<Symbol, Symbol> symbolMapping = ImmutableListMultimap.of(A, B, A, C, A, E);
        PlanNode node = new UnionNode(newId(),
                ImmutableList.of(
                        filter(baseTableScan, greaterThan(AE, bigintLiteral(10))),
                        filter(baseTableScan, and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100)))),
                        filter(baseTableScan, and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100))))),
                symbolMapping,
                ImmutableList.copyOf(symbolMapping.keySet()));

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Only the common conjuncts can be inferred through a Union
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(greaterThan(AE, bigintLiteral(10))));
    }

    @Test
    public void testInnerJoin()
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));

        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.INNER,
                left,
                right,
                criteria,
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.of(lessThanOrEqual(BE, EE)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // All predicates having output symbol should be carried through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100)),
                        equals(AE, DE),
                        equals(BE, EE),
                        lessThanOrEqual(BE, EE)));
    }

    @Test
    public void testInnerJoinPropagatesPredicatesViaEquiConditions()
    {
        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan, equals(AE, bigintLiteral(10)));

        // predicates on "a" column should be propagated to output symbols via join equi conditions
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.INNER,
                left,
                rightScan,
                ImmutableList.of(new JoinNode.EquiJoinClause(A, D)),
                ImmutableList.<Symbol>builder()
                        .addAll(rightScan.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        assertEquals(
                normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(equals(DE, bigintLiteral(10))));
    }

    @Test
    public void testInnerJoinWithFalseFilter()
    {
        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.INNER,
                leftScan,
                rightScan,
                ImmutableList.of(new JoinNode.EquiJoinClause(A, D)),
                ImmutableList.<Symbol>builder()
                        .addAll(leftScan.getOutputSymbols())
                        .addAll(rightScan.getOutputSymbols())
                        .build(),
                Optional.of(FALSE_LITERAL),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        assertEquals(effectivePredicate, FALSE_LITERAL);
    }

    @Test
    public void testLeftJoin()
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.LEFT,
                left,
                right,
                criteria,
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // All right side symbols having output symbols should be checked against NULL
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        or(equals(DE, EE), and(isNull(DE), isNull(EE))),
                        or(lessThan(FE, bigintLiteral(100)), isNull(FE)),
                        or(equals(AE, DE), isNull(DE)),
                        or(equals(BE, EE), isNull(EE))));
    }

    @Test
    public void testLeftJoinWithFalseInner()
    {
        List<JoinNode.EquiJoinClause> criteria = ImmutableList.of(new JoinNode.EquiJoinClause(A, D));

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan, FALSE_LITERAL);
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.LEFT,
                left,
                right,
                criteria,
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // False literal on the right side should be ignored
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        or(equals(AE, DE), isNull(DE))));
    }

    @Test
    public void testRightJoin()
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.RIGHT,
                left,
                right,
                criteria,
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // All left side symbols should be checked against NULL
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(or(lessThan(BE, AE), and(isNull(BE), isNull(AE))),
                        or(lessThan(CE, bigintLiteral(10)), isNull(CE)),
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100)),
                        or(equals(AE, DE), isNull(AE)),
                        or(equals(BE, EE), isNull(BE))));
    }

    @Test
    public void testRightJoinWithFalseInner()
    {
        List<JoinNode.EquiJoinClause> criteria = ImmutableList.of(new JoinNode.EquiJoinClause(A, D));

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan, FALSE_LITERAL);
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.RIGHT,
                left,
                right,
                criteria,
                ImmutableList.<Symbol>builder()
                        .addAll(left.getOutputSymbols())
                        .addAll(right.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // False literal on the left side should be ignored
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(equals(DE, EE),
                        lessThan(FE, bigintLiteral(100)),
                        or(equals(AE, DE), isNull(AE))));
    }

    @Test
    public void testSemiJoin()
    {
        PlanNode node = new SemiJoinNode(newId(),
                filter(baseTableScan, and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100)))),
                filter(baseTableScan, greaterThan(AE, bigintLiteral(5))),
                A, B, C,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(SESSION, node, TypeProvider.empty(), typeAnalyzer);

        // Currently, only pull predicates through the source plan
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100)))));
    }

    private static TableScanNode tableScanNode(Map<Symbol, ColumnHandle> scanAssignments)
    {
        return new TableScanNode(
                newId(),
                makeTableHandle(TupleDomain.all()),
                ImmutableList.copyOf(scanAssignments.keySet()),
                scanAssignments,
                TupleDomain.all());
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
        if (number < Integer.MAX_VALUE && number > Integer.MIN_VALUE) {
            return new GenericLiteral("BIGINT", String.valueOf(number));
        }
        return new LongLiteral(String.valueOf(number));
    }

    private static ComparisonExpression equals(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(EQUAL, expression1, expression2);
    }

    private static ComparisonExpression lessThan(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, expression1, expression2);
    }

    private static ComparisonExpression lessThanOrEqual(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, expression1, expression2);
    }

    private static ComparisonExpression greaterThan(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, expression1, expression2);
    }

    private static IsNullPredicate isNull(Expression expression)
    {
        return new IsNullPredicate(expression);
    }

    private static ResolvedFunction fakeFunction(String name, FunctionKind kind)
    {
        Signature boundSignature = new Signature(name, kind, UNKNOWN.getTypeSignature(), ImmutableList.of());
        return new ResolvedFunction(boundSignature, toFunctionId(boundSignature));
    }

    private Set<Expression> normalizeConjuncts(Expression... conjuncts)
    {
        return normalizeConjuncts(Arrays.asList(conjuncts));
    }

    private Set<Expression> normalizeConjuncts(Collection<Expression> conjuncts)
    {
        return normalizeConjuncts(combineConjuncts(metadata, conjuncts));
    }

    private Set<Expression> normalizeConjuncts(Expression predicate)
    {
        // Normalize the predicate by identity so that the EqualityInference will produce stable rewrites in this test
        // and thereby produce comparable Sets of conjuncts from this method.
        predicate = expressionNormalizer.normalize(predicate);

        // Equality inference rewrites and equality generation will always be stable across multiple runs in the same JVM
        EqualityInference inference = EqualityInference.newInstance(metadata, predicate);

        Set<Symbol> scope = SymbolsExtractor.extractUnique(predicate);
        Set<Expression> rewrittenSet = new HashSet<>();
        for (Expression expression : EqualityInference.nonInferrableConjuncts(metadata, predicate)) {
            Expression rewritten = inference.rewrite(expression, scope);
            Preconditions.checkState(rewritten != null, "Rewrite with full symbol scope should always be possible");
            rewrittenSet.add(rewritten);
        }
        rewrittenSet.addAll(inference.generateEqualitiesPartitionedBy(scope).getScopeEqualities());

        return rewrittenSet;
    }

    private static TableHandle makeTableHandle(TupleDomain<ColumnHandle> predicate)
    {
        return new TableHandle(
                new CatalogName("test"),
                new PredicatedTableHandle(predicate),
                TestingTransactionHandle.create(),
                Optional.empty());
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
                SubExpressionExtractor.extract(expression).stream()
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
