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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.BuiltinFunctionCallBuilder;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.PlanTester;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.cache.CanonicalSubplanExtractor.canonicalExpressionToColumnId;
import static io.trino.cache.CommonSubqueriesExtractor.aggregationKey;
import static io.trino.cache.CommonSubqueriesExtractor.combine;
import static io.trino.cache.CommonSubqueriesExtractor.scanFilterProjectKey;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.cacheDataPlanNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.chooseAlternativeNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.loadCachedDataPlanNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.symbol;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.testing.PlanTesterBuilder.planTesterBuilder;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestCacheCommonSubqueries
        extends BasePlanTest
{
    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema("tiny")
            // disable so join order is not changed in tests
            .setSystemProperty(JOIN_REORDERING_STRATEGY, "none")
            .build();
    private static final CacheColumnId NATIONKEY_COLUMN_ID = new CacheColumnId("[nationkey:bigint]");
    private static final CacheColumnId REGIONKEY_COLUMN_ID = new CacheColumnId("[regionkey:bigint]");

    private String testCatalogId;

    @Override
    protected PlanTester createPlanTester()
    {
        PlanTester planTester = planTesterBuilder(TEST_SESSION)
                .withCacheConfig(new CacheConfig()
                        .setEnabled(true)
                        .setCacheCommonSubqueriesEnabled(true))
                .build();

        planTester.createCatalog(planTester.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1, false),
                ImmutableMap.of());
        testCatalogId = planTester.getCatalogHandle(TEST_SESSION.getCatalog().orElseThrow()).getId();

        return planTester;
    }

    @Test
    public void testCacheCommonSubqueries()
    {
        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(
                        scanFilterProjectKey(new CacheTableId(testCatalogId + ":tiny:nation:0.01")),
                        Optional.empty(),
                        ImmutableList.of(REGIONKEY_COLUMN_ID, NATIONKEY_COLUMN_ID),
                        ImmutableList.of(BIGINT, BIGINT)),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        NATIONKEY_COLUMN_ID, Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 5L), greaterThan(BIGINT, 10L)), false))));
        Map<CacheColumnId, ColumnHandle> columnHandles = ImmutableMap.of(
                NATIONKEY_COLUMN_ID, new TpchColumnHandle("nationkey", BIGINT),
                REGIONKEY_COLUMN_ID, new TpchColumnHandle("regionkey", BIGINT));
        assertPlan("""
                        SELECT * FROM
                        (SELECT regionkey FROM nation WHERE nationkey > 10)
                        UNION ALL
                        (SELECT regionkey FROM nation WHERE nationkey < 5)
                        """,
                anyTree(exchange(LOCAL,
                        chooseAlternativeNode(
                                // original subplan
                                strictProject(ImmutableMap.of("REGIONKEY_A", expression("REGIONKEY_A")),
                                        filter("NATIONKEY_A > BIGINT '10'",
                                                tableScan("nation", ImmutableMap.of("NATIONKEY_A", "nationkey", "REGIONKEY_A", "regionkey")))),
                                // store data in cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_A", expression("REGIONKEY_A")),
                                        filter("NATIONKEY_A > BIGINT '10'",
                                                cacheDataPlanNode(
                                                        strictProject(ImmutableMap.of("REGIONKEY_A", expression("REGIONKEY_A"), "NATIONKEY_A", expression("NATIONKEY_A")),
                                                                filter("NATIONKEY_A > BIGINT '10' OR NATIONKEY_A < BIGINT '5'",
                                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_A", "nationkey", "REGIONKEY_A", "regionkey"))))))),
                                // load data from cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_A", expression("REGIONKEY_A")),
                                        filter("NATIONKEY_A > BIGINT '10'",
                                                loadCachedDataPlanNode(signature, columnHandles, "REGIONKEY_A", "NATIONKEY_A")))),
                        chooseAlternativeNode(
                                // original subplan
                                strictProject(ImmutableMap.of("REGIONKEY_B", expression("REGIONKEY_B")),
                                        filter("NATIONKEY_B < BIGINT '5'",
                                                tableScan("nation", ImmutableMap.of("NATIONKEY_B", "nationkey", "REGIONKEY_B", "regionkey")))),
                                // store data in cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_B", expression("REGIONKEY_B")),
                                        filter("NATIONKEY_B < BIGINT '5'",
                                                cacheDataPlanNode(
                                                        strictProject(ImmutableMap.of("REGIONKEY_B", expression("REGIONKEY_B"), "NATIONKEY_B", expression("NATIONKEY_B")),
                                                                filter("NATIONKEY_B > BIGINT '10' OR NATIONKEY_B < BIGINT '5'",
                                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_B", "nationkey", "REGIONKEY_B", "regionkey"))))))),
                                // load data from cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_B", expression("REGIONKEY_B")),
                                        filter("NATIONKEY_B < BIGINT '5'",
                                                loadCachedDataPlanNode(signature, columnHandles, "REGIONKEY_B", "NATIONKEY_B")))))));
    }

    @Test
    public void testJoinQuery()
    {
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NATIONKEY_COLUMN_ID, REGIONKEY_COLUMN_ID);
        List<Type> cacheColumnTypes = ImmutableList.of(BIGINT, BIGINT);
        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(
                        scanFilterProjectKey(new CacheTableId(testCatalogId + ":tiny:nation:0.01")),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnTypes),
                TupleDomain.all());
        Predicate<FilterNode> isNationKeyDynamicFilter = node -> DynamicFilters.getDescriptor(node.getPredicate())
                .map(descriptor -> descriptor.getInput().equals(new SymbolReference("nationkey")))
                .orElse(false);
        assertPlan("""
                        SELECT * FROM
                        (SELECT nationkey FROM nation)
                        JOIN
                        (SELECT regionkey FROM nation)
                        ON nationkey = regionkey
                        """,
                anyTree(node(JoinNode.class,
                        chooseAlternativeNode(
                                // original subplan
                                filter(TRUE_LITERAL, // for DF on nationkey
                                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey")))
                                        .with(FilterNode.class, isNationKeyDynamicFilter),
                                // store data in cache alternative
                                strictProject(ImmutableMap.of("NATIONKEY", expression("NATIONKEY")),
                                        cacheDataPlanNode(
                                                filter(TRUE_LITERAL, // for DF on nationkey
                                                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey")))
                                                        .with(FilterNode.class, isNationKeyDynamicFilter))),
                                // load data from cache alternative
                                strictProject(ImmutableMap.of("NATIONKEY", expression("NATIONKEY")),
                                        loadCachedDataPlanNode(
                                                signature,
                                                dfDisjuncts -> dfDisjuncts.size() == 1,
                                                "NATIONKEY", "REGIONKEY"))),
                        anyTree(
                                chooseAlternativeNode(
                                        // original subplan
                                        tableScan("nation", ImmutableMap.of("REGIONKEY", "regionkey")),
                                        // store data in cache alternative
                                        strictProject(ImmutableMap.of("REGIONKEY", expression("REGIONKEY")),
                                                cacheDataPlanNode(
                                                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey")))),
                                        // load data from cache alternative
                                        strictProject(ImmutableMap.of("REGIONKEY", expression("REGIONKEY")),
                                                loadCachedDataPlanNode(signature, "NATIONKEY", "REGIONKEY")))))));
    }

    @Test
    public void testJoinQueryWithCommonDynamicFilters()
    {
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(NATIONKEY_COLUMN_ID, REGIONKEY_COLUMN_ID);
        List<Type> cacheColumnTypes = ImmutableList.of(BIGINT, BIGINT);
        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(
                        combine(
                                scanFilterProjectKey(new CacheTableId(testCatalogId + ":tiny:nation:0.01")),
                                "filters=((\"[nationkey:bigint]\" IN (BIGINT '0', BIGINT '1')) OR (\"[regionkey:bigint]\" IN (BIGINT '0', BIGINT '1')))"),
                        Optional.empty(),
                        cacheColumnIds,
                        cacheColumnTypes),
                TupleDomain.all());
        Map<CacheColumnId, ColumnHandle> columnHandles = ImmutableMap.of(
                NATIONKEY_COLUMN_ID, new TpchColumnHandle("nationkey", BIGINT),
                REGIONKEY_COLUMN_ID, new TpchColumnHandle("regionkey", BIGINT));
        assertPlan("""
                        (SELECT nationkey FROM nation n JOIN (SELECT * FROM (VALUES 0, 1) t(a)) t ON n.nationkey = t.a)
                        UNION ALL
                        (SELECT regionkey FROM nation n JOIN (SELECT * FROM (VALUES 0, 1) t(a)) t ON n.regionkey = t.a)
                        """,
                anyTree(exchange(LOCAL,
                        node(JoinNode.class,
                                chooseAlternativeNode(
                                        anyTree(tableScan("nation")),
                                        anyTree(cacheDataPlanNode(
                                                anyTree(tableScan("nation")))),
                                        anyTree(loadCachedDataPlanNode(signature, columnHandles, dfDisjuncts -> dfDisjuncts.size() == 2, "NATIONKEY", "REGIONKEY"))),
                                anyTree(node(ValuesNode.class))),
                        node(JoinNode.class,
                                chooseAlternativeNode(
                                        anyTree(tableScan("nation")),
                                        anyTree(cacheDataPlanNode(
                                                anyTree(tableScan("nation")))),
                                        anyTree(loadCachedDataPlanNode(signature, columnHandles, dfDisjuncts -> dfDisjuncts.size() == 2, "NATIONKEY", "REGIONKEY"))),
                                anyTree(node(ValuesNode.class))))));
    }

    @Test
    public void testAggregationQuery()
    {
        ExpressionWithType nationkey = new ExpressionWithType("\"[nationkey:bigint]\"", BIGINT);
        Expression max = getFunctionCallBuilder("max", nationkey).build();
        Expression sum = getFunctionCallBuilder("sum", nationkey).build();
        Expression avg = getFunctionCallBuilder("avg", nationkey).build();
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(REGIONKEY_COLUMN_ID, canonicalExpressionToColumnId(max), canonicalExpressionToColumnId(sum), canonicalExpressionToColumnId(avg));
        List<Type> cacheColumnTypes = ImmutableList.of(BIGINT, BIGINT, RowType.anonymousRow(BIGINT, BIGINT), RowType.anonymousRow(DOUBLE, BIGINT));
        PlanSignatureWithPredicate signature = new PlanSignatureWithPredicate(
                new PlanSignature(
                        aggregationKey(scanFilterProjectKey(new CacheTableId(testCatalogId + ":tiny:nation:0.01"))),
                        Optional.of(ImmutableList.of(REGIONKEY_COLUMN_ID)),
                        cacheColumnIds,
                        cacheColumnTypes),
                TupleDomain.all());
        assertPlan("""
                        SELECT sum(nationkey), max(nationkey) FROM nation GROUP BY regionkey
                        UNION ALL
                        SELECT avg(nationkey), sum(nationkey) FROM nation GROUP BY regionkey""",
                anyTree(anyTree(aggregation(
                                singleGroupingSet("REGIONKEY_A"),
                                ImmutableMap.of(
                                        Optional.of("MAX_A"), functionCall("max", false, ImmutableList.of(symbol("MAX_PARTIAL_A"))),
                                        Optional.of("SUM_A"), functionCall("sum", false, ImmutableList.of(symbol("SUM_PARTIAL_A")))),
                                Optional.empty(),
                                FINAL,
                                anyTree(
                                        chooseAlternativeNode(
                                                // original subplan
                                                aggregation(
                                                        singleGroupingSet("REGIONKEY_A"),
                                                        ImmutableMap.of(
                                                                Optional.of("MAX_PARTIAL_A"), functionCall("max", false, ImmutableList.of(symbol("NATIONKEY_A"))),
                                                                Optional.of("SUM_PARTIAL_A"), functionCall("sum", false, ImmutableList.of(symbol("NATIONKEY_A")))),
                                                        Optional.empty(),
                                                        PARTIAL,
                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_A", "nationkey", "REGIONKEY_A", "regionkey"))),
                                                // store data in cache alternative
                                                strictProject(ImmutableMap.of(
                                                                "REGIONKEY_A", expression("REGIONKEY_A"),
                                                                "MAX_PARTIAL_A", expression("MAX_PARTIAL_A"),
                                                                "SUM_PARTIAL_A", expression("SUM_PARTIAL_A")),
                                                        cacheDataPlanNode(
                                                                aggregation(
                                                                        singleGroupingSet("REGIONKEY_A"),
                                                                        ImmutableMap.of(
                                                                                Optional.of("MAX_PARTIAL_A"), functionCall("max", false, ImmutableList.of(symbol("NATIONKEY_A"))),
                                                                                Optional.of("SUM_PARTIAL_A"), functionCall("sum", false, ImmutableList.of(symbol("NATIONKEY_A"))),
                                                                                Optional.of("AVG_PARTIAL_A"), functionCall("avg", false, ImmutableList.of(symbol("NATIONKEY_A")))),
                                                                        Optional.empty(),
                                                                        PARTIAL,
                                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_A", "nationkey", "REGIONKEY_A", "regionkey"))))),
                                                // load data from cache alternative
                                                strictProject(ImmutableMap.of(
                                                                "REGIONKEY_A", expression("REGIONKEY_A"),
                                                                "MAX_PARTIAL_A", expression("MAX_PARTIAL_A"),
                                                                "SUM_PARTIAL_A", expression("SUM_PARTIAL_A")),
                                                        loadCachedDataPlanNode(signature, "REGIONKEY_A", "MAX_PARTIAL_A", "SUM_PARTIAL_A", "AVG_PARTIAL_A")))))),
                        anyTree(aggregation(
                                singleGroupingSet("REGIONKEY_B"),
                                ImmutableMap.of(
                                        Optional.of("AVG_B"), functionCall("avg", false, ImmutableList.of(symbol("AVG_PARTIAL_B"))),
                                        Optional.of("SUM_B"), functionCall("sum", false, ImmutableList.of(symbol("SUM_PARTIAL_B")))),
                                Optional.empty(),
                                FINAL,
                                anyTree(
                                        chooseAlternativeNode(
                                                // original subplan
                                                aggregation(
                                                        singleGroupingSet("REGIONKEY_B"),
                                                        ImmutableMap.of(
                                                                Optional.of("SUM_PARTIAL_B"), functionCall("sum", false, ImmutableList.of(symbol("NATIONKEY_B"))),
                                                                Optional.of("AVG_PARTIAL_B"), functionCall("avg", false, ImmutableList.of(symbol("NATIONKEY_B")))),
                                                        Optional.empty(),
                                                        PARTIAL,
                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_B", "nationkey", "REGIONKEY_B", "regionkey"))),
                                                // store data in cache alternative
                                                strictProject(ImmutableMap.of(
                                                                "REGIONKEY_B", expression("REGIONKEY_B"),
                                                                "SUM_PARTIAL_B", expression("SUM_PARTIAL_B"),
                                                                "AVG_PARTIAL_B", expression("AVG_PARTIAL_B")),
                                                        cacheDataPlanNode(
                                                                aggregation(
                                                                        singleGroupingSet("REGIONKEY_B"),
                                                                        ImmutableMap.of(
                                                                                Optional.of("MAX_PARTIAL_B"), functionCall("max", false, ImmutableList.of(symbol("NATIONKEY_B"))),
                                                                                Optional.of("SUM_PARTIAL_B"), functionCall("sum", false, ImmutableList.of(symbol("NATIONKEY_B"))),
                                                                                Optional.of("AVG_PARTIAL_B"), functionCall("avg", false, ImmutableList.of(symbol("NATIONKEY_B")))),
                                                                        Optional.empty(),
                                                                        PARTIAL,
                                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_B", "nationkey", "REGIONKEY_B", "regionkey"))))),
                                                // load data from cache alternative
                                                strictProject(ImmutableMap.of(
                                                                "REGIONKEY_B", expression("REGIONKEY_B"),
                                                                "SUM_PARTIAL_B", expression("SUM_PARTIAL_B"),
                                                                "AVG_PARTIAL_B", expression("AVG_PARTIAL_B")),
                                                        loadCachedDataPlanNode(signature, "REGIONKEY_B", "MAX_PARTIAL_B", "SUM_PARTIAL_B", "AVG_PARTIAL_B"))))))));
    }

    private BuiltinFunctionCallBuilder getFunctionCallBuilder(String name, ExpressionWithType... arguments)
    {
        PlanTester planTester = getPlanTester();
        BuiltinFunctionCallBuilder builder = BuiltinFunctionCallBuilder.resolve(planTester.getPlannerContext().getMetadata())
                .setName(name);
        for (ExpressionWithType argument : arguments) {
            builder.addArgument(argument.type, argument.expression);
        }
        return builder;
    }

    // workaround for https://github.com/google/error-prone/issues/2713
    @SuppressWarnings("unused")
    private record ExpressionWithType(Expression expression, Type type)
    {
        public ExpressionWithType(@Language("SQL") String expression, Type type)
        {
            this(PlanBuilder.expression(expression), type);
        }
    }
}
