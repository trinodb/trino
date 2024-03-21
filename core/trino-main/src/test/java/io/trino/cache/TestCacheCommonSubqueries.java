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
import io.trino.metadata.ResolvedFunction;
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
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.cache.CanonicalSubplanExtractor.canonicalAggregationToColumnId;
import static io.trino.cache.CommonSubqueriesExtractor.aggregationKey;
import static io.trino.cache.CommonSubqueriesExtractor.combine;
import static io.trino.cache.CommonSubqueriesExtractor.scanFilterProjectKey;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.cacheDataPlanNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.chooseAlternativeNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.loadCachedDataPlanNode;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.symbol;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
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
                                strictProject(ImmutableMap.of("REGIONKEY_A", expression(new Reference(BIGINT, "REGIONKEY_A"))),
                                        filter(
                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "NATIONKEY_A"), new Constant(BIGINT, 10L)),
                                                tableScan("nation", ImmutableMap.of("NATIONKEY_A", "nationkey", "REGIONKEY_A", "regionkey")))),
                                // store data in cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_A", expression(new Reference(BIGINT, "REGIONKEY_A"))),
                                        filter(
                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "NATIONKEY_A"), new Constant(BIGINT, 10L)),
                                                cacheDataPlanNode(
                                                        strictProject(ImmutableMap.of("REGIONKEY_A", expression(new Reference(BIGINT, "REGIONKEY_A")), "NATIONKEY_A", expression(new Reference(BIGINT, "NATIONKEY_A"))),
                                                                filter(
                                                                        new Logical(OR, ImmutableList.of(
                                                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "NATIONKEY_A"), new Constant(BIGINT, 10L)),
                                                                                new Comparison(LESS_THAN, new Reference(BIGINT, "NATIONKEY_A"), new Constant(BIGINT, 5L)))),
                                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_A", "nationkey", "REGIONKEY_A", "regionkey"))))))),
                                // load data from cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_A", expression(new Reference(BIGINT, "REGIONKEY_A"))),
                                        filter(
                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "NATIONKEY_A"), new Constant(BIGINT, 10L)),
                                                loadCachedDataPlanNode(signature, columnHandles, "REGIONKEY_A", "NATIONKEY_A")))),
                        chooseAlternativeNode(
                                // original subplan
                                strictProject(ImmutableMap.of("REGIONKEY_B", expression(new Reference(BIGINT, "REGIONKEY_B"))),
                                        filter(
                                                new Comparison(LESS_THAN, new Reference(BIGINT, "NATIONKEY_B"), new Constant(BIGINT, 5L)),
                                                tableScan("nation", ImmutableMap.of("NATIONKEY_B", "nationkey", "REGIONKEY_B", "regionkey")))),
                                // store data in cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_B", expression(new Reference(BIGINT, "REGIONKEY_B"))),
                                        filter(
                                                new Comparison(LESS_THAN, new Reference(BIGINT, "NATIONKEY_B"), new Constant(BIGINT, 5L)),
                                                cacheDataPlanNode(
                                                        strictProject(ImmutableMap.of("REGIONKEY_B", expression(new Reference(BIGINT, "REGIONKEY_B")), "NATIONKEY_B", expression(new Reference(BIGINT, "NATIONKEY_B"))),
                                                                filter(
                                                                        new Logical(OR, ImmutableList.of(
                                                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "NATIONKEY_B"), new Constant(BIGINT, 10L)),
                                                                                new Comparison(LESS_THAN, new Reference(BIGINT, "NATIONKEY_B"), new Constant(BIGINT, 5L)))),
                                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_B", "nationkey", "REGIONKEY_B", "regionkey"))))))),
                                // load data from cache alternative
                                strictProject(ImmutableMap.of("REGIONKEY_B", expression(new Reference(BIGINT, "REGIONKEY_B"))),
                                        filter(
                                                new Comparison(LESS_THAN, new Reference(BIGINT, "NATIONKEY_B"), new Constant(BIGINT, 5L)),
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
                .map(descriptor -> descriptor.getInput().equals(new Reference(BIGINT, "nationkey")))
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
                                filter(TRUE, // for DF on nationkey
                                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey")))
                                        .with(FilterNode.class, isNationKeyDynamicFilter),
                                // store data in cache alternative
                                strictProject(ImmutableMap.of("NATIONKEY", expression(new Reference(BIGINT, "NATIONKEY"))),
                                        cacheDataPlanNode(
                                                filter(TRUE, // for DF on nationkey
                                                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey")))
                                                        .with(FilterNode.class, isNationKeyDynamicFilter))),
                                // load data from cache alternative
                                strictProject(ImmutableMap.of("NATIONKEY", expression(new Reference(BIGINT, "NATIONKEY"))),
                                        loadCachedDataPlanNode(
                                                signature,
                                                dfDisjuncts -> dfDisjuncts.size() == 1,
                                                "NATIONKEY", "REGIONKEY"))),
                        anyTree(
                                chooseAlternativeNode(
                                        // original subplan
                                        tableScan("nation", ImmutableMap.of("REGIONKEY", "regionkey")),
                                        // store data in cache alternative
                                        strictProject(ImmutableMap.of("REGIONKEY", expression(new Reference(BIGINT, "REGIONKEY"))),
                                                cacheDataPlanNode(
                                                        tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey", "REGIONKEY", "regionkey")))),
                                        // load data from cache alternative
                                        strictProject(ImmutableMap.of("REGIONKEY", expression(new Reference(BIGINT, "REGIONKEY"))),
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
                                "filters=(([nationkey:bigint] IN (bigint '0', bigint '1')) OR ([regionkey:bigint] IN (bigint '0', bigint '1')))"),
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
        Reference nationkey = new Reference(BIGINT, "[nationkey:bigint]");
        CanonicalAggregation max = canonicalAggregation("max", nationkey);
        CanonicalAggregation sum = canonicalAggregation("sum", nationkey);
        CanonicalAggregation avg = canonicalAggregation("avg", nationkey);
        List<CacheColumnId> cacheColumnIds = ImmutableList.of(REGIONKEY_COLUMN_ID, canonicalAggregationToColumnId(max), canonicalAggregationToColumnId(sum), canonicalAggregationToColumnId(avg));
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
                                        Optional.of("MAX_A"), aggregationFunction("max", false, ImmutableList.of(symbol("MAX_PARTIAL_A"))),
                                        Optional.of("SUM_A"), aggregationFunction("sum", false, ImmutableList.of(symbol("SUM_PARTIAL_A")))),
                                Optional.empty(),
                                FINAL,
                                anyTree(
                                        chooseAlternativeNode(
                                                // original subplan
                                                aggregation(
                                                        singleGroupingSet("REGIONKEY_A"),
                                                        ImmutableMap.of(
                                                                Optional.of("MAX_PARTIAL_A"), aggregationFunction("max", false, ImmutableList.of(symbol("NATIONKEY_A"))),
                                                                Optional.of("SUM_PARTIAL_A"), aggregationFunction("sum", false, ImmutableList.of(symbol("NATIONKEY_A")))),
                                                        Optional.empty(),
                                                        PARTIAL,
                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_A", "nationkey", "REGIONKEY_A", "regionkey"))),
                                                // store data in cache alternative
                                                strictProject(ImmutableMap.of(
                                                                "REGIONKEY_A", expression(new Reference(BIGINT, "REGIONKEY_A")),
                                                                "MAX_PARTIAL_A", expression(new Reference(BIGINT, "MAX_PARTIAL_A")),
                                                                "SUM_PARTIAL_A", expression(new Reference(BIGINT, "SUM_PARTIAL_A"))),
                                                        cacheDataPlanNode(
                                                                aggregation(
                                                                        singleGroupingSet("REGIONKEY_A"),
                                                                        ImmutableMap.of(
                                                                                Optional.of("MAX_PARTIAL_A"), aggregationFunction("max", false, ImmutableList.of(symbol("NATIONKEY_A"))),
                                                                                Optional.of("SUM_PARTIAL_A"), aggregationFunction("sum", false, ImmutableList.of(symbol("NATIONKEY_A"))),
                                                                                Optional.of("AVG_PARTIAL_A"), aggregationFunction("avg", false, ImmutableList.of(symbol("NATIONKEY_A")))),
                                                                        Optional.empty(),
                                                                        PARTIAL,
                                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_A", "nationkey", "REGIONKEY_A", "regionkey"))))),
                                                // load data from cache alternative
                                                strictProject(ImmutableMap.of(
                                                                "REGIONKEY_A", expression(new Reference(BIGINT, "REGIONKEY_A")),
                                                                "MAX_PARTIAL_A", expression(new Reference(BIGINT, "MAX_PARTIAL_A")),
                                                                "SUM_PARTIAL_A", expression(new Reference(BIGINT, "SUM_PARTIAL_A"))),
                                                        loadCachedDataPlanNode(signature, "REGIONKEY_A", "MAX_PARTIAL_A", "SUM_PARTIAL_A", "AVG_PARTIAL_A")))))),
                        anyTree(aggregation(
                                singleGroupingSet("REGIONKEY_B"),
                                ImmutableMap.of(
                                        Optional.of("AVG_B"), aggregationFunction("avg", false, ImmutableList.of(symbol("AVG_PARTIAL_B"))),
                                        Optional.of("SUM_B"), aggregationFunction("sum", false, ImmutableList.of(symbol("SUM_PARTIAL_B")))),
                                Optional.empty(),
                                FINAL,
                                anyTree(
                                        chooseAlternativeNode(
                                                // original subplan
                                                aggregation(
                                                        singleGroupingSet("REGIONKEY_B"),
                                                        ImmutableMap.of(
                                                                Optional.of("SUM_PARTIAL_B"), aggregationFunction("sum", false, ImmutableList.of(symbol("NATIONKEY_B"))),
                                                                Optional.of("AVG_PARTIAL_B"), aggregationFunction("avg", false, ImmutableList.of(symbol("NATIONKEY_B")))),
                                                        Optional.empty(),
                                                        PARTIAL,
                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_B", "nationkey", "REGIONKEY_B", "regionkey"))),
                                                // store data in cache alternative
                                                strictProject(ImmutableMap.of(
                                                                "REGIONKEY_B", expression(new Reference(BIGINT, "REGIONKEY_B")),
                                                                "SUM_PARTIAL_B", expression(new Reference(BIGINT, "SUM_PARTIAL_B")),
                                                                "AVG_PARTIAL_B", expression(new Reference(DOUBLE, "AVG_PARTIAL_B"))),
                                                        cacheDataPlanNode(
                                                                aggregation(
                                                                        singleGroupingSet("REGIONKEY_B"),
                                                                        ImmutableMap.of(
                                                                                Optional.of("MAX_PARTIAL_B"), aggregationFunction("max", false, ImmutableList.of(symbol("NATIONKEY_B"))),
                                                                                Optional.of("SUM_PARTIAL_B"), aggregationFunction("sum", false, ImmutableList.of(symbol("NATIONKEY_B"))),
                                                                                Optional.of("AVG_PARTIAL_B"), aggregationFunction("avg", false, ImmutableList.of(symbol("NATIONKEY_B")))),
                                                                        Optional.empty(),
                                                                        PARTIAL,
                                                                        tableScan("nation", ImmutableMap.of("NATIONKEY_B", "nationkey", "REGIONKEY_B", "regionkey"))))),
                                                // load data from cache alternative
                                                strictProject(ImmutableMap.of(
                                                                "REGIONKEY_B", expression(new Reference(BIGINT, "REGIONKEY_B")),
                                                                "SUM_PARTIAL_B", expression(new Reference(BIGINT, "SUM_PARTIAL_B")),
                                                                "AVG_PARTIAL_B", expression(new Reference(DOUBLE, "AVG_PARTIAL_B"))),
                                                        loadCachedDataPlanNode(signature, "REGIONKEY_B", "MAX_PARTIAL_B", "SUM_PARTIAL_B", "AVG_PARTIAL_B"))))))));
    }

    private CanonicalAggregation canonicalAggregation(String name, Expression input)
    {
        ResolvedFunction resolvedFunction = getPlanTester().getPlannerContext().getMetadata().resolveBuiltinFunction(name, TypeSignatureProvider.fromTypes(input.type()));
        return new CanonicalAggregation(resolvedFunction, Optional.empty(), ImmutableList.of(input));
    }
}
