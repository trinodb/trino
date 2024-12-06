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

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cache.CanonicalSubplan.AggregationKey;
import io.trino.cache.CanonicalSubplan.FilterProjectKey;
import io.trino.cache.CanonicalSubplan.Key;
import io.trino.cache.CanonicalSubplan.ScanFilterProjectKey;
import io.trino.cache.CanonicalSubplan.TableScan;
import io.trino.cache.CanonicalSubplan.TopNKey;
import io.trino.cache.CanonicalSubplan.TopNRankingKey;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TestingPlannerContext;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingHandles;
import io.trino.testing.TestingMetadata;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.cache.CanonicalSubplanExtractor.canonicalAggregationToColumnId;
import static io.trino.cache.CanonicalSubplanExtractor.canonicalExpressionToColumnId;
import static io.trino.cache.CanonicalSubplanExtractor.columnIdToSymbol;
import static io.trino.cache.CanonicalSubplanExtractor.extractCanonicalSubplans;
import static io.trino.metadata.TestMetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TestCanonicalSubplanExtractor
        extends BasePlanTest
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private static final CacheTableId CACHE_TABLE_ID = new CacheTableId("cache_table_id");
    private static final PlanNodeId SCAN_NODE_ID = new PlanNodeId("scan_id");
    private static final String CATALOG_ID = TEST_TABLE_HANDLE.catalogHandle().getId();
    private static final CacheTableId CATALOG_CACHE_TABLE_ID = new CacheTableId(CATALOG_ID + ":" + CACHE_TABLE_ID);
    private static final CacheColumnId CACHE_COL1 = new CacheColumnId("[cache_column1]");
    private static final CacheColumnId CACHE_COL2 = new CacheColumnId("[cache_column2]");
    private static final CacheColumnId REGIONKEY_ID = new CacheColumnId("[regionkey:bigint]");
    private static final CacheColumnId NATIONKEY_ID = new CacheColumnId("[nationkey:bigint]");
    private static final CacheColumnId NAME_ID = new CacheColumnId("[name:varchar(25)]");
    private static final Reference REGIONKEY_REF = new Reference(BIGINT, "[regionkey:bigint]");
    private static final Reference NATIONKEY_REF = new Reference(BIGINT, "[nationkey:bigint]");
    private static final Reference NAME_REF = new Reference(createVarcharType(25), "[name:varchar(25)]");
    private static final Reference CACHE_COL1_REF = new Reference(BIGINT, "[cache_column1]");
    private static final Reference CACHE_COL2_REF = new Reference(BIGINT, "[cache_column2]");

    private static final PlannerContext PLANNER_CONTEXT = TestingPlannerContext.plannerContextBuilder()
            .withMetadata(new MockMetadata())
            .withCacheMetadata(new TestCacheMetadata())
            .build();
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

    private PlanBuilder planBuilder;
    private String tpchCatalogId;

    public TestCanonicalSubplanExtractor()
    {
        super(ImmutableMap.of(
                // increase task concurrency to get parallel plans
                TASK_CONCURRENCY, "4"));
    }

    @BeforeAll
    public void setup()
    {
        planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);
        tpchCatalogId = getPlanTester().getCatalogHandle(getPlanTester().getDefaultSession().getCatalog().orElseThrow()).getId();
    }

    @Test
    public void testAggregationWithMultipleGroupByColumnsAndPredicate()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT sum(nationkey), sum(nationkey) filter(where nationkey > 10)
                FROM nation
                WHERE regionkey > BIGINT '10'
                GROUP BY name, regionkey * 2
                HAVING name = '0123456789012345689012345' AND sum(nationkey) > BIGINT '10'""");
        assertThat(subplans).hasSize(2);

        CacheTableId tableId = new CacheTableId(tpchCatalogId + ":tiny:nation:0.01");
        Expression nonPullableConjunct = new Comparison(GREATER_THAN, REGIONKEY_REF, new Constant(BIGINT, 10L));
        Expression pullableConjunct = new Comparison(EQUAL, NAME_REF, new Constant(createVarcharType(25), utf8Slice("0123456789012345689012345")));
        CanonicalSubplan nonAggregatedSubplan = subplans.get(0);
        ScanFilterProjectKey scanFilterProjectKey = new ScanFilterProjectKey(tableId, ImmutableSet.of(nonPullableConjunct, pullableConjunct));
        assertThat(nonAggregatedSubplan.getKeyChain()).containsExactly(scanFilterProjectKey);
        assertThat(nonAggregatedSubplan.getGroupByColumns()).isEmpty();
        assertThat(nonAggregatedSubplan.getConjuncts()).containsExactly(nonPullableConjunct, pullableConjunct);
        assertThat(nonAggregatedSubplan.getPullableConjuncts()).containsExactlyElementsOf(nonAggregatedSubplan.getConjuncts());
        assertThat(nonAggregatedSubplan.getDynamicConjuncts()).isEmpty();
        assertThat(nonAggregatedSubplan.getTableScan()).isPresent();
        assertThat(nonAggregatedSubplan.getChildSubplan()).isEmpty();
        CacheColumnId regionKeyGreaterThan10 = canonicalExpressionToColumnId(new Comparison(GREATER_THAN, NATIONKEY_REF, new Constant(BIGINT, 10L)));
        CacheColumnId regionKeyMultiplyBy2 = canonicalExpressionToColumnId(new Call(MULTIPLY_BIGINT, ImmutableList.of(REGIONKEY_REF, new Constant(BIGINT, 2L))));
        assertThat(nonAggregatedSubplan.getAssignments()).containsExactly(
                entry(NATIONKEY_ID, CacheExpression.ofProjection(NATIONKEY_REF)),
                entry(regionKeyGreaterThan10, CacheExpression.ofProjection(new Comparison(GREATER_THAN, NATIONKEY_REF, new Constant(BIGINT, 10L)))),
                entry(NAME_ID, CacheExpression.ofProjection(NAME_REF)),
                entry(regionKeyMultiplyBy2, CacheExpression.ofProjection(new Call(MULTIPLY_BIGINT, ImmutableList.of(REGIONKEY_REF, new Constant(BIGINT, 2L))))));
        assertThat(nonAggregatedSubplan.getTableScan().get().getColumnHandles()).containsExactly(
                entry(NATIONKEY_ID, new TpchColumnHandle("nationkey", BIGINT)),
                entry(NAME_ID, new TpchColumnHandle("name", createVarcharType(25))),
                entry(REGIONKEY_ID, new TpchColumnHandle("regionkey", BIGINT)));
        assertThat(nonAggregatedSubplan.getTableScan().get().getTableId()).isEqualTo(tableId);

        CanonicalAggregation sum = sumNationkey();
        CanonicalAggregation filteredSum = new CanonicalAggregation(
                sumBigint(),
                Optional.of(columnIdToSymbol(regionKeyGreaterThan10, BOOLEAN)),
                List.of(NATIONKEY_REF));
        CanonicalSubplan aggregatedSubplan = subplans.get(1);
        assertThat(aggregatedSubplan.getKeyChain()).containsExactly(scanFilterProjectKey, new AggregationKey(aggregatedSubplan.getGroupByColumns().get(), ImmutableSet.of(nonPullableConjunct)));
        assertThat(aggregatedSubplan.getConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getPullableConjuncts()).containsExactly(pullableConjunct);
        assertThat(aggregatedSubplan.getDynamicConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getChildSubplan()).contains(nonAggregatedSubplan);
        assertThat(aggregatedSubplan.getOriginalPlanNode()).isInstanceOf(AggregationNode.class);
        assertThat(getGroupByExpressions(aggregatedSubplan)).contains(ImmutableList.of(
                NAME_REF,
                columnIdToSymbol(regionKeyMultiplyBy2, BIGINT).toSymbolReference()));
        assertThat(aggregatedSubplan.getOriginalSymbolMapping()).containsOnlyKeys(
                NATIONKEY_ID,
                NAME_ID,
                REGIONKEY_ID,
                canonicalExpressionToColumnId(new Comparison(GREATER_THAN, NATIONKEY_REF, new Constant(BIGINT, 10L))),
                canonicalExpressionToColumnId(new Call(MULTIPLY_BIGINT, ImmutableList.of(REGIONKEY_REF, new Constant(BIGINT, 2L)))),
                canonicalAggregationToColumnId(filteredSum),
                canonicalAggregationToColumnId(sum));
        assertThat(aggregatedSubplan.getAssignments()).containsExactlyInAnyOrderEntriesOf(ImmutableMap.<CacheColumnId, CacheExpression>builder()
                .put(NAME_ID, CacheExpression.ofProjection(NAME_REF))
                .put(canonicalExpressionToColumnId(new Call(MULTIPLY_BIGINT, ImmutableList.of(REGIONKEY_REF, new Constant(BIGINT, 2L)))), CacheExpression.ofProjection(columnIdToSymbol(regionKeyMultiplyBy2, BIGINT).toSymbolReference()))
                .put(canonicalAggregationToColumnId(filteredSum), CacheExpression.ofAggregation(filteredSum))
                .put(canonicalAggregationToColumnId(sum), CacheExpression.ofAggregation(sum))
                .buildOrThrow());
    }

    @Test
    public void testAggregationWithMultipleGroupByColumns()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT sum(nationkey + 1)
                FROM nation
                GROUP BY name, regionkey""");
        assertThat(subplans).hasSize(2);

        CacheTableId tableId = new CacheTableId(tpchCatalogId + ":tiny:nation:0.01");
        CacheColumnId nationKeyPlusOne = canonicalExpressionToColumnId(new Call(ADD_BIGINT, ImmutableList.of(NATIONKEY_REF, new Constant(BIGINT, 1L))));
        CanonicalSubplan nonAggregatedSubplan = subplans.get(0);
        assertThat(nonAggregatedSubplan.getKeyChain()).containsExactly(new ScanFilterProjectKey(tableId, ImmutableSet.of()));
        assertThat(nonAggregatedSubplan.getGroupByColumns()).isEmpty();
        assertThat(nonAggregatedSubplan.getAssignments()).containsExactly(
                entry(nationKeyPlusOne, CacheExpression.ofProjection(new Call(ADD_BIGINT, ImmutableList.of(NATIONKEY_REF, new Constant(BIGINT, 1L))))),
                entry(NAME_ID, CacheExpression.ofProjection(NAME_REF)),
                entry(REGIONKEY_ID, CacheExpression.ofProjection(REGIONKEY_REF)));
        assertThat(nonAggregatedSubplan.getTableScan()).isPresent();
        assertThat(nonAggregatedSubplan.getChildSubplan()).isEmpty();
        assertThat(nonAggregatedSubplan.getTableScan().get().getColumnHandles()).containsExactly(
                entry(NATIONKEY_ID, new TpchColumnHandle("nationkey", BIGINT)),
                entry(NAME_ID, new TpchColumnHandle("name", createVarcharType(25))),
                entry(REGIONKEY_ID, new TpchColumnHandle("regionkey", BIGINT)));
        assertThat(nonAggregatedSubplan.getTableScan().get().getTableId()).isEqualTo(tableId);

        CanonicalAggregation sum = new CanonicalAggregation(
                sumBigint(),
                Optional.empty(),
                List.of(columnIdToSymbol(nationKeyPlusOne, BIGINT).toSymbolReference()));
        CanonicalSubplan aggregatedSubplan = subplans.get(1);
        assertThat(aggregatedSubplan.getKeyChain()).containsExactly(new ScanFilterProjectKey(tableId, ImmutableSet.of()), new AggregationKey(aggregatedSubplan.getGroupByColumns().get(), ImmutableSet.of()));
        assertThat(aggregatedSubplan.getOriginalPlanNode()).isInstanceOf(AggregationNode.class);
        assertThat(getGroupByExpressions(aggregatedSubplan)).contains(ImmutableList.of(NAME_REF, REGIONKEY_REF));
        assertThat(aggregatedSubplan.getOriginalSymbolMapping()).containsOnlyKeys(
                NATIONKEY_ID,
                NAME_ID,
                REGIONKEY_ID,
                canonicalExpressionToColumnId(new Call(ADD_BIGINT, ImmutableList.of(NATIONKEY_REF, new Constant(BIGINT, 1L)))),
                canonicalAggregationToColumnId(sum));
        assertThat(aggregatedSubplan.getAssignments()).containsExactly(
                entry(NAME_ID, CacheExpression.ofProjection(NAME_REF)),
                entry(REGIONKEY_ID, CacheExpression.ofProjection(REGIONKEY_REF)),
                entry(canonicalAggregationToColumnId(sum), CacheExpression.ofAggregation(sum)));
        assertThat(aggregatedSubplan.getConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getPullableConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getDynamicConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getTableScan()).isEmpty();
        assertThat(aggregatedSubplan.getChildSubplan()).contains(nonAggregatedSubplan);
    }

    @Test
    public void testNestedProjections()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT regionkey
                FROM (SELECT nationkey * 2 as nationkey_mul, regionkey FROM nation)
                WHERE nationkey_mul + nationkey_mul > BIGINT '10' AND regionkey > BIGINT '10'""");
        assertThat(subplans).hasSize(2);

        Expression nationKeyMultiplyBy2 = new Call(MULTIPLY_BIGINT, ImmutableList.of(NATIONKEY_REF, new Constant(BIGINT, 2L)));
        Expression regionKeyPredicate = new Comparison(GREATER_THAN, REGIONKEY_REF, new Constant(BIGINT, 10L));
        CacheTableId tableId = new CacheTableId(tpchCatalogId + ":tiny:nation:0.01");
        CanonicalSubplan nestedSubplan = subplans.get(0);
        ScanFilterProjectKey scanFilterProjectKey = new ScanFilterProjectKey(tableId, ImmutableSet.of(regionKeyPredicate));
        assertThat(nestedSubplan.getKeyChain()).containsExactly(scanFilterProjectKey);
        assertThat(nestedSubplan.getGroupByColumns()).isEmpty();
        assertThat(nestedSubplan.getConjuncts()).containsExactly(regionKeyPredicate);
        assertThat(nestedSubplan.getPullableConjuncts()).containsExactly(regionKeyPredicate);
        assertThat(nestedSubplan.getDynamicConjuncts()).isEmpty();
        assertThat(nestedSubplan.getTableScan()).isPresent();
        assertThat(nestedSubplan.getChildSubplan()).isEmpty();
        assertThat(nestedSubplan.getAssignments()).containsExactly(
                entry(REGIONKEY_ID, CacheExpression.ofProjection(REGIONKEY_REF)),
                entry(canonicalExpressionToColumnId(nationKeyMultiplyBy2), CacheExpression.ofProjection(nationKeyMultiplyBy2)));
        assertThat(nestedSubplan.getTableScan().get().getTableId()).isEqualTo(tableId);

        Reference nationKeyMultiplyBy2Reference = columnIdToSymbol(canonicalExpressionToColumnId(nationKeyMultiplyBy2), BIGINT).toSymbolReference();
        Expression nationKeyPredicate = new Comparison(GREATER_THAN, new Call(ADD_BIGINT, ImmutableList.of(nationKeyMultiplyBy2Reference, nationKeyMultiplyBy2Reference)), new Constant(BIGINT, 10L));
        CanonicalSubplan topSubplan = subplans.get(1);
        assertThat(topSubplan.getKeyChain()).containsExactly(scanFilterProjectKey, new FilterProjectKey(ImmutableSet.of()));
        assertThat(topSubplan.getConjuncts()).containsExactly(nationKeyPredicate);
        assertThat(topSubplan.getPullableConjuncts()).containsExactly(regionKeyPredicate, nationKeyPredicate);
        assertThat(topSubplan.getDynamicConjuncts()).isEmpty();
        assertThat(topSubplan.getTableScan()).isEmpty();
        assertThat(topSubplan.getChildSubplan()).contains(nestedSubplan);
        assertThat(topSubplan.getAssignments()).containsExactly(
                entry(REGIONKEY_ID, CacheExpression.ofProjection(REGIONKEY_REF)));
    }

    @Test
    public void testUnsafeProjections()
    {
        // nationkey * 2 is unsafe expression
        assertRequiredConjuncts(
                "SELECT nationkey * 2 FROM nation WHERE regionkey > 10",
                ScanFilterProjectKey.class,
                new Comparison(GREATER_THAN, REGIONKEY_REF, new Constant(BIGINT, 10L)));
        // safe expressions
        assertRequiredConjuncts(
                "SELECT nationkey, 42 FROM nation WHERE regionkey > 10",
                ScanFilterProjectKey.class);
        // nested projection; nationkey_mul is reference; "nationkey_mul * nationkey_mul > 10" is not pushed to table scan level
        // therefore nationkey * nationkey (potentially unsafe) is evaluated for every input row
        assertRequiredConjuncts(
                "SELECT nationkey_mul FROM (SELECT nationkey * nationkey as nationkey_mul FROM nation) WHERE nationkey_mul * nationkey_mul > 10",
                FilterProjectKey.class);
        // nationkey * nationkey is unsafe expression
        Symbol nationKeyMul = columnIdToSymbol(canonicalExpressionToColumnId(new Call(MULTIPLY_BIGINT, ImmutableList.of(NATIONKEY_REF, NATIONKEY_REF))), BIGINT);
        Expression nationKeyMulMul = new Call(MULTIPLY_BIGINT, ImmutableList.of(nationKeyMul.toSymbolReference(), nationKeyMul.toSymbolReference()));
        assertRequiredConjuncts(
                "SELECT nationkey_mul * nationkey_mul FROM (SELECT nationkey * nationkey as nationkey_mul FROM nation) WHERE nationkey_mul * nationkey_mul > 10",
                FilterProjectKey.class,
                new Comparison(GREATER_THAN, nationKeyMulMul, new Constant(BIGINT, 10L)));
    }

    private void assertRequiredConjuncts(@Language("SQL") String query, Class<? extends Key> keyType, Expression... expectedConjuncts)
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery(query);
        assertThat(subplans).isNotEmpty();
        CanonicalSubplan topLevelSubplan = getLast(subplans);
        Key topLevelKey = topLevelSubplan.getKey();
        assertThat(topLevelKey).isInstanceOf(keyType);
        switch (topLevelKey) {
            case ScanFilterProjectKey scanFilterProjectKey -> {
                assertThat(scanFilterProjectKey.requiredConjuncts()).containsExactly(expectedConjuncts);
            }
            case FilterProjectKey filterProjectKey -> {
                assertThat(filterProjectKey.requiredConjuncts()).containsExactly(expectedConjuncts);
            }
            default -> fail("Unexpected key type: " + topLevelKey.getClass());
        }
    }

    @Test
    public void testBigintAggregation()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT sum(nationkey)
                FROM nation
                GROUP BY regionkey""");
        assertThat(subplans).hasSize(2);

        CanonicalSubplan nonAggregatedSubplan = subplans.get(0);
        assertThat(nonAggregatedSubplan.getGroupByColumns()).isEmpty();
        assertThat(nonAggregatedSubplan.getAssignments()).containsExactly(
                entry(NATIONKEY_ID, CacheExpression.ofProjection(NATIONKEY_REF)),
                entry(REGIONKEY_ID, CacheExpression.ofProjection(REGIONKEY_REF)));

        CanonicalAggregation sum = sumNationkey();
        CanonicalSubplan aggregatedSubplan = subplans.get(1);
        assertThat(aggregatedSubplan.getOriginalPlanNode()).isInstanceOf(AggregationNode.class);
        assertThat(getGroupByExpressions(aggregatedSubplan)).contains(ImmutableList.of(REGIONKEY_REF));
        assertThat(aggregatedSubplan.getOriginalSymbolMapping()).containsOnlyKeys(
                NATIONKEY_ID,
                REGIONKEY_ID,
                canonicalAggregationToColumnId(sum));
        assertThat(aggregatedSubplan.getAssignments()).containsExactly(
                entry(REGIONKEY_ID, CacheExpression.ofProjection(REGIONKEY_REF)),
                entry(canonicalAggregationToColumnId(sum), CacheExpression.ofAggregation(sum)));
        assertThat(aggregatedSubplan.getConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getDynamicConjuncts()).isEmpty();
    }

    @Test
    public void testGlobalAggregation()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT sum(nationkey)
                FROM nation""");
        assertThat(subplans).hasSize(2);

        CanonicalSubplan nonAggregatedSubplan = subplans.get(0);
        assertThat(nonAggregatedSubplan.getGroupByColumns()).isEmpty();

        CanonicalAggregation sum = sumNationkey();
        CanonicalSubplan aggregatedSubplan = subplans.get(1);
        assertThat(aggregatedSubplan.getOriginalPlanNode()).isInstanceOf(AggregationNode.class);
        assertThat(aggregatedSubplan.getGroupByColumns()).contains(ImmutableSet.of());
        assertThat(aggregatedSubplan.getOriginalSymbolMapping()).containsOnlyKeys(
                NATIONKEY_ID,
                canonicalAggregationToColumnId(sum));
        assertThat(aggregatedSubplan.getAssignments()).containsExactly(
                entry(canonicalAggregationToColumnId(sum), CacheExpression.ofAggregation(sum)));
        assertThat(aggregatedSubplan.getConjuncts()).isEmpty();
        assertThat(aggregatedSubplan.getDynamicConjuncts()).isEmpty();
    }

    @Test
    public void testNestedAggregations()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT sum(sum_nationkey)
                FROM (SELECT sum(nationkey) sum_nationkey, name
                      FROM nation
                      GROUP BY name, regionkey)
                GROUP BY name || 'abc'""");
        assertThat(subplans).hasSize(2);

        CanonicalSubplan nonAggregatedSubplan = subplans.get(0);
        assertThat(nonAggregatedSubplan.getGroupByColumns()).isEmpty();

        CanonicalSubplan aggregatedSubplan = subplans.get(1);
        assertThat(aggregatedSubplan.getOriginalPlanNode()).isInstanceOf(AggregationNode.class);
        assertThat(getGroupByExpressions(aggregatedSubplan)).contains(ImmutableList.of(NAME_REF, REGIONKEY_REF));
    }

    @Test
    public void testUnsupportedAggregations()
    {
        assertUnsupportedAggregation("SELECT array_agg(nationkey order by nationkey) FROM nation");
        assertUnsupportedAggregation("SELECT sum(nationkey) FROM nation GROUP BY ROLLUP (nationkey)");
        assertUnsupportedAggregation("SELECT sum(distinct nationkey), sum(distinct regionkey) FROM nation");
    }

    private void assertUnsupportedAggregation(@Language("SQL") String query)
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery(query);
        assertThat(subplans).hasSize(1);
        assertThat(getOnlyElement(subplans).getGroupByColumns()).isEmpty();
    }

    @Test
    public void testTopNRankingRank()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("SELECT name, regionkey FROM nation ORDER BY regionkey FETCH FIRST 6 ROWS WITH TIES", false);
        assertThat(subplans).hasSize(2);
        CanonicalSubplan scanSubplan = subplans.get(0);
        assertThat(scanSubplan.getAssignments()).containsExactly(
                entry(NAME_ID, CacheExpression.ofProjection(NAME_REF)),
                entry(REGIONKEY_ID, CacheExpression.ofProjection(REGIONKEY_REF)));
        CanonicalSubplan topNSubplan = subplans.get(1);
        assertThat(topNSubplan.getChildSubplan().get()).isEqualTo(scanSubplan);
        assertThat(topNSubplan.getKey()).isInstanceOf(TopNRankingKey.class);
        TopNRankingKey key = (TopNRankingKey) topNSubplan.getKey();
        assertThat(key.partitionBy()).isEqualTo(ImmutableList.of());
        assertThat(key.orderings()).containsExactly(entry(REGIONKEY_ID, SortOrder.ASC_NULLS_LAST));
        assertThat(key.rankingType()).isEqualTo(RANK);
        assertThat(key.maxRankingPerPartition()).isEqualTo(6);
    }

    @Test
    public void testTopNRankingRowNumber()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT *
                FROM (SELECT nationkey, ROW_NUMBER () OVER (PARTITION BY name, nationkey ORDER BY regionkey DESC) update_rank FROM nation) AS t
                WHERE t.update_rank = 1""", false);
        assertThat(subplans).hasSize(2);
        CanonicalSubplan scanSubplan = subplans.get(0);
        assertThat(scanSubplan.getAssignments()).containsExactly(
                entry(NATIONKEY_ID, CacheExpression.ofProjection(NATIONKEY_REF)),
                entry(NAME_ID, CacheExpression.ofProjection(NAME_REF)),
                entry(REGIONKEY_ID, CacheExpression.ofProjection(REGIONKEY_REF)));
        CanonicalSubplan topNSubplan = subplans.get(1);
        assertThat(topNSubplan.getChildSubplan().get()).isEqualTo(scanSubplan);
        assertThat(topNSubplan.getKey()).isInstanceOf(TopNRankingKey.class);
        TopNRankingKey key = (TopNRankingKey) topNSubplan.getKey();
        assertThat(key.partitionBy().stream().toList()).isEqualTo(ImmutableList.of(NAME_ID, NATIONKEY_ID));
        assertThat(key.orderings()).containsExactly(entry(REGIONKEY_ID, SortOrder.DESC_NULLS_LAST));
        assertThat(key.rankingType()).isEqualTo(ROW_NUMBER);
        assertThat(key.maxRankingPerPartition()).isEqualTo(1);
    }

    @Test
    public void testProjectionWithLambdas()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("SELECT any_match(array[nationkey], x -> x > 5) FROM nation");
        assertThat(subplans).hasSize(1);
        assertThat(subplans.get(0).getOriginalPlanNode()).isInstanceOf(TableScanNode.class);
    }

    @Test
    public void testFilterWithLambdas()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("SELECT nationkey FROM nation WHERE any_match(array[nationkey], x -> x > 5)");
        assertThat(subplans).hasSize(1);
        assertThat(subplans.get(0).getOriginalPlanNode()).isInstanceOf(TableScanNode.class);
    }

    @Test
    public void testTopN()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("SELECT nationkey FROM nation ORDER BY name LIMIT 5");
        assertThat(subplans).hasSize(2);
        CanonicalSubplan scanSubplan = subplans.get(0);
        assertThat(scanSubplan.getAssignments()).containsExactly(
                entry(NATIONKEY_ID, CacheExpression.ofProjection(NATIONKEY_REF)),
                entry(NAME_ID, CacheExpression.ofProjection(NAME_REF)));
        CanonicalSubplan topNSubplan = subplans.get(1);
        assertThat(topNSubplan.getChildSubplan().get()).isEqualTo(scanSubplan);
        assertThat(topNSubplan.getKey()).isInstanceOf(TopNKey.class);
    }

    @Test
    public void testTopNWithMultipleOrderByColumns()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("SELECT nationkey FROM nation ORDER BY regionkey, nationkey DESC offset 10 LIMIT 5");
        CanonicalSubplan scanSubplan = subplans.get(0);
        assertThat(scanSubplan.getAssignments()).containsExactly(
                entry(NATIONKEY_ID, CacheExpression.ofProjection(NATIONKEY_REF)),
                entry(REGIONKEY_ID, CacheExpression.ofProjection(REGIONKEY_REF)));
        CanonicalSubplan topNSubplan = subplans.get(1);
        assertThat(topNSubplan.getKey()).isInstanceOf(TopNKey.class);
        TopNKey key = (TopNKey) topNSubplan.getKey();
        assertThat(key.orderings()).containsExactly(
                entry(REGIONKEY_ID, SortOrder.ASC_NULLS_LAST),
                entry(NATIONKEY_ID, SortOrder.DESC_NULLS_LAST));
        assertThat(key.count()).isEqualTo(15);
    }

    @Test
    public void testTopNWithExpressionInOrderByColumn()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("SELECT nationkey FROM nation ORDER BY regionkey + 5 offset 10 LIMIT 5");
        CanonicalSubplan scanSubplan = subplans.get(0);
        CacheColumnId regionKeyAdded5 = canonicalExpressionToColumnId(new Call(ADD_BIGINT, ImmutableList.of(REGIONKEY_REF, new Constant(BIGINT, 5L))));
        assertThat(scanSubplan.getAssignments()).containsExactly(
                entry(NATIONKEY_ID, CacheExpression.ofProjection(NATIONKEY_REF)),
                entry(regionKeyAdded5, CacheExpression.ofProjection(new Call(ADD_BIGINT, ImmutableList.of(REGIONKEY_REF, new Constant(BIGINT, 5L))))));
        CanonicalSubplan topNSubplan = subplans.get(1);
        assertThat(topNSubplan.getKey()).isInstanceOf(TopNKey.class);
        TopNKey key = (TopNKey) topNSubplan.getKey();
        assertThat(key.orderings()).containsExactly(entry(regionKeyAdded5, SortOrder.ASC_NULLS_LAST));
        assertThat(key.count()).isEqualTo(15);
    }

    @Test
    public void testNestedTopN()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("SELECT nationkey FROM (SELECT nationkey, name FROM nation ORDER BY nationkey limit 5) ORDER BY 1 limit 15");
        assertThat(subplans).hasSize(2);
        CanonicalSubplan scanSubplan = subplans.get(0);
        assertThat(scanSubplan.getAssignments()).containsExactly(entry(NATIONKEY_ID, CacheExpression.ofProjection(NATIONKEY_REF)));
        CanonicalSubplan topNSubplan = subplans.get(1);
        assertThat(topNSubplan.getKey()).isInstanceOf(TopNKey.class);
        TopNKey key = (TopNKey) topNSubplan.getKey();
        assertThat(key.orderings()).containsExactly(entry(NATIONKEY_ID, SortOrder.ASC_NULLS_LAST));
        assertThat(key.count()).isEqualTo(5);
    }

    @Test
    public void testUnsupportedTopNWithGroupBy()
    {
        // unsupported final aggregation and exchanges between two topN
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("SELECT max(nationkey) FROM nation GROUP BY name ORDER BY name LIMIT 1");
        assertThat(subplans).hasSize(2);
        assertThat(subplans).noneMatch((subplan) -> subplan.getKey() instanceof TopNKey);
    }

    @Test
    public void testNondeterministicTopN()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("SELECT * FROM nation ORDER BY RANDOM() LIMIT 1");
        assertThat(subplans).hasSize(1);
        assertThat(getOnlyElement(subplans).getKey()).isNotExactlyInstanceOf(TopNKey.class);
    }

    @Test
    public void testConjunctionOfNonDeterministicPredicateAndDynamicFilter()
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplansForQuery("""
                SELECT n.comment
                FROM
                    (SELECT * FROM nation WHERE random(regionkey) > 20) n
                JOIN
                    (SELECT * FROM region WHERE random(regionkey) > 15) r
                ON
                    n.regionkey = r.regionkey""");
        assertThat(subplans).hasSize(1);
        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getDynamicConjuncts()).isEmpty();
        assertThat(subplan.getTableScan()).isPresent();
        TableScan scan = subplan.getTableScan().get();
        assertThat(scan.getTableId()).isEqualTo(new CacheTableId(tpchCatalogId + ":tiny:region:0.01"));
    }

    private Optional<List<Expression>> getGroupByExpressions(CanonicalSubplan subplan)
    {
        return subplan.getGroupByColumns()
                .map(columns -> columns.stream()
                        .map(column -> requireNonNull(subplan.getAssignments().get(column), "No assignment for column: " + column))
                        .map(cacheExpression -> cacheExpression.projection().orElseThrow())
                        .collect(toImmutableList()));
    }

    @Test
    public void testExtractCanonicalScanAndProject()
    {
        ProjectNode projectNode = createScanAndProjectNode();
        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                PLANNER_CONTEXT,
                TEST_SESSION,
                projectNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(projectNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                entry(CACHE_COL1, new Symbol(BIGINT, "symbol1")),
                entry(CACHE_COL2, new Symbol(BIGINT, "symbol2")),
                entry(canonicalExpressionToColumnId(new Call(ADD_BIGINT, ImmutableList.of(CACHE_COL1_REF, new Constant(BIGINT, 1L)))), new Symbol(BIGINT, "projection1")));
        assertThat(subplan.getAssignments()).containsExactly(
                entry(canonicalExpressionToColumnId(new Call(ADD_BIGINT, ImmutableList.of(CACHE_COL1_REF, new Constant(BIGINT, 1L)))), CacheExpression.ofProjection(new Call(ADD_BIGINT, ImmutableList.of(CACHE_COL1_REF, new Constant(BIGINT, 1L))))),
                entry(CACHE_COL2, CacheExpression.ofProjection(CACHE_COL2_REF)));

        assertThat(subplan.getConjuncts()).isEmpty();
        assertThat(subplan.getDynamicConjuncts()).isEmpty();
        TableScan tableScan = subplan.getTableScan().orElseThrow();
        assertThat(tableScan.getColumnHandles()).containsExactly(
                entry(CACHE_COL1, new TestingColumnHandle("column1")),
                entry(CACHE_COL2, new TestingColumnHandle("column2")));
        assertThat(tableScan.getTableId()).isEqualTo(CATALOG_CACHE_TABLE_ID);
        assertThat(tableScan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
        assertThat(subplan.getTableScanId()).isEqualTo(SCAN_NODE_ID);
    }

    @Test
    public void testExtractCanonicalFilterAndProject()
    {
        ProjectNode projectNode = createFilterAndProjectNode();
        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                PLANNER_CONTEXT,
                TEST_SESSION,
                projectNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(projectNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                entry(CACHE_COL1, new Symbol(BIGINT, "symbol1")),
                entry(CACHE_COL2, new Symbol(BIGINT, "symbol2")),
                entry(canonicalExpressionToColumnId(new Call(ADD_BIGINT, ImmutableList.of(CACHE_COL1_REF, new Constant(BIGINT, 1L)))), new Symbol(BIGINT, "projection1")));
        assertThat(subplan.getAssignments()).containsExactly(
                entry(canonicalExpressionToColumnId(new Call(ADD_BIGINT, ImmutableList.of(CACHE_COL1_REF, new Constant(BIGINT, 1L)))), CacheExpression.ofProjection(new Call(ADD_BIGINT, ImmutableList.of(CACHE_COL1_REF, new Constant(BIGINT, 1L))))),
                entry(CACHE_COL2, CacheExpression.ofProjection(CACHE_COL2_REF)));

        assertThat(subplan.getConjuncts()).hasSize(1);
        Expression predicate = getOnlyElement(subplan.getConjuncts());
        assertThat(predicate).isEqualTo(new Comparison(GREATER_THAN, new Call(ADD_BIGINT, ImmutableList.of(CACHE_COL1_REF, CACHE_COL2_REF)), new Constant(BIGINT, 0L)));

        assertThat(subplan.getDynamicConjuncts()).hasSize(1);
        Expression dynamicFilterExpression = getOnlyElement(subplan.getDynamicConjuncts());
        assertThat(DynamicFilters.getDescriptor(dynamicFilterExpression)).contains(
                new DynamicFilters.Descriptor(new DynamicFilterId("dynamic_filter_id"), CACHE_COL1_REF));

        TableScan tableScan = subplan.getTableScan().orElseThrow();
        assertThat(tableScan.getColumnHandles()).containsExactly(
                entry(CACHE_COL1, new TestingColumnHandle("column1")),
                entry(CACHE_COL2, new TestingColumnHandle("column2")));
        assertThat(tableScan.getTableId()).isEqualTo(CATALOG_CACHE_TABLE_ID);
        assertThat(tableScan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
        assertThat(subplan.getTableScanId()).isEqualTo(SCAN_NODE_ID);
    }

    @Test
    public void testExtractCanonicalFilter()
    {
        FilterNode filterNode = createFilterNode();
        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                PLANNER_CONTEXT,
                TEST_SESSION,
                filterNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(filterNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                entry(CACHE_COL1, new Symbol(BIGINT, "symbol1")),
                entry(CACHE_COL2, new Symbol(BIGINT, "symbol2")));
        assertThat(subplan.getAssignments()).containsExactly(
                entry(CACHE_COL1, CacheExpression.ofProjection(CACHE_COL1_REF)),
                entry(CACHE_COL2, CacheExpression.ofProjection(CACHE_COL2_REF)));

        assertThat(subplan.getConjuncts()).hasSize(1);
        Expression predicate = getOnlyElement(subplan.getConjuncts());
        assertThat(predicate).isEqualTo(new Comparison(GREATER_THAN, new Call(ADD_BIGINT, ImmutableList.of(CACHE_COL1_REF, CACHE_COL2_REF)), new Constant(BIGINT, 0L)));

        assertThat(subplan.getDynamicConjuncts()).hasSize(1);
        Expression dynamicFilterExpression = getOnlyElement(subplan.getDynamicConjuncts());
        assertThat(DynamicFilters.getDescriptor(dynamicFilterExpression)).contains(
                new DynamicFilters.Descriptor(new DynamicFilterId("dynamic_filter_id"), CACHE_COL1_REF));

        TableScan tableScan = subplan.getTableScan().orElseThrow();
        assertThat(tableScan.getColumnHandles()).containsExactly(
                entry(CACHE_COL1, new TestingColumnHandle("column1")),
                entry(CACHE_COL2, new TestingColumnHandle("column2")));
        assertThat(tableScan.getTableId()).isEqualTo(CATALOG_CACHE_TABLE_ID);
        assertThat(tableScan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
        assertThat(subplan.getTableScanId()).isEqualTo(SCAN_NODE_ID);
    }

    @Test
    public void testExtractCanonicalTableScan()
    {
        // no cache id, therefore no canonical plan
        TableScanNode tableScanNode = createTableScan();
        assertThat(extractCanonicalSubplans(
                TestingPlannerContext.plannerContextBuilder()
                        .withMetadata(new MockMetadata())
                        .withCacheMetadata(new TestCacheMetadata(Optional.empty(), handle -> Optional.of(new CacheColumnId(handle.getName()))))
                        .build(),
                TEST_SESSION,
                tableScanNode))
                .isEmpty();

        // no column id, therefore no canonical plan
        assertThat(extractCanonicalSubplans(
                TestingPlannerContext.plannerContextBuilder()
                        .withMetadata(new MockMetadata())
                        .withCacheMetadata(new TestCacheMetadata(Optional.of(CACHE_TABLE_ID), handle -> Optional.empty()))
                        .build(),
                TEST_SESSION,
                tableScanNode))
                .isEmpty();

        List<CanonicalSubplan> subplans = extractCanonicalSubplans(
                PLANNER_CONTEXT,
                TEST_SESSION,
                tableScanNode);
        assertThat(subplans).hasSize(1);

        CanonicalSubplan subplan = getOnlyElement(subplans);
        assertThat(subplan.getOriginalPlanNode()).isEqualTo(tableScanNode);
        assertThat(subplan.getOriginalSymbolMapping()).containsExactly(
                entry(CACHE_COL1, new Symbol(BIGINT, "symbol1")),
                entry(CACHE_COL2, new Symbol(BIGINT, "symbol2")));
        assertThat(subplan.getAssignments()).containsExactly(
                entry(CACHE_COL1, CacheExpression.ofProjection(CACHE_COL1_REF)),
                entry(CACHE_COL2, CacheExpression.ofProjection(CACHE_COL2_REF)));
        assertThat(subplan.getConjuncts()).isEmpty();
        assertThat(subplan.getDynamicConjuncts()).isEmpty();

        TableScan tableScan = subplan.getTableScan().orElseThrow();
        assertThat(tableScan.getColumnHandles()).containsExactly(
                entry(CACHE_COL1, new TestingColumnHandle("column1")),
                entry(CACHE_COL2, new TestingColumnHandle("column2")));
        assertThat(tableScan.getTableId()).isEqualTo(CATALOG_CACHE_TABLE_ID);
        assertThat(tableScan.getTable()).isEqualTo(TEST_TABLE_HANDLE);
        assertThat(subplan.getTableScanId()).isEqualTo(SCAN_NODE_ID);
    }

    @Test
    public void testProjectionWithDuplicatedExpressions()
    {
        assertThatCanonicalSubplanIsForTableScan(new ProjectNode(
                new PlanNodeId("project_node"),
                createTableScan(),
                Assignments.of(
                        new Symbol(BIGINT, "alias1"),
                        new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "symbol1"), new Constant(BIGINT, 2L))),
                        new Symbol(BIGINT, "alias2"),
                        new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "symbol1"), new Constant(BIGINT, 2L))))));
    }

    @Test
    public void testAliasingProjection()
    {
        assertThatCanonicalSubplanIsForTableScan(new ProjectNode(
                new PlanNodeId("project_node"),
                createTableScan(),
                Assignments.of(
                        new Symbol(BIGINT, "alias"),
                        new Reference(BIGINT, "symbol1"))));
        assertThatCanonicalSubplanIsForTableScan(new ProjectNode(
                new PlanNodeId("project_node"),
                createTableScan(),
                Assignments.of(
                        new Symbol(BIGINT, "symbol1"),
                        new Reference(BIGINT, "symbol1"),
                        new Symbol(BIGINT, "alias"),
                        new Reference(BIGINT, "symbol1"))));
        assertThatCanonicalSubplanIsForTableScan(new ProjectNode(
                new PlanNodeId("project_node"),
                createTableScan(),
                Assignments.of(
                        new Symbol(BIGINT, "alias"),
                        new Reference(BIGINT, "symbol1"),
                        new Symbol(BIGINT, "symbol1"),
                        new Reference(BIGINT, "symbol1"))));
    }

    private void assertThatCanonicalSubplanIsForTableScan(PlanNode root)
    {
        List<CanonicalSubplan> subplans = extractCanonicalSubplans(PLANNER_CONTEXT, TEST_SESSION, root);
        assertThat(subplans).hasSize(1);
        assertThat(getOnlyElement(subplans).getOriginalPlanNode()).isInstanceOf(TableScanNode.class);
    }

    @Test
    public void testTableScanWithDuplicatedColumnHandle()
    {
        Symbol symbol1 = new Symbol(BIGINT, "symbol1");
        Symbol symbol2 = new Symbol(BIGINT, "symbol2");
        TestingColumnHandle columnHandle = new TestingColumnHandle("column1");
        TableScanNode tableScanNode = new TableScanNode(
                SCAN_NODE_ID,
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol1, symbol2),
                ImmutableMap.of(symbol2, columnHandle, symbol1, columnHandle),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
        assertThat(extractCanonicalSubplans(PLANNER_CONTEXT, TEST_SESSION, tableScanNode)).isEmpty();
    }

    @Test
    public void testTableHandlesCanonization()
    {
        TableHandle tableHandle1 = TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "table1"));
        TableHandle tableHandle2 = TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "table2"));

        PlanNode root = planBuilder.union(ImmutableListMultimap.of(), ImmutableList.of(
                planBuilder.tableScan(tableHandle1, ImmutableList.of(), ImmutableMap.of(), Optional.of(false)),
                planBuilder.tableScan(tableHandle2, ImmutableList.of(), ImmutableMap.of(), Optional.of(false))));

        // TableHandles will be turned into common canonical version
        TableHandle canonicalTableHandle = TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "common"));
        List<TableScan> canonicalTableScans = extractCanonicalSubplans(
                TestingPlannerContext.plannerContextBuilder()
                        .withMetadata(new MockMetadata())
                        .withCacheMetadata(new TestCacheMetadata(
                                handle -> Optional.of(new CacheColumnId(handle.getName())),
                                (tableHandle) -> canonicalTableHandle,
                                (tableHandle) -> Optional.of(new CacheTableId(tableHandle.connectorHandle().toString()))))
                        .build(),
                TEST_SESSION,
                root).stream()
                .map(subplan -> subplan.getTableScan().orElseThrow())
                .collect(toImmutableList());
        List<CacheTableId> tableIds = canonicalTableScans.stream()
                .map(TableScan::getTableId)
                .collect(toImmutableList());
        CacheTableId schemaCommonId = new CacheTableId(CATALOG_ID + ":schema.common");
        assertThat(tableIds).isEqualTo(ImmutableList.of(schemaCommonId, schemaCommonId));
        assertThat(canonicalTableScans).allMatch(scan -> scan.getTable().equals(canonicalTableHandle));

        // TableHandles will not be turned into common canonical version
        tableIds = extractCanonicalSubplans(
                TestingPlannerContext.plannerContextBuilder()
                        .withMetadata(new MockMetadata())
                        .withCacheMetadata(new TestCacheMetadata(
                                handle -> Optional.of(new CacheColumnId(handle.getName())),
                                (tableHandle) -> {
                                    TestingMetadata.TestingTableHandle handle = (TestingMetadata.TestingTableHandle) tableHandle.connectorHandle();
                                    if (handle.getTableName().getTableName().equals("table1")) {
                                        return TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "common1"));
                                    }
                                    else {
                                        return TestingHandles.createTestTableHandle(SchemaTableName.schemaTableName("schema", "common2"));
                                    }
                                },
                                (tableHandle) -> Optional.of(new CacheTableId(tableHandle.connectorHandle().toString()))))
                        .build(),
                TEST_SESSION,
                root).stream()
                .map(subplan -> subplan.getTableScan().orElseThrow().getTableId())
                .collect(toImmutableList());
        assertThat(tableIds).isEqualTo(ImmutableList.of(
                new CacheTableId(CATALOG_ID + ":schema.common1"),
                new CacheTableId(CATALOG_ID + ":schema.common2")));
    }

    private List<CanonicalSubplan> extractCanonicalSubplansForQuery(@Language("SQL") String query)
    {
        return extractCanonicalSubplansForQuery(query, true);
    }

    private List<CanonicalSubplan> extractCanonicalSubplansForQuery(@Language("SQL") String query, boolean forceSingleNode)
    {
        Plan plan = plan(query, OPTIMIZED_AND_VALIDATED, forceSingleNode);
        PlanTester planTester = getPlanTester();
        return planTester.inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> planTester.getPlannerContext().getMetadata().getCatalogHandle(session, catalog));
            return extractCanonicalSubplans(getPlanTester().getPlannerContext(), session, plan.getRoot());
        });
    }

    private CanonicalAggregation sumNationkey()
    {
        return new CanonicalAggregation(
                sumBigint(),
                Optional.empty(),
                List.of(NATIONKEY_REF));
    }

    private ResolvedFunction sumBigint()
    {
        return getPlanTester().getPlannerContext().getMetadata().resolveBuiltinFunction("sum", TypeSignatureProvider.fromTypes(BIGINT));
    }

    private ProjectNode createScanAndProjectNode()
    {
        return new ProjectNode(
                new PlanNodeId("project_node"),
                createTableScan(),
                Assignments.of(
                        new Symbol(BIGINT, "projection1"),
                        new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "symbol1"), new Constant(BIGINT, 1L))),
                        new Symbol(BIGINT, "symbol2"),
                        new Reference(BIGINT, "symbol2")));
    }

    private ProjectNode createFilterAndProjectNode()
    {
        return new ProjectNode(
                new PlanNodeId("project_node"),
                createFilterNode(),
                Assignments.of(
                        new Symbol(BIGINT, "projection1"),
                        new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "symbol1"), new Constant(BIGINT, 1L))),
                        new Symbol(BIGINT, "symbol2"),
                        new Reference(BIGINT, "symbol2")));
    }

    private FilterNode createFilterNode()
    {
        MetadataManager metadataManager = createTestMetadataManager();
        return new FilterNode(
                new PlanNodeId("filter_node"),
                createTableScan(),
                and(
                        new Comparison(GREATER_THAN, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "symbol1"), new Reference(BIGINT, "symbol2"))), new Constant(BIGINT, 0L)),
                        createDynamicFilterExpression(
                                metadataManager,
                                new DynamicFilterId("dynamic_filter_id"),
                                BIGINT,
                                new Reference(BIGINT, "symbol1"))));
    }

    private TableScanNode createTableScan()
    {
        Symbol symbol1 = new Symbol(BIGINT, "symbol1");
        Symbol symbol2 = new Symbol(BIGINT, "symbol2");
        TestingColumnHandle handle1 = new TestingColumnHandle("column1");
        TestingColumnHandle handle2 = new TestingColumnHandle("column2");
        return new TableScanNode(
                SCAN_NODE_ID,
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol1, symbol2),
                ImmutableMap.of(symbol2, handle2, symbol1, handle1),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.of(false));
    }

    private static class TestCacheMetadata
            extends CacheMetadata
    {
        private final Function<TableHandle, Optional<CacheTableId>> tableHandleCacheTableIdMapper;

        private final Function<TestingColumnHandle, Optional<CacheColumnId>> cacheColumnIdMapper;
        private final Function<TableHandle, TableHandle> canonicalizeTableHande;

        private TestCacheMetadata()
        {
            this(handle -> Optional.of(new CacheColumnId("cache_" + handle.getName())), Functions.identity(), (any) -> Optional.of(CACHE_TABLE_ID));
        }

        private TestCacheMetadata(
                Optional<CacheTableId> cacheTableId,
                Function<TestingColumnHandle, Optional<CacheColumnId>> cacheColumnIdMapper)
        {
            this(cacheColumnIdMapper, Function.identity(), (any) -> cacheTableId);
        }

        private TestCacheMetadata(
                Function<TestingColumnHandle, Optional<CacheColumnId>> cacheColumnIdMapper,
                Function<TableHandle, TableHandle> canonicalizeTableHande,
                Function<TableHandle, Optional<CacheTableId>> tableHandleCacheTableIdMapper)
        {
            super(catalogHandle -> Optional.empty());
            this.cacheColumnIdMapper = cacheColumnIdMapper;
            this.canonicalizeTableHande = canonicalizeTableHande;
            this.tableHandleCacheTableIdMapper = tableHandleCacheTableIdMapper;
        }

        @Override
        public Optional<CacheTableId> getCacheTableId(Session session, TableHandle tableHandle)
        {
            return tableHandleCacheTableIdMapper.apply(tableHandle);
        }

        @Override
        public Optional<CacheColumnId> getCacheColumnId(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return cacheColumnIdMapper.apply((TestingColumnHandle) columnHandle);
        }

        @Override
        public TableHandle getCanonicalTableHandle(Session session, TableHandle tableHandle)
        {
            return canonicalizeTableHande.apply(tableHandle);
        }
    }

    protected static class MockMetadata
            extends AbstractMockMetadata
    {
        @Override
        public TableProperties getTableProperties(Session session, TableHandle handle)
        {
            return new TableProperties(
                    handle.catalogHandle(),
                    handle.transaction(),
                    new ConnectorTableProperties(
                            TupleDomain.all(),
                            Optional.empty(),
                            Optional.empty(), ImmutableList.of()));
        }
    }
}
