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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cache.CacheController.CacheCandidate;
import io.trino.cache.CanonicalSubplan.AggregationKey;
import io.trino.cache.CanonicalSubplan.ScanFilterProjectKey;
import io.trino.cache.CanonicalSubplan.TopNRankingKey;
import io.trino.metadata.TableHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.SystemSessionProperties.CACHE_AGGREGATIONS_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_COMMON_SUBQUERIES_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_PROJECTIONS_ENABLED;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCacheController
{
    private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("id");
    private static final CacheTableId TABLE_ID = new CacheTableId("table");

    @Test
    public void testCacheController()
    {
        CacheColumnId columnA = new CacheColumnId("A");
        CacheColumnId columnB = new CacheColumnId("B");
        CanonicalSubplan firstGroupByAB = createCanonicalAggregationSubplan(ImmutableSet.of(columnA, columnB));
        CanonicalSubplan secondGroupByAB = createCanonicalAggregationSubplan(ImmutableSet.of(columnA, columnB));
        CanonicalSubplan groupByA = createCanonicalAggregationSubplan(ImmutableSet.of(columnA));
        CanonicalSubplan firstProjection = createCanonicalTableScanSubplan();
        CanonicalSubplan secondProjection = createCanonicalTableScanSubplan();
        CanonicalSubplan topN = createCanonicalTopNSubplan(ImmutableMap.of(columnA, SortOrder.ASC_NULLS_FIRST), 10);
        CanonicalSubplan topNRanking = createCanonicalTopNRankingSubplan(ImmutableList.of(columnB), ImmutableMap.of(columnA, SortOrder.ASC_NULLS_FIRST), RankingType.ROW_NUMBER, 10);
        List<CanonicalSubplan> subplans = ImmutableList.of(secondProjection, firstProjection, groupByA, secondGroupByAB, firstGroupByAB);

        CacheController cacheController = new CacheController();
        assertThat(cacheController.getCachingCandidates(cacheProperties(true, true, true), subplans))
                .containsExactly(
                        // common aggregations are first
                        new CacheCandidate(ImmutableList.of(secondGroupByAB, firstGroupByAB), 2),
                        // then common projections
                        new CacheCandidate(ImmutableList.of(secondProjection, firstProjection), 2),
                        // then single aggregations
                        new CacheCandidate(ImmutableList.of(groupByA), 1),
                        new CacheCandidate(ImmutableList.of(secondGroupByAB), 1),
                        new CacheCandidate(ImmutableList.of(firstGroupByAB), 1),
                        // then single projections
                        new CacheCandidate(ImmutableList.of(secondProjection), 1),
                        new CacheCandidate(ImmutableList.of(firstProjection), 1));

        assertThat(cacheController.getCachingCandidates(cacheProperties(true, false, false), subplans))
                .containsExactly(
                        new CacheCandidate(ImmutableList.of(secondGroupByAB, firstGroupByAB), 2),
                        new CacheCandidate(ImmutableList.of(secondProjection, firstProjection), 2));

        assertThat(cacheController.getCachingCandidates(cacheProperties(false, true, false), subplans))
                .containsExactly(
                        new CacheCandidate(ImmutableList.of(groupByA), 1),
                        new CacheCandidate(ImmutableList.of(secondGroupByAB), 1),
                        new CacheCandidate(ImmutableList.of(firstGroupByAB), 1));

        assertThat(cacheController.getCachingCandidates(cacheProperties(false, false, true), subplans))
                .containsExactly(
                        new CacheCandidate(ImmutableList.of(secondProjection), 1),
                        new CacheCandidate(ImmutableList.of(firstProjection), 1));

        subplans = ImmutableList.of(secondProjection, firstProjection, topN);
        assertThat(cacheController.getCachingCandidates(cacheProperties(true, true, true), subplans))
                .containsExactly(
                        // common projections are first
                        new CacheCandidate(ImmutableList.of(secondProjection, firstProjection), 2),
                        // then single topN
                        new CacheCandidate(ImmutableList.of(topN), 1),
                        // then single projections
                        new CacheCandidate(ImmutableList.of(secondProjection), 1),
                        new CacheCandidate(ImmutableList.of(firstProjection), 1));

        assertThat(cacheController.getCachingCandidates(cacheProperties(true, false, false), subplans))
                .containsExactly(new CacheCandidate(ImmutableList.of(secondProjection, firstProjection), 2));
        assertThat(cacheController.getCachingCandidates(cacheProperties(false, true, false), subplans))
                .containsExactly(new CacheCandidate(ImmutableList.of(topN), 1));
        assertThat(cacheController.getCachingCandidates(cacheProperties(false, false, true), subplans))
                .containsExactly(
                        new CacheCandidate(ImmutableList.of(secondProjection), 1),
                        new CacheCandidate(ImmutableList.of(firstProjection), 1));

        subplans = ImmutableList.of(secondProjection, firstProjection, topNRanking);

        assertThat(cacheController.getCachingCandidates(cacheProperties(true, true, true), subplans))
                .containsExactly(
                        // common projections are first
                        new CacheCandidate(ImmutableList.of(secondProjection, firstProjection), 2),
                        // then single topNRanking
                        new CacheCandidate(ImmutableList.of(topNRanking), 1),
                        // then single projections
                        new CacheCandidate(ImmutableList.of(secondProjection), 1),
                        new CacheCandidate(ImmutableList.of(firstProjection), 1));

        assertThat(cacheController.getCachingCandidates(cacheProperties(true, false, false), subplans))
                .containsExactly(new CacheCandidate(ImmutableList.of(secondProjection, firstProjection), 2));
        assertThat(cacheController.getCachingCandidates(cacheProperties(false, true, false), subplans))
                .containsExactly(new CacheCandidate(ImmutableList.of(topNRanking), 1));
        assertThat(cacheController.getCachingCandidates(cacheProperties(false, false, true), subplans))
                .containsExactly(
                        new CacheCandidate(ImmutableList.of(secondProjection), 1),
                        new CacheCandidate(ImmutableList.of(firstProjection), 1));
    }

    private CanonicalSubplan createCanonicalAggregationSubplan(Set<CacheColumnId> groupByColumns)
    {
        CanonicalSubplan tableScanPlan = createCanonicalTableScanSubplan();

        return CanonicalSubplan.builderForChildSubplan(new AggregationKey(groupByColumns, ImmutableSet.of()), tableScanPlan)
                .originalPlanNode(new ValuesNode(PLAN_NODE_ID, 0))
                .originalSymbolMapping(ImmutableBiMap.of())
                .assignments(ImmutableMap.of())
                .pullableConjuncts(ImmutableSet.of())
                .groupByColumns(groupByColumns)
                .build();
    }

    private CanonicalSubplan createCanonicalTopNRankingSubplan(List<CacheColumnId> partitionBy, Map<CacheColumnId, SortOrder> orderBy, RankingType rankingType, int maxRankingPerPartition)
    {
        CanonicalSubplan tableScanPlan = createCanonicalTableScanSubplan();

        return CanonicalSubplan.builderForChildSubplan(new TopNRankingKey(partitionBy, orderBy.keySet().stream().toList(), orderBy, rankingType, maxRankingPerPartition, ImmutableSet.of()), tableScanPlan)
                .originalPlanNode(new ValuesNode(PLAN_NODE_ID, 0))
                .originalSymbolMapping(ImmutableBiMap.of())
                .assignments(ImmutableMap.of())
                .pullableConjuncts(ImmutableSet.of())
                .build();
    }

    private CanonicalSubplan createCanonicalTopNSubplan(Map<CacheColumnId, SortOrder> orderBy, long count)
    {
        CanonicalSubplan tableScanPlan = createCanonicalTableScanSubplan();

        return CanonicalSubplan.builderForChildSubplan(new CanonicalSubplan.TopNKey(orderBy.keySet().stream().toList(), orderBy, count, ImmutableSet.of()), tableScanPlan)
                .originalPlanNode(new ValuesNode(PLAN_NODE_ID, 0))
                .originalSymbolMapping(ImmutableBiMap.of())
                .assignments(ImmutableMap.of())
                .pullableConjuncts(ImmutableSet.of())
                .build();
    }

    private static CanonicalSubplan createCanonicalTableScanSubplan()
    {
        return CanonicalSubplan.builderForTableScan(
                        new ScanFilterProjectKey(TABLE_ID),
                        ImmutableMap.of(),
                        new TableHandle(createRootCatalogHandle("catalog", new CatalogVersion("version")), new ConnectorTableHandle() {}, new ConnectorTransactionHandle() {}),
                        TABLE_ID,
                        false,
                        PLAN_NODE_ID)
                .originalPlanNode(new ValuesNode(PLAN_NODE_ID, 0))
                .originalSymbolMapping(ImmutableBiMap.of())
                .assignments(ImmutableMap.of())
                .pullableConjuncts(ImmutableSet.of())
                .build();
    }

    private Session cacheProperties(boolean cacheSubqueries, boolean cacheAggregations, boolean cacheProjections)
    {
        return testSessionBuilder()
                .setSystemProperty(CACHE_COMMON_SUBQUERIES_ENABLED, Boolean.toString(cacheSubqueries))
                .setSystemProperty(CACHE_AGGREGATIONS_ENABLED, Boolean.toString(cacheAggregations))
                .setSystemProperty(CACHE_PROJECTIONS_ENABLED, Boolean.toString(cacheProjections))
                .build();
    }
}
