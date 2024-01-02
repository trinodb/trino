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
import io.trino.cache.CacheController.SubplanKey;
import io.trino.metadata.TableHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.SystemSessionProperties.CACHE_AGGREGATIONS_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_COMMON_SUBQUERIES_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_PROJECTIONS_ENABLED;
import static io.trino.cache.CacheController.toSubplanKey;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCacheController
{
    private static final CacheTableId TABLE_ID = new CacheTableId("table");

    @Test
    public void testToSubplanKey()
    {
        CacheTableId tableId = new CacheTableId("table_id");
        Expression predicateA = expression("a > b");
        Expression predicateB = expression("c");
        assertThat(toSubplanKey(tableId, Optional.empty(), ImmutableList.of(predicateA, predicateB)))
                .isEqualTo(new SubplanKey(tableId, Optional.empty(), ImmutableSet.of()));
        assertThat(toSubplanKey(tableId, Optional.of(ImmutableSet.of(new CacheColumnId("b"), new CacheColumnId("c"))), ImmutableList.of(predicateA, predicateB)))
                .isEqualTo(new SubplanKey(tableId, Optional.of(ImmutableSet.of(new CacheColumnId("b"), new CacheColumnId("c"))), ImmutableSet.of(predicateA)));
    }

    @Test
    public void testCacheController()
    {
        CacheColumnId columnA = new CacheColumnId("A");
        CacheColumnId columnB = new CacheColumnId("B");
        CanonicalSubplan firstGroupByAB = createCanonicalSubplan("firstAB", Optional.of(ImmutableSet.of(columnA, columnB)));
        CanonicalSubplan secondGroupByAB = createCanonicalSubplan("secondAB", Optional.of(ImmutableSet.of(columnA, columnB)));
        CanonicalSubplan groupByA = createCanonicalSubplan("groupA", Optional.of(ImmutableSet.of(columnA)));
        CanonicalSubplan firstProjection = createCanonicalSubplan("projection", Optional.empty());
        CanonicalSubplan secondProjection = createCanonicalSubplan("projection", Optional.empty());
        List<CanonicalSubplan> subplans = ImmutableList.of(secondProjection, firstProjection, groupByA, secondGroupByAB, firstGroupByAB);

        CacheController cacheController = new CacheController();
        assertThat(cacheController.getCachingCandidates(cacheProperties(true, true, true), subplans))
                .containsExactly(
                        // common aggregations are first
                        new CacheCandidate(TABLE_ID, Optional.of(ImmutableSet.of(columnA, columnB)), ImmutableList.of(secondGroupByAB, firstGroupByAB), 2),
                        // then common projections
                        new CacheCandidate(TABLE_ID, Optional.empty(), ImmutableList.of(secondProjection, firstProjection), 2),
                        // then single aggregations
                        new CacheCandidate(TABLE_ID, Optional.of(ImmutableSet.of(columnA)), ImmutableList.of(groupByA), 1),
                        new CacheCandidate(TABLE_ID, Optional.of(ImmutableSet.of(columnA, columnB)), ImmutableList.of(secondGroupByAB), 1),
                        new CacheCandidate(TABLE_ID, Optional.of(ImmutableSet.of(columnA, columnB)), ImmutableList.of(firstGroupByAB), 1),
                        // then single projections
                        new CacheCandidate(TABLE_ID, Optional.empty(), ImmutableList.of(secondProjection), 1),
                        new CacheCandidate(TABLE_ID, Optional.empty(), ImmutableList.of(firstProjection), 1));

        assertThat(cacheController.getCachingCandidates(cacheProperties(true, false, false), subplans))
                .containsExactly(
                        new CacheCandidate(TABLE_ID, Optional.of(ImmutableSet.of(columnA, columnB)), ImmutableList.of(secondGroupByAB, firstGroupByAB), 2),
                        new CacheCandidate(TABLE_ID, Optional.empty(), ImmutableList.of(secondProjection, firstProjection), 2));

        assertThat(cacheController.getCachingCandidates(cacheProperties(false, true, false), subplans))
                .containsExactly(
                        new CacheCandidate(TABLE_ID, Optional.of(ImmutableSet.of(columnA)), ImmutableList.of(groupByA), 1),
                        new CacheCandidate(TABLE_ID, Optional.of(ImmutableSet.of(columnA, columnB)), ImmutableList.of(secondGroupByAB), 1),
                        new CacheCandidate(TABLE_ID, Optional.of(ImmutableSet.of(columnA, columnB)), ImmutableList.of(firstGroupByAB), 1));

        assertThat(cacheController.getCachingCandidates(cacheProperties(false, false, true), subplans))
                .containsExactly(
                        new CacheCandidate(TABLE_ID, Optional.empty(), ImmutableList.of(secondProjection), 1),
                        new CacheCandidate(TABLE_ID, Optional.empty(), ImmutableList.of(firstProjection), 1));
    }

    private CanonicalSubplan createCanonicalSubplan(String planId, Optional<Set<CacheColumnId>> groupByColumns)
    {
        return new CanonicalSubplan(
                new ValuesNode(new PlanNodeId(planId), 0),
                ImmutableBiMap.of(),
                groupByColumns,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                new TableHandle(createRootCatalogHandle("catalog", new CatalogVersion("version")), new ConnectorTableHandle() {}, new ConnectorTransactionHandle() {}),
                TABLE_ID,
                false,
                new PlanNodeId(planId));
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
