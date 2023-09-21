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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import org.junit.jupiter.api.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.AbstractMockMetadata.dummyMetadata;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;

public class TestCardinalityExtractorPlanVisitor
{
    @Test
    public void testLimitOnTopOfValues()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata(), TEST_SESSION);

        assertEquals(
                extractCardinality(planBuilder.limit(3, planBuilder.values(emptyList(), ImmutableList.of(emptyList())))),
                new Cardinality(Range.singleton(1L)));

        assertEquals(
                extractCardinality(planBuilder.limit(3, planBuilder.values(emptyList(), ImmutableList.of(emptyList(), emptyList(), emptyList(), emptyList())))),
                new Cardinality(Range.singleton(3L)));
    }

    @Test
    public void testAggregation()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata(), TEST_SESSION);
        Symbol symbol = planBuilder.symbol("symbol");
        ColumnHandle columnHandle = new TestingColumnHandle("column");

        // single default aggregation
        assertEquals(extractCardinality(
                        planBuilder.aggregation(builder -> builder
                                .singleGroupingSet()
                                .source(planBuilder.values(10)))),
                new Cardinality(Range.singleton(1L)));

        // multiple grouping sets with default aggregation with source that produces arbitrary number of rows
        assertEquals(extractCardinality(
                        planBuilder.aggregation(builder -> builder
                                .groupingSets(AggregationNode.groupingSets(
                                        ImmutableList.of(symbol),
                                        2,
                                        ImmutableSet.of(0)))
                                .source(planBuilder.tableScan(ImmutableList.of(symbol), ImmutableMap.of(symbol, columnHandle))))),
                new Cardinality(Range.atLeast(1L)));

        // multiple grouping sets with default aggregation with source that produces exact number of rows
        assertEquals(extractCardinality(
                        planBuilder.aggregation(builder -> builder
                                .groupingSets(AggregationNode.groupingSets(
                                        ImmutableList.of(symbol),
                                        2,
                                        ImmutableSet.of(0)))
                                .source(planBuilder.values(10, symbol)))),
                new Cardinality(Range.closed(1L, 10L)));

        // multiple grouping sets with default aggregation with source that produces no rows
        assertEquals(extractCardinality(
                        planBuilder.aggregation(builder -> builder
                                .groupingSets(AggregationNode.groupingSets(
                                        ImmutableList.of(symbol),
                                        2,
                                        ImmutableSet.of(0)))
                                .source(planBuilder.values(0, symbol)))),
                new Cardinality(Range.singleton(1L)));

        // single non-default aggregation with source that produces arbitrary number of rows
        assertEquals(extractCardinality(
                        planBuilder.aggregation(builder -> builder
                                .singleGroupingSet(symbol)
                                .source(planBuilder.tableScan(ImmutableList.of(symbol), ImmutableMap.of(symbol, columnHandle))))),
                new Cardinality(Range.atLeast(0L)));

        // single non-default aggregation with source that produces at least single row
        assertEquals(extractCardinality(
                        planBuilder.aggregation(builder -> builder
                                .singleGroupingSet(symbol)
                                .source(planBuilder.values(10, symbol)))),
                new Cardinality(Range.closed(1L, 10L)));

        // single non-default aggregation with source that produces no rows
        assertEquals(extractCardinality(
                        planBuilder.aggregation(builder -> builder
                                .singleGroupingSet(symbol)
                                .source(planBuilder.values(0, symbol)))),
                new Cardinality(Range.singleton(0L)));
    }
}
