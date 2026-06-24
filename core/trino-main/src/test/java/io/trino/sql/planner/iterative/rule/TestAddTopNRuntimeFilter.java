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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.sql.DynamicFilters;
import io.trino.sql.DynamicFilters.Descriptor;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.MatchContext;
import io.trino.sql.planner.assertions.MatchResult;
import io.trino.sql.planner.assertions.Matcher;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TopNNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.TopNNode.Step.FINAL;
import static io.trino.sql.planner.plan.TopNNode.Step.PARTIAL;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class TestAddTopNRuntimeFilter
        extends BaseRuleTest
{
    private static final Session DYNAMIC_FILTERING_DISABLED = testSessionBuilder()
            .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
            .build();

    @Test
    public void testAddsRuntimeFilter()
    {
        tester().assertThat(rule())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    return distributedTopN(
                            p,
                            orderkey,
                            p.values(orderkey));
                })
                .matches(topNWithRuntimeFilter(
                        1000,
                        "orderkey",
                        exchange(
                                REMOTE,
                                GATHER,
                                topN(
                                        1000,
                                        ImmutableList.of(sort("orderkey", ASCENDING, FIRST)),
                                        PARTIAL,
                                        filterWithTopNRuntimeFilter(
                                                "orderkey",
                                                values("orderkey"))))));
    }

    @Test
    public void testDoesNotFireWhenDynamicFilteringDisabled()
    {
        tester().assertThat(rule())
                .withSession(DYNAMIC_FILTERING_DISABLED)
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    return distributedTopN(p, orderkey, p.values(orderkey));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithoutRemoteExchange()
    {
        tester().assertThat(rule())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    return p.topN(
                            1000,
                            ImmutableList.of(orderkey),
                            FINAL,
                            p.values(orderkey));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForPartialTopN()
    {
        tester().assertThat(rule())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    return p.topN(
                            1000,
                            ImmutableList.of(orderkey),
                            PARTIAL,
                            p.gatheringExchange(REMOTE, p.values(orderkey)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForMultipleSortKeys()
    {
        tester().assertThat(rule())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    Symbol partkey = p.symbol("partkey", BIGINT);
                    return distributedTopN(p, ImmutableList.of(orderkey, partkey), p.values(orderkey, partkey));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForLargeLimit()
    {
        tester().assertThat(rule())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    return distributedTopN(p, orderkey, 10_001, p.values(orderkey));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForZeroLimit()
    {
        tester().assertThat(rule())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    return distributedTopN(p, orderkey, 0, p.values(orderkey));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForFloatingPointSortKey()
    {
        tester().assertThat(rule())
                .on(p -> {
                    Symbol sortKey = p.symbol("sort_key", DOUBLE);
                    return distributedTopN(p, sortKey, p.values(sortKey));
                })
                .doesNotFire();

        tester().assertThat(rule())
                .on(p -> {
                    Symbol sortKey = p.symbol("sort_key", REAL);
                    return distributedTopN(p, sortKey, p.values(sortKey));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenRuntimeFilterAlreadyExists()
    {
        tester().assertThat(rule())
                .on(p -> {
                    Symbol orderkey = p.symbol("orderkey", BIGINT);
                    return distributedTopN(p, orderkey, p.values(orderkey))
                            .withRuntimeFilter(new TopNNode.RuntimeFilter(new DynamicFilterId("existing"), orderkey));
                })
                .doesNotFire();
    }

    private AddTopNRuntimeFilter rule()
    {
        return new AddTopNRuntimeFilter(tester().getPlannerContext());
    }

    private static TopNNode distributedTopN(PlanBuilder p, Symbol orderkey, PlanNode source)
    {
        return distributedTopN(p, orderkey, 1000, source);
    }

    private static TopNNode distributedTopN(PlanBuilder p, Symbol orderkey, long count, PlanNode source)
    {
        return distributedTopN(p, ImmutableList.of(orderkey), count, source);
    }

    private static TopNNode distributedTopN(PlanBuilder p, ImmutableList<Symbol> orderBy, PlanNode source)
    {
        return distributedTopN(p, orderBy, 1000, source);
    }

    private static TopNNode distributedTopN(PlanBuilder p, ImmutableList<Symbol> orderBy, long count, PlanNode source)
    {
        return p.topN(
                count,
                orderBy,
                FINAL,
                p.gatheringExchange(
                        REMOTE,
                        p.topN(count, orderBy, PARTIAL, source)));
    }

    private static PlanMatchPattern topNWithRuntimeFilter(long count, String runtimeFilterSymbol, PlanMatchPattern source)
    {
        return topN(
                count,
                ImmutableList.of(sort(runtimeFilterSymbol, ASCENDING, FIRST)),
                FINAL,
                source)
                .with(new TopNRuntimeFilterMatcher(runtimeFilterSymbol));
    }

    private static PlanMatchPattern filterWithTopNRuntimeFilter(String symbol, PlanMatchPattern source)
    {
        return node(FilterNode.class, source)
                .with(new FilterRuntimeFilterMatcher(symbol));
    }

    private record TopNRuntimeFilterMatcher(String symbol)
            implements Matcher
    {
        private TopNRuntimeFilterMatcher
        {
            requireNonNull(symbol, "symbol is null");
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TopNNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, MatchContext context)
        {
            TopNNode topNNode = (TopNNode) node;
            Optional<TopNNode.RuntimeFilter> runtimeFilter = topNNode.getRuntimeFilter();
            if (runtimeFilter.isEmpty()) {
                return MatchResult.NO_MATCH;
            }
            if (!runtimeFilter.orElseThrow().id().toString().startsWith("topn_rf_")) {
                return MatchResult.NO_MATCH;
            }
            if (!runtimeFilter.orElseThrow().symbol().name().equals(symbol)) {
                return MatchResult.NO_MATCH;
            }
            return MatchResult.match();
        }
    }

    private record FilterRuntimeFilterMatcher(String symbol)
            implements Matcher
    {
        private FilterRuntimeFilterMatcher
        {
            requireNonNull(symbol, "symbol is null");
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof FilterNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, MatchContext context)
        {
            FilterNode filterNode = (FilterNode) node;
            ImmutableList<Descriptor> dynamicFilters = ImmutableList.copyOf(DynamicFilters.extractDynamicFilters(filterNode.getPredicate()).getDynamicConjuncts());
            if (dynamicFilters.size() != 1) {
                return MatchResult.NO_MATCH;
            }
            Descriptor descriptor = dynamicFilters.get(0);
            if (!descriptor.getId().toString().startsWith("topn_rf_")) {
                return MatchResult.NO_MATCH;
            }
            if (!(descriptor.getInput() instanceof Reference reference) || !reference.name().equals(symbol)) {
                return MatchResult.NO_MATCH;
            }
            return MatchResult.match();
        }
    }
}
