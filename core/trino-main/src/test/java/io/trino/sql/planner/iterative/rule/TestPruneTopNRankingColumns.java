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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.TopNRankingSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topNRanking;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;

public class TestPruneTopNRankingColumns
        extends BaseRuleTest
{
    @Test
    public void testDoNotPrunePartitioningSymbol()
    {
        tester().assertThat(new PruneTopNRankingColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol ranking = p.symbol("ranking");
                    return p.project(
                            Assignments.identity(b, ranking),
                            p.topNRanking(
                                    new DataOrganizationSpecification(
                                            ImmutableList.of(a),
                                            Optional.of(new OrderingScheme(ImmutableList.of(b), ImmutableMap.of(b, SortOrder.ASC_NULLS_FIRST)))),
                                    ROW_NUMBER,
                                    5,
                                    ranking,
                                    Optional.empty(),
                                    p.values(a, b)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneOrderingSymbol()
    {
        tester().assertThat(new PruneTopNRankingColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.project(
                            Assignments.identity(ranking),
                            p.topNRanking(
                                    new DataOrganizationSpecification(
                                            ImmutableList.of(),
                                            Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST)))),
                                    ROW_NUMBER,
                                    5,
                                    ranking,
                                    Optional.empty(),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneHashSymbol()
    {
        tester().assertThat(new PruneTopNRankingColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol hash = p.symbol("hash");
                    Symbol ranking = p.symbol("ranking");
                    return p.project(
                            Assignments.identity(a, ranking),
                            p.topNRanking(
                                    new DataOrganizationSpecification(
                                            ImmutableList.of(),
                                            Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST)))),
                                    ROW_NUMBER,
                                    5,
                                    ranking,
                                    Optional.of(hash),
                                    p.values(a, hash)));
                })
                .doesNotFire();
    }

    @Test
    public void testSourceSymbolNotReferenced()
    {
        tester().assertThat(new PruneTopNRankingColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol ranking = p.symbol("ranking");
                    return p.project(
                            Assignments.identity(a, ranking),
                            p.topNRanking(
                                    new DataOrganizationSpecification(
                                            ImmutableList.of(),
                                            Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST)))),
                                    ROW_NUMBER,
                                    5,
                                    ranking,
                                    Optional.empty(),
                                    p.values(a, b)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression("a"), "ranking", expression("ranking")),
                                topNRanking(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of(),
                                                        ImmutableList.of("a"),
                                                        ImmutableMap.of("a", SortOrder.ASC_NULLS_FIRST))
                                                .rankingType(ROW_NUMBER)
                                                .maxRankingPerPartition(5),
                                        strictProject(
                                                ImmutableMap.of("a", expression("a")),
                                                values("a", "b")))
                                        .withAlias("ranking", new TopNRankingSymbolMatcher())));
    }

    @Test
    public void testAllSymbolsReferenced()
    {
        tester().assertThat(new PruneTopNRankingColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol ranking = p.symbol("ranking");
                    return p.project(
                            Assignments.identity(a, b, ranking),
                            p.topNRanking(
                                    new DataOrganizationSpecification(
                                            ImmutableList.of(),
                                            Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST)))),
                                    ROW_NUMBER,
                                    5,
                                    ranking,
                                    Optional.empty(),
                                    p.values(a, b)));
                })
                .doesNotFire();
    }

    @Test
    public void testRankingSymbolNotReferenced()
    {
        tester().assertThat(new PruneTopNRankingColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.project(
                            Assignments.identity(a),
                            p.topNRanking(
                                    new DataOrganizationSpecification(
                                            ImmutableList.of(),
                                            Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST)))),
                                    ROW_NUMBER,
                                    5,
                                    ranking,
                                    Optional.empty(),
                                    p.values(a)));
                })
                .doesNotFire();
    }
}
