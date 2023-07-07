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
import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.MergeExcept;
import io.trino.sql.planner.iterative.rule.MergeIntersect;
import io.trino.sql.planner.iterative.rule.MergeUnion;
import io.trino.sql.planner.iterative.rule.PruneDistinctAggregation;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.sql.planner.PlanOptimizers.columnPruningRules;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.except;
import static io.trino.sql.planner.assertions.PlanMatchPattern.intersect;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;

public class TestSetFlattening
        extends BasePlanTest
{
    @Test
    public void testFlattensUnion()
    {
        assertPlan(
                "(SELECT * FROM nation UNION SELECT * FROM nation)" +
                        "UNION (SELECT * FROM nation UNION SELECT * FROM nation)",
                anyTree(
                        union(
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"))));
    }

    @Test
    public void testFlattensUnionAll()
    {
        assertPlan(
                "(SELECT * FROM nation UNION ALL SELECT * FROM nation)" +
                        "UNION ALL (SELECT * FROM nation UNION ALL SELECT * FROM nation)",
                anyTree(
                        union(
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"))));
    }

    @Test
    public void testFlattensUnionAndUnionAllWhenAllowed()
    {
        assertPlan(
                "SELECT * FROM nation " +
                        "UNION ALL (SELECT * FROM nation " +
                        "UNION (SELECT * FROM nation UNION ALL select * FROM nation))",
                anyTree(
                        union(
                                tableScan("nation"),
                                anyTree(
                                        union(
                                                tableScan("nation"),
                                                tableScan("nation"),
                                                tableScan("nation"))))));
    }

    @Test
    public void testFlattensIntersect()
    {
        assertPlan(
                "(SELECT * FROM nation INTERSECT SELECT * FROM nation)" +
                        "INTERSECT (SELECT * FROM nation INTERSECT SELECT * FROM nation)",
                anyTree(
                        intersect(
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"))));
    }

    @Test
    public void testFlattensOnlyFirstInputOfExcept()
    {
        assertPlan(
                "(SELECT * FROM nation EXCEPT SELECT * FROM nation)" +
                        "EXCEPT (SELECT * FROM nation EXCEPT SELECT * FROM nation)",
                anyTree(
                        except(
                                tableScan("nation"),
                                tableScan("nation"),
                                except(
                                        tableScan("nation"),
                                        tableScan("nation")))));
    }

    @Test
    public void testDoesNotFlattenDifferentSetOperations()
    {
        assertPlan(
                "(SELECT * FROM nation EXCEPT SELECT * FROM nation)" +
                        "UNION (SELECT * FROM nation INTERSECT SELECT * FROM nation)",
                anyTree(
                        union(
                                except(
                                        tableScan("nation"),
                                        tableScan("nation")),
                                intersect(
                                        tableScan("nation"),
                                        tableScan("nation")))));
    }

    @Override
    protected void assertPlan(String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(getQueryRunner().getMetadata()),
                new IterativeOptimizer(
                        getQueryRunner().getPlannerContext(),
                        new RuleStatsRecorder(),
                        getQueryRunner().getStatsCalculator(),
                        getQueryRunner().getEstimatedExchangesCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new RemoveRedundantIdentityProjections())
                                .add(new MergeUnion())
                                .add(new MergeIntersect())
                                .add(new MergeExcept())
                                .add(new PruneDistinctAggregation())
                                .addAll(columnPruningRules(getQueryRunner().getMetadata()))
                                .build()));
        assertPlan(sql, pattern, optimizers);
    }
}
