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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.JoinNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;

public class TestAddDynamicFilterSourceRules
        extends BaseRuleTest
{
    private static Rule<?> rewriteJoinDynamicFilter()
    {
        return AddDynamicFilterSource.rules().stream()
                .filter(rule -> rule.getClass().getSimpleName().equals("RewriteJoinDynamicFilter"))
                .findFirst()
                .orElseThrow();
    }

    @Test
    public void testPushDynamicFilterSourceBelowAssignUniqueId()
    {
        tester().assertThat(rewriteJoinDynamicFilter())
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .on(p -> {
                    Symbol probeKey = p.symbol("probe_key", BIGINT);
                    Symbol buildKey = p.symbol("build_key", BIGINT);
                    Symbol unique = p.symbol("unique", BIGINT);
                    return p.join(
                            INNER,
                            p.values(probeKey),
                            p.assignUniqueId(unique, p.values(buildKey)),
                            ImmutableList.of(new JoinNode.EquiJoinClause(probeKey, buildKey)),
                            ImmutableList.of(probeKey),
                            ImmutableList.of(buildKey),
                            Optional.empty(),
                            ImmutableMap.of(new DynamicFilterId("DF"), buildKey));
                })
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("probe_key", "build_key")
                                .left(values("probe_key"))
                                .right(assignUniqueId(
                                        "unique",
                                        node(DynamicFilterSourceNode.class, values("build_key"))))));
    }

    @Test
    public void testRemoveDynamicFilterOnUniqueIdSymbol()
    {
        // the unique symbol is created by AssignUniqueId, so a dynamic filter on it cannot move below that node
        tester().assertThat(rewriteJoinDynamicFilter())
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .on(p -> {
                    Symbol probeKey = p.symbol("probe_key", BIGINT);
                    Symbol buildKey = p.symbol("build_key", BIGINT);
                    Symbol unique = p.symbol("unique", BIGINT);
                    return p.join(
                            INNER,
                            p.values(probeKey),
                            p.assignUniqueId(unique, p.values(buildKey)),
                            ImmutableList.of(new JoinNode.EquiJoinClause(probeKey, unique)),
                            ImmutableList.of(probeKey),
                            ImmutableList.of(unique),
                            Optional.empty(),
                            ImmutableMap.of(new DynamicFilterId("DF"), unique));
                })
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("probe_key", "unique")
                                .left(values("probe_key"))
                                .right(assignUniqueId("unique", values("build_key")))));
    }
}
