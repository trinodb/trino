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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.assertions.BasePlanTest;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;

/**
 * An equality shared by every branch of an IF join predicate is extracted to a top-level conjunct by
 * {@link io.trino.sql.ir.optimizer.rule.ExtractCommonConjunctFromCase} and recognized as a hash-join key.
 */
final class TestExtractCommonConjunctFromCase
        extends BasePlanTest
{
    @Test
    void conditionalEqualityPlansAsHashJoin()
    {
        // Equality o.custkey = lookup.join_key appears in both IF branches, so it is extracted to a hash-join key.
        assertPlan(
                """
                SELECT o.orderkey FROM orders o
                JOIN (VALUES (BIGINT '1', CAST(NULL AS bigint)), (BIGINT '2', BIGINT '7')) lookup(join_key, optional_key)
                ON IF(lookup.optional_key IS NULL, o.custkey = lookup.join_key, lookup.optional_key = o.orderkey AND o.custkey = lookup.join_key)
                """,
                anyTree(join(INNER, builder -> builder
                        .equiCriteria("CUSTKEY", "JOIN_KEY")
                        .filter(new Case(
                                ImmutableList.of(new WhenClause(new IsNull(new Reference(BIGINT, "OPTIONAL_KEY")), TRUE)),
                                comparison(EQUAL, new Reference(BIGINT, "OPTIONAL_KEY"), new Reference(BIGINT, "ORDERKEY"))))
                        .left(anyTree(tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey", "ORDERKEY", "orderkey"))))
                        .right(values("JOIN_KEY", "OPTIONAL_KEY")))));
    }
}
