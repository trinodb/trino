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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;

/**
 * A {@code Case} and a {@code Coalesce} are both opaque to {@link io.trino.sql.planner.DomainTranslator}, so a
 * predicate buried in one cannot be pushed down. {@link io.trino.sql.ir.optimizer.rule.SimplifyBooleanCase} and
 * {@link io.trino.sql.ir.optimizer.rule.SimplifyBooleanCoalesce} dissolve them into logical expressions.
 */
final class TestSimplifyBooleanCasePushdown
        extends BasePlanTest
{
    @Test
    void conditionalPredicateBecomesTopLevelConjunct()
    {
        // The IF collapses to And(orderkey > 5, $identical(custkey = 1, true)), so `orderkey > 5` becomes a
        // standalone conjunct that extractConjuncts can hand to the connector.
        assertPlan(
                "SELECT orderkey FROM orders WHERE IF(custkey = 1, orderkey > 5, false)",
                anyTree(filter(
                        and(
                                comparison(GREATER_THAN, new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 5L)),
                                comparison(IDENTICAL, comparison(EQUAL, new Reference(BIGINT, "custkey"), new Constant(BIGINT, 1L)), TRUE)),
                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "custkey", "custkey")))));
    }

    @Test
    void nullSafeCoalesceIsDissolved()
    {
        // COALESCE(pred, false) collapses to $identical(pred, true) instead of staying an opaque Coalesce.
        assertPlan(
                "SELECT orderkey FROM orders WHERE COALESCE(orderkey > 5, false)",
                anyTree(filter(
                        comparison(IDENTICAL, comparison(GREATER_THAN, new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 5L)), TRUE),
                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey")))));
    }
}
