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
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WhenClause;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.LogicalPlanner.failFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.correlatedJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;

public class TestTransformCorrelatedScalarSubquery
        extends BaseRuleTest
{
    private static final ImmutableList<List<Expression>> ONE_ROW = ImmutableList.of(ImmutableList.of(new LongLiteral("1")));
    private static final ImmutableList<List<Expression>> TWO_ROWS = ImmutableList.of(ImmutableList.of(new LongLiteral("1")), ImmutableList.of(new LongLiteral("2")));

    private Rule<?> rule = new TransformCorrelatedScalarSubquery(createTestMetadataManager());

    @Test
    public void doesNotFireOnPlanWithoutCorrelatedJoinlNode()
    {
        tester().assertThat(rule)
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnCorrelatedNonScalar()
    {
        tester().assertThat(rule)
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnUncorrelated()
    {
        tester().assertThat(rule)
                .on(p -> p.correlatedJoin(
                        ImmutableList.<Symbol>of(),
                        p.values(p.symbol("a")),
                        p.values(ImmutableList.of(p.symbol("b")), ImmutableList.of(ImmutableList.of(new LongLiteral("1"))))))
                .doesNotFire();
    }

    @Test
    public void rewritesOnSubqueryWithoutProjection()
    {
        tester().assertThat(rule)
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.enforceSingleRow(
                                p.filter(
                                        new ComparisonExpression(EQUAL, new LongLiteral("1"), new SymbolReference("a")), // TODO use correlated predicate, it requires support for correlated subqueries in plan matchers
                                        p.values(ImmutableList.of(p.symbol("a")), TWO_ROWS)))))
                .matches(
                        project(
                                filter(
                                        ensureScalarSubquery(),
                                        markDistinct(
                                                "is_distinct",
                                                ImmutableList.of("corr", "unique"),
                                                correlatedJoin(
                                                        ImmutableList.of("corr"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values("corr")),
                                                        filter(
                                                                new ComparisonExpression(EQUAL, new LongLiteral("1"), new SymbolReference("a")),
                                                                values("a")))))));
    }

    @Test
    public void rewritesOnSubqueryWithProjection()
    {
        tester().assertThat(rule)
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.enforceSingleRow(
                                p.project(
                                        Assignments.of(p.symbol("a2"), new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new LongLiteral("2"))),
                                        p.filter(
                                                new ComparisonExpression(EQUAL, new LongLiteral("1"), new SymbolReference("a")), // TODO use correlated predicate, it requires support for correlated subqueries in plan matchers
                                                p.values(ImmutableList.of(p.symbol("a")), TWO_ROWS))))))
                .matches(
                        project(
                                filter(
                                        ensureScalarSubquery(),
                                        markDistinct(
                                                "is_distinct",
                                                ImmutableList.of("corr", "unique"),
                                                correlatedJoin(
                                                        ImmutableList.of("corr"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values("corr")),
                                                        project(ImmutableMap.of("a2", expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new LongLiteral("2")))),
                                                                filter(
                                                                        new ComparisonExpression(EQUAL, new LongLiteral("1"), new SymbolReference("a")),
                                                                        values("a"))))))));
    }

    @Test
    public void rewritesOnSubqueryWithProjectionOnTopEnforceSingleNode()
    {
        tester().assertThat(rule)
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.project(
                                Assignments.of(p.symbol("a3"), new ArithmeticBinaryExpression(ADD, new SymbolReference("a2"), new LongLiteral("1"))),
                                p.enforceSingleRow(
                                        p.project(
                                                Assignments.of(p.symbol("a2"), new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new LongLiteral("2"))),
                                                p.filter(
                                                        new ComparisonExpression(EQUAL, new LongLiteral("1"), new SymbolReference("a")), // TODO use correlated predicate, it requires support for correlated subqueries in plan matchers
                                                        p.values(ImmutableList.of(p.symbol("a")), TWO_ROWS)))))))
                .matches(
                        project(
                                filter(
                                        ensureScalarSubquery(),
                                        markDistinct(
                                                "is_distinct",
                                                ImmutableList.of("corr", "unique"),
                                                correlatedJoin(
                                                        ImmutableList.of("corr"),
                                                        assignUniqueId(
                                                                "unique",
                                                                values("corr")),
                                                        project(
                                                                ImmutableMap.of("a3", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("a2"), new LongLiteral("1")))),
                                                                project(
                                                                        ImmutableMap.of("a2", expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new LongLiteral("2")))),
                                                                        filter(
                                                                                new ComparisonExpression(EQUAL, new LongLiteral("1"), new SymbolReference("a")),
                                                                                values("a")))))))));
    }

    @Test
    public void rewritesScalarSubquery()
    {
        tester().assertThat(rule)
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        // make sure INNER correlated join is transformed to LEFT join if subplan could produce 0 rows
                        INNER,
                        TRUE_LITERAL,
                        p.enforceSingleRow(
                                p.filter(
                                        new ComparisonExpression(EQUAL, new LongLiteral("1"), new SymbolReference("a")), // TODO use correlated predicate, it requires support for correlated subqueries in plan matchers
                                        p.values(ImmutableList.of(p.symbol("a")), ONE_ROW)))))
                .matches(
                        correlatedJoin(
                                ImmutableList.of("corr"),
                                values("corr"),
                                filter(
                                        new ComparisonExpression(EQUAL, new LongLiteral("1"), new SymbolReference("a")),
                                        values("a")))
                                .with(CorrelatedJoinNode.class, join -> join.getType() == LEFT));
    }

    private Expression ensureScalarSubquery()
    {
        return new SimpleCaseExpression(
                new SymbolReference("is_distinct"),
                ImmutableList.of(new WhenClause(TRUE_LITERAL, TRUE_LITERAL)),
                Optional.of(new Cast(
                        failFunction(tester().getMetadata(), SUBQUERY_MULTIPLE_ROWS, "Scalar sub-query has returned multiple rows"),
                        toSqlType(BOOLEAN))));
    }
}
