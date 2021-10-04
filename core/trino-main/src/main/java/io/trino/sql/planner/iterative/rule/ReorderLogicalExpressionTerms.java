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

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.SymbolReference;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public class ReorderLogicalExpressionTerms
        extends ExpressionRewriteRuleSet
{
    private static class CostAndPosition
    {
        private final int cost;
        private final int position;

        public CostAndPosition(int cost, int position)
        {
            this.cost = cost;
            this.position = position;
        }

        public int getCost()
        {
            return cost;
        }

        public int getPosition()
        {
            return position;
        }
    }

    public static Expression rewrite(Expression expression, Session session, SymbolAllocator symbolAllocator, Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        if (expression instanceof LogicalExpression) {
            Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression);

            LogicalExpression logicalExpression = (LogicalExpression) expression;

            Map<NodeRef<Expression>, CostAndPosition> costs = new HashMap<>();

            for (int i = 0; i < logicalExpression.getTerms().size(); i++) {
                Expression term = logicalExpression.getTerms().get(i);

                int cost = Integer.MAX_VALUE;

                if (term instanceof IsNullPredicate && ((IsNullPredicate) term).getValue() instanceof SymbolReference) {
                    cost = 0;
                }
                else if (term instanceof ComparisonExpression) {
                    Expression left = ((ComparisonExpression) term).getLeft();
                    Expression right = ((ComparisonExpression) term).getRight();

                    Type type = expressionTypes.get(NodeRef.of(left));

                    if (type == BOOLEAN ||
                            type == TINYINT ||
                            type == SMALLINT ||
                            type == INTEGER ||
                            type == BIGINT ||
                            type == REAL ||
                            type == DOUBLE ||
                            type == DATE ||
                            type instanceof TimeType ||
                            (type instanceof TimestampType && ((TimestampType) type).isShort()) ||
                            (type instanceof TimestampWithTimeZoneType && ((TimestampWithTimeZoneType) type).isShort()) ||
                            (type instanceof TimeWithTimeZoneType && ((TimeWithTimeZoneType) type).isShort()) ||
                            (type instanceof DecimalType && ((DecimalType) type).isShort())) {
                        if ((left instanceof SymbolReference && right instanceof Literal) ||
                                (left instanceof Literal && right instanceof SymbolReference)) {
                            cost = 1;
                        }
                        else if (left instanceof SymbolReference && right instanceof SymbolReference) {
                            cost = 2;
                        }
                    }
                }

                costs.put(NodeRef.of(term), new CostAndPosition(cost, i));
            }

            List<Expression> terms = logicalExpression.getTerms().stream()
                    .sorted(Comparator.<Expression>comparingDouble(o -> costs.get(NodeRef.of(o)).getCost())
                            .thenComparing(o -> costs.get(NodeRef.of(o)).getPosition()))
                    .collect(Collectors.toList());

            return new LogicalExpression(logicalExpression.getOperator(), terms);
        }

        return expression;
    }

    public ReorderLogicalExpressionTerms(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        super(createRewrite(metadata, typeAnalyzer));
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectExpressionRewrite(),
                filterExpressionRewrite(),
                joinExpressionRewrite(),
                valuesExpressionRewrite());
    }

    private static ExpressionRewriter createRewrite(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        return (expression, context) -> rewrite(expression, context.getSession(), context.getSymbolAllocator(), metadata, typeAnalyzer);
    }
}
