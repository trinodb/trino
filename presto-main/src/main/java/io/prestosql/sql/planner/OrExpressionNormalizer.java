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
package io.prestosql.sql.planner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.ExpressionUtils.combineDisjuncts;
import static io.prestosql.sql.ExpressionUtils.or;
import static io.prestosql.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.LogicalBinaryExpression.Operator.AND;
import static io.prestosql.sql.tree.LogicalBinaryExpression.Operator.OR;

public class OrExpressionNormalizer
{
    private OrExpressionNormalizer()
    {}

    public static Expression rewrite(Metadata metadata, Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(metadata), expression, Optional.empty());
    }

    private static class Visitor
            extends ExpressionRewriter<Optional<Context>>
    {
        private final Metadata metadata;

        public Visitor(Metadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public Expression rewriteComparisonExpression(ComparisonExpression comparisonExpression, Optional<Context> context, ExpressionTreeRewriter<Optional<Context>> treeRewriter)
        {
            if (comparisonExpression.getOperator().equals(EQUAL) && context.isPresent()) {
                context.get().addMapping(comparisonExpression.getLeft(), treeRewriter.rewrite(comparisonExpression.getRight(), Optional.empty()));
                return FALSE_LITERAL;
            }
            return comparisonExpression;
        }

        @Override
        public Expression rewriteExpression(Expression expression, Optional<Context> context, ExpressionTreeRewriter<Optional<Context>> treeRewriter)
        {
            return treeRewriter.defaultRewrite(expression, Optional.empty());
        }

        @Override
        public Expression rewriteInPredicate(InPredicate inPredicate, Optional<Context> context, ExpressionTreeRewriter<Optional<Context>> treeRewriter)
        {
            if (inPredicate.getValueList() instanceof InListExpression && context.isPresent()) {
                context.get().addMapping(inPredicate.getValue(), ((InListExpression) inPredicate.getValueList()).getValues());
                return FALSE_LITERAL;
            }
            return inPredicate;
        }

        @Override
        public Expression rewriteLogicalBinaryExpression(LogicalBinaryExpression logicalBinaryExpression, Optional<Context> context, ExpressionTreeRewriter<Optional<Context>> treeRewriter)
        {
            Context baseContext = context.orElse(new Context());

            Context leftExpressionContext = new Context();
            Expression rewrittenLeft = treeRewriter.rewrite(logicalBinaryExpression.getLeft(), Optional.of(leftExpressionContext));
            Context rightExpressionContext = new Context();
            Expression rewrittenRight = treeRewriter.rewrite(logicalBinaryExpression.getRight(), Optional.of(rightExpressionContext));

            if (logicalBinaryExpression.getOperator().equals(OR)) {
                baseContext.merge(leftExpressionContext);
                baseContext.merge(rightExpressionContext);
                Expression rewrittenExpression = combineDisjuncts(metadata, rewrittenLeft, rewrittenRight);
                if (!context.isPresent()) {
                    return combineDisjuncts(metadata, rewrittenExpression, baseContext.getTranslatedExpression());
                }
                return rewrittenExpression;
            }
            return new LogicalBinaryExpression(
                    AND,
                    combineDisjuncts(metadata, rewrittenLeft, leftExpressionContext.getTranslatedExpression()),
                    combineDisjuncts(metadata, rewrittenRight, rightExpressionContext.getTranslatedExpression()));
        }
    }

    private static class Context
    {
        private final Multimap<Expression, Expression> expressions = HashMultimap.create();

        public void addMapping(Expression key, Expression value)
        {
            this.expressions.put(key, value);
        }

        public void addMapping(Expression key, Collection<Expression> value)
        {
            this.expressions.putAll(key, value);
        }

        public void merge(Context context)
        {
            this.expressions.putAll(context.expressions);
        }

        public Expression getTranslatedExpression()
        {
            ImmutableList.Builder<Expression> predicates = ImmutableList.builder();
            for (Expression expression : expressions.keySet()) {
                List<Expression> valueList = expressions.get(expression).stream()
                        .distinct()
                        .collect(toImmutableList());
                if (valueList.size() == 1) {
                    predicates.add(new ComparisonExpression(EQUAL, expression, getOnlyElement(valueList)));
                }
                else {
                    predicates.add(new InPredicate(expression, new InListExpression(valueList)));
                }
            }
            return or(predicates.build());
        }
    }
}
