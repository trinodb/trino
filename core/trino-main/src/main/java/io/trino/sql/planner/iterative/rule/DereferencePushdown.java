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
import io.trino.spi.type.RowType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.DefaultExpressionTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.SymbolsExtractor.extractAll;

/**
 * Provides helper methods to push down dereferences in the query plan.
 */
class DereferencePushdown
{
    private DereferencePushdown() {}

    public static Set<SubscriptExpression> extractRowSubscripts(Collection<Expression> expressions, boolean allowOverlap, Session session, TypeAnalyzer typeAnalyzer, TypeProvider types)
    {
        Set<Expression> symbolReferencesAndRowSubscripts = expressions.stream()
                .flatMap(expression -> getSymbolReferencesAndRowSubscripts(expression, session, typeAnalyzer, types).stream())
                .collect(Collectors.toSet());

        // Remove overlap if required
        Set<Expression> candidateExpressions = symbolReferencesAndRowSubscripts;
        if (!allowOverlap) {
            candidateExpressions = symbolReferencesAndRowSubscripts.stream()
                    .filter(expression -> !prefixExists(expression, symbolReferencesAndRowSubscripts))
                    .collect(Collectors.toSet());
        }

        // Retain row subscript expressions
        return candidateExpressions.stream()
                .filter(SubscriptExpression.class::isInstance)
                .map(SubscriptExpression.class::cast)
                .collect(Collectors.toSet());
    }

    public static boolean exclusiveDereferences(Set<Expression> projections, Session session, TypeAnalyzer typeAnalyzer, TypeProvider types)
    {
        return projections.stream()
                .allMatch(expression -> expression instanceof SymbolReference ||
                        (expression instanceof SubscriptExpression &&
                                isRowSubscriptChain((SubscriptExpression) expression, session, typeAnalyzer, types) &&
                                !prefixExists(expression, projections)));
    }

    public static Symbol getBase(SubscriptExpression expression)
    {
        return getOnlyElement(extractAll(expression));
    }

    /**
     * Extract the sub-expressions of type {@link SubscriptExpression} or {@link SymbolReference} from the {@param expression}
     * in a top-down manner. The expressions within the base of a valid {@link SubscriptExpression} sequence are not extracted.
     */
    private static List<Expression> getSymbolReferencesAndRowSubscripts(Expression expression, Session session, TypeAnalyzer typeAnalyzer, TypeProvider types)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();

        new DefaultExpressionTraversalVisitor<ImmutableList.Builder<Expression>>()
        {
            @Override
            protected Void visitSubscriptExpression(SubscriptExpression node, ImmutableList.Builder<Expression> context)
            {
                if (isRowSubscriptChain(node, session, typeAnalyzer, types)) {
                    context.add(node);
                }
                return null;
            }

            @Override
            protected Void visitSymbolReference(SymbolReference node, ImmutableList.Builder<Expression> context)
            {
                context.add(node);
                return null;
            }

            @Override
            protected Void visitLambdaExpression(LambdaExpression node, ImmutableList.Builder<Expression> context)
            {
                return null;
            }
        }.process(expression, builder);

        return builder.build();
    }

    private static boolean isRowSubscriptChain(SubscriptExpression expression, Session session, TypeAnalyzer typeAnalyzer, TypeProvider types)
    {
        if (!(typeAnalyzer.getType(session, types, expression.getBase()) instanceof RowType)) {
            return false;
        }

        return (expression.getBase() instanceof SymbolReference) ||
                ((expression.getBase() instanceof SubscriptExpression) && isRowSubscriptChain((SubscriptExpression) (expression.getBase()), session, typeAnalyzer, types));
    }

    private static boolean prefixExists(Expression expression, Set<Expression> expressions)
    {
        Expression current = expression;
        while (current instanceof SubscriptExpression) {
            current = ((SubscriptExpression) current).getBase();
            if (expressions.contains(current)) {
                return true;
            }
        }

        verify(current instanceof SymbolReference);
        return false;
    }
}
