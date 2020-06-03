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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.planner.SymbolsExtractor.extractAll;

/**
 * Provides helper methods to push down dereferences in the query plan.
 */
class DereferencePushdown
{
    private DereferencePushdown() {}

    public static Set<DereferenceExpression> extractDereferences(Collection<Expression> expressions, boolean allowOverlap)
    {
        Set<Expression> symbolReferencesAndDereferences = expressions.stream()
                .flatMap(expression -> getSymbolReferencesAndDereferences(expression).stream())
                .collect(Collectors.toSet());

        // Remove overlap if required
        Set<Expression> candidateExpressions = symbolReferencesAndDereferences;
        if (!allowOverlap) {
            candidateExpressions = symbolReferencesAndDereferences.stream()
                    .filter(expression -> !prefixExists(expression, symbolReferencesAndDereferences))
                    .collect(Collectors.toSet());
        }

        // Retain dereference expressions
        return candidateExpressions.stream()
                .filter(DereferenceExpression.class::isInstance)
                .map(DereferenceExpression.class::cast)
                .collect(Collectors.toSet());
    }

    public static boolean exclusiveDereferences(Set<Expression> projections)
    {
        return projections.stream()
                .allMatch(expression -> expression instanceof SymbolReference ||
                        (expression instanceof DereferenceExpression &&
                                isDereferenceChain((DereferenceExpression) expression) &&
                                !prefixExists(expression, projections)));
    }

    public static Symbol getBase(DereferenceExpression expression)
    {
        return getOnlyElement(extractAll(expression));
    }

    /**
     * Extract the sub-expressions of type {@link DereferenceExpression} or {@link SymbolReference} from the {@param expression}
     * in a top-down manner. The expressions within the base of a valid {@link DereferenceExpression} sequence are not extracted.
     */
    private static List<Expression> getSymbolReferencesAndDereferences(Expression expression)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();

        new DefaultExpressionTraversalVisitor<ImmutableList.Builder<Expression>>()
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableList.Builder<Expression> context)
            {
                if (isDereferenceChain(node)) {
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

    private static boolean isDereferenceChain(DereferenceExpression expression)
    {
        return (expression.getBase() instanceof SymbolReference) ||
                ((expression.getBase() instanceof DereferenceExpression) && isDereferenceChain((DereferenceExpression) (expression.getBase())));
    }

    private static boolean prefixExists(Expression expression, Set<Expression> expressions)
    {
        Expression current = expression;
        while (current instanceof DereferenceExpression) {
            current = ((DereferenceExpression) current).getBase();
            if (expressions.contains(current)) {
                return true;
            }
        }

        verify(current instanceof SymbolReference);
        return false;
    }
}
