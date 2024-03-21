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
import io.trino.spi.type.RowType;
import io.trino.sql.ir.DefaultTraversalVisitor;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Subscript;
import io.trino.sql.planner.Symbol;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.SymbolsExtractor.extractAll;

/**
 * Provides helper methods to push down dereferences in the query plan.
 */
class DereferencePushdown
{
    private DereferencePushdown() {}

    public static Set<Subscript> extractRowSubscripts(Collection<Expression> expressions, boolean allowOverlap)
    {
        Set<Expression> symbolReferencesAndRowSubscripts = expressions.stream()
                .flatMap(expression -> getSymbolReferencesAndRowSubscripts(expression).stream())
                .collect(toImmutableSet());

        // Remove overlap if required
        Set<Expression> candidateExpressions = symbolReferencesAndRowSubscripts;
        if (!allowOverlap) {
            candidateExpressions = symbolReferencesAndRowSubscripts.stream()
                    .filter(expression -> !prefixExists(expression, symbolReferencesAndRowSubscripts))
                    .collect(toImmutableSet());
        }

        // Retain row subscript expressions
        return candidateExpressions.stream()
                .filter(Subscript.class::isInstance)
                .map(Subscript.class::cast)
                .collect(toImmutableSet());
    }

    public static boolean exclusiveDereferences(Set<Expression> projections)
    {
        return projections.stream()
                .allMatch(expression -> expression instanceof Reference ||
                        (expression instanceof Subscript &&
                                isRowSubscriptChain((Subscript) expression) &&
                                !prefixExists(expression, projections)));
    }

    public static Symbol getBase(Subscript expression)
    {
        return getOnlyElement(extractAll(expression));
    }

    /**
     * Extract the sub-expressions of type {@link Subscript} or {@link Reference} from the expression
     * in a top-down manner. The expressions within the base of a valid {@link Subscript} sequence are not extracted.
     */
    private static List<Expression> getSymbolReferencesAndRowSubscripts(Expression expression)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();

        new DefaultTraversalVisitor<ImmutableList.Builder<Expression>>()
        {
            @Override
            protected Void visitSubscript(Subscript node, ImmutableList.Builder<Expression> context)
            {
                if (isRowSubscriptChain(node)) {
                    context.add(node);
                }
                return null;
            }

            @Override
            protected Void visitReference(Reference node, ImmutableList.Builder<Expression> context)
            {
                context.add(node);
                return null;
            }

            @Override
            protected Void visitLambda(Lambda node, ImmutableList.Builder<Expression> context)
            {
                return null;
            }
        }.process(expression, builder);

        return builder.build();
    }

    private static boolean isRowSubscriptChain(Subscript expression)
    {
        if (!(expression.base().type() instanceof RowType)) {
            return false;
        }

        return (expression.base() instanceof Reference) ||
                ((expression.base() instanceof Subscript) && isRowSubscriptChain((Subscript) expression.base()));
    }

    private static boolean prefixExists(Expression expression, Set<Expression> expressions)
    {
        Expression current = expression;
        while (current instanceof Subscript) {
            current = ((Subscript) current).base();
            if (expressions.contains(current)) {
                return true;
            }
        }

        verify(current instanceof Reference);
        return false;
    }
}
