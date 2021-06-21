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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.DeterminismEvaluator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LogicalExpression.Operator;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SymbolReference;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class ExpressionUtils
{
    private ExpressionUtils() {}

    public static List<Expression> extractConjuncts(Expression expression)
    {
        return extractPredicates(LogicalExpression.Operator.AND, expression);
    }

    public static List<Expression> extractDisjuncts(Expression expression)
    {
        return extractPredicates(LogicalExpression.Operator.OR, expression);
    }

    public static List<Expression> extractPredicates(LogicalExpression expression)
    {
        return extractPredicates(expression.getOperator(), expression);
    }

    public static List<Expression> extractPredicates(LogicalExpression.Operator operator, Expression expression)
    {
        ImmutableList.Builder<Expression> resultBuilder = ImmutableList.builder();
        extractPredicates(operator, expression, resultBuilder);
        return resultBuilder.build();
    }

    private static void extractPredicates(LogicalExpression.Operator operator, Expression expression, ImmutableList.Builder<Expression> resultBuilder)
    {
        if (expression instanceof LogicalExpression && ((LogicalExpression) expression).getOperator() == operator) {
            LogicalExpression logicalExpression = (LogicalExpression) expression;
            for (Expression term : logicalExpression.getTerms()) {
                extractPredicates(operator, term, resultBuilder);
            }
        }
        else {
            resultBuilder.add(expression);
        }
    }

    public static Expression and(Expression... expressions)
    {
        return and(Arrays.asList(expressions));
    }

    public static Expression and(Collection<Expression> expressions)
    {
        return logicalExpression(LogicalExpression.Operator.AND, expressions);
    }

    public static Expression or(Expression... expressions)
    {
        return or(Arrays.asList(expressions));
    }

    public static Expression or(Collection<Expression> expressions)
    {
        return logicalExpression(LogicalExpression.Operator.OR, expressions);
    }

    public static Expression logicalExpression(LogicalExpression.Operator operator, Collection<Expression> expressions)
    {
        requireNonNull(operator, "operator is null");
        requireNonNull(expressions, "expressions is null");

        if (expressions.isEmpty()) {
            switch (operator) {
                case AND:
                    return TRUE_LITERAL;
                case OR:
                    return FALSE_LITERAL;
            }
            throw new IllegalArgumentException("Unsupported LogicalExpression operator");
        }

        if (expressions.size() == 1) {
            return Iterables.getOnlyElement(expressions);
        }

        return new LogicalExpression(operator, ImmutableList.copyOf(expressions));
    }

    public static Expression combinePredicates(Metadata metadata, Operator operator, Expression... expressions)
    {
        return combinePredicates(metadata, operator, Arrays.asList(expressions));
    }

    public static Expression combinePredicates(Metadata metadata, Operator operator, Collection<Expression> expressions)
    {
        if (operator == LogicalExpression.Operator.AND) {
            return combineConjuncts(metadata, expressions);
        }

        return combineDisjuncts(metadata, expressions);
    }

    public static Expression combineConjuncts(Metadata metadata, Expression... expressions)
    {
        return combineConjuncts(metadata, Arrays.asList(expressions));
    }

    public static Expression combineConjuncts(Metadata metadata, Collection<Expression> expressions)
    {
        requireNonNull(expressions, "expressions is null");

        List<Expression> conjuncts = expressions.stream()
                .flatMap(e -> ExpressionUtils.extractConjuncts(e).stream())
                .filter(e -> !e.equals(TRUE_LITERAL))
                .collect(toList());

        conjuncts = removeDuplicates(metadata, conjuncts);

        if (conjuncts.contains(FALSE_LITERAL)) {
            return FALSE_LITERAL;
        }

        return and(conjuncts);
    }

    public static Expression combineConjunctsWithDuplicates(Collection<Expression> expressions)
    {
        requireNonNull(expressions, "expressions is null");

        List<Expression> conjuncts = expressions.stream()
                .flatMap(e -> ExpressionUtils.extractConjuncts(e).stream())
                .filter(e -> !e.equals(TRUE_LITERAL))
                .collect(toList());

        if (conjuncts.contains(FALSE_LITERAL)) {
            return FALSE_LITERAL;
        }

        return and(conjuncts);
    }

    public static Expression combineDisjuncts(Metadata metadata, Expression... expressions)
    {
        return combineDisjuncts(metadata, Arrays.asList(expressions));
    }

    public static Expression combineDisjuncts(Metadata metadata, Collection<Expression> expressions)
    {
        return combineDisjunctsWithDefault(metadata, expressions, FALSE_LITERAL);
    }

    public static Expression combineDisjunctsWithDefault(Metadata metadata, Collection<Expression> expressions, Expression emptyDefault)
    {
        requireNonNull(expressions, "expressions is null");

        List<Expression> disjuncts = expressions.stream()
                .flatMap(e -> ExpressionUtils.extractDisjuncts(e).stream())
                .filter(e -> !e.equals(FALSE_LITERAL))
                .collect(toList());

        disjuncts = removeDuplicates(metadata, disjuncts);

        if (disjuncts.contains(TRUE_LITERAL)) {
            return TRUE_LITERAL;
        }

        return disjuncts.isEmpty() ? emptyDefault : or(disjuncts);
    }

    public static Expression filterDeterministicConjuncts(Metadata metadata, Expression expression)
    {
        return filterConjuncts(metadata, expression, expression1 -> DeterminismEvaluator.isDeterministic(expression1, metadata));
    }

    public static Expression filterNonDeterministicConjuncts(Metadata metadata, Expression expression)
    {
        return filterConjuncts(metadata, expression, not(testExpression -> DeterminismEvaluator.isDeterministic(testExpression, metadata)));
    }

    public static Expression filterConjuncts(Metadata metadata, Expression expression, Predicate<Expression> predicate)
    {
        List<Expression> conjuncts = extractConjuncts(expression).stream()
                .filter(predicate)
                .collect(toList());

        return combineConjuncts(metadata, conjuncts);
    }

    @SafeVarargs
    public static Function<Expression, Expression> expressionOrNullSymbols(Predicate<Symbol>... nullSymbolScopes)
    {
        return expression -> {
            ImmutableList.Builder<Expression> resultDisjunct = ImmutableList.builder();
            resultDisjunct.add(expression);

            for (Predicate<Symbol> nullSymbolScope : nullSymbolScopes) {
                List<Symbol> symbols = SymbolsExtractor.extractUnique(expression).stream()
                        .filter(nullSymbolScope)
                        .collect(toImmutableList());

                if (symbols.isEmpty()) {
                    continue;
                }

                ImmutableList.Builder<Expression> nullConjuncts = ImmutableList.builder();
                for (Symbol symbol : symbols) {
                    nullConjuncts.add(new IsNullPredicate(symbol.toSymbolReference()));
                }

                resultDisjunct.add(and(nullConjuncts.build()));
            }

            return or(resultDisjunct.build());
        };
    }

    /**
     * Removes duplicate deterministic expressions. Preserves the relative order
     * of the expressions in the list.
     */
    private static List<Expression> removeDuplicates(Metadata metadata, List<Expression> expressions)
    {
        Set<Expression> seen = new HashSet<>();

        ImmutableList.Builder<Expression> result = ImmutableList.builder();
        for (Expression expression : expressions) {
            if (!DeterminismEvaluator.isDeterministic(expression, metadata)) {
                result.add(expression);
            }
            else if (!seen.contains(expression)) {
                result.add(expression);
                seen.add(expression);
            }
        }

        return result.build();
    }

    public static Expression rewriteIdentifiersToSymbolReferences(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new SymbolReference(node.getValue());
            }

            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new LambdaExpression(node.getArguments(), treeRewriter.rewrite(node.getBody(), context));
            }

            @Override
            public Expression rewriteGenericDataType(GenericDataType node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                // do not rewrite identifiers within type parameters
                return node;
            }

            @Override
            public Expression rewriteRowDataType(RowDataType node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                // do not rewrite identifiers in field names
                return node;
            }
        }, expression);
    }
}
