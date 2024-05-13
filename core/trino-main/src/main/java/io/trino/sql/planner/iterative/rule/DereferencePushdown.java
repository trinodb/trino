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
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.DefaultTraversalVisitor;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.FunctionCall;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_NAME;
import static io.trino.sql.planner.SymbolsExtractor.extractAll;

/**
 * Provides helper methods to push down dereferences in the query plan.
 */
class DereferencePushdown
{
    private DereferencePushdown() {}

    public static Set<FieldReference> extractRowSubscripts(Collection<Expression> expressions, boolean allowOverlap)
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
                .filter(FieldReference.class::isInstance)
                .map(FieldReference.class::cast)
                .collect(toImmutableSet());
    }

    public static boolean exclusiveDereferences(Set<Expression> projections)
    {
        return projections.stream()
                .allMatch(expression -> expression instanceof Reference ||
                        (expression instanceof FieldReference fieldReference &&
                                isRowSubscriptChain(fieldReference) &&
                                !prefixExists(expression, projections)));
    }

    public static Symbol getBase(FieldReference expression)
    {
        return getOnlyElement(extractAll(expression));
    }

    /**
     * Extract the sub-expressions of type {@link FieldReference} or {@link Reference} from the expression
     * in a top-down manner. The expressions within the base of a valid {@link FieldReference} sequence are not extracted.
     */
    private static List<Expression> getSymbolReferencesAndRowSubscripts(Expression expression)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();

        new DefaultTraversalVisitor<ImmutableList.Builder<Expression>>()
        {
            @Override
            protected Void visitFieldReference(FieldReference node, ImmutableList.Builder<Expression> context)
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

    private static boolean isRowSubscriptChain(FieldReference expression)
    {
        if (!(expression.base().type() instanceof RowType)) {
            return false;
        }

        return (expression.base() instanceof Reference) ||
                ((expression.base() instanceof FieldReference fieldReference) && isRowSubscriptChain(fieldReference));
    }

    private static boolean prefixExists(Expression expression, Set<Expression> expressions)
    {
        Expression current = expression;
        while (current instanceof FieldReference fieldReference) {
            current = fieldReference.base();
            if (expressions.contains(current)) {
                return true;
            }
        }

        verify(current instanceof Reference);
        return false;
    }

    // Common methods for subscript lambda pushdown
    /**
     * Extract the sub-expressions of type subscript lambda {@link FunctionCall} from the {@param expression}
     */
    public static Map<Call, Reference> extractSubscriptLambdas(Collection<Expression> expressions)
    {
        List<Map<Expression, Reference>> referencesAndFieldDereferenceLambdas =
                expressions.stream()
                        .map(expression -> getSymbolReferencesAndSubscriptLambdas(expression))
                        .collect(toImmutableList());

        Set<Reference> symbolReferences =
                referencesAndFieldDereferenceLambdas.stream()
                        .flatMap(m -> m.keySet().stream())
                        .filter(Reference.class::isInstance)
                        .map(Reference.class::cast)
                        .collect(Collectors.toSet());

        // Returns the subscript expression and its target input expression
        Map<Call, Reference> subscriptLambdas =
                referencesAndFieldDereferenceLambdas.stream()
                        .flatMap(m -> m.entrySet().stream())
                        .filter(e -> e.getKey() instanceof Call && !symbolReferences.contains(e.getValue()))
                        .collect(Collectors.toMap(e -> (Call) e.getKey(), e -> e.getValue()));

        return subscriptLambdas;
    }

    /**
     * Extract the sub-expressions of type {@link Reference} and subscript lambda {@link FunctionCall} from the {@param expression}
     */
    private static Map<Expression, Reference> getSymbolReferencesAndSubscriptLambdas(Expression expression)
    {
        Map<Expression, Reference> symbolMappings = new HashMap<>();

        new DefaultTraversalVisitor<Map<Expression, Reference>>()
        {
            @Override
            protected Void visitReference(Reference node, Map<Expression, Reference> context)
            {
                context.put(node, node);
                return null;
            }

            @Override
            protected Void visitCall(Call node, Map<Expression, Reference> context)
            {
                Optional<Reference> inputExpression = getSubscriptLambdaInputExpression(node);
                if (inputExpression.isPresent()) {
                    context.put(node, inputExpression.get());
                }

                return null;
            }
        }.process(expression, symbolMappings);

        return symbolMappings;
    }

    /**
     * Extract the sub-expressions of type {@link Reference} from the {@param expression}
     */
    public static List<Reference> getReferences(Expression expression)
    {
        ImmutableList.Builder<Reference> builder = ImmutableList.builder();

        new DefaultTraversalVisitor<ImmutableList.Builder<Reference>>()
        {
            @Override
            protected Void visitReference(Reference node, ImmutableList.Builder<Reference> context)
            {
                context.add(node);
                return null;
            }
        }.process(expression, builder);

        return builder.build();
    }

    /**
     * Common pattern matching util function to look for subscript lambda function
     */
    public static Optional<Reference> getSubscriptLambdaInputExpression(Expression expression)
    {
        if (expression instanceof Call functionCall) {
            CatalogSchemaFunctionName functionName = functionCall.function().name();

            if (functionName.equals(builtinFunctionName(ARRAY_TRANSFORM_NAME))) {
                List<Expression> allNodeArgument = functionCall.arguments();
                // at this point, FieldDereference expression should already been replaced with reference expression,
                // if not, it means its referenced by other expressions. we only care about FieldReference at this moment
                List<Reference> inputExpressions = allNodeArgument.stream()
                        .filter(Reference.class::isInstance)
                        .map(Reference.class::cast)
                        .collect(toImmutableList());
                List<Lambda> lambdaExpressions = allNodeArgument.stream().filter(e -> e instanceof Lambda lambda
                                && lambda.arguments().size() == 1)
                        .map(Lambda.class::cast)
                        .collect(toImmutableList());
                if (inputExpressions.size() == 1 && lambdaExpressions.size() == 1 &&
                        ((lambdaExpressions.get(0).body() instanceof Row row &&
                        row.items().stream().allMatch(FieldReference.class::isInstance)) || (lambdaExpressions.get(0).body() instanceof FieldReference))) {
                    return Optional.of(inputExpressions.get(0));
                }
            }
        }
        return Optional.empty();
    }
}
