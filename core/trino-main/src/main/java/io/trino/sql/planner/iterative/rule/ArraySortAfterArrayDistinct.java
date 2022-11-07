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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.ArrayDistinctFunction;
import io.trino.operator.scalar.ArraySortFunction;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;

public class ArraySortAfterArrayDistinct
        extends ExpressionRewriteRuleSet
{
    public ArraySortAfterArrayDistinct(PlannerContext plannerContext)
    {
        super((expression, context) -> rewrite(expression, context, plannerContext.getMetadata()));
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectExpressionRewrite(),
                filterExpressionRewrite(),
                joinExpressionRewrite(),
                valuesExpressionRewrite(),
                patternRecognitionExpressionRewrite());
    }

    private static Expression rewrite(Expression expression, Rule.Context context, Metadata metadata)
    {
        if (expression instanceof SymbolReference) {
            return expression;
        }
        Session session = context.getSession();
        return ExpressionTreeRewriter.rewriteWith(new Visitor(metadata, session), expression);
    }

    private static class Visitor
            extends io.trino.sql.tree.ExpressionRewriter<Void>
    {
        private final Metadata metadata;
        private final Session session;

        public Visitor(Metadata metadata, Session session)
        {
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            FunctionCall rewritten = treeRewriter.defaultRewrite(node, context);
            if (metadata.decodeFunction(rewritten.getName()).getSignature().getName().equals(ArrayDistinctFunction.NAME) &&
                    getOnlyElement(rewritten.getArguments()) instanceof FunctionCall) {
                Expression expression = getOnlyElement(rewritten.getArguments());
                FunctionCall functionCall = (FunctionCall) expression;
                ResolvedFunction resolvedFunction = metadata.decodeFunction(functionCall.getName());
                if (resolvedFunction.getSignature().getName().equals(ArraySortFunction.NAME)) {
                    List<Expression> arraySortArguments = functionCall.getArguments();
                    List<Type> arraySortArgumentsTypes = resolvedFunction.getSignature().getArgumentTypes();

                    FunctionCall arrayDistinctCall = FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of(ArrayDistinctFunction.NAME))
                            .setArguments(
                                    ImmutableList.of(arraySortArgumentsTypes.get(0)),
                                    ImmutableList.of(arraySortArguments.get(0)))
                            .build();

                    FunctionCallBuilder arraySortCallBuilder = FunctionCallBuilder.resolve(session, metadata)
                            .setName(QualifiedName.of(ArraySortFunction.NAME))
                            .addArgument(arraySortArgumentsTypes.get(0), arrayDistinctCall);

                    if (arraySortArguments.size() == 2) {
                        arraySortCallBuilder.addArgument(arraySortArgumentsTypes.get(1), arraySortArguments.get(1));
                    }
                    return arraySortCallBuilder.build();
                }
            }
            return rewritten;
        }
    }
}
