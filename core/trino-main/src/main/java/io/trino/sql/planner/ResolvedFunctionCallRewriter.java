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

import io.trino.metadata.ResolvedFunction;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.NodeRef;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ResolvedFunctionCallRewriter
{
    private ResolvedFunctionCallRewriter() {}

    public static Expression rewriteResolvedFunctions(Expression expression, Map<NodeRef<Expression>, ResolvedFunction> resolvedFunctions)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(resolvedFunctions), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private final Map<NodeRef<Expression>, ResolvedFunction> resolvedFunctions;

        public Visitor(Map<NodeRef<Expression>, ResolvedFunction> resolvedFunctions)
        {
            this.resolvedFunctions = requireNonNull(resolvedFunctions, "resolvedFunctions is null");
        }

        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            ResolvedFunction resolvedFunction = resolvedFunctions.get(NodeRef.of(node));
            checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

            FunctionCall rewritten = treeRewriter.defaultRewrite(node, context);
            return new FunctionCall(
                    rewritten.getLocation(),
                    resolvedFunction.toQualifiedName(),
                    rewritten.getWindow(),
                    rewritten.getFilter(),
                    rewritten.getOrderBy(),
                    rewritten.isDistinct(),
                    rewritten.getNullTreatment(),
                    rewritten.getProcessingMode(),
                    rewritten.getArguments());
        }
    }
}
