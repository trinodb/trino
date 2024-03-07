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

import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.SymbolReference;

import java.util.Locale;
import java.util.Map;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.ResolvedFunction.extractFunctionName;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.sql.ir.IrUtils.isEffectivelyLiteral;
import static java.util.Objects.requireNonNull;

public class RemoveRedundantDateTrunc
        extends ExpressionRewriteRuleSet
{
    public RemoveRedundantDateTrunc(PlannerContext plannerContext, IrTypeAnalyzer typeAnalyzer)
    {
        super((expression, context) -> rewrite(expression, context.getSession(), plannerContext, typeAnalyzer, context.getSymbolAllocator().getTypes()));
    }

    private static Expression rewrite(Expression expression, Session session, PlannerContext plannerContext, IrTypeAnalyzer typeAnalyzer, TypeProvider types)
    {
        requireNonNull(plannerContext, "plannerContext is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        if (expression instanceof SymbolReference) {
            return expression;
        }
        return ExpressionTreeRewriter.rewriteWith(new Visitor(session, plannerContext, typeAnalyzer, types), expression);
    }

    private static class Visitor
            extends io.trino.sql.tree.ExpressionRewriter<Void>
    {
        private final Session session;
        private final PlannerContext plannerContext;
        private final IrTypeAnalyzer typeAnalyzer;
        private final TypeProvider types;

        public Visitor(Session session, PlannerContext plannerContext, IrTypeAnalyzer typeAnalyzer, TypeProvider types)
        {
            this.session = requireNonNull(session, "session is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            CatalogSchemaFunctionName functionName = extractFunctionName(node.getName());
            if (functionName.equals(builtinFunctionName("date_trunc")) && node.getArguments().size() == 2) {
                Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, types, node);
                Expression unitExpression = node.getArguments().get(0);
                Expression argument = node.getArguments().get(1);
                if (expressionTypes.get(NodeRef.of(argument)) == DATE && expressionTypes.get(NodeRef.of(unitExpression)) instanceof VarcharType && isEffectivelyLiteral(plannerContext, session, unitExpression)) {
                    Slice unitValue = (Slice) new IrExpressionInterpreter(unitExpression, plannerContext, session, expressionTypes)
                            .optimize(NoOpSymbolResolver.INSTANCE);
                    if (unitValue != null && "day".equals(unitValue.toStringUtf8().toLowerCase(Locale.ENGLISH))) {
                        // date_trunc(day, a_date) is a no-op
                        return treeRewriter.rewrite(argument, context);
                    }
                }
            }

            return treeRewriter.defaultRewrite(node, context);
        }
    }
}
