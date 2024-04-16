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
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.IrExpressionInterpreter;

import java.util.Locale;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.DateType.DATE;
import static java.util.Objects.requireNonNull;

public class RemoveRedundantDateTrunc
        extends ExpressionRewriteRuleSet
{
    public RemoveRedundantDateTrunc(PlannerContext plannerContext)
    {
        super((expression, context) -> rewrite(expression, context.getSession(), plannerContext));
    }

    private static Expression rewrite(Expression expression, Session session, PlannerContext plannerContext)
    {
        requireNonNull(plannerContext, "plannerContext is null");

        if (expression instanceof Reference) {
            return expression;
        }
        return ExpressionTreeRewriter.rewriteWith(new Visitor(session, plannerContext), expression);
    }

    private static class Visitor
            extends io.trino.sql.ir.ExpressionRewriter<Void>
    {
        private final Session session;
        private final PlannerContext plannerContext;

        public Visitor(Session session, PlannerContext plannerContext)
        {
            this.session = requireNonNull(session, "session is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        }

        @Override
        public Expression rewriteCall(Call node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            CatalogSchemaFunctionName functionName = node.function().name();
            if (functionName.equals(builtinFunctionName("date_trunc")) && node.arguments().size() == 2) {
                Expression unitExpression = node.arguments().get(0);
                Expression argument = node.arguments().get(1);
                if (argument.type() == DATE && unitExpression.type() instanceof VarcharType && unitExpression instanceof Constant) {
                    Slice unitValue = (Slice) new IrExpressionInterpreter(unitExpression, plannerContext, session).evaluate();
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
