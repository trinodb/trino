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
package io.trino.sql.planner.sanity;

import com.google.common.collect.ImmutableList.Builder;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ExpressionExtractor;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.tree.DefaultExpressionTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;

import static com.google.common.base.Preconditions.checkArgument;

public final class AllFunctionsResolved
        implements PlanSanityChecker.Checker
{
    private static final Visitor VISITOR = new Visitor();

    @Override
    public void validate(
            PlanNode planNode,
            Session session,
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types,
            WarningCollector warningCollector)
    {
        ExpressionExtractor.forEachExpression(planNode, AllFunctionsResolved::validate);
    }

    private static void validate(Expression expression)
    {
        VISITOR.process(expression, null);
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<Builder<Symbol>>
    {
        @Override
        protected Void visitFunctionCall(FunctionCall node, Builder<Symbol> context)
        {
            checkArgument(ResolvedFunction.isResolved(node.getName()), "Function call has not been resolved: %s", node);
            return super.visitFunctionCall(node, context);
        }
    }
}
