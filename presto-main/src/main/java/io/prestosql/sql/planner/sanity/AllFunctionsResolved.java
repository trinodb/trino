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
package io.prestosql.sql.planner.sanity;

import com.google.common.collect.ImmutableList.Builder;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.sql.planner.ExpressionExtractor;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;

import static com.google.common.base.Preconditions.checkArgument;

public final class AllFunctionsResolved
        implements PlanSanityChecker.Checker
{
    private static final Visitor VISITOR = new Visitor();

    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        ExpressionExtractor.forEachExpression(planNode, AllFunctionsResolved::validate);
    }

    private static void validate(Expression expression)
    {
        VISITOR.process(expression, null);
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<Void, Builder<Symbol>>
    {
        @Override
        protected Void visitFunctionCall(FunctionCall node, Builder<Symbol> context)
        {
            checkArgument(ResolvedFunction.fromQualifiedName(node.getName()).isPresent(), "Function call has not been resolved: %s", node);
            return super.visitFunctionCall(node, context);
        }
    }
}
