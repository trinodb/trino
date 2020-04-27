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
import io.prestosql.sql.planner.ExpressionExtractor;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.AtTimeZone;
import io.prestosql.sql.tree.CurrentPath;
import io.prestosql.sql.tree.CurrentUser;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Extract;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.TryExpression;

/**
 * Verifies the plan does not contain any "syntactic sugar" from the AST.
 */
public final class SugarFreeChecker
        implements PlanSanityChecker.Checker
{
    private static final Visitor VISITOR = new Visitor();

    @Override
    public void validate(PlanNode planNode, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        ExpressionExtractor.forEachExpression(planNode, SugarFreeChecker::validate);
    }

    private static void validate(Expression expression)
    {
        VISITOR.process(expression, null);
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<Builder<Symbol>>
    {
        @Override
        protected Void visitExtract(Extract node, Builder<Symbol> context)
        {
            throw createIllegalNodeException(node);
        }

        @Override
        protected Void visitTryExpression(TryExpression node, Builder<Symbol> context)
        {
            throw createIllegalNodeException(node);
        }

        @Override
        protected Void visitAtTimeZone(AtTimeZone node, Builder<Symbol> context)
        {
            throw createIllegalNodeException(node);
        }

        @Override
        protected Void visitCurrentPath(CurrentPath node, Builder<Symbol> context)
        {
            throw createIllegalNodeException(node);
        }

        @Override
        protected Void visitCurrentUser(CurrentUser node, Builder<Symbol> context)
        {
            throw createIllegalNodeException(node);
        }

        @Override
        protected Void visitLikePredicate(LikePredicate node, Builder<Symbol> context)
        {
            throw createIllegalNodeException(node);
        }

        @Override
        protected Void visitArrayConstructor(ArrayConstructor node, Builder<Symbol> context)
        {
            throw createIllegalNodeException(node);
        }

        private static IllegalArgumentException createIllegalNodeException(Node node)
        {
            return new IllegalArgumentException(node.getClass().getSimpleName() + " should have been replaced with a function call");
        }
    }
}
