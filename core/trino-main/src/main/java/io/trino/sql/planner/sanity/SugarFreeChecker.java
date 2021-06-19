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
import io.trino.metadata.Metadata;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.ExpressionExtractor;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DefaultExpressionTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.TryExpression;

/**
 * Verifies the plan does not contain any "syntactic sugar" from the AST.
 */
public final class SugarFreeChecker
        implements PlanSanityChecker.Checker
{
    private static final Visitor VISITOR = new Visitor();

    @Override
    public void validate(PlanNode planNode,
            Session session,
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types,
            WarningCollector warningCollector)
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
        protected Void visitCurrentCatalog(CurrentCatalog node, Builder<Symbol> context)
        {
            throw createIllegalNodeException(node);
        }

        @Override
        protected Void visitCurrentSchema(CurrentSchema node, Builder<Symbol> context)
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

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Builder<Symbol> context)
        {
            throw new IllegalArgumentException("DereferenceExpression should've been replaced with SubscriptExpression");
        }

        private static IllegalArgumentException createIllegalNodeException(Node node)
        {
            return new IllegalArgumentException(node.getClass().getSimpleName() + " should have been replaced with a function call");
        }
    }
}
