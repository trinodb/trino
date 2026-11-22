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
package io.trino.sql.ir;

import com.google.common.annotations.VisibleForTesting;

import java.util.List;

import static io.trino.sql.ir.IrUtils.rewriteItems;

public final class ExpressionTreeRewriter<C>
{
    private final ExpressionRewriter<C> rewriter;
    private final IrVisitor<Expression, Context<C>> visitor;

    public static <T extends Expression> T rewriteWith(ExpressionRewriter<Void> rewriter, T node)
    {
        return new ExpressionTreeRewriter<>(rewriter).rewrite(node, null);
    }

    public static <C, T extends Expression> T rewriteWith(ExpressionRewriter<C> rewriter, T node, C context)
    {
        return new ExpressionTreeRewriter<>(rewriter).rewrite(node, context);
    }

    public ExpressionTreeRewriter(ExpressionRewriter<C> rewriter)
    {
        this.rewriter = rewriter;
        this.visitor = new RewritingVisitor();
    }

    private List<Expression> rewrite(List<Expression> items, Context<C> context)
    {
        Context<C> childContext = context.forChildren();
        return rewriteItems(items, item -> visitor.process(item, childContext));
    }

    private Expression rewriteChild(Expression node, Context<C> context)
    {
        return visitor.process(node, context.forChildren());
    }

    @SuppressWarnings("unchecked")
    public <T extends Expression> T rewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, false));
    }

    /**
     * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the expression rewriter for the provided node.
     */
    @SuppressWarnings("unchecked")
    public <T extends Expression> T defaultRewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, true));
    }

    @VisibleForTesting
    class RewritingVisitor
            extends IrVisitor<Expression, Context<C>>
    {
        @Override
        protected Expression visitExpression(Expression node, Context<C> context)
        {
            // RewritingVisitor must have explicit support for each expression type, with a dedicated visit method,
            // so visitExpression() should never be called.
            throw new UnsupportedOperationException("visit() not implemented for " + node.getClass().getName());
        }

        @Override
        protected Expression visitArray(Array node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteArray(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> elements = rewrite(node.elements(), context);

            if (elements != node.elements()) {
                return new Array(node.elementType(), elements);
            }

            return node;
        }

        @Override
        protected Expression visitRow(Row node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteRow(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> items = rewrite(node.items(), context);

            if (items != node.items()) {
                return new Row(items, node.type());
            }

            return node;
        }

        @Override
        protected Expression visitFieldReference(FieldReference node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSubscript(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression base = rewriteChild(node.base(), context);

            if (base != node.base()) {
                return new FieldReference(base, node.field());
            }

            return node;
        }

        @Override
        public Expression visitLogical(Logical node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLogical(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> terms = rewrite(node.terms(), context);
            if (terms != node.terms()) {
                return new Logical(node.operator(), terms);
            }

            return node;
        }

        @Override
        protected Expression visitIsNull(IsNull node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIsNull(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewriteChild(node.value(), context);

            if (value != node.value()) {
                return new IsNull(value);
            }

            return node;
        }

        @Override
        protected Expression visitCase(Case node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCase(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Context<C> childContext = context.forChildren();
            List<WhenClause> whenClauses = rewriteItems(node.whenClauses(), clause -> rewriteWhenClause(clause, childContext));
            Expression defaultValue = rewriteChild(node.defaultValue(), context);

            if (whenClauses != node.whenClauses() || node.defaultValue() != defaultValue) {
                return new Case(whenClauses, defaultValue);
            }

            return node;
        }

        @Override
        protected Expression visitMatch(Match node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteMatch(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression operand = rewriteChild(node.operand(), context);

            Context<C> childContext = context.forChildren();
            List<MatchClause> clauses = rewriteItems(node.clauses(), clause -> rewriteMatchClause(clause, childContext));
            Expression defaultValue = rewriteChild(node.defaultValue(), context);

            if (operand != node.operand() || clauses != node.clauses() || node.defaultValue() != defaultValue) {
                return new Match(operand, clauses, defaultValue);
            }

            return node;
        }

        private MatchClause rewriteMatchClause(MatchClause clause, Context<C> context)
        {
            Expression predicate = rewriteChild(clause.predicate(), context);
            Expression result = rewriteChild(clause.result(), context);
            if (predicate == clause.predicate() && result == clause.result()) {
                return clause;
            }
            return new MatchClause(predicate, result);
        }

        protected WhenClause rewriteWhenClause(WhenClause node, Context<C> context)
        {
            Expression operand = rewriteChild(node.operand(), context);
            Expression result = rewriteChild(node.result(), context);

            if (operand != node.operand() || result != node.result()) {
                return new WhenClause(operand, result);
            }
            return node;
        }

        @Override
        protected Expression visitCoalesce(Coalesce node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCoalesce(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> operands = rewrite(node.operands(), context);

            if (operands != node.operands()) {
                return new Coalesce(operands);
            }

            return node;
        }

        @Override
        public Expression visitCall(Call node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCall(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> arguments = rewrite(node.arguments(), context);

            if (arguments != node.arguments()) {
                return new Call(node.function(), arguments);
            }
            return node;
        }

        @Override
        protected Expression visitLambda(Lambda node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLambda(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression body = rewriteChild(node.body(), context);
            if (body != node.body()) {
                return new Lambda(node.arguments(), body);
            }

            return node;
        }

        @Override
        protected Expression visitBind(Bind node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteBind(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> values = rewrite(node.values(), context);
            Expression function = rewriteChild(node.function(), context);

            if (values != node.values() || (function != node.function())) {
                return new Bind(values, (Lambda) function);
            }
            return node;
        }

        @Override
        protected Expression visitLet(Let node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLet(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewriteChild(node.value(), context);
            Expression body = rewriteChild(node.body(), context);

            if (value != node.value() || body != node.body()) {
                return new Let(node.name(), value, body);
            }
            return node;
        }

        @Override
        public Expression visitIn(In node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIn(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewriteChild(node.value(), context);
            List<Expression> values = rewrite(node.valueList(), context);

            if (node.value() != value || values != node.valueList()) {
                return new In(value, values);
            }

            return node;
        }

        @Override
        public Expression visitConstant(Constant node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteConstant(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public Expression visitCast(Cast node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCast(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression expression = rewriteChild(node.expression(), context);

            if (node.expression() != expression) {
                return new Cast(expression, node.type(), node.kind());
            }

            return node;
        }

        @Override
        protected Expression visitReference(Reference node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteReference(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }
    }

    public static class Context<C>
    {
        private final boolean defaultRewrite;
        private final C context;

        private Context(C context, boolean defaultRewrite)
        {
            this.context = context;
            this.defaultRewrite = defaultRewrite;
        }

        public Context<C> forChildren()
        {
            if (defaultRewrite) {
                return new Context<>(context, false);
            }
            return this;
        }

        public C get()
        {
            return context;
        }

        public boolean isDefaultRewrite()
        {
            return defaultRewrite;
        }
    }
}
