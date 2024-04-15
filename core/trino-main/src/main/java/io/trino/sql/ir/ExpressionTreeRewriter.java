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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

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
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (Expression expression : items) {
            builder.add(rewrite(expression, context.get()));
        }
        return builder.build();
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

    private class RewritingVisitor
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

            if (!sameElements(node.elements(), elements)) {
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

            if (!sameElements(node.items(), items)) {
                return new Row(items);
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

            Expression base = rewrite(node.base(), context.get());

            if (base != node.base()) {
                return new FieldReference(base, node.field());
            }

            return node;
        }

        @Override
        public Expression visitComparison(Comparison node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteComparison(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression left = rewrite(node.left(), context.get());
            Expression right = rewrite(node.right(), context.get());

            if (left != node.left() || right != node.right()) {
                return new Comparison(node.operator(), left, right);
            }

            return node;
        }

        @Override
        protected Expression visitBetween(Between node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteBetween(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.value(), context.get());
            Expression min = rewrite(node.min(), context.get());
            Expression max = rewrite(node.max(), context.get());

            if (value != node.value() || min != node.min() || max != node.max()) {
                return new Between(value, min, max);
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
            if (!sameElements(node.terms(), terms)) {
                return new Logical(node.operator(), terms);
            }

            return node;
        }

        @Override
        public Expression visitNot(Not node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteNot(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.value(), context.get());

            if (value != node.value()) {
                return new Not(value);
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

            Expression value = rewrite(node.value(), context.get());

            if (value != node.value()) {
                return new IsNull(value);
            }

            return node;
        }

        @Override
        protected Expression visitNullIf(NullIf node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteNullIf(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression first = rewrite(node.first(), context.get());
            Expression second = rewrite(node.second(), context.get());

            if (first != node.first() || second != node.second()) {
                return new NullIf(first, second);
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

            ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
            for (WhenClause expression : node.whenClauses()) {
                builder.add(rewriteWhenClause(expression, context));
            }

            Expression defaultValue = rewrite(node.defaultValue(), context.get());

            if (node.defaultValue() != defaultValue || !sameElements(node.whenClauses(), builder.build())) {
                return new Case(builder.build(), defaultValue);
            }

            return node;
        }

        @Override
        protected Expression visitSwitch(Switch node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSwitch(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression operand = rewrite(node.operand(), context.get());

            ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
            for (WhenClause expression : node.whenClauses()) {
                builder.add(rewriteWhenClause(expression, context));
            }

            Expression defaultValue = rewrite(node.defaultValue(), context.get());

            if (operand != node.operand() ||
                    node.defaultValue() != defaultValue ||
                    !sameElements(node.whenClauses(), builder.build())) {
                return new Switch(operand, builder.build(), defaultValue);
            }

            return node;
        }

        protected WhenClause rewriteWhenClause(WhenClause node, Context<C> context)
        {
            Expression operand = rewrite(node.getOperand(), context.get());
            Expression result = rewrite(node.getResult(), context.get());

            if (operand != node.getOperand() || result != node.getResult()) {
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

            if (!sameElements(node.operands(), operands)) {
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

            if (!sameElements(node.arguments(), arguments)) {
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

            Expression body = rewrite(node.body(), context.get());
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

            List<Expression> values = node.values().stream()
                    .map(value -> rewrite(value, context.get()))
                    .collect(toImmutableList());
            Expression function = rewrite(node.function(), context.get());

            if (!sameElements(values, node.values()) || (function != node.function())) {
                return new Bind(values, (Lambda) function);
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

            Expression value = rewrite(node.value(), context.get());
            List<Expression> values = node.valueList().stream()
                    .map(entry -> rewrite(entry, context.get()))
                    .collect(toImmutableList());

            if (node.value() != value || !sameElements(values, node.valueList())) {
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

            Expression expression = rewrite(node.expression(), context.get());

            if (node.expression() != expression) {
                return new Cast(expression, node.type(), node.safe());
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

        public C get()
        {
            return context;
        }

        public boolean isDefaultRewrite()
        {
            return defaultRewrite;
        }
    }

    private static <T> boolean sameElements(Optional<T> a, Optional<T> b)
    {
        if (a.isEmpty() && b.isEmpty()) {
            return true;
        }
        if (a.isPresent() != b.isPresent()) {
            return false;
        }

        return a.get() == b.get();
    }

    @SuppressWarnings("ObjectEquality")
    private static <T> boolean sameElements(Iterable<? extends T> a, Iterable<? extends T> b)
    {
        if (Iterables.size(a) != Iterables.size(b)) {
            return false;
        }

        Iterator<? extends T> first = a.iterator();
        Iterator<? extends T> second = b.iterator();

        while (first.hasNext() && second.hasNext()) {
            if (first.next() != second.next()) {
                return false;
            }
        }

        return true;
    }
}
