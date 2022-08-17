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
import io.trino.sql.tree.AstVisitor;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class AstToIrExpressionTreeRewriter<C>
{
    private final AstToIrExpressionRewriter<C> rewriter;

    // some Expression will take an argument that is a subclass of Node instead of Expression, we need another rewriter that can copy Node subclass.
    private final AstToIrNodeCopier copier;

    private final AstVisitor<Expression, Context<C>> visitor;

    public static <I extends Expression, A extends io.trino.sql.tree.Expression> I rewriteWith(AstToIrExpressionRewriter<Void> rewriter, A node)
    {
        return new AstToIrExpressionTreeRewriter<>(rewriter).rewrite(node, null);
    }

    public static <C, I extends Expression, A extends io.trino.sql.tree.Expression> I rewriteWith(AstToIrExpressionRewriter<C> rewriter, A node, C context)
    {
        return new AstToIrExpressionTreeRewriter<>(rewriter).rewrite(node, context);
    }

    public static <I extends Node, A extends io.trino.sql.tree.Node> I copyAstNodeToIrNode(A node)
    {
        return (I) new AstToIrExpressionTreeRewriter<>(null).copier.process(node, null);
    }

    public AstToIrExpressionTreeRewriter(AstToIrExpressionRewriter<C> rewriter)
    {
        this.rewriter = rewriter;
        this.copier = new AstToIrNodeCopier();
        this.visitor = new RewritingVisitor();
    }

    private List<Expression> rewrite(List<io.trino.sql.tree.Expression> items, Context<C> context)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (io.trino.sql.tree.Expression expression : items) {
            builder.add(rewrite(expression, context.get()));
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    public <I extends Expression, A extends io.trino.sql.tree.Expression> I rewrite(A node, C context)
    {
        return (I) visitor.process(node, new Context<>(context, false));
    }

    @SuppressWarnings("unchecked")
    public <I extends Expression, A extends io.trino.sql.tree.Expression> List<I> defaultRewrite(List<A> items, C context)
    {
        ImmutableList.Builder<I> builder = ImmutableList.builder();
        for (io.trino.sql.tree.Expression expression : items) {
            builder.add(defaultRewrite(expression, context));
        }
        return builder.build();
    }

    /**
     * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the expression rewriter for the provided node.
     */
    @SuppressWarnings("unchecked")
    public <I extends Expression, A extends io.trino.sql.tree.Expression> I defaultRewrite(A node, C context)
    {
        return (I) visitor.process(node, new Context<>(context, true));
    }

    @SuppressWarnings("unchecked")
    public <I extends Expression, A extends io.trino.sql.tree.Expression> Optional<I> defaultRewrite(Optional<A> optionalNode, C context)
    {
        return optionalNode.map((node -> (I) visitor.process(node, new Context<>(context, true))));
    }

    @SuppressWarnings("unchecked")
    public <I extends Node, A extends io.trino.sql.tree.Node> I copy(A node, Void v)
    {
        return (I) copier.process(node, v);
    }

    @SuppressWarnings("unchecked")
    public <I extends Node, A extends io.trino.sql.tree.Node> Optional<I> copy(Optional<A> optionalNode, Void v)
    {
        return optionalNode.map(node -> copy(node, v));
    }

    public <I extends Node, A extends io.trino.sql.tree.Node> List<I> copy(List<A> items, Void v)
    {
        ImmutableList.Builder<I> builder = ImmutableList.builder();
        for (io.trino.sql.tree.Node node : items) {
            builder.add(copy(node, v));
        }
        return builder.build();
    }

    private class RewritingVisitor
            extends AstVisitor<Expression, AstToIrExpressionTreeRewriter.Context<C>>
    {
        @Override
        protected Expression visitExpression(io.trino.sql.tree.Expression node, Context<C> context)
        {
            // RewritingVisitor must have explicit support for each expression type, with a dedicated visit method,
            // so visitExpression() should never be called.
            throw new UnsupportedOperationException("visit() not implemented for " + node.getClass().getName());
        }

        @Override
        protected Expression visitRow(io.trino.sql.tree.Row node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteRow(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> items = rewrite(node.getItems(), context);
            return new Row(items);
        }

        @Override
        protected Expression visitArithmeticUnary(io.trino.sql.tree.ArithmeticUnaryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteArithmeticUnary(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression child = rewrite(node.getValue(), context.get());

            return new ArithmeticUnaryExpression(node.getSign(), child);
        }

        @Override
        public Expression visitArithmeticBinary(io.trino.sql.tree.ArithmeticBinaryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteArithmeticBinary(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression left = rewrite(node.getLeft(), context.get());
            Expression right = rewrite(node.getRight(), context.get());
            return new ArithmeticBinaryExpression(node.getOperator(), left, right);
        }

        @Override
        protected Expression visitArrayConstructor(io.trino.sql.tree.ArrayConstructor node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteArrayConstructor(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> values = rewrite(node.getValues(), context);
            return new ArrayConstructor(values);
        }

        @Override
        protected Expression visitAtTimeZone(io.trino.sql.tree.AtTimeZone node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteAtTimeZone(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression timeZone = rewrite(node.getTimeZone(), context.get());
            return new AtTimeZone(value, timeZone);
        }

        @Override
        protected Expression visitSubscriptExpression(io.trino.sql.tree.SubscriptExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSubscriptExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression base = rewrite(node.getBase(), context.get());
            Expression index = rewrite(node.getIndex(), context.get());

            return new SubscriptExpression(base, index);
        }

        @Override
        public Expression visitComparisonExpression(io.trino.sql.tree.ComparisonExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteComparisonExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression left = rewrite(node.getLeft(), context.get());
            Expression right = rewrite(node.getRight(), context.get());

            return new ComparisonExpression(node.getOperator(), left, right);
        }

        @Override
        protected Expression visitBetweenPredicate(io.trino.sql.tree.BetweenPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteBetweenPredicate(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression min = rewrite(node.getMin(), context.get());
            Expression max = rewrite(node.getMax(), context.get());

            return new BetweenPredicate(value, min, max);
        }

        @Override
        public Expression visitLogicalExpression(io.trino.sql.tree.LogicalExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLogicalExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> terms = rewrite(node.getTerms(), context);

            return new LogicalExpression(node.getOperator(), terms);
        }

        @Override
        public Expression visitNotExpression(io.trino.sql.tree.NotExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteNotExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());

            return new NotExpression(value);
        }

        @Override
        protected Expression visitIsNullPredicate(io.trino.sql.tree.IsNullPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIsNullPredicate(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());

            return new IsNullPredicate(value);
        }

        @Override
        protected Expression visitIsNotNullPredicate(io.trino.sql.tree.IsNotNullPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIsNotNullPredicate(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());

            return new IsNotNullPredicate(value);
        }

        @Override
        protected Expression visitNullIfExpression(io.trino.sql.tree.NullIfExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteNullIfExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression first = rewrite(node.getFirst(), context.get());
            Expression second = rewrite(node.getSecond(), context.get());

            return new NullIfExpression(first, second);
        }

        @Override
        protected Expression visitIfExpression(io.trino.sql.tree.IfExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIfExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression condition = rewrite(node.getCondition(), context.get());
            Expression trueValue = rewrite(node.getTrueValue(), context.get());
            Expression falseValue = null;
            if (node.getFalseValue().isPresent()) {
                falseValue = rewrite(node.getFalseValue().get(), context.get());
            }

            return new IfExpression(condition, trueValue, falseValue);
        }

        @Override
        protected Expression visitSearchedCaseExpression(io.trino.sql.tree.SearchedCaseExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSearchedCaseExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
            for (io.trino.sql.tree.WhenClause expression : node.getWhenClauses()) {
                builder.add(rewrite(expression, context.get()));
            }

            Optional<Expression> defaultValue = node.getDefaultValue()
                    .map(value -> rewrite(value, context.get()));

            return new SearchedCaseExpression(builder.build(), defaultValue);
        }

        @Override
        protected Expression visitSimpleCaseExpression(io.trino.sql.tree.SimpleCaseExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSimpleCaseExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression operand = rewrite(node.getOperand(), context.get());

            ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
            for (io.trino.sql.tree.WhenClause expression : node.getWhenClauses()) {
                builder.add(rewrite(expression, context.get()));
            }

            Optional<Expression> defaultValue = node.getDefaultValue()
                    .map(value -> rewrite(value, context.get()));

            return new SimpleCaseExpression(operand, builder.build(), defaultValue);
        }

        @Override
        protected Expression visitWhenClause(io.trino.sql.tree.WhenClause node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteWhenClause(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression operand = rewrite(node.getOperand(), context.get());
            Expression result = rewrite(node.getResult(), context.get());

            return new WhenClause(operand, result);
        }

        @Override
        protected Expression visitCoalesceExpression(io.trino.sql.tree.CoalesceExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCoalesceExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> operands = rewrite(node.getOperands(), context);

            return new CoalesceExpression(operands);
        }

        @Override
        public Expression visitTryExpression(io.trino.sql.tree.TryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteTryExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression expression = rewrite(node.getInnerExpression(), context.get());

            return new TryExpression(expression);
        }

        @Override
        public Expression visitFunctionCall(io.trino.sql.tree.FunctionCall node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteFunctionCall(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Optional<io.trino.sql.tree.Expression> filter = node.getFilter();
            Optional<Expression> rewrittenFilter = Optional.empty();
            if (filter.isPresent()) {
                io.trino.sql.tree.Expression filterExpression = filter.get();
                Expression newFilterExpression = rewrite(filterExpression, context.get());
                rewrittenFilter = Optional.of(newFilterExpression);
            }

            Optional<io.trino.sql.tree.Window> window = node.getWindow();
            Optional<Window> rewrittenWindow = Optional.empty();
            if (window.isPresent()) {
                rewrittenWindow = Optional.of(rewriteWindow(window.get(), context));
            }

            List<Expression> arguments = rewrite(node.getArguments(), context);

            QualifiedName rewrittenQualifiedName = QualifiedName.of(node.getName().getOriginalParts().stream().map(id -> new Identifier(id.getValue(), id.isDelimited())).collect(toImmutableList()));

            return new FunctionCall(
                    rewrittenQualifiedName,
                    rewrittenWindow,
                    rewrittenFilter,
                    node.getOrderBy().map(orderBy -> rewriteOrderBy(orderBy, context)),
                    node.isDistinct(),
                    node.getNullTreatment(),
                    node.getProcessingMode().map(a -> new ProcessingMode(a.getMode())),
                    arguments);
        }

        // Since OrderBy contains list of SortItems, we want to process each SortItem's key, which is an expression
        private OrderBy rewriteOrderBy(io.trino.sql.tree.OrderBy orderBy, Context<C> context)
        {
            List<SortItem> rewrittenSortItems = rewriteSortItems(orderBy.getSortItems(), context);
            return new OrderBy(rewrittenSortItems);
        }

        private List<SortItem> rewriteSortItems(List<io.trino.sql.tree.SortItem> sortItems, Context<C> context)
        {
            ImmutableList.Builder<SortItem> rewrittenSortItems = ImmutableList.builder();
            for (io.trino.sql.tree.SortItem sortItem : sortItems) {
                Expression sortKey = rewrite(sortItem.getSortKey(), context.get());
                rewrittenSortItems.add(new SortItem(sortKey, sortItem.getOrdering(), sortItem.getNullOrdering()));
            }
            return rewrittenSortItems.build();
        }

        private Window rewriteWindow(io.trino.sql.tree.Window window, Context<C> context)
        {
            if (window instanceof io.trino.sql.tree.WindowReference) {
                io.trino.sql.tree.WindowReference windowReference = (io.trino.sql.tree.WindowReference) window;
                Identifier rewrittenName = rewrite(windowReference.getName(), context.get());
                return new WindowReference(rewrittenName);
            }

            io.trino.sql.tree.WindowSpecification windowSpecification = (io.trino.sql.tree.WindowSpecification) window;
            Optional<Identifier> existingWindowName = windowSpecification.getExistingWindowName().map(name -> rewrite(name, context.get()));

            List<Expression> partitionBy = rewrite(windowSpecification.getPartitionBy(), context);

            Optional<OrderBy> orderBy = Optional.empty();
            if (windowSpecification.getOrderBy().isPresent()) {
                orderBy = Optional.of(rewriteOrderBy(windowSpecification.getOrderBy().get(), context));
            }

            Optional<io.trino.sql.tree.WindowFrame> windowFrame = windowSpecification.getFrame();
            Optional<WindowFrame> rewrittenWindowFrame = Optional.empty();
            if (windowFrame.isPresent()) {
                io.trino.sql.tree.WindowFrame frame = windowFrame.get();

                io.trino.sql.tree.FrameBound start = frame.getStart();
                FrameBound rewrittenStart;
                if (start.getValue().isPresent()) {
                    Expression value = rewrite(start.getValue().get(), context.get());
                    rewrittenStart = new FrameBound(start.getType(), value);
                }
                else {
                    rewrittenStart = new FrameBound(start.getType());
                }

                Optional<io.trino.sql.tree.FrameBound> end = frame.getEnd();
                Optional<FrameBound> rewrittenEnd = Optional.empty();
                if (end.isPresent()) {
                    Optional<io.trino.sql.tree.Expression> value = end.get().getValue();
                    if (value.isPresent()) {
                        Expression rewrittenValue = rewrite(value.get(), context.get());
                        rewrittenEnd = Optional.of(new FrameBound(end.get().getType(), rewrittenValue));
                    }
                }

                // Frame properties for row pattern matching are not rewritten. They are planned as parts of
                // PatternRecognitionNode, and shouldn't be accessed past the Planner phase.
                // There are nested expressions in Measures and VariableDefinitions. They are not rewritten by default.
                // Rewriting them requires special handling of DereferenceExpression, aware of pattern labels.
                if (!frame.getMeasures().isEmpty() ||
                        frame.getAfterMatchSkipTo().isPresent() ||
                        frame.getPatternSearchMode().isPresent() ||
                        frame.getPattern().isPresent() ||
                        !frame.getSubsets().isEmpty() ||
                        !frame.getVariableDefinitions().isEmpty()) {
                    throw new UnsupportedOperationException("cannot rewrite pattern recognition clauses in window");
                }

                rewrittenWindowFrame = Optional.of(new WindowFrame(
                        frame.getType(),
                        rewrittenStart,
                        rewrittenEnd,
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(),
                        ImmutableList.of()));
            }
            return new WindowSpecification(existingWindowName, partitionBy, orderBy, rewrittenWindowFrame);
        }

        @Override
        protected Expression visitWindowOperation(io.trino.sql.tree.WindowOperation node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteWindowOperation(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Identifier name = rewrite(node.getName(), context.get());
            Window window = rewriteWindow(node.getWindow(), context);
            return new WindowOperation(name, window);
        }

        @Override
        protected Expression visitLambdaExpression(io.trino.sql.tree.LambdaExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLambdaExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression body = rewrite(node.getBody(), context.get());
            List<Expression> argumentDeclaration = defaultRewrite(node.getArguments(), context.get());
            checkState(argumentDeclaration.stream().allMatch(arg -> arg instanceof LambdaArgumentDeclaration));
            return new LambdaExpression(argumentDeclaration.stream().map(arg -> (LambdaArgumentDeclaration) arg).collect(toImmutableList()), body);
        }

        @Override
        protected Expression visitLambdaArgumentDeclaration(io.trino.sql.tree.LambdaArgumentDeclaration node, Context<C> context)
        {
            Identifier name = defaultRewrite(node.getName(), context.get());
            return new LambdaArgumentDeclaration(name);
        }

        @Override
        protected Expression visitBindExpression(io.trino.sql.tree.BindExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteBindExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> values = rewrite(node.getValues(), context);
            Expression function = rewrite(node.getFunction(), context.get());

            return new BindExpression(values, function);
        }

        @Override
        public Expression visitLikePredicate(io.trino.sql.tree.LikePredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLikePredicate(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression pattern = rewrite(node.getPattern(), context.get());
            Optional<Expression> rewrittenEscape = node.getEscape()
                    .map(escape -> rewrite(escape, context.get()));

            return new LikePredicate(value, pattern, rewrittenEscape);
        }

        @Override
        public Expression visitInPredicate(io.trino.sql.tree.InPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteInPredicate(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression list = rewrite(node.getValueList(), context.get());

            return new InPredicate(value, list);
        }

        @Override
        protected Expression visitInListExpression(io.trino.sql.tree.InListExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteInListExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> values = rewrite(node.getValues(), context);

            return new InListExpression(values);
        }

        @Override
        protected Expression visitExists(io.trino.sql.tree.ExistsPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteExists(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new ExistsPredicate(rewrite(node.getSubquery(), context.get()));
        }

        @Override
        public Expression visitSubqueryExpression(io.trino.sql.tree.SubqueryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSubqueryExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return new SubqueryExpression(node.getQuery());
        }

        @Override
        public Expression visitLiteral(io.trino.sql.tree.Literal node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            // should not reach here because it should get dispatched according to its concrete type
            throw new IllegalStateException();
        }

        @Override
        public Expression visitBinaryLiteral(io.trino.sql.tree.BinaryLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteBinaryLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new BinaryLiteral(node.toHexString());
        }

        @Override
        public Expression visitBooleanLiteral(io.trino.sql.tree.BooleanLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteBooleanLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return BooleanLiteral.of(Boolean.toString(node.getValue()));
        }

        @Override
        public Expression visitCharLiteral(io.trino.sql.tree.CharLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCharLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return CharLiteral.of(node.getValue());
        }

        @Override
        public Expression visitDecimalLiteral(io.trino.sql.tree.DecimalLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteDecimalLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new DecimalLiteral(node.getValue());
        }

        @Override
        public Expression visitDoubleLiteral(io.trino.sql.tree.DoubleLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteDoubleLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new DoubleLiteral(Double.toString(node.getValue()));
        }

        @Override
        public Expression visitGenericLiteral(io.trino.sql.tree.GenericLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteGenericLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new GenericLiteral(node.getType(), node.getValue());
        }

        @Override
        public Expression visitIntervalLiteral(io.trino.sql.tree.IntervalLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIntervalLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new IntervalLiteral(node.getValue(), node.getSign(), node.getStartField(), node.getEndField());
        }

        @Override
        public Expression visitLongLiteral(io.trino.sql.tree.LongLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLongLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new LongLiteral(String.valueOf(node.getValue()));
        }

        @Override
        public Expression visitNullLiteral(io.trino.sql.tree.NullLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteNullLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new NullLiteral();
        }

        @Override
        public Expression visitStringLiteral(io.trino.sql.tree.StringLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteStringLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new StringLiteral(node.getValue());
        }

        @Override
        public Expression visitTimeLiteral(io.trino.sql.tree.TimeLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteTimeLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new TimeLiteral(node.getValue());
        }

        @Override
        public Expression visitTimestampLiteral(io.trino.sql.tree.TimestampLiteral node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteTimestampLiteral(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new TimestampLiteral(node.getValue());
        }

        @Override
        public Expression visitParameter(io.trino.sql.tree.Parameter node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteParameter(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new Parameter(node.getPosition());
        }

        @Override
        public Expression visitIdentifier(io.trino.sql.tree.Identifier node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIdentifier(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new Identifier(node.getValue(), node.isDelimited());
        }

        @Override
        public Expression visitDereferenceExpression(io.trino.sql.tree.DereferenceExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteDereferenceExpression(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression base = rewrite(node.getBase(), context.get());
            if (node.getField().isPresent()) {
                return new DereferenceExpression(base, defaultRewrite(node.getField().get(), context.get()));
            }
            return new DereferenceExpression((Identifier) base);
        }

        @Override
        protected Expression visitExtract(io.trino.sql.tree.Extract node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteExtract(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression expression = rewrite(node.getExpression(), context.get());
            return new Extract(expression, node.getField());
        }

        @Override
        protected Expression visitCurrentTime(io.trino.sql.tree.CurrentTime node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCurrentTime(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new CurrentTime(node.getFunction(), node.getPrecision());
        }

        @Override
        public Expression visitCast(io.trino.sql.tree.Cast node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCast(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression expression = rewrite(node.getExpression(), context.get());
            DataType type = rewrite(node.getType(), context.get());

            return new Cast(expression, type, node.isSafe(), node.isTypeOnly());
        }

        @Override
        protected Expression visitRowDataType(io.trino.sql.tree.RowDataType node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteRowDataType(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<RowDataType.Field> rewritten = ImmutableList.builder();
            for (io.trino.sql.tree.RowDataType.Field field : node.getFields()) {
                DataType dataType = rewrite(field.getType(), context.get());

                Optional<io.trino.sql.tree.Identifier> name = field.getName();
                Optional<Identifier> rewrittenName = Optional.empty();

                if (field.getName().isPresent()) {
                    io.trino.sql.tree.Identifier identifier = field.getName().get();
                    Identifier rewrittenIdentifier = rewrite(identifier, context.get());
                    rewrittenName = Optional.of(rewrittenIdentifier);
                }
                rewritten.add(new RowDataType.Field(rewrittenName, dataType));
            }

            List<RowDataType.Field> fields = rewritten.build();

            return new RowDataType(fields);
        }

        @Override
        protected Expression visitGenericDataType(io.trino.sql.tree.GenericDataType node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteGenericDataType(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Identifier name = rewrite(node.getName(), context.get());

            ImmutableList.Builder<DataTypeParameter> arguments = ImmutableList.builder();
            for (io.trino.sql.tree.DataTypeParameter argument : node.getArguments()) {
                if (argument instanceof io.trino.sql.tree.NumericParameter) {
                    arguments.add(copy(argument, null));
                }
                else if (argument instanceof io.trino.sql.tree.TypeParameter) {
                    io.trino.sql.tree.TypeParameter parameter = (io.trino.sql.tree.TypeParameter) argument;
                    DataType value = (DataType) process(parameter.getValue(), context);
                    arguments.add(new TypeParameter(value));
                }
            }

            List<DataTypeParameter> rewrittenArguments = arguments.build();
            return new GenericDataType(name, rewrittenArguments);
        }

        @Override
        protected Expression visitIntervalDataType(io.trino.sql.tree.IntervalDayTimeDataType node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIntervalDayTimeDataType(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new IntervalDayTimeDataType(node.getFrom(), node.getTo());
        }

        @Override
        protected Expression visitDateTimeType(io.trino.sql.tree.DateTimeDataType node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteDateTimeDataType(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            return new DateTimeDataType(node.getType(), node.isWithTimeZone(), node.getPrecision().map(precision -> copy(precision, null)));
        }

        @Override
        protected Expression visitFieldReference(io.trino.sql.tree.FieldReference node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteFieldReference(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new FieldReference(node.getFieldIndex());
        }

        @Override
        protected Expression visitSymbolReference(io.trino.sql.tree.SymbolReference node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSymbolReference(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new SymbolReference(node.getName());
        }

        @Override
        protected Expression visitQuantifiedComparisonExpression(io.trino.sql.tree.QuantifiedComparisonExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteQuantifiedComparison(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression subquery = rewrite(node.getSubquery(), context.get());

            return new QuantifiedComparisonExpression(node.getOperator(), node.getQuantifier(), value, subquery);
        }

        @Override
        public Expression visitGroupingOperation(io.trino.sql.tree.GroupingOperation node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteGroupingOperation(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }
            List<Expression> groupingColumnExpressions = defaultRewrite(node.getGroupingColumns(), context.get());
            return new GroupingOperation(groupingColumnExpressions);
        }

        @Override
        protected Expression visitCurrentCatalog(io.trino.sql.tree.CurrentCatalog node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCurrentCatalog(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new CurrentCatalog();
        }

        @Override
        protected Expression visitCurrentSchema(io.trino.sql.tree.CurrentSchema node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCurrentSchema(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new CurrentSchema();
        }

        @Override
        protected Expression visitCurrentUser(io.trino.sql.tree.CurrentUser node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCurrentUser(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new CurrentUser();
        }

        @Override
        protected Expression visitCurrentPath(io.trino.sql.tree.CurrentPath node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCurrentPath(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return new CurrentPath();
        }

        @Override
        protected Expression visitTrim(io.trino.sql.tree.Trim node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteTrim(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
            expressions.add(defaultRewrite(node.getTrimSource(), context.get()));
            node.getTrimCharacter().ifPresent(trimCharacter -> expressions.add(defaultRewrite(node.getTrimSource(), context.get())));

            Expression trimSource = rewrite(node.getTrimSource(), context.get());
            Optional<Expression> trimChar = node.getTrimCharacter().isPresent() ? Optional.of(rewrite(node.getTrimCharacter().get(), context.get())) : Optional.empty();

            return new Trim(node.getSpecification(), trimSource, trimChar);
        }

        @Override
        protected Expression visitFormat(io.trino.sql.tree.Format node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteFormat(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> arguments = rewrite(node.getArguments(), context);
            return new Format(arguments);
        }

        @Override
        protected Expression visitLabelDereference(io.trino.sql.tree.LabelDereference node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLabelDereference(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            if (node.getReference().isPresent()) {
                SymbolReference reference = rewrite(node.getReference().get(), context.get());
                return new LabelDereference(node.getLabel(), reference);
            }

            return new LabelDereference(node.getLabel());
        }

        @Override
        protected Expression visitJsonExists(io.trino.sql.tree.JsonExists node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteJsonExists(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            JsonPathInvocation jsonPathInvocation = rewriteJsonPathInvocation(node.getJsonPathInvocation(), context);

            return new JsonExists(jsonPathInvocation, node.getErrorBehavior());
        }

        @Override
        protected Expression visitJsonValue(io.trino.sql.tree.JsonValue node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteJsonValue(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            JsonPathInvocation jsonPathInvocation = rewriteJsonPathInvocation(node.getJsonPathInvocation(), context);

            Optional<Expression> emptyDefault = node.getEmptyDefault().map(expression -> rewrite(expression, context.get()));
            Optional<Expression> errorDefault = node.getErrorDefault().map(expression -> rewrite(expression, context.get()));

            return new JsonValue(
                        jsonPathInvocation,
                        node.getReturnedType().map(returnedType -> defaultRewrite(returnedType, context.get())),
                        node.getEmptyBehavior(),
                        emptyDefault,
                        node.getErrorBehavior(),
                        errorDefault);
        }

        @Override
        protected Expression visitJsonQuery(io.trino.sql.tree.JsonQuery node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteJsonQuery(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            JsonPathInvocation jsonPathInvocation = rewriteJsonPathInvocation(node.getJsonPathInvocation(), context);

            return new JsonQuery(
                    jsonPathInvocation,
                    node.getReturnedType().map(returnedType -> defaultRewrite(returnedType, context.get())),
                    node.getOutputFormat(),
                    node.getWrapperBehavior(),
                    node.getQuotesBehavior(),
                    node.getEmptyBehavior(),
                    node.getErrorBehavior());
        }

        @Override
        protected Expression visitJsonObject(io.trino.sql.tree.JsonObject node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteJsonObject(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<JsonObjectMember> members = node.getMembers().stream()
                    .map(member -> {
                        Expression key = rewrite(member.getKey(), context.get());
                        Expression value = rewrite(member.getValue(), context.get());
                        return new JsonObjectMember(key, value, member.getFormat());
                    })
                    .collect(toImmutableList());

            return new JsonObject(
                    members,
                    node.isNullOnNull(),
                    node.isUniqueKeys(),
                    node.getReturnedType().map(returnedType -> defaultRewrite(returnedType, context.get())),
                    node.getOutputFormat());
        }

        @Override
        protected Expression visitJsonArray(io.trino.sql.tree.JsonArray node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteJsonArray(node, context.get(), AstToIrExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<JsonArrayElement> elements = node.getElements().stream()
                    .map(element -> {
                        Expression value = rewrite(element.getValue(), context.get());
                        return new JsonArrayElement(value, element.getFormat());
                    })
                    .collect(toImmutableList());

            return new JsonArray(
                    elements,
                    node.isNullOnNull(),
                    node.getReturnedType().map(returnedType -> defaultRewrite(returnedType, context.get())),
                    node.getOutputFormat());
        }

        private JsonPathInvocation rewriteJsonPathInvocation(io.trino.sql.tree.JsonPathInvocation pathInvocation, Context<C> context)
        {
            Expression inputExpression = rewrite(pathInvocation.getInputExpression(), context.get());

            List<JsonPathParameter> pathParameters = pathInvocation.getPathParameters().stream()
                    .map(pathParameter -> {
                        Expression expression = rewrite(pathParameter.getParameter(), context.get());
                        return new JsonPathParameter(defaultRewrite(pathParameter.getName(), context.get()), expression, pathParameter.getFormat());
                    })
                    .collect(toImmutableList());

            return new JsonPathInvocation(inputExpression, pathInvocation.getInputFormat(), new StringLiteral(pathInvocation.getJsonPath().getValue()), pathParameters);
        }
    }

    public class AstToIrNodeCopier
            extends AstVisitor<Node, Void>
    {
        @Override
        protected Node visitNode(io.trino.sql.tree.Node node, Void v)
        {
            throw new UnsupportedOperationException("visit() not implemented for " + node.getClass().getName());
        }

        @Override
        protected Node visitExpression(io.trino.sql.tree.Expression node, Void v)
        {
            throw new UnsupportedOperationException("should not copy expression on node copier");
        }

        @Override
        protected Node visitRow(io.trino.sql.tree.Row row, Void v)
        {
            List<Expression> items = copy(row.getItems(), v);
            return new Row(items);
        }

        @Override
        protected Node visitArithmeticUnary(io.trino.sql.tree.ArithmeticUnaryExpression node, Void v)
        {
            Expression child = copy(node.getValue(), v);

            return new ArithmeticUnaryExpression(node.getSign(), child);
        }

        @Override
        public Node visitArithmeticBinary(io.trino.sql.tree.ArithmeticBinaryExpression node, Void v)
        {
            Expression left = copy(node.getLeft(), v);
            Expression right = copy(node.getRight(), v);
            return new ArithmeticBinaryExpression(node.getOperator(), left, right);
        }

        @Override
        protected Node visitArrayConstructor(io.trino.sql.tree.ArrayConstructor node, Void v)
        {
            List<Expression> values = copy(node.getValues(), v);
            return new ArrayConstructor(values);
        }

        @Override
        protected Node visitAtTimeZone(io.trino.sql.tree.AtTimeZone node, Void v)
        {
            Expression value = copy(node.getValue(), v);
            Expression timeZone = copy(node.getTimeZone(), v);
            return new AtTimeZone(value, timeZone);
        }

        @Override
        protected Node visitSubscriptExpression(io.trino.sql.tree.SubscriptExpression node, Void v)
        {
            Expression base = copy(node.getBase(), v);
            Expression index = copy(node.getIndex(), v);

            return new SubscriptExpression(base, index);
        }

        @Override
        public Node visitComparisonExpression(io.trino.sql.tree.ComparisonExpression node, Void v)
        {
            Expression left = copy(node.getLeft(), v);
            Expression right = copy(node.getRight(), v);

            return new ComparisonExpression(node.getOperator(), left, right);
        }

        @Override
        protected Node visitBetweenPredicate(io.trino.sql.tree.BetweenPredicate node, Void v)
        {
            Expression value = copy(node.getValue(), v);
            Expression min = copy(node.getMin(), v);
            Expression max = copy(node.getMax(), v);

            return new BetweenPredicate(value, min, max);
        }

        @Override
        public Node visitLogicalExpression(io.trino.sql.tree.LogicalExpression node, Void v)
        {
            List<Expression> terms = copy(node.getTerms(), v);

            return new LogicalExpression(node.getOperator(), terms);
        }

        @Override
        public Node visitNotExpression(io.trino.sql.tree.NotExpression node, Void v)
        {
            Expression value = copy(node.getValue(), v);

            return new NotExpression(value);
        }

        @Override
        protected Node visitIsNullPredicate(io.trino.sql.tree.IsNullPredicate node, Void v)
        {
            Expression value = copy(node.getValue(), v);

            return new IsNullPredicate(value);
        }

        @Override
        protected Node visitIsNotNullPredicate(io.trino.sql.tree.IsNotNullPredicate node, Void v)
        {
            Expression value = copy(node.getValue(), v);

            return new IsNotNullPredicate(value);
        }

        @Override
        protected Node visitNullIfExpression(io.trino.sql.tree.NullIfExpression node, Void v)
        {
            Expression first = copy(node.getFirst(), v);
            Expression second = copy(node.getSecond(), v);

            return new NullIfExpression(first, second);
        }

        @Override
        protected Node visitIfExpression(io.trino.sql.tree.IfExpression node, Void v)
        {
            Expression condition = copy(node.getCondition(), v);
            Expression trueValue = copy(node.getTrueValue(), v);
            Expression falseValue = null;
            if (node.getFalseValue().isPresent()) {
                falseValue = copy(node.getFalseValue().get(), v);
            }

            return new IfExpression(condition, trueValue, falseValue);
        }

        @Override
        protected Node visitSearchedCaseExpression(io.trino.sql.tree.SearchedCaseExpression node, Void v)
        {
            ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
            for (io.trino.sql.tree.WhenClause expression : node.getWhenClauses()) {
                builder.add(copy(expression, v));
            }

            Optional<Expression> defaultValue = node.getDefaultValue()
                    .map(value -> copy(value, v));

            return new SearchedCaseExpression(builder.build(), defaultValue);
        }

        @Override
        protected Node visitSimpleCaseExpression(io.trino.sql.tree.SimpleCaseExpression node, Void v)
        {
            Expression operand = copy(node.getOperand(), v);

            ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
            for (io.trino.sql.tree.WhenClause expression : node.getWhenClauses()) {
                builder.add(copy(expression, v));
            }

            Optional<Expression> defaultValue = node.getDefaultValue()
                    .map(value -> copy(value, v));

            return new SimpleCaseExpression(operand, builder.build(), defaultValue);
        }

        @Override
        protected Node visitWhenClause(io.trino.sql.tree.WhenClause node, Void v)
        {
            Expression operand = copy(node.getOperand(), v);
            Expression result = copy(node.getResult(), v);

            return new WhenClause(operand, result);
        }

        @Override
        protected Node visitCoalesceExpression(io.trino.sql.tree.CoalesceExpression node, Void v)
        {
            List<Expression> operands = copy(node.getOperands(), v);

            return new CoalesceExpression(operands);
        }

        @Override
        public Node visitTryExpression(io.trino.sql.tree.TryExpression node, Void v)
        {
            Expression expression = copy(node.getInnerExpression(), v);

            return new TryExpression(expression);
        }

        @Override
        public Node visitFunctionCall(io.trino.sql.tree.FunctionCall node, Void v)
        {
            Optional<io.trino.sql.tree.Expression> filter = node.getFilter();
            Optional<Expression> rewrittenFilter = Optional.empty();
            if (filter.isPresent()) {
                io.trino.sql.tree.Expression filterExpression = filter.get();
                Expression newFilterExpression = copy(filterExpression, v);
                rewrittenFilter = Optional.of(newFilterExpression);
            }

            Optional<io.trino.sql.tree.Window> window = node.getWindow();
            Optional<Window> rewrittenWindow = Optional.empty();
            if (window.isPresent()) {
                rewrittenWindow = Optional.of(copyWindow(window.get(), v));
            }

            List<Expression> arguments = copy(node.getArguments(), v);

            QualifiedName rewrittenQualifiedName = QualifiedName.of(node.getName().getOriginalParts().stream().map(id -> new Identifier(id.getValue(), id.isDelimited())).collect(toImmutableList()));

            return new FunctionCall(
                    rewrittenQualifiedName,
                    rewrittenWindow,
                    rewrittenFilter,
                    node.getOrderBy().map(orderBy -> copy(orderBy, v)),
                    node.isDistinct(),
                    node.getNullTreatment(),
                    node.getProcessingMode().map(a -> new ProcessingMode(a.getMode())),
                    arguments);
        }

        // Since OrderBy contains list of SortItems, we want to process each SortItem's key, which is an expression
        private OrderBy copyOrderBy(io.trino.sql.tree.OrderBy orderBy, Void v)
        {
            List<SortItem> rewrittenSortItems = copySortItems(orderBy.getSortItems(), v);
            return new OrderBy(rewrittenSortItems);
        }

        private List<SortItem> copySortItems(List<io.trino.sql.tree.SortItem> sortItems, Void v)
        {
            ImmutableList.Builder<SortItem> rewrittenSortItems = ImmutableList.builder();
            for (io.trino.sql.tree.SortItem sortItem : sortItems) {
                Expression sortKey = copy(sortItem.getSortKey(), v);
                rewrittenSortItems.add(new SortItem(sortKey, sortItem.getOrdering(), sortItem.getNullOrdering()));
            }
            return rewrittenSortItems.build();
        }

        private Window copyWindow(io.trino.sql.tree.Window window, Void v)
        {
            if (window instanceof io.trino.sql.tree.WindowReference) {
                io.trino.sql.tree.WindowReference windowReference = (io.trino.sql.tree.WindowReference) window;
                Identifier rewrittenName = copy(windowReference.getName(), v);
                return new WindowReference(rewrittenName);
            }

            io.trino.sql.tree.WindowSpecification windowSpecification = (io.trino.sql.tree.WindowSpecification) window;
            Optional<Identifier> existingWindowName = windowSpecification.getExistingWindowName().map(name -> copy(name, v));

            List<Expression> partitionBy = copy(windowSpecification.getPartitionBy(), v);

            Optional<OrderBy> orderBy = Optional.empty();
            if (windowSpecification.getOrderBy().isPresent()) {
                orderBy = Optional.of(copyOrderBy(windowSpecification.getOrderBy().get(), v));
            }

            Optional<io.trino.sql.tree.WindowFrame> windowFrame = windowSpecification.getFrame();
            Optional<WindowFrame> rewrittenWindowFrame = Optional.empty();
            if (windowFrame.isPresent()) {
                io.trino.sql.tree.WindowFrame frame = windowFrame.get();

                io.trino.sql.tree.FrameBound start = frame.getStart();
                FrameBound rewrittenStart;
                if (start.getValue().isPresent()) {
                    Expression value = copy(start.getValue().get(), v);
                    rewrittenStart = new FrameBound(start.getType(), value);
                }
                else {
                    rewrittenStart = new FrameBound(start.getType());
                }

                Optional<io.trino.sql.tree.FrameBound> end = frame.getEnd();
                Optional<FrameBound> rewrittenEnd = Optional.empty();
                if (end.isPresent()) {
                    Optional<io.trino.sql.tree.Expression> value = end.get().getValue();
                    if (value.isPresent()) {
                        Expression rewrittenValue = copy(value.get(), v);
                        rewrittenEnd = Optional.of(new FrameBound(end.get().getType(), rewrittenValue));
                    }
                }

                // Frame properties for row pattern matching are not rewritten. They are planned as parts of
                // PatternRecognitionNode, and shouldn't be accessed past the Planner phase.
                // There are nested expressions in Measures and VariableDefinitions. They are not rewritten by default.
                // Rewriting them requires special handling of DereferenceExpression, aware of pattern labels.
                if (!frame.getMeasures().isEmpty() ||
                        frame.getAfterMatchSkipTo().isPresent() ||
                        frame.getPatternSearchMode().isPresent() ||
                        frame.getPattern().isPresent() ||
                        !frame.getSubsets().isEmpty() ||
                        !frame.getVariableDefinitions().isEmpty()) {
                    throw new UnsupportedOperationException("cannot rewrite pattern recognition clauses in window");
                }

                rewrittenWindowFrame = Optional.of(new WindowFrame(
                        frame.getType(),
                        rewrittenStart,
                        rewrittenEnd,
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(),
                        ImmutableList.of()));
            }
            return new WindowSpecification(existingWindowName, partitionBy, orderBy, rewrittenWindowFrame);
        }

        @Override
        protected Node visitWindowOperation(io.trino.sql.tree.WindowOperation node, Void v)
        {
            Identifier name = copy(node.getName(), v);
            Window window = copyWindow(node.getWindow(), v);
            return new WindowOperation(name, window);
        }

        @Override
        protected Node visitLambdaExpression(io.trino.sql.tree.LambdaExpression node, Void v)
        {
            Expression body = copy(node.getBody(), v);
            List<Expression> argumentDeclaration = copy(node.getArguments(), v);
            checkState(argumentDeclaration.stream().allMatch(arg -> arg instanceof LambdaArgumentDeclaration));
            return new LambdaExpression(argumentDeclaration.stream().map(arg -> (LambdaArgumentDeclaration) arg).collect(toImmutableList()), body);
        }

        @Override
        protected Node visitLambdaArgumentDeclaration(io.trino.sql.tree.LambdaArgumentDeclaration node, Void v)
        {
            Identifier name = copy(node.getName(), v);
            return new LambdaArgumentDeclaration(name);
        }

        @Override
        protected Node visitBindExpression(io.trino.sql.tree.BindExpression node, Void v)
        {
            List<Expression> values = copy(node.getValues(), v);
            Expression function = copy(node.getFunction(), v);

            return new BindExpression(values, function);
        }

        @Override
        public Node visitLikePredicate(io.trino.sql.tree.LikePredicate node, Void v)
        {
            Expression value = copy(node.getValue(), v);
            Expression pattern = copy(node.getPattern(), v);
            Optional<Expression> rewrittenEscape = node.getEscape()
                    .map(escape -> copy(escape, v));

            return new LikePredicate(value, pattern, rewrittenEscape);
        }

        @Override
        public Node visitInPredicate(io.trino.sql.tree.InPredicate node, Void v)
        {
            Expression value = copy(node.getValue(), v);
            Expression list = copy(node.getValueList(), v);

            return new InPredicate(value, list);
        }

        @Override
        protected Node visitInListExpression(io.trino.sql.tree.InListExpression node, Void v)
        {
            List<Expression> values = copy(node.getValues(), v);

            return new InListExpression(values);
        }

        @Override
        protected Node visitExists(io.trino.sql.tree.ExistsPredicate node, Void v)
        {
            return new ExistsPredicate(copy(node.getSubquery(), v));
        }

        @Override
        public Node visitSubqueryExpression(io.trino.sql.tree.SubqueryExpression node, Void v)
        {
            return new SubqueryExpression(node.getQuery());
        }

        @Override
        public Node visitLiteral(io.trino.sql.tree.Literal node, Void v)
        {
            // should not reach here because it should get dispatched according to its concrete type
            throw new IllegalStateException();
        }

        @Override
        public Node visitBinaryLiteral(io.trino.sql.tree.BinaryLiteral node, Void v)
        {
            return new BinaryLiteral(node.toHexString());
        }

        @Override
        public Node visitBooleanLiteral(io.trino.sql.tree.BooleanLiteral node, Void v)
        {
            return BooleanLiteral.of(Boolean.toString(node.getValue()));
        }

        @Override
        public Node visitCharLiteral(io.trino.sql.tree.CharLiteral node, Void v)
        {
            return CharLiteral.of(node.getValue());
        }

        @Override
        public Node visitDecimalLiteral(io.trino.sql.tree.DecimalLiteral node, Void v)
        {
            return new DecimalLiteral(node.getValue());
        }

        @Override
        public Node visitDoubleLiteral(io.trino.sql.tree.DoubleLiteral node, Void v)
        {
            return new DoubleLiteral(Double.toString(node.getValue()));
        }

        @Override
        public Node visitGenericLiteral(io.trino.sql.tree.GenericLiteral node, Void v)
        {
            return new GenericLiteral(node.getType(), node.getValue());
        }

        @Override
        public Node visitIntervalLiteral(io.trino.sql.tree.IntervalLiteral node, Void v)
        {
            return new IntervalLiteral(node.getValue(), node.getSign(), node.getStartField(), node.getEndField());
        }

        @Override
        public Node visitLongLiteral(io.trino.sql.tree.LongLiteral node, Void v)
        {
            return new LongLiteral(String.valueOf(node.getValue()));
        }

        @Override
        public Node visitNullLiteral(io.trino.sql.tree.NullLiteral node, Void v)
        {
            return new NullLiteral();
        }

        @Override
        public Node visitProcessingMode(io.trino.sql.tree.ProcessingMode node, Void v)
        {
            return new ProcessingMode(node.getMode());
        }

        @Override
        public Node visitStringLiteral(io.trino.sql.tree.StringLiteral node, Void v)
        {
            return new StringLiteral(node.getValue());
        }

        @Override
        public Node visitTimeLiteral(io.trino.sql.tree.TimeLiteral node, Void v)
        {
            return new TimeLiteral(node.getValue());
        }

        @Override
        public Node visitTimestampLiteral(io.trino.sql.tree.TimestampLiteral node, Void v)
        {
            return new TimestampLiteral(node.getValue());
        }

        @Override
        public Node visitParameter(io.trino.sql.tree.Parameter node, Void v)
        {
            return new Parameter(node.getPosition());
        }

        @Override
        public Node visitDereferenceExpression(io.trino.sql.tree.DereferenceExpression node, Void v)
        {
            Expression base = copy(node.getBase(), v);
            if (node.getField().isPresent()) {
                return new DereferenceExpression(base, copy(node.getField().get(), v));
            }
            return new DereferenceExpression((Identifier) base);
        }

        @Override
        protected Node visitExtract(io.trino.sql.tree.Extract node, Void v)
        {
            Expression expression = copy(node.getExpression(), v);
            return new Extract(expression, node.getField());
        }

        @Override
        protected Node visitCurrentTime(io.trino.sql.tree.CurrentTime node, Void v)
        {
            return new CurrentTime(node.getFunction(), node.getPrecision());
        }

        @Override
        public Node visitCast(io.trino.sql.tree.Cast node, Void v)
        {
            Expression expression = copy(node.getExpression(), v);
            DataType type = copy(node.getType(), v);

            return new Cast(expression, type, node.isSafe(), node.isTypeOnly());
        }

        @Override
        protected Node visitFieldReference(io.trino.sql.tree.FieldReference node, Void v)
        {
            return new FieldReference(node.getFieldIndex());
        }

        @Override
        protected Node visitSymbolReference(io.trino.sql.tree.SymbolReference node, Void v)
        {
            return new SymbolReference(node.getName());
        }

        @Override
        protected Node visitQuantifiedComparisonExpression(io.trino.sql.tree.QuantifiedComparisonExpression node, Void v)
        {
            Expression value = copy(node.getValue(), v);
            Expression subquery = copy(node.getSubquery(), v);

            return new QuantifiedComparisonExpression(node.getOperator(), node.getQuantifier(), value, subquery);
        }

        @Override
        public Node visitGroupingOperation(io.trino.sql.tree.GroupingOperation node, Void v)
        {
            List<Expression> groupingColumnExpressions = copy(node.getGroupingColumns(), v);
            return new GroupingOperation(groupingColumnExpressions);
        }

        @Override
        protected Node visitCurrentCatalog(io.trino.sql.tree.CurrentCatalog node, Void v)
        {
            return new CurrentCatalog();
        }

        @Override
        protected Node visitCurrentSchema(io.trino.sql.tree.CurrentSchema node, Void v)
        {
            return new CurrentSchema();
        }

        @Override
        protected Node visitCurrentUser(io.trino.sql.tree.CurrentUser node, Void v)
        {
            return new CurrentUser();
        }

        @Override
        protected Node visitCurrentPath(io.trino.sql.tree.CurrentPath node, Void v)
        {
            return new CurrentPath();
        }

        @Override
        protected Node visitTrim(io.trino.sql.tree.Trim node, Void v)
        {
            ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
            expressions.add(copy(node.getTrimSource(), v));
            node.getTrimCharacter().ifPresent(trimCharacter -> expressions.add(copy(node.getTrimSource(), v)));

            Expression trimSource = copy(node.getTrimSource(), v);
            Optional<Expression> trimChar = node.getTrimCharacter().isPresent() ? Optional.of(copy(node.getTrimCharacter().get(), v)) : Optional.empty();

            return new Trim(node.getSpecification(), trimSource, trimChar);
        }

        @Override
        protected Node visitFormat(io.trino.sql.tree.Format node, Void v)
        {
            List<Expression> arguments = copy(node.getArguments(), v);
            return new Format(arguments);
        }

        @Override
        protected Node visitLabelDereference(io.trino.sql.tree.LabelDereference node, Void v)
        {
            if (node.getReference().isPresent()) {
                SymbolReference reference = copy(node.getReference().get(), v);
                return new LabelDereference(node.getLabel(), reference);
            }

            return new LabelDereference(node.getLabel());
        }

        @Override
        protected Node visitJsonExists(io.trino.sql.tree.JsonExists node, Void v)
        {
            JsonPathInvocation jsonPathInvocation = copyJsonPathInvocation(node.getJsonPathInvocation(), v);

            return new JsonExists(jsonPathInvocation, node.getErrorBehavior());
        }

        @Override
        protected Node visitJsonValue(io.trino.sql.tree.JsonValue node, Void v)
        {
            JsonPathInvocation jsonPathInvocation = copyJsonPathInvocation(node.getJsonPathInvocation(), v);

            Optional<Expression> emptyDefault = node.getEmptyDefault().map(expression -> copy(expression, v));
            Optional<Expression> errorDefault = node.getErrorDefault().map(expression -> copy(expression, v));

            return new JsonValue(
                    jsonPathInvocation,
                    node.getReturnedType().map(returnedType -> copy(returnedType, v)),
                    node.getEmptyBehavior(),
                    emptyDefault,
                    node.getErrorBehavior(),
                    errorDefault);
        }

        @Override
        protected Node visitJsonQuery(io.trino.sql.tree.JsonQuery node, Void v)
        {
            JsonPathInvocation jsonPathInvocation = copyJsonPathInvocation(node.getJsonPathInvocation(), v);

            return new JsonQuery(
                    jsonPathInvocation,
                    node.getReturnedType().map(returnedType -> copy(returnedType, v)),
                    node.getOutputFormat(),
                    node.getWrapperBehavior(),
                    node.getQuotesBehavior(),
                    node.getEmptyBehavior(),
                    node.getErrorBehavior());
        }

        @Override
        protected Node visitJsonObject(io.trino.sql.tree.JsonObject node, Void v)
        {
            List<JsonObjectMember> members = node.getMembers().stream()
                    .map(member -> {
                        Expression key = copy(member.getKey(), v);
                        Expression value = copy(member.getValue(), v);
                        return new JsonObjectMember(key, value, member.getFormat());
                    })
                    .collect(toImmutableList());

            return new JsonObject(
                    members,
                    node.isNullOnNull(),
                    node.isUniqueKeys(),
                    node.getReturnedType().map(returnedType -> copy(returnedType, v)),
                    node.getOutputFormat());
        }

        @Override
        protected Node visitJsonArray(io.trino.sql.tree.JsonArray node, Void v)
        {
            List<JsonArrayElement> elements = node.getElements().stream()
                    .map(element -> {
                        Expression value = copy(element.getValue(), v);
                        return new JsonArrayElement(value, element.getFormat());
                    })
                    .collect(toImmutableList());

            return new JsonArray(
                    elements,
                    node.isNullOnNull(),
                    node.getReturnedType().map(returnedType -> copy(returnedType, v)),
                    node.getOutputFormat());
        }

        private JsonPathInvocation copyJsonPathInvocation(io.trino.sql.tree.JsonPathInvocation pathInvocation, Void v)
        {
            Expression inputExpression = copy(pathInvocation.getInputExpression(), v);

            List<JsonPathParameter> pathParameters = pathInvocation.getPathParameters().stream()
                    .map(pathParameter -> {
                        Expression expression = copy(pathParameter.getParameter(), v);
                        return new JsonPathParameter(copy(pathParameter.getName(), v), expression, pathParameter.getFormat());
                    })
                    .collect(toImmutableList());

            return new JsonPathInvocation(inputExpression, pathInvocation.getInputFormat(), new StringLiteral(pathInvocation.getJsonPath().getValue()), pathParameters);
        }

        @Override
        protected Node visitDataType(io.trino.sql.tree.DataType node, Void v)
        {
            return visitNode(node, v);
        }

        @Override
        protected Node visitDateTimeType(io.trino.sql.tree.DateTimeDataType node, Void v)
        {
            return new DateTimeDataType(node.getType(), node.isWithTimeZone(), node.getPrecision().map(precision -> copy(precision, v)));
        }

        @Override
        protected Node visitGenericDataType(io.trino.sql.tree.GenericDataType node, Void v)
        {
            return new GenericDataType(copy(node.getName(), v), copy(node.getArguments(), v));
        }

        @Override
        protected Node visitIntervalDataType(io.trino.sql.tree.IntervalDayTimeDataType node, Void v)
        {
            return new IntervalDayTimeDataType(node.getFrom(), node.getTo());
        }

        @Override
        protected Node visitRowDataType(io.trino.sql.tree.RowDataType node, Void v)
        {
            return new RowDataType(node.getFields().stream().map(field -> (RowDataType.Field) copy(field, v)).collect(toImmutableList()));
        }

        @Override
        protected Node visitRowField(io.trino.sql.tree.RowDataType.Field node, Void v)
        {
            return new RowDataType.Field(node.getName().map(name -> copy(name, v)), copy(node.getType(), v));
        }

        @Override
        protected Node visitIdentifier(io.trino.sql.tree.Identifier node, Void v)
        {
            return new Identifier(node.getValue(), node.isDelimited());
        }

        @Override
        protected Node visitDataTypeParameter(io.trino.sql.tree.DataTypeParameter node, Void v)
        {
            return visitNode(node, v);
        }

        @Override
        protected Node visitNumericTypeParameter(io.trino.sql.tree.NumericParameter node, Void v)
        {
            return new NumericParameter(node.getValue());
        }

        protected Node visitTypeParameter(io.trino.sql.tree.TypeParameter node, Void v)
        {
            return new TypeParameter(copy(node.getValue(), v));
        }

        @Override
        protected Node visitQuery(io.trino.sql.tree.Query node, Void v)
        {
            return new Query(
                    node.getWith().map(with -> (With) visitWith(with, v)),
                    (QueryBody) visitQueryBody(node.getQueryBody(), v),
                    node.getOrderBy().map(orderby -> (OrderBy) visitOrderBy(orderby, v)),
                    node.getOffset().map(offset -> (Offset) visitOffset(offset, v)),
                    node.getLimit().map(limit -> copy(limit, v)));
        }

        @Override
        protected Node visitWith(io.trino.sql.tree.With with, Void v)
        {
            return new With(with.isRecursive(), copy(with.getChildren(), v));
        }

        @Override
        protected Node visitWithQuery(io.trino.sql.tree.WithQuery withQuery, Void v)
        {
            return new WithQuery(copy(withQuery.getName(), v), copy(withQuery.getQuery(), v), withQuery.getColumnNames().map(columnNames -> copy(columnNames, v)));
        }

        @Override
        protected Node visitQueryBody(io.trino.sql.tree.QueryBody queryBody, Void v)
        {
            return visitNode(queryBody, v);
        }

        @Override
        protected Node visitExcept(io.trino.sql.tree.Except except, Void v)
        {
            return visitNode(except, v);
        }

        @Override
        protected Node visitRelation(io.trino.sql.tree.Relation relation, Void v)
        {
            return visitNode(relation, v);
        }

        @Override
        protected Node visitAliasedRelation(io.trino.sql.tree.AliasedRelation aliasedRelation, Void v)
        {
            return new AliasedRelation(copy(aliasedRelation.getRelation(), v), copy(aliasedRelation.getAlias(), v), copy(aliasedRelation.getChildren(), v));
        }

        @Override
        protected Node visitIntersect(io.trino.sql.tree.Intersect intersect, Void v)
        {
            return new Intersect(copy(intersect.getChildren(), v), intersect.isDistinct());
        }

        @Override
        protected Node visitJoin(io.trino.sql.tree.Join join, Void v)
        {
            return new Join(join.getType(), copy(join.getLeft(), v), copy(join.getRight(), v), join.getCriteria().map(this::copyJoinCriteria));
        }

        private JoinCriteria copyJoinCriteria(io.trino.sql.tree.JoinCriteria joinCriteria)
        {
            if (joinCriteria instanceof io.trino.sql.tree.JoinOn) {
                return new JoinOn(defaultRewrite(((io.trino.sql.tree.JoinOn) joinCriteria).getExpression(), null));
            }
            else if (joinCriteria instanceof io.trino.sql.tree.JoinUsing) {
                return new JoinUsing(copy(((io.trino.sql.tree.JoinUsing) joinCriteria).getColumns(), null));
            }
            else if (joinCriteria instanceof io.trino.sql.tree.NaturalJoin) {
                return new NaturalJoin();
            }
            throw new IllegalStateException("no other implementation for joinCriteria");
        }

        @Override
        protected Node visitLateral(io.trino.sql.tree.Lateral lateral, Void v)
        {
            return new Lateral(copy(lateral, v));
        }

        @Override
        protected Node visitPatternRecognitionRelation(io.trino.sql.tree.PatternRecognitionRelation lateral, Void v)
        {
            return new PatternRecognitionRelation(
                    copy(lateral.getInput(), v),
                    defaultRewrite(lateral.getPartitionBy(), null),
                    copy(lateral.getOrderBy(), v),
                    copy(lateral.getMeasures(), v),
                    lateral.getRowsPerMatch(),
                    copy(lateral.getAfterMatchSkipTo(), v),
                    copy(lateral.getPatternSearchMode(), v),
                    copy(lateral.getPattern(), v),
                    copy(lateral.getSubsets(), v),
                    copy(lateral.getVariableDefinitions(), v));
        }

        @Override
        protected Node visitMeasureDefinition(io.trino.sql.tree.MeasureDefinition measureDefinition, Void v)
        {
            return new MeasureDefinition(defaultRewrite(measureDefinition.getExpression(), null), copy(measureDefinition.getName(), v));
        }

        @Override
        protected Node visitOrderBy(io.trino.sql.tree.OrderBy orderBy, Void v)
        {
            return new OrderBy(copy(orderBy.getSortItems(), v));
        }

        @Override
        protected Node visitSortItem(io.trino.sql.tree.SortItem sortItem, Void v)
        {
            return new SortItem(defaultRewrite(sortItem.getSortKey(), null), sortItem.getOrdering(), sortItem.getNullOrdering());
        }

        @Override
        protected Node visitSkipTo(io.trino.sql.tree.SkipTo skipTo, Void v)
        {
            return new SkipTo(skipTo.getPosition(), skipTo.getIdentifier().map(identifier -> copy(identifier, v)));
        }

        @Override
        protected Node visitPatternSearchMode(io.trino.sql.tree.PatternSearchMode skipTo, Void v)
        {
            return new PatternSearchMode(skipTo.getMode());
        }

        @Override
        protected Node visitRowPattern(io.trino.sql.tree.RowPattern rowPattern, Void v)
        {
            return visitNode(rowPattern, v);
        }

        @Override
        protected Node visitAnchorPattern(io.trino.sql.tree.AnchorPattern anchorPattern, Void v)
        {
            return new AnchorPattern(anchorPattern.getType());
        }

        @Override
        protected Node visitEmptyPattern(io.trino.sql.tree.EmptyPattern emptyPattern, Void v)
        {
            return new EmptyPattern();
        }

        @Override
        protected Node visitExcludedPattern(io.trino.sql.tree.ExcludedPattern excludedPattern, Void v)
        {
            return new ExcludedPattern(copy(excludedPattern, v));
        }

        @Override
        protected Node visitPatternAlternation(io.trino.sql.tree.PatternAlternation patternAlternation, Void v)
        {
            return new PatternAlternation(copy(patternAlternation.getPatterns(), v));
        }

        @Override
        protected Node visitPatternConcatenation(io.trino.sql.tree.PatternConcatenation patternConcatenation, Void v)
        {
            return new PatternConcatenation(copy(patternConcatenation.getPatterns(), v));
        }

        @Override
        protected Node visitPatternPermutation(io.trino.sql.tree.PatternPermutation patternPermutation, Void v)
        {
            return new PatternPermutation(copy(patternPermutation.getPatterns(), v));
        }

        @Override
        protected Node visitPatternVariable(io.trino.sql.tree.PatternVariable patternVariable, Void v)
        {
            return new PatternVariable(copy(patternVariable.getName(), v));
        }

        @Override
        protected Node visitQuantifiedPattern(io.trino.sql.tree.QuantifiedPattern quantifiedPattern, Void v)
        {
            return new QuantifiedPattern(copy(quantifiedPattern.getPattern(), v), copy(quantifiedPattern.getPatternQuantifier(), v));
        }

        @Override
        protected Node visitPatternQuantifier(io.trino.sql.tree.PatternQuantifier patternQuantifier, Void v)
        {
            return visitNode(patternQuantifier, v);
        }

        @Override
        protected Node visitOneOrMoreQuantifier(io.trino.sql.tree.OneOrMoreQuantifier patternQuantifier, Void v)
        {
            return new OneOrMoreQuantifier(patternQuantifier.isGreedy());
        }

        @Override
        protected Node visitZeroOrMoreQuantifier(io.trino.sql.tree.ZeroOrMoreQuantifier patternQuantifier, Void v)
        {
            return new ZeroOrMoreQuantifier(patternQuantifier.isGreedy());
        }

        @Override
        protected Node visitZeroOrOneQuantifier(io.trino.sql.tree.ZeroOrOneQuantifier patternQuantifier, Void v)
        {
            return new ZeroOrOneQuantifier(patternQuantifier.isGreedy());
        }

        @Override
        protected Node visitRangeQuantifier(io.trino.sql.tree.RangeQuantifier rangeQuantifier, Void v)
        {
            return new RangeQuantifier(rangeQuantifier.isGreedy(), rangeQuantifier.getAtLeast().map(atLeast -> defaultRewrite(atLeast, null)), rangeQuantifier.getAtMost().map(atMost -> defaultRewrite(atMost, null)));
        }

        @Override
        protected Node visitSubsetDefinition(io.trino.sql.tree.SubsetDefinition subsetDefinition, Void v)
        {
            return new SubsetDefinition(copy(subsetDefinition.getName(), v), copy(subsetDefinition.getIdentifiers(), v));
        }

        @Override
        protected Node visitVariableDefinition(io.trino.sql.tree.VariableDefinition variableDefinition, Void v)
        {
            return new VariableDefinition(copy(variableDefinition.getName(), v), defaultRewrite(variableDefinition.getExpression(), null));
        }

        @Override
        protected Node visitQuerySpecification(io.trino.sql.tree.QuerySpecification querySpecification, Void v)
        {
            return visitNode(querySpecification, v);
        }

        @Override
        protected Node visitSelect(io.trino.sql.tree.Select select, Void v)
        {
            return new Select(select.isDistinct(), copy(select.getSelectItems(), v));
        }

        @Override
        protected Node visitSelectItem(io.trino.sql.tree.SelectItem selectItem, Void v)
        {
            return visitNode(selectItem, v);
        }

        @Override
        protected Node visitAllColumns(io.trino.sql.tree.AllColumns allColumns, Void v)
        {
            return new AllColumns(defaultRewrite(allColumns.getTarget(), null), copy(allColumns.getAliases(), v));
        }

        @Override
        protected Node visitSingleColumn(io.trino.sql.tree.SingleColumn singleColumn, Void v)
        {
            return new SingleColumn(defaultRewrite(singleColumn.getExpression(), null), copy(singleColumn.getAlias(), v));
        }

        @Override
        protected Node visitGroupBy(io.trino.sql.tree.GroupBy groupBy, Void v)
        {
            return new GroupBy(groupBy.isDistinct(), copy(groupBy, v));
        }

        @Override
        protected Node visitGroupingElement(io.trino.sql.tree.GroupingElement groupingElement, Void v)
        {
            return visitNode(groupingElement, v);
        }

        @Override
        protected Node visitCube(io.trino.sql.tree.Cube cube, Void v)
        {
            return new Cube(defaultRewrite(cube.getExpressions(), null));
        }

        @Override
        protected Node visitGroupingSets(io.trino.sql.tree.GroupingSets groupingSets, Void v)
        {
            return new GroupingSets(groupingSets.getSets().stream().map(groupingSet -> defaultRewrite(groupingSet, null)).collect(toImmutableList()));
        }

        @Override
        protected Node visitRollup(io.trino.sql.tree.Rollup rollUp, Void v)
        {
            return new Rollup(defaultRewrite(rollUp.getExpressions(), null));
        }

        @Override
        protected Node visitSimpleGroupBy(io.trino.sql.tree.SimpleGroupBy simpleGroupBy, Void v)
        {
            return new SimpleGroupBy(defaultRewrite(simpleGroupBy.getExpressions(), null));
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
        if (!a.isPresent() && !b.isPresent()) {
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
