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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ValuePointer;
import io.trino.sql.tree.DefaultExpressionTraversalVisitor;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.ExpressionExtractor.extractExpressions;
import static io.trino.sql.planner.ExpressionExtractor.extractExpressionsNonRecursive;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.util.Objects.requireNonNull;

public final class SymbolsExtractor
{
    private SymbolsExtractor() {}

    public static Set<Symbol> extractUnique(PlanNode node)
    {
        ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
        extractExpressions(node).forEach(expression -> uniqueSymbols.addAll(extractUnique(expression)));

        return uniqueSymbols.build();
    }

    public static Set<Symbol> extractUniqueNonRecursive(PlanNode node)
    {
        ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
        extractExpressionsNonRecursive(node).forEach(expression -> uniqueSymbols.addAll(extractUnique(expression)));

        return uniqueSymbols.build();
    }

    public static Set<Symbol> extractUnique(PlanNode node, Lookup lookup)
    {
        ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
        extractExpressions(node, lookup).forEach(expression -> uniqueSymbols.addAll(extractUnique(expression)));

        return uniqueSymbols.build();
    }

    public static Set<Symbol> extractUnique(Expression expression)
    {
        return ImmutableSet.copyOf(extractAll(expression));
    }

    public static Set<Symbol> extractUnique(Iterable<? extends Expression> expressions)
    {
        ImmutableSet.Builder<Symbol> unique = ImmutableSet.builder();
        for (Expression expression : expressions) {
            unique.addAll(extractAll(expression));
        }
        return unique.build();
    }

    public static Set<Symbol> extractUnique(Aggregation aggregation)
    {
        return ImmutableSet.copyOf(extractAll(aggregation));
    }

    public static Set<Symbol> extractUnique(WindowNode.Function function)
    {
        return ImmutableSet.copyOf(extractAll(function));
    }

    public static Set<Symbol> extractUnique(ExpressionAndValuePointers expressionAndValuePointers)
    {
        // Extract only the input symbols of ValuePointers. These are the symbols produced by the source node.
        // Other symbols present in the ExpressionAndValuePointers structure are synthetic unique symbols
        // with no outer usage or dependencies.
        Set<Symbol> syntheticClassifierSymbols = expressionAndValuePointers.getClassifierSymbols();
        Set<Symbol> syntheticMatchNumberSymbols = expressionAndValuePointers.getMatchNumberSymbols();

        Set<Symbol> valuePointersInputSymbols = expressionAndValuePointers.getValuePointers().stream()
                .map(ValuePointer::getInputSymbol)
                .collect(toImmutableSet());

        return Sets.difference(valuePointersInputSymbols, Sets.union(syntheticClassifierSymbols, syntheticMatchNumberSymbols));
    }

    public static List<Symbol> extractAll(Expression expression)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        new SymbolBuilderVisitor().process(expression, builder);
        return builder.build();
    }

    public static List<Symbol> extractAll(Aggregation aggregation)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        for (Expression argument : aggregation.getArguments()) {
            builder.addAll(extractAll(argument));
        }
        aggregation.getFilter().ifPresent(builder::add);
        aggregation.getOrderingScheme().ifPresent(orderBy -> builder.addAll(orderBy.getOrderBy()));
        aggregation.getMask().ifPresent(builder::add);
        return builder.build();
    }

    public static List<Symbol> extractAll(WindowNode.Function function)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        for (Expression argument : function.getArguments()) {
            builder.addAll(extractAll(argument));
        }
        function.getFrame().getEndValue().ifPresent(builder::add);
        function.getFrame().getSortKeyCoercedForFrameEndComparison().ifPresent(builder::add);
        function.getFrame().getStartValue().ifPresent(builder::add);
        function.getFrame().getSortKeyCoercedForFrameStartComparison().ifPresent(builder::add);
        return builder.build();
    }

    // to extract qualified name with prefix
    public static Set<QualifiedName> extractNames(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
        new QualifiedNameBuilderVisitor(columnReferences, true).process(expression, builder);
        return builder.build();
    }

    public static Set<QualifiedName> extractNamesNoSubqueries(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
        new QualifiedNameBuilderVisitor(columnReferences, false).process(expression, builder);
        return builder.build();
    }

    public static Set<Symbol> extractOutputSymbols(PlanNode planNode)
    {
        return extractOutputSymbols(planNode, noLookup());
    }

    public static Set<Symbol> extractOutputSymbols(PlanNode planNode, Lookup lookup)
    {
        return searchFrom(planNode, lookup)
                .findAll()
                .stream()
                .flatMap(node -> node.getOutputSymbols().stream())
                .collect(toImmutableSet());
    }

    private static class SymbolBuilderVisitor
            extends DefaultExpressionTraversalVisitor<ImmutableList.Builder<Symbol>>
    {
        @Override
        protected Void visitSymbolReference(SymbolReference node, ImmutableList.Builder<Symbol> builder)
        {
            builder.add(Symbol.from(node));
            return null;
        }

        @Override
        protected Void visitLambdaExpression(LambdaExpression node, ImmutableList.Builder<Symbol> context)
        {
            // Symbols in lambda expression are bound to lambda arguments, so no need to extract them
            return null;
        }
    }

    private static class QualifiedNameBuilderVisitor
            extends DefaultTraversalVisitor<ImmutableSet.Builder<QualifiedName>>
    {
        private final Set<NodeRef<Expression>> columnReferences;
        private final boolean recurseIntoSubqueries;

        private QualifiedNameBuilderVisitor(Set<NodeRef<Expression>> columnReferences, boolean recurseIntoSubqueries)
        {
            this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
            this.recurseIntoSubqueries = recurseIntoSubqueries;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableSet.Builder<QualifiedName> builder)
        {
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                builder.add(DereferenceExpression.getQualifiedName(node));
            }
            else {
                process(node.getBase(), builder);
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<QualifiedName> builder)
        {
            builder.add(QualifiedName.of(node.getValue()));
            return null;
        }

        @Override
        protected Void visitSubqueryExpression(SubqueryExpression node, ImmutableSet.Builder<QualifiedName> context)
        {
            if (!recurseIntoSubqueries) {
                return null;
            }

            return super.visitSubqueryExpression(node, context);
        }
    }
}
