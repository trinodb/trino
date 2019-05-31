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
package io.prestosql.sql.planner.optimizations;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.NestedColumn;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class MergeNestedColumn
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;

    public MergeNestedColumn(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        return SimplePlanRewriter.rewriteWith(new Optimizer(session, symbolAllocator, idAllocator, metadata, typeAnalyzer, warningCollector), plan);
    }

    public static boolean prefixExist(Expression expression, final Set<Expression> allDereferences)
    {
        int[] referenceCount = {0};
        new DefaultExpressionTraversalVisitor<Void, int[]>()
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, int[] referenceCount)
            {
                if (allDereferences.contains(node)) {
                    referenceCount[0] += 1;
                }
                process(node.getBase(), referenceCount);
                return null;
            }

            @Override
            protected Void visitSymbolReference(SymbolReference node, int[] context)
            {
                if (allDereferences.contains(node)) {
                    referenceCount[0] += 1;
                }
                return null;
            }
        }.process(expression, referenceCount);
        return referenceCount[0] > 1;
    }

    private static class Optimizer
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final TypeAnalyzer typeAnalyzer;

        public Optimizer(Session session, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, TypeAnalyzer typeAnalyzer, WarningCollector warningCollector)
        {
            this.session = requireNonNull(session);
            this.symbolAllocator = requireNonNull(symbolAllocator);
            this.idAllocator = requireNonNull(idAllocator);
            this.metadata = requireNonNull(metadata);
            this.typeAnalyzer = requireNonNull(typeAnalyzer);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            if (node.getSource() instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) node.getSource();
                return mergeProjectWithTableScan(node, tableScanNode, context);
            }
            return context.defaultRewrite(node);
        }

        private Type extractType(Expression expression)
        {
            Type type = typeAnalyzer.getType(session, symbolAllocator.getTypes(), expression);
            return type;
        }

        public PlanNode mergeProjectWithTableScan(ProjectNode node, TableScanNode tableScanNode, RewriteContext<Void> context)
        {
            Set<Expression> allExpressions = node.getAssignments().getExpressions().stream().map(MergeNestedColumn::validDereferenceExpression).filter(Objects::nonNull).collect(toImmutableSet());
            Set<Expression> dereferences = allExpressions.stream()
                    .filter(expression -> !prefixExist(expression, allExpressions))
                    .filter(expression -> expression instanceof DereferenceExpression)
                    .collect(toImmutableSet());

            if (dereferences.isEmpty()) {
                return context.defaultRewrite(node);
            }

            NestedColumnTranslator nestedColumnTranslator = new NestedColumnTranslator(tableScanNode.getAssignments(), tableScanNode.getTable());
            Map<Expression, NestedColumn> nestedColumns = dereferences.stream().collect(Collectors.toMap(Function.identity(), nestedColumnTranslator::toNestedColumn));

            Map<NestedColumn, ColumnHandle> nestedColumnHandles =
                    metadata.getNestedColumnHandles(session, tableScanNode.getTable(), nestedColumns.values())
                            .entrySet().stream()
                            .filter(entry -> !nestedColumnTranslator.columnHandleExists(entry.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (nestedColumnHandles.isEmpty()) {
                return context.defaultRewrite(node);
            }

            ImmutableMap.Builder<Symbol, ColumnHandle> columnHandleBuilder = ImmutableMap.builder();
            columnHandleBuilder.putAll(tableScanNode.getAssignments());

            // Use to replace expression in original dereference expression
            ImmutableMap.Builder<Expression, Symbol> symbolExpressionBuilder = ImmutableMap.builder();
            for (Map.Entry<NestedColumn, ColumnHandle> entry : nestedColumnHandles.entrySet()) {
                NestedColumn nestedColumn = entry.getKey();
                Expression expression = nestedColumnTranslator.toExpression(nestedColumn);
                Symbol symbol = symbolAllocator.newSymbol(nestedColumn.getName(), extractType(expression));
                symbolExpressionBuilder.put(expression, symbol);
                columnHandleBuilder.put(symbol, entry.getValue());
            }
            ImmutableMap<Symbol, ColumnHandle> nestedColumnsMap = columnHandleBuilder.build();

            TableScanNode newTableScan = new TableScanNode(
                    idAllocator.getNextId(),
                    tableScanNode.getTable(),
                    ImmutableList.copyOf(nestedColumnsMap.keySet()),
                    nestedColumnsMap,
                    tableScanNode.getEnforcedConstraint());

            Rewriter rewriter = new Rewriter(symbolExpressionBuilder.build());
            Map<Symbol, Expression> assignments = node.getAssignments().entrySet().stream().collect(
                    Collectors.toMap(Map.Entry::getKey, entry -> ExpressionTreeRewriter.rewriteWith(rewriter, entry.getValue())));
            return new ProjectNode(idAllocator.getNextId(), newTableScan, Assignments.copyOf(assignments));
        }

        private class NestedColumnTranslator
        {
            private final Map<Symbol, String> symbolToColumnName;
            private final Map<String, Symbol> columnNameToSymbol;

            NestedColumnTranslator(Map<Symbol, ColumnHandle> columnHandleMap, TableHandle tableHandle)
            {
                BiMap<Symbol, String> symbolToColumnName = HashBiMap.create(columnHandleMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> metadata.getColumnMetadata(session, tableHandle, entry.getValue()).getName())));
                this.symbolToColumnName = symbolToColumnName;
                this.columnNameToSymbol = symbolToColumnName.inverse();
            }

            boolean columnHandleExists(NestedColumn nestedColumn)
            {
                return columnNameToSymbol.containsKey(nestedColumn.getName());
            }

            NestedColumn toNestedColumn(Expression expression)
            {
                ImmutableList.Builder<String> builder = ImmutableList.builder();
                new DefaultExpressionTraversalVisitor<Void, Void>()
                {
                    @Override
                    protected Void visitSubscriptExpression(SubscriptExpression node, Void context)
                    {
                        return null;
                    }

                    @Override
                    protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
                    {
                        process(node.getBase(), context);
                        builder.add(node.getField().getValue());
                        return null;
                    }

                    @Override
                    protected Void visitSymbolReference(SymbolReference node, Void context)
                    {
                        Symbol baseName = Symbol.from(node);
                        Preconditions.checkArgument(symbolToColumnName.containsKey(baseName), "base [%s] doesn't exist in assignments [%s]", baseName, symbolToColumnName);
                        builder.add(symbolToColumnName.get(baseName));
                        return null;
                    }
                }.process(expression, null);
                List<String> names = builder.build();
                Preconditions.checkArgument(names.size() > 1, "names size is less than 0", names);
                return new NestedColumn(names);
            }

            Expression toExpression(NestedColumn nestedColumn)
            {
                Expression result = null;
                for (String part : nestedColumn.getNames()) {
                    if (result == null) {
                        Preconditions.checkArgument(columnNameToSymbol.containsKey(part), "element %s doesn't exist in map %s", part, columnNameToSymbol);
                        result = columnNameToSymbol.get(part).toSymbolReference();
                    }
                    else {
                        result = new DereferenceExpression(result, new Identifier(part));
                    }
                }
                return result;
            }
        }
    }

    // expression: msg_12.foo -> nestedColumn: msg.foo -> expression: msg_12.foo

    private static class Rewriter
            extends ExpressionRewriter<Void>
    {
        private final Map<Expression, Symbol> map;

        Rewriter(Map<Expression, Symbol> map)
        {
            this.map = map;
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (map.containsKey(node)) {
                return map.get(node).toSymbolReference();
            }
            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (map.containsKey(node)) {
                return map.get(node).toSymbolReference();
            }
            return super.rewriteSymbolReference(node, context, treeRewriter);
        }
    }

    public static Expression validDereferenceExpression(Expression expression)
    {
        //Preconditions.checkArgument(expression instanceof DereferenceExpression, "express must be dereference expression first");
        SubscriptExpression[] shortestSubscriptExp = new SubscriptExpression[1];
        boolean[] valid = new boolean[1];
        valid[0] = true;
        new DefaultExpressionTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitSubscriptExpression(SubscriptExpression node, Void context)
            {
                shortestSubscriptExp[0] = node;
                process(node.getBase(), context);
                return null;
            }

            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
            {
                valid[0] &= (node.getBase() instanceof SymbolReference || node.getBase() instanceof DereferenceExpression || node.getBase() instanceof SubscriptExpression);
                process(node.getBase(), context);
                return null;
            }
        }.process(expression, null);
        if (valid[0]) {
            return shortestSubscriptExp[0] == null ? expression : shortestSubscriptExp[0].getBase();
        }
        else {
            return null;
        }
    }
}
