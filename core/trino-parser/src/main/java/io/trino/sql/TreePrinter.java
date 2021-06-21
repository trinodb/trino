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
package io.trino.sql;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Rollup;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.WindowDefinition;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;

import java.io.PrintStream;
import java.util.IdentityHashMap;
import java.util.List;

public class TreePrinter
{
    private static final String INDENT = "   ";

    private final IdentityHashMap<Expression, QualifiedName> resolvedNameReferences;
    private final PrintStream out;

    public TreePrinter(IdentityHashMap<Expression, QualifiedName> resolvedNameReferences, PrintStream out)
    {
        this.resolvedNameReferences = new IdentityHashMap<>(resolvedNameReferences);
        this.out = out;
    }

    public void print(Node root)
    {
        AstVisitor<Void, Integer> printer = new DefaultTraversalVisitor<Integer>()
        {
            @Override
            protected Void visitNode(Node node, Integer indentLevel)
            {
                throw new UnsupportedOperationException("not yet implemented: " + node);
            }

            @Override
            protected Void visitQuery(Query node, Integer indentLevel)
            {
                print(indentLevel, "Query ");

                indentLevel++;

                print(indentLevel, "QueryBody");
                process(node.getQueryBody(), indentLevel);
                if (node.getOrderBy().isPresent()) {
                    print(indentLevel, "OrderBy");
                    process(node.getOrderBy().get(), indentLevel + 1);
                }

                if (node.getLimit().isPresent()) {
                    print(indentLevel, "Limit: " + node.getLimit().get());
                }

                return null;
            }

            @Override
            protected Void visitQuerySpecification(QuerySpecification node, Integer indentLevel)
            {
                print(indentLevel, "QuerySpecification ");

                indentLevel++;

                process(node.getSelect(), indentLevel);

                if (node.getFrom().isPresent()) {
                    print(indentLevel, "From");
                    process(node.getFrom().get(), indentLevel + 1);
                }

                if (node.getWhere().isPresent()) {
                    print(indentLevel, "Where");
                    process(node.getWhere().get(), indentLevel + 1);
                }

                if (node.getGroupBy().isPresent()) {
                    String distinct = "";
                    if (node.getGroupBy().get().isDistinct()) {
                        distinct = "[DISTINCT]";
                    }
                    print(indentLevel, "GroupBy" + distinct);
                    for (GroupingElement groupingElement : node.getGroupBy().get().getGroupingElements()) {
                        print(indentLevel, "SimpleGroupBy");
                        if (groupingElement instanceof SimpleGroupBy) {
                            for (Expression column : groupingElement.getExpressions()) {
                                process(column, indentLevel + 1);
                            }
                        }
                        else if (groupingElement instanceof GroupingSets) {
                            print(indentLevel + 1, "GroupingSets");
                            for (List<Expression> set : ((GroupingSets) groupingElement).getSets()) {
                                print(indentLevel + 2, "GroupingSet[");
                                for (Expression expression : set) {
                                    process(expression, indentLevel + 3);
                                }
                                print(indentLevel + 2, "]");
                            }
                        }
                        else if (groupingElement instanceof Cube) {
                            print(indentLevel + 1, "Cube");
                            for (Expression column : groupingElement.getExpressions()) {
                                process(column, indentLevel + 1);
                            }
                        }
                        else if (groupingElement instanceof Rollup) {
                            print(indentLevel + 1, "Rollup");
                            for (Expression column : groupingElement.getExpressions()) {
                                process(column, indentLevel + 1);
                            }
                        }
                    }
                }

                if (node.getHaving().isPresent()) {
                    print(indentLevel, "Having");
                    process(node.getHaving().get(), indentLevel + 1);
                }

                if (!node.getWindows().isEmpty()) {
                    print(indentLevel, "Window");
                    for (WindowDefinition windowDefinition : node.getWindows()) {
                        process(windowDefinition, indentLevel + 1);
                    }
                }

                if (node.getOrderBy().isPresent()) {
                    print(indentLevel, "OrderBy");
                    process(node.getOrderBy().get(), indentLevel + 1);
                }

                if (node.getLimit().isPresent()) {
                    print(indentLevel, "Limit: " + node.getLimit().get());
                }

                return null;
            }

            @Override
            protected Void visitOrderBy(OrderBy node, Integer indentLevel)
            {
                for (SortItem sortItem : node.getSortItems()) {
                    process(sortItem, indentLevel);
                }

                return null;
            }

            @Override
            protected Void visitWindowDefinition(WindowDefinition node, Integer indentLevel)
            {
                print(indentLevel, "WindowDefinition[" + node.getName() + "]");
                process(node.getWindow(), indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitWindowReference(WindowReference node, Integer indentLevel)
            {
                print(indentLevel, "WindowReference[" + node.getName() + "]");

                return null;
            }

            @Override
            public Void visitWindowSpecification(WindowSpecification node, Integer indentLevel)
            {
                if (node.getExistingWindowName().isPresent()) {
                    print(indentLevel, "ExistingWindowName " + node.getExistingWindowName().get());
                }

                if (!node.getPartitionBy().isEmpty()) {
                    print(indentLevel, "PartitionBy");
                    for (Expression expression : node.getPartitionBy()) {
                        process(expression, indentLevel + 1);
                    }
                }

                if (node.getOrderBy().isPresent()) {
                    print(indentLevel, "OrderBy");
                    process(node.getOrderBy().get(), indentLevel + 1);
                }

                if (node.getFrame().isPresent()) {
                    print(indentLevel, "Frame");
                    process(node.getFrame().get(), indentLevel + 1);
                }

                return null;
            }

            @Override
            protected Void visitSelect(Select node, Integer indentLevel)
            {
                String distinct = "";
                if (node.isDistinct()) {
                    distinct = "[DISTINCT]";
                }
                print(indentLevel, "Select" + distinct);

                super.visitSelect(node, indentLevel + 1); // visit children

                return null;
            }

            @Override
            protected Void visitAllColumns(AllColumns node, Integer indent)
            {
                StringBuilder aliases = new StringBuilder();
                if (!node.getAliases().isEmpty()) {
                    aliases.append(" [Aliases: ");
                    Joiner.on(", ").appendTo(aliases, node.getAliases());
                    aliases.append("]");
                }
                print(indent, "All columns" + aliases.toString());

                if (node.getTarget().isPresent()) {
                    super.visitAllColumns(node, indent + 1); // visit child
                }

                return null;
            }

            @Override
            protected Void visitSingleColumn(SingleColumn node, Integer indent)
            {
                if (node.getAlias().isPresent()) {
                    print(indent, "Alias: " + node.getAlias().get());
                }

                super.visitSingleColumn(node, indent + 1); // visit children

                return null;
            }

            @Override
            protected Void visitComparisonExpression(ComparisonExpression node, Integer indentLevel)
            {
                print(indentLevel, node.getOperator().toString());

                super.visitComparisonExpression(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, Integer indentLevel)
            {
                print(indentLevel, node.getOperator().toString());

                super.visitArithmeticBinary(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitLogicalExpression(LogicalExpression node, Integer indentLevel)
            {
                print(indentLevel, node.getOperator().toString());

                super.visitLogicalExpression(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitStringLiteral(StringLiteral node, Integer indentLevel)
            {
                print(indentLevel, "String[" + node.getValue() + "]");
                return null;
            }

            @Override
            protected Void visitBinaryLiteral(BinaryLiteral node, Integer indentLevel)
            {
                print(indentLevel, "Binary[" + node.toHexString() + "]");
                return null;
            }

            @Override
            protected Void visitBooleanLiteral(BooleanLiteral node, Integer indentLevel)
            {
                print(indentLevel, "Boolean[" + node.getValue() + "]");
                return null;
            }

            @Override
            protected Void visitLongLiteral(LongLiteral node, Integer indentLevel)
            {
                print(indentLevel, "Long[" + node.getValue() + "]");
                return null;
            }

            @Override
            protected Void visitLikePredicate(LikePredicate node, Integer indentLevel)
            {
                print(indentLevel, "LIKE");

                super.visitLikePredicate(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitIdentifier(Identifier node, Integer indentLevel)
            {
                QualifiedName resolved = resolvedNameReferences.get(node);
                String resolvedName = "";
                if (resolved != null) {
                    resolvedName = "=>" + resolved.toString();
                }
                print(indentLevel, "Identifier[" + node.getValue() + resolvedName + "]");
                return null;
            }

            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, Integer indentLevel)
            {
                QualifiedName resolved = resolvedNameReferences.get(node);
                String resolvedName = "";
                if (resolved != null) {
                    resolvedName = "=>" + resolved.toString();
                }
                print(indentLevel, "DereferenceExpression[" + node + resolvedName + "]");
                return null;
            }

            @Override
            protected Void visitFunctionCall(FunctionCall node, Integer indentLevel)
            {
                String name = Joiner.on('.').join(node.getName().getParts());
                print(indentLevel, "FunctionCall[" + name + "]");

                super.visitFunctionCall(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitTable(Table node, Integer indentLevel)
            {
                String name = Joiner.on('.').join(node.getName().getParts());
                print(indentLevel, "Table[" + name + "]");

                return null;
            }

            @Override
            protected Void visitValues(Values node, Integer indentLevel)
            {
                print(indentLevel, "Values");

                super.visitValues(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitRow(Row node, Integer indentLevel)
            {
                print(indentLevel, "Row");

                super.visitRow(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitAliasedRelation(AliasedRelation node, Integer indentLevel)
            {
                print(indentLevel, "Alias[" + node.getAlias() + "]");

                super.visitAliasedRelation(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitSampledRelation(SampledRelation node, Integer indentLevel)
            {
                print(indentLevel, "TABLESAMPLE[" + node.getType() + " (" + node.getSamplePercentage() + ")]");

                super.visitSampledRelation(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitTableSubquery(TableSubquery node, Integer indentLevel)
            {
                print(indentLevel, "SubQuery");

                super.visitTableSubquery(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitInPredicate(InPredicate node, Integer indentLevel)
            {
                print(indentLevel, "IN");

                super.visitInPredicate(node, indentLevel + 1);

                return null;
            }

            @Override
            protected Void visitSubqueryExpression(SubqueryExpression node, Integer indentLevel)
            {
                print(indentLevel, "SubQuery");

                super.visitSubqueryExpression(node, indentLevel + 1);

                return null;
            }
        };

        printer.process(root, 0);
    }

    private void print(Integer indentLevel, String value)
    {
        out.println(Strings.repeat(INDENT, indentLevel) + value);
    }
}
