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
import io.trino.sql.ir.AllColumns;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FetchFirst;
import io.trino.sql.ir.Identifier;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.Lateral;
import io.trino.sql.ir.Limit;
import io.trino.sql.ir.Node;
import io.trino.sql.ir.Offset;
import io.trino.sql.ir.OrderBy;
import io.trino.sql.ir.QualifiedName;
import io.trino.sql.ir.RowPattern;
import io.trino.sql.ir.Select;
import io.trino.sql.ir.SelectItem;
import io.trino.sql.ir.SingleColumn;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.ExpressionFormatter.formatOrderBy;
import static io.trino.sql.RowPatternFormatter.formatPattern;
import static java.util.stream.Collectors.joining;

public final class NodeFormatter
{
    private static final String INDENT = "   ";

    private NodeFormatter() {}

    public static String formatNode(Node root)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    static String formatName(QualifiedName name)
    {
        return name.getOriginalParts().stream()
                .map(ExpressionFormatter::formatExpression)
                .collect(joining("."));
    }

    private static class Formatter
            extends IrVisitor<Void, Integer>
    {
        private final StringBuilder builder;

        public Formatter(StringBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        protected Void visitNode(Node node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent)
        {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node));
            return null;
        }

        @Override
        protected Void visitRowPattern(RowPattern node, Integer indent)
        {
            checkArgument(indent == 0, "visitRowPattern should only be called at root");
            builder.append(formatPattern(node));
            return null;
        }

        @Override
        protected Void visitLateral(Lateral node, Integer indent)
        {
            append(indent, "LATERAL (");
            process(node.getQuery(), indent + 1);
            append(indent, ")");
            return null;
        }

        @Override
        protected Void visitOrderBy(OrderBy node, Integer indent)
        {
            append(indent, formatOrderBy(node))
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitOffset(Offset node, Integer indent)
        {
            append(indent, "OFFSET ")
                    .append(formatExpression(node.getRowCount()))
                    .append(" ROWS\n");
            return null;
        }

        @Override
        protected Void visitFetchFirst(FetchFirst node, Integer indent)
        {
            append(indent, "FETCH FIRST " + node.getRowCount().map(count -> formatExpression(count) + " ROWS ").orElse("ROW "))
                    .append(node.isWithTies() ? "WITH TIES" : "ONLY")
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitLimit(Limit node, Integer indent)
        {
            append(indent, "LIMIT ")
                    .append(formatExpression(node.getRowCount()))
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent)
        {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            }
            else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent)
        {
            builder.append(formatExpression(node.getExpression()));
            node.getAlias().ifPresent(alias -> builder
                    .append(' ')
                    .append(formatExpression(alias)));

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer indent)
        {
            node.getTarget().ifPresent(value -> builder
                    .append(formatExpression(value))
                    .append("."));
            builder.append("*");

            if (!node.getAliases().isEmpty()) {
                builder.append(" AS (")
                        .append(Joiner.on(", ").join(node.getAliases().stream()
                                .map(ExpressionFormatter::formatExpression)
                                .collect(toImmutableList())))
                        .append(")");
            }

            return null;
        }

        private void appendMergeCaseWhen(boolean matched, Optional<Expression> expression)
        {
            builder.append(matched ? "WHEN MATCHED" : "WHEN NOT MATCHED");
            expression.ifPresent(value -> builder
                    .append(" AND ")
                    .append(formatExpression(value)));
            builder.append("\n");
        }

        private StringBuilder append(int indent, String value)
        {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private static String indentString(int indent)
        {
            return Strings.repeat(INDENT, indent);
        }

        private void formatDefinitionList(List<String> elements, int indent)
        {
            if (elements.size() == 1) {
                builder.append(" ")
                        .append(getOnlyElement(elements))
                        .append("\n");
            }
            else {
                builder.append("\n");
                for (int i = 0; i < elements.size() - 1; i++) {
                    append(indent, elements.get(i))
                            .append(",\n");
                }
                append(indent, elements.get(elements.size() - 1))
                        .append("\n");
            }
        }
    }

    private static void appendAliasColumns(StringBuilder builder, List<Identifier> columns)
    {
        if ((columns != null) && (!columns.isEmpty())) {
            String formattedColumns = columns.stream()
                    .map(ExpressionFormatter::formatExpression)
                    .collect(Collectors.joining(", "));

            builder.append(" (")
                    .append(formattedColumns)
                    .append(')');
        }
    }
}
