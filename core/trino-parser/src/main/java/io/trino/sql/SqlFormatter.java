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
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.AssignmentStatement;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.CaseStatement;
import io.trino.sql.tree.CaseStatementWhenClause;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.ColumnPosition;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.CommentCharacteristic;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.CompoundStatement;
import io.trino.sql.tree.ControlStatement;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.CreateFunction;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.Deny;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.DeterministicCharacteristic;
import io.trino.sql.tree.DropCatalog;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropFunction;
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.DropNotNullConstraint;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.ElseClause;
import io.trino.sql.tree.ElseIfClause;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.ExecuteImmediate;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.ExplainFormat;
import io.trino.sql.tree.ExplainOption;
import io.trino.sql.tree.ExplainType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.FunctionSpecification;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GrantObject;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.GrantorSpecification;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfStatement;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.Isolation;
import io.trino.sql.tree.IterateStatement;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.JsonTable;
import io.trino.sql.tree.JsonTableColumnDefinition;
import io.trino.sql.tree.JsonTableDefaultPlan;
import io.trino.sql.tree.LanguageCharacteristic;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.LeaveStatement;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LoopStatement;
import io.trino.sql.tree.Merge;
import io.trino.sql.tree.MergeCase;
import io.trino.sql.tree.MergeDelete;
import io.trino.sql.tree.MergeInsert;
import io.trino.sql.tree.MergeUpdate;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.NestedColumns;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NullInputCharacteristic;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.OrdinalityColumn;
import io.trino.sql.tree.ParameterDeclaration;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.PlanLeaf;
import io.trino.sql.tree.PlanParentChild;
import io.trino.sql.tree.PlanSiblings;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.PrincipalSpecification;
import io.trino.sql.tree.PropertiesCharacteristic;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryColumn;
import io.trino.sql.tree.QueryPeriod;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.RenameColumn;
import io.trino.sql.tree.RenameMaterializedView;
import io.trino.sql.tree.RenameSchema;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.RenameView;
import io.trino.sql.tree.RepeatStatement;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.ResetSessionAuthorization;
import io.trino.sql.tree.ReturnStatement;
import io.trino.sql.tree.ReturnsClause;
import io.trino.sql.tree.Revoke;
import io.trino.sql.tree.RevokeRoles;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.RoutineCharacteristic;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.SecurityCharacteristic;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SetColumnType;
import io.trino.sql.tree.SetPath;
import io.trino.sql.tree.SetProperties;
import io.trino.sql.tree.SetRole;
import io.trino.sql.tree.SetSchemaAuthorization;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.SetSessionAuthorization;
import io.trino.sql.tree.SetTableAuthorization;
import io.trino.sql.tree.SetTimeZone;
import io.trino.sql.tree.SetViewAuthorization;
import io.trino.sql.tree.ShowCatalogs;
import io.trino.sql.tree.ShowColumns;
import io.trino.sql.tree.ShowCreate;
import io.trino.sql.tree.ShowFunctions;
import io.trino.sql.tree.ShowGrants;
import io.trino.sql.tree.ShowRoleGrants;
import io.trino.sql.tree.ShowRoles;
import io.trino.sql.tree.ShowSchemas;
import io.trino.sql.tree.ShowSession;
import io.trino.sql.tree.ShowStats;
import io.trino.sql.tree.ShowTables;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableExecute;
import io.trino.sql.tree.TableFunctionArgument;
import io.trino.sql.tree.TableFunctionDescriptorArgument;
import io.trino.sql.tree.TableFunctionInvocation;
import io.trino.sql.tree.TableFunctionTableArgument;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TransactionAccessMode;
import io.trino.sql.tree.TransactionMode;
import io.trino.sql.tree.TruncateTable;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Update;
import io.trino.sql.tree.UpdateAssignment;
import io.trino.sql.tree.ValueColumn;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.VariableDeclaration;
import io.trino.sql.tree.WhileStatement;
import io.trino.sql.tree.WithQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.ExpressionFormatter.formatGroupBy;
import static io.trino.sql.ExpressionFormatter.formatJsonPathInvocation;
import static io.trino.sql.ExpressionFormatter.formatOrderBy;
import static io.trino.sql.ExpressionFormatter.formatSkipTo;
import static io.trino.sql.ExpressionFormatter.formatStringLiteral;
import static io.trino.sql.ExpressionFormatter.formatWindowSpecification;
import static io.trino.sql.RowPatternFormatter.formatPattern;
import static io.trino.sql.tree.SaveMode.IGNORE;
import static io.trino.sql.tree.SaveMode.REPLACE;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class SqlFormatter
{
    private static final String INDENT = "   ";

    private SqlFormatter() {}

    public static String formatSql(Node root)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    private static String formatName(Identifier identifier)
    {
        return ExpressionFormatter.formatExpression(identifier);
    }

    public static String formatName(QualifiedName name)
    {
        return name.getOriginalParts().stream()
                .map(SqlFormatter::formatName)
                .collect(joining("."));
    }

    private static String formatExpression(Expression expression)
    {
        return ExpressionFormatter.formatExpression(expression);
    }

    private static class Formatter
            extends AstVisitor<Void, Integer>
    {
        private static class SqlBuilder
        {
            private final StringBuilder builder;

            public SqlBuilder(StringBuilder builder)
            {
                this.builder = requireNonNull(builder, "builder is null");
            }

            @CanIgnoreReturnValue
            public SqlBuilder append(CharSequence value)
            {
                builder.append(value);
                return this;
            }

            @CanIgnoreReturnValue
            public SqlBuilder append(char c)
            {
                builder.append(c);
                return this;
            }
        }

        private final SqlBuilder builder;

        public Formatter(StringBuilder builder)
        {
            this.builder = new SqlBuilder(builder);
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
        protected Void visitUnnest(Unnest node, Integer indent)
        {
            builder.append("UNNEST(")
                    .append(node.getExpressions().stream()
                            .map(SqlFormatter::formatExpression)
                            .collect(joining(", ")))
                    .append(")");
            if (node.isWithOrdinality()) {
                builder.append(" WITH ORDINALITY");
            }
            return null;
        }

        @Override
        protected Void visitJsonTable(JsonTable node, Integer indent)
        {
            builder.append("JSON_TABLE (")
                    .append(formatJsonPathInvocation(node.getJsonPathInvocation()))
                    .append("\n");
            appendJsonTableColumns(node.getColumns(), indent + 1);
            node.getPlan().ifPresent(plan -> {
                builder.append("\n");
                if (plan instanceof JsonTableDefaultPlan) {
                    append(indent + 1, "PLAN DEFAULT (");
                }
                else {
                    append(indent + 1, "PLAN (");
                }
                process(plan, indent + 1);
                builder.append(")");
            });
            node.getErrorBehavior().ifPresent(behavior -> {
                builder.append("\n");
                append(indent + 1, behavior + " ON ERROR");
            });
            builder.append(")\n");
            return null;
        }

        private void appendJsonTableColumns(List<JsonTableColumnDefinition> columns, int indent)
        {
            append(indent, "COLUMNS (\n");
            for (int i = 0; i < columns.size() - 1; i++) {
                process(columns.get(i), indent + 1);
                builder.append(",\n");
            }
            process(columns.getLast(), indent + 1);
            builder.append(")");
        }

        @Override
        protected Void visitOrdinalityColumn(OrdinalityColumn node, Integer indent)
        {
            append(indent, formatName(node.getName()) + " FOR ORDINALITY");
            return null;
        }

        @Override
        protected Void visitValueColumn(ValueColumn node, Integer indent)
        {
            append(indent, formatName(node.getName()))
                    .append(" ")
                    .append(formatExpression(node.getType()));
            node.getJsonPath().ifPresent(path ->
                    builder.append(" PATH ")
                            .append(formatExpression(path)));
            builder.append(" ")
                    .append(node.getEmptyBehavior().name())
                    .append(node.getEmptyDefault().map(expression -> " " + formatExpression(expression)).orElse(""))
                    .append(" ON EMPTY");
            node.getErrorBehavior().ifPresent(behavior ->
                    builder.append(" ")
                            .append(behavior.name())
                            .append(node.getErrorDefault().map(expression -> " " + formatExpression(expression)).orElse(""))
                            .append(" ON ERROR"));
            return null;
        }

        @Override
        protected Void visitQueryColumn(QueryColumn node, Integer indent)
        {
            append(indent, formatName(node.getName()))
                    .append(" ")
                    .append(formatExpression(node.getType()))
                    .append(" FORMAT ")
                    .append(node.getFormat().toString());
            node.getJsonPath().ifPresent(path ->
                    builder.append(" PATH ")
                            .append(formatExpression(path)));
            builder.append(switch (node.getWrapperBehavior()) {
                case WITHOUT -> " WITHOUT ARRAY WRAPPER";
                case CONDITIONAL -> " WITH CONDITIONAL ARRAY WRAPPER";
                case UNCONDITIONAL -> " WITH UNCONDITIONAL ARRAY WRAPPER";
            });

            if (node.getQuotesBehavior().isPresent()) {
                builder.append(switch (node.getQuotesBehavior().get()) {
                    case KEEP -> " KEEP QUOTES ON SCALAR STRING";
                    case OMIT -> " OMIT QUOTES ON SCALAR STRING";
                });
            }
            builder.append(" ")
                    .append(node.getEmptyBehavior().toString())
                    .append(" ON EMPTY");
            node.getErrorBehavior().ifPresent(behavior ->
                    builder.append(" ")
                            .append(behavior.toString())
                            .append(" ON ERROR"));
            return null;
        }

        @Override
        protected Void visitNestedColumns(NestedColumns node, Integer indent)
        {
            append(indent, "NESTED PATH ")
                    .append(formatExpression(node.getJsonPath()));
            node.getPathName().ifPresent(name ->
                    builder.append(" AS ")
                            .append(formatName(name)));
            builder.append("\n");
            appendJsonTableColumns(node.getColumns(), indent + 1);
            return null;
        }

        @Override
        protected Void visitJsonTableDefaultPlan(JsonTableDefaultPlan node, Integer indent)
        {
            builder.append(node.getParentChild().name())
                    .append(", ")
                    .append(node.getSiblings().name());
            return null;
        }

        @Override
        protected Void visitPlanParentChild(PlanParentChild node, Integer indent)
        {
            process(node.getParent());
            builder.append(" ")
                    .append(node.getType().name())
                    .append(" (");
            process(node.getChild());
            builder.append(")");
            return null;
        }

        @Override
        protected Void visitPlanSiblings(PlanSiblings node, Integer context)
        {
            for (int i = 0; i < node.getSiblings().size() - 1; i++) {
                builder.append("(");
                process(node.getSiblings().get(i));
                builder.append(") ")
                        .append(node.getType().name())
                        .append(" ");
            }
            builder.append("(");
            process(node.getSiblings().getLast());
            builder.append(")");
            return null;
        }

        @Override
        protected Void visitPlanLeaf(PlanLeaf node, Integer context)
        {
            builder.append(formatName(node.getName()));
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
        protected Void visitTableFunctionInvocation(TableFunctionInvocation node, Integer indent)
        {
            append(indent, "TABLE(");
            appendTableFunctionInvocation(node, indent + 1);
            builder.append(")");
            return null;
        }

        private void appendTableFunctionInvocation(TableFunctionInvocation node, Integer indent)
        {
            builder.append(formatName(node.getName()))
                    .append("(\n");
            appendTableFunctionArguments(node.getArguments(), indent + 1);
            if (!node.getCopartitioning().isEmpty()) {
                builder.append("\n");
                append(indent + 1, "COPARTITION ");
                builder.append(node.getCopartitioning().stream()
                        .map(tableList -> tableList.stream()
                                .map(SqlFormatter::formatName)
                                .collect(joining(", ", "(", ")")))
                        .collect(joining(", ")));
            }
            builder.append(")");
        }

        private void appendTableFunctionArguments(List<TableFunctionArgument> arguments, int indent)
        {
            for (int i = 0; i < arguments.size(); i++) {
                TableFunctionArgument argument = arguments.get(i);
                if (argument.getName().isPresent()) {
                    append(indent, formatName(argument.getName().get()));
                    builder.append(" => ");
                }
                else {
                    append(indent, "");
                }
                Node value = argument.getValue();
                if (value instanceof Expression) {
                    builder.append(formatExpression((Expression) value));
                }
                else {
                    process(value, indent + 1);
                }
                if (i < arguments.size() - 1) {
                    builder.append(",\n");
                }
            }
        }

        @Override
        protected Void visitTableArgument(TableFunctionTableArgument node, Integer indent)
        {
            Relation relation = node.getTable();
            Node unaliased = relation instanceof AliasedRelation ? ((AliasedRelation) relation).getRelation() : relation;
            if (unaliased instanceof TableSubquery) {
                // unpack the relation from TableSubquery to avoid adding another pair of parentheses
                unaliased = ((TableSubquery) unaliased).getQuery();
            }
            builder.append("TABLE(");
            process(unaliased, indent);
            builder.append(")");
            if (relation instanceof AliasedRelation aliasedRelation) {
                builder.append(" AS ")
                        .append(formatName(aliasedRelation.getAlias()));
                appendAliasColumns(builder, aliasedRelation.getColumnNames());
            }
            if (node.getPartitionBy().isPresent()) {
                builder.append("\n");
                append(indent, "PARTITION BY ")
                        .append(node.getPartitionBy().get().stream()
                                .map(SqlFormatter::formatExpression)
                                .collect(joining(", ")));
            }
            node.getEmptyTableTreatment().ifPresent(treatment -> {
                builder.append("\n");
                append(indent, treatment.getTreatment().name() + " WHEN EMPTY");
            });
            node.getOrderBy().ifPresent(orderBy -> {
                builder.append("\n");
                append(indent, formatOrderBy(orderBy));
            });

            return null;
        }

        @Override
        protected Void visitDescriptorArgument(TableFunctionDescriptorArgument node, Integer indent)
        {
            if (node.getDescriptor().isPresent()) {
                builder.append(node.getDescriptor().get().getFields().stream()
                        .map(field -> {
                            String formattedField = formatName(field.getName());
                            if (field.getType().isPresent()) {
                                formattedField = formattedField + " " + formatExpression(field.getType().get());
                            }
                            return formattedField;
                        })
                        .collect(joining(", ", "DESCRIPTOR(", ")")));
            }
            else {
                builder.append("CAST (NULL AS DESCRIPTOR)");
            }

            return null;
        }

        @Override
        protected Void visitPrepare(Prepare node, Integer indent)
        {
            append(indent, "PREPARE ");
            builder.append(formatName(node.getName()));
            builder.append(" FROM");
            builder.append("\n");
            process(node.getStatement(), indent + 1);
            return null;
        }

        @Override
        protected Void visitDeallocate(Deallocate node, Integer indent)
        {
            append(indent, "DEALLOCATE PREPARE ");
            builder.append(formatName(node.getName()));
            return null;
        }

        @Override
        protected Void visitExecute(Execute node, Integer indent)
        {
            append(indent, "EXECUTE ");
            builder.append(formatName(node.getName()));
            List<Expression> parameters = node.getParameters();
            if (!parameters.isEmpty()) {
                builder.append(" USING ");
                builder.append(parameters.stream()
                        .map(SqlFormatter::formatExpression)
                        .collect(joining(", ")));
            }
            return null;
        }

        @Override
        protected Void visitExecuteImmediate(ExecuteImmediate node, Integer indent)
        {
            append(indent, "EXECUTE IMMEDIATE\n")
                    .append(formatStringLiteral(node.getStatement().getValue()));
            List<Expression> parameters = node.getParameters();
            if (!parameters.isEmpty()) {
                builder.append("\nUSING ");
                builder.append(parameters.stream()
                        .map(SqlFormatter::formatExpression)
                        .collect(joining(", ")));
            }
            return null;
        }

        @Override
        protected Void visitDescribeOutput(DescribeOutput node, Integer indent)
        {
            append(indent, "DESCRIBE OUTPUT ");
            builder.append(formatName(node.getName()));
            return null;
        }

        @Override
        protected Void visitDescribeInput(DescribeInput node, Integer indent)
        {
            append(indent, "DESCRIBE INPUT ");
            builder.append(formatName(node.getName()));
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent)
        {
            if (!node.getFunctions().isEmpty()) {
                builder.append("WITH\n");
                Iterator<FunctionSpecification> functions = node.getFunctions().iterator();
                while (functions.hasNext()) {
                    process(functions.next(), indent + 1);
                    if (functions.hasNext()) {
                        builder.append(',');
                    }
                    builder.append('\n');
                }
            }

            node.getWith().ifPresent(with -> {
                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, formatName(query.getName()));
                    query.getColumnNames().ifPresent(columnNames -> appendAliasColumns(builder, columnNames));
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            });

            processRelation(node.getQueryBody(), indent);
            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
            node.getOffset().ifPresent(offset -> process(offset, indent));
            node.getLimit().ifPresent(limit -> process(limit, indent));
            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent)
        {
            process(node.getSelect(), indent);

            node.getFrom().ifPresent(from -> {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(from, indent);
            });

            builder.append('\n');

            node.getWhere().ifPresent(where ->
                    append(indent, "WHERE " + formatExpression(where)).append('\n'));

            node.getGroupBy().ifPresent(groupBy ->
                    append(indent, "GROUP BY " + (groupBy.isDistinct() ? " DISTINCT " : "") + formatGroupBy(groupBy.getGroupingElements())).append('\n'));

            node.getHaving().ifPresent(having -> append(indent, "HAVING " + formatExpression(having))
                    .append('\n'));

            if (!node.getWindows().isEmpty()) {
                append(indent, "WINDOW");
                formatDefinitionList(node.getWindows().stream()
                        .map(definition -> formatName(definition.getName()) + " AS " + formatWindowSpecification(definition.getWindow()))
                        .collect(toImmutableList()), indent + 1);
            }

            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
            node.getOffset().ifPresent(offset -> process(offset, indent));
            node.getLimit().ifPresent(limit -> process(limit, indent));
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
                    .append(formatName(alias)));

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
                                .map(SqlFormatter::formatName)
                                .collect(toImmutableList())))
                        .append(")");
            }

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent)
        {
            builder.append(formatName(node.getName()));
            node.getQueryPeriod().ifPresent(queryPeriod -> builder
                    .append(" " + queryPeriod));
            return null;
        }

        @Override
        protected Void visitQueryPeriod(QueryPeriod node, Integer indent)
        {
            builder.append("FOR " + node.getRangeType().name() + " AS OF " + formatExpression(node.getEnd().get()));
            return null;
        }

        @Override
        protected Void visitJoin(Join node, Integer indent)
        {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append('(');
            }
            process(node.getLeft(), indent);

            builder.append('\n');
            if (node.getType() == Join.Type.IMPLICIT) {
                append(indent, ", ");
            }
            else {
                append(indent, type).append(" JOIN ");
            }

            process(node.getRight(), indent);

            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                if (criteria instanceof JoinUsing using) {
                    builder.append(" USING (")
                            .append(Joiner.on(", ").join(using.getColumns()))
                            .append(")");
                }
                else if (criteria instanceof JoinOn on) {
                    builder.append(" ON ")
                            .append(formatExpression(on.getExpression()));
                }
                else if (!(criteria instanceof NaturalJoin)) {
                    throw new UnsupportedOperationException("unknown join criteria: " + criteria);
                }
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append(")");
            }

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent)
        {
            processRelationSuffix(node.getRelation(), indent);

            builder.append(' ')
                    .append(formatName(node.getAlias()));
            appendAliasColumns(builder, node.getColumnNames());

            return null;
        }

        @Override
        protected Void visitPatternRecognitionRelation(PatternRecognitionRelation node, Integer indent)
        {
            processRelationSuffix(node.getInput(), indent);

            builder.append(" MATCH_RECOGNIZE (\n");
            if (!node.getPartitionBy().isEmpty()) {
                append(indent + 1, "PARTITION BY ")
                        .append(node.getPartitionBy().stream()
                                .map(SqlFormatter::formatExpression)
                                .collect(joining(", ")))
                        .append("\n");
            }
            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent + 1));

            if (!node.getMeasures().isEmpty()) {
                append(indent + 1, "MEASURES");
                formatDefinitionList(node.getMeasures().stream()
                        .map(measure -> formatExpression(measure.getExpression()) + " AS " + formatName(measure.getName()))
                        .collect(toImmutableList()), indent + 2);
            }

            node.getRowsPerMatch().ifPresent(rowsPerMatch -> {
                String rowsPerMatchDescription = switch (rowsPerMatch) {
                    case ONE -> "ONE ROW PER MATCH";
                    case ALL_SHOW_EMPTY -> "ALL ROWS PER MATCH SHOW EMPTY MATCHES";
                    case ALL_OMIT_EMPTY -> "ALL ROWS PER MATCH OMIT EMPTY MATCHES";
                    case ALL_WITH_UNMATCHED -> "ALL ROWS PER MATCH WITH UNMATCHED ROWS";
                    default -> // RowsPerMatch of type WINDOW cannot occur in MATCH_RECOGNIZE clause
                            throw new IllegalStateException("unexpected rowsPerMatch: " + node.getRowsPerMatch().get());
                };
                append(indent + 1, rowsPerMatchDescription)
                        .append("\n");
            });

            node.getAfterMatchSkipTo().ifPresent(afterMatchSkipTo -> {
                String skipTo = formatSkipTo(afterMatchSkipTo);
                append(indent + 1, skipTo)
                        .append("\n");
            });

            node.getPatternSearchMode().ifPresent(patternSearchMode ->
                    append(indent + 1, patternSearchMode.getMode().name())
                            .append("\n"));

            append(indent + 1, "PATTERN (")
                    .append(formatPattern(node.getPattern()))
                    .append(")\n");
            if (!node.getSubsets().isEmpty()) {
                append(indent + 1, "SUBSET");
                formatDefinitionList(node.getSubsets().stream()
                        .map(subset -> formatName(subset.getName()) + " = " + subset.getIdentifiers().stream()
                                .map(SqlFormatter::formatName).collect(joining(", ", "(", ")")))
                        .collect(toImmutableList()), indent + 2);
            }
            append(indent + 1, "DEFINE");
            formatDefinitionList(node.getVariableDefinitions().stream()
                    .map(variable -> formatName(variable.getName()) + " AS " + formatExpression(variable.getExpression()))
                    .collect(toImmutableList()), indent + 2);

            builder.append(")");

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indent)
        {
            processRelationSuffix(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType().name())
                    .append(" (")
                    .append(formatExpression(node.getSamplePercentage()))
                    .append(')');

            return null;
        }

        private void processRelationSuffix(Relation relation, Integer indent)
        {
            if ((relation instanceof AliasedRelation) || (relation instanceof SampledRelation) || (relation instanceof PatternRecognitionRelation)) {
                builder.append("( ");
                process(relation, indent + 1);
                append(indent, ")");
            }
            else {
                process(relation, indent);
            }
        }

        @Override
        protected Void visitValues(Values node, Integer indent)
        {
            builder.append(" VALUES ");

            boolean first = true;
            for (Expression row : node.getRows()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                builder.append(formatExpression(row));
                first = false;
            }
            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent)
        {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");

            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("UNION ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitExcept(Except node, Integer indent)
        {
            processRelation(node.getLeft(), indent);

            builder.append("EXCEPT ");
            if (!node.isDistinct()) {
                builder.append("ALL ");
            }

            processRelation(node.getRight(), indent);

            return null;
        }

        @Override
        protected Void visitIntersect(Intersect node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("INTERSECT ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitMerge(Merge node, Integer indent)
        {
            builder.append("MERGE INTO ")
                    .append(formatName(node.getTargetTable().getName()));

            node.getTargetAlias().ifPresent(value -> builder
                    .append(' ')
                    .append(formatName(value)));
            builder.append("\n");

            append(indent + 1, "USING ");

            processRelation(node.getSource(), indent + 2);

            builder.append("\n");
            append(indent + 1, "ON ");
            builder.append(formatExpression(node.getPredicate()));

            for (MergeCase mergeCase : node.getMergeCases()) {
                builder.append("\n");
                process(mergeCase, indent);
            }

            return null;
        }

        @Override
        protected Void visitMergeInsert(MergeInsert node, Integer indent)
        {
            appendMergeCaseWhen(false, node.getExpression());
            append(indent + 1, "THEN INSERT ");

            if (!node.getColumns().isEmpty()) {
                builder.append(node.getColumns().stream()
                        .map(SqlFormatter::formatName)
                        .collect(joining(", ", "(", ")")));
            }

            builder.append("VALUES ");
            builder.append(node.getValues().stream()
                    .map(SqlFormatter::formatExpression)
                    .collect(joining(", ", "(", ")")));

            return null;
        }

        @Override
        protected Void visitMergeUpdate(MergeUpdate node, Integer indent)
        {
            appendMergeCaseWhen(true, node.getExpression());
            append(indent + 1, "THEN UPDATE SET");

            boolean first = true;
            for (MergeUpdate.Assignment assignment : node.getAssignments()) {
                builder.append("\n");
                append(indent + 1, first ? "  " : ", ");
                builder.append(formatName(assignment.getTarget()))
                        .append(" = ")
                        .append(formatExpression(assignment.getValue()));
                first = false;
            }

            return null;
        }

        @Override
        protected Void visitMergeDelete(MergeDelete node, Integer indent)
        {
            appendMergeCaseWhen(true, node.getExpression());
            append(indent + 1, "THEN DELETE");
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

        @Override
        protected Void visitCreateView(CreateView node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("VIEW ")
                    .append(formatName(node.getName()));

            node.getComment().ifPresent(comment -> builder
                    .append(" COMMENT ")
                    .append(formatStringLiteral(comment)));

            node.getSecurity().ifPresent(security -> builder
                    .append(" SECURITY ")
                    .append(security.name()));

            builder.append(formatPropertiesMultiLine(node.getProperties()));

            builder.append(" AS\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitRenameView(RenameView node, Integer indent)
        {
            builder.append("ALTER VIEW ")
                    .append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatName(node.getTarget()));

            return null;
        }

        @Override
        protected Void visitRenameMaterializedView(RenameMaterializedView node, Integer indent)
        {
            builder.append("ALTER MATERIALIZED VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatName(node.getTarget()));

            return null;
        }

        @Override
        protected Void visitSetViewAuthorization(SetViewAuthorization node, Integer indent)
        {
            builder.append("ALTER VIEW ")
                    .append(formatName(node.getSource()))
                    .append(" SET AUTHORIZATION ")
                    .append(formatPrincipal(node.getPrincipal()));

            return null;
        }

        @Override
        protected Void visitCreateMaterializedView(CreateMaterializedView node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("MATERIALIZED VIEW ");

            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }

            builder.append(formatName(node.getName()));
            node.getGracePeriod().ifPresent(interval ->
                    builder.append("\nGRACE PERIOD ").append(formatExpression(interval)));
            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(formatStringLiteral(comment)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
            builder.append(" AS\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitRefreshMaterializedView(RefreshMaterializedView node, Integer indent)
        {
            builder.append("REFRESH MATERIALIZED VIEW ");
            builder.append(formatName(node.getName()));

            return null;
        }

        @Override
        protected Void visitDropMaterializedView(DropMaterializedView node, Integer indent)
        {
            builder.append("DROP MATERIALIZED VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName()));
            return null;
        }

        @Override
        protected Void visitDropView(DropView node, Integer indent)
        {
            builder.append("DROP VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName()));

            return null;
        }

        @Override
        protected Void visitExplain(Explain node, Integer indent)
        {
            builder.append("EXPLAIN ");

            List<String> options = new ArrayList<>();

            for (ExplainOption option : node.getOptions()) {
                if (option instanceof ExplainType) {
                    options.add("TYPE " + ((ExplainType) option).getType());
                }
                else if (option instanceof ExplainFormat) {
                    options.add("FORMAT " + ((ExplainFormat) option).getType());
                }
                else {
                    throw new UnsupportedOperationException("unhandled explain option: " + option);
                }
            }

            if (!options.isEmpty()) {
                builder.append(options.stream()
                        .collect(joining(", ", "(", ")")));
            }

            builder.append("\n");

            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitExplainAnalyze(ExplainAnalyze node, Integer indent)
        {
            builder.append("EXPLAIN ANALYZE");
            if (node.isVerbose()) {
                builder.append(" VERBOSE");
            }
            builder.append("\n");

            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitShowCatalogs(ShowCatalogs node, Integer indent)
        {
            builder.append("SHOW CATALOGS");

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowSchemas(ShowSchemas node, Integer indent)
        {
            builder.append("SHOW SCHEMAS");

            node.getCatalog().ifPresent(catalog -> builder
                    .append(" FROM ")
                    .append(formatName(catalog)));

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowTables(ShowTables node, Integer indent)
        {
            builder.append("SHOW TABLES");

            node.getSchema().ifPresent(value -> builder
                    .append(" FROM ")
                    .append(formatName(value)));

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowCreate(ShowCreate node, Integer indent)
        {
            builder.append("SHOW CREATE ")
                    .append(switch (node.getType()) {
                        case SCHEMA -> "SCHEMA";
                        case TABLE -> "TABLE";
                        case VIEW -> "VIEW";
                        case MATERIALIZED_VIEW -> "MATERIALIZED VIEW";
                        case FUNCTION -> "FUNCTION";
                    })
                    .append(" ")
                    .append(formatName(node.getName()));
            return null;
        }

        @Override
        protected Void visitShowColumns(ShowColumns node, Integer indent)
        {
            builder.append("SHOW COLUMNS FROM ")
                    .append(formatName(node.getTable()));

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowStats(ShowStats node, Integer indent)
        {
            builder.append("SHOW STATS FOR ");
            process(node.getRelation(), 0);

            return null;
        }

        @Override
        protected Void visitShowFunctions(ShowFunctions node, Integer indent)
        {
            builder.append("SHOW FUNCTIONS");

            node.getSchema().ifPresent(value -> builder
                    .append(" FROM ")
                    .append(formatName(value)));

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowSession(ShowSession node, Integer indent)
        {
            builder.append("SHOW SESSION");

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitDelete(Delete node, Integer indent)
        {
            builder.append("DELETE FROM ")
                    .append(formatName(node.getTable().getName()));

            node.getWhere().ifPresent(where -> builder
                    .append(" WHERE ")
                    .append(formatExpression(where)));

            return null;
        }

        @Override
        protected Void visitCreateCatalog(CreateCatalog node, Integer indent)
        {
            builder.append("CREATE CATALOG ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getCatalogName()));
            builder.append(" USING ").append(formatName(node.getConnectorName()));
            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(formatStringLiteral(comment)));
            node.getPrincipal().ifPresent(principal -> builder
                    .append("\nAUTHORIZATION ")
                    .append(formatPrincipal(principal)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));

            return null;
        }

        @Override
        protected Void visitDropCatalog(DropCatalog node, Integer indent)
        {
            builder.append("DROP CATALOG ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getCatalogName()))
                    .append(" ")
                    .append(node.isCascade() ? "CASCADE" : "RESTRICT");

            return null;
        }

        @Override
        protected Void visitCreateSchema(CreateSchema node, Integer indent)
        {
            builder.append("CREATE SCHEMA ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getSchemaName()));
            node.getPrincipal().ifPresent(principal -> builder
                    .append("\nAUTHORIZATION ")
                    .append(formatPrincipal(principal)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));

            return null;
        }

        @Override
        protected Void visitDropSchema(DropSchema node, Integer indent)
        {
            builder.append("DROP SCHEMA ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSchemaName()))
                    .append(" ")
                    .append(node.isCascade() ? "CASCADE" : "RESTRICT");

            return null;
        }

        @Override
        protected Void visitRenameSchema(RenameSchema node, Integer indent)
        {
            builder.append("ALTER SCHEMA ")
                    .append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatName(node.getTarget()));

            return null;
        }

        @Override
        protected Void visitSetSchemaAuthorization(SetSchemaAuthorization node, Integer indent)
        {
            builder.append("ALTER SCHEMA ")
                    .append(formatName(node.getSource()))
                    .append(" SET AUTHORIZATION ")
                    .append(formatPrincipal(node.getPrincipal()));

            return null;
        }

        @Override
        protected Void visitCreateTableAsSelect(CreateTableAsSelect node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.getSaveMode() == REPLACE) {
                builder.append("OR REPLACE ");
            }
            builder.append("TABLE ");
            if (node.getSaveMode() == IGNORE) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getName()));

            node.getColumnAliases().ifPresent(columnAliases -> {
                builder.append(columnAliases.stream()
                        .map(SqlFormatter::formatName)
                        .collect(joining(", ", "( ", " )")));
            });

            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(formatStringLiteral(comment)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));

            builder.append(" AS ");
            process(node.getQuery(), indent);

            if (!node.isWithData()) {
                builder.append(" WITH NO DATA");
            }

            return null;
        }

        @Override
        protected Void visitCreateTable(CreateTable node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.getSaveMode() == REPLACE) {
                builder.append("OR REPLACE ");
            }
            builder.append("TABLE ");
            if (node.getSaveMode() == IGNORE) {
                builder.append("IF NOT EXISTS ");
            }
            String tableName = formatName(node.getName());
            builder.append(tableName).append(" (\n");

            String elementIndent = indentString(indent + 1);
            String columnList = node.getElements().stream()
                    .map(element -> {
                        if (element instanceof ColumnDefinition column) {
                            return elementIndent + formatColumnDefinition(column);
                        }
                        if (element instanceof LikeClause likeClause) {
                            StringBuilder builder = new StringBuilder(elementIndent);
                            builder.append("LIKE ")
                                    .append(formatName(likeClause.getTableName()));

                            likeClause.getPropertiesOption().ifPresent(propertiesOption -> builder
                                    .append(" ")
                                    .append(propertiesOption.name())
                                    .append(" PROPERTIES"));

                            return builder.toString();
                        }
                        throw new UnsupportedOperationException("unknown table element: " + element);
                    })
                    .collect(joining(",\n"));
            builder.append(columnList);
            builder.append("\n").append(")");

            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(formatStringLiteral(comment)));

            builder.append(formatPropertiesMultiLine(node.getProperties()));

            return null;
        }

        private static String formatPropertiesMultiLine(List<Property> properties)
        {
            if (properties.isEmpty()) {
                return "";
            }

            return properties.stream()
                    .map(property -> INDENT + formatProperty(property))
                    .collect(joining(",\n", "\nWITH (\n", "\n)"));
        }

        private static String formatColumnDefinition(ColumnDefinition column)
        {
            StringBuilder builder = new StringBuilder()
                    .append(formatName(column.getName()))
                    .append(" ").append(column.getType());
            if (!column.isNullable()) {
                builder.append(" NOT NULL");
            }
            column.getComment().ifPresent(comment -> builder
                    .append(" COMMENT ")
                    .append(formatStringLiteral(comment)));
            if (!column.getProperties().isEmpty()) {
                builder.append(" WITH (")
                        .append(joinProperties(column.getProperties()))
                        .append(")");
            }
            return builder.toString();
        }

        private static String formatGrantor(GrantorSpecification grantor)
        {
            GrantorSpecification.Type type = grantor.type();
            return switch (type) {
                case CURRENT_ROLE, CURRENT_USER -> type.name();
                case PRINCIPAL -> formatPrincipal(grantor.principal().get());
            };
        }

        private static String formatPrincipal(PrincipalSpecification principal)
        {
            PrincipalSpecification.Type type = principal.type();
            return switch (type) {
                case UNSPECIFIED -> principal.name().toString();
                case USER, ROLE -> type.name() + " " + principal.name();
            };
        }

        @Override
        protected Void visitDropTable(DropTable node, Integer indent)
        {
            builder.append("DROP TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTableName()));

            return null;
        }

        @Override
        protected Void visitRenameTable(RenameTable node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatName(node.getTarget()));

            return null;
        }

        @Override
        protected Void visitSetProperties(SetProperties node, Integer context)
        {
            SetProperties.Type type = node.getType();
            builder.append("ALTER ");
            builder.append(switch (type) {
                case TABLE -> "TABLE ";
                case MATERIALIZED_VIEW -> "MATERIALIZED VIEW ";
            });

            builder.append(formatName(node.getName()))
                    .append(" SET PROPERTIES ")
                    .append(joinProperties(node.getProperties()));

            return null;
        }

        private static String joinProperties(List<Property> properties)
        {
            return properties.stream()
                    .map(Formatter::formatProperty)
                    .collect(joining(", "));
        }

        private static String formatProperty(Property property)
        {
            return formatName(property.getName()) + " = " +
                    (property.isSetToDefault() ? "DEFAULT" : formatExpression(property.getNonDefaultValue()));
        }

        @Override
        protected Void visitComment(Comment node, Integer context)
        {
            String comment = node.getComment()
                    .map(ExpressionFormatter::formatStringLiteral)
                    .orElse("NULL");

            String type = switch (node.getType()) {
                case TABLE -> "TABLE";
                case VIEW -> "VIEW";
                case COLUMN -> "COLUMN";
            };

            builder.append("COMMENT ON " + type + " " + formatName(node.getName()) + " IS " + comment);

            return null;
        }

        @Override
        protected Void visitRenameColumn(RenameColumn node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTable()))
                    .append(" RENAME COLUMN ");
            if (node.isColumnExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSource()))
                    .append(" TO ")
                    .append(formatName(node.getTarget()));

            return null;
        }

        @Override
        protected Void visitDropColumn(DropColumn node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTable()))
                    .append(" DROP COLUMN ");
            if (node.isColumnExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getField()));

            return null;
        }

        @Override
        protected Void visitTableExecute(TableExecute node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            builder.append(formatName(node.getTable().getName()));
            builder.append(" EXECUTE ");
            builder.append(formatName(node.getProcedureName()));
            if (!node.getArguments().isEmpty()) {
                builder.append("(");
                formatCallArguments(indent, node.getArguments());
                builder.append(")");
            }
            node.getWhere().ifPresent(where -> builder
                    .append("\n")
                    .append(indentString(indent))
                    .append("WHERE ")
                    .append(formatExpression(where)));
            return null;
        }

        @Override
        protected Void visitAnalyze(Analyze node, Integer indent)
        {
            builder.append("ANALYZE ")
                    .append(formatName(node.getTableName()));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
            return null;
        }

        @Override
        protected Void visitAddColumn(AddColumn node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName()))
                    .append(" ADD COLUMN ");
            if (node.isColumnNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatColumnDefinition(node.getColumn()));

            node.getPosition().ifPresent(position -> {
                switch (position) {
                    case ColumnPosition.First _ -> builder.append(" FIRST");
                    case ColumnPosition.After after -> builder.append(" AFTER ").append(formatName(after.column()));
                    case ColumnPosition.Last _ -> builder.append(" LAST");
                }
            });

            return null;
        }

        @Override
        protected Void visitSetColumnType(SetColumnType node, Integer context)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTableName()))
                    .append(" ALTER COLUMN ")
                    .append(formatName(node.getColumnName()))
                    .append(" SET DATA TYPE ")
                    .append(node.getType().toString());

            return null;
        }

        @Override
        protected Void visitDropNotNullConstraint(DropNotNullConstraint node, Integer context)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTable()))
                    .append(" ALTER COLUMN ")
                    .append(formatName(node.getColumn()))
                    .append(" DROP NOT NULL");

            return null;
        }

        @Override
        protected Void visitSetTableAuthorization(SetTableAuthorization node, Integer indent)
        {
            builder.append("ALTER TABLE ")
                    .append(formatName(node.getSource()))
                    .append(" SET AUTHORIZATION ")
                    .append(formatPrincipal(node.getPrincipal()));

            return null;
        }

        @Override
        protected Void visitInsert(Insert node, Integer indent)
        {
            builder.append("INSERT INTO ")
                    .append(formatName(node.getTarget()));

            node.getColumns().ifPresent(columns -> builder
                    .append(" (")
                    .append(Joiner.on(", ").join(columns))
                    .append(")"));

            builder.append("\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitUpdate(Update node, Integer indent)
        {
            builder.append("UPDATE ")
                    .append(formatName(node.getTable().getName()))
                    .append(" SET");
            int setCounter = node.getAssignments().size() - 1;
            for (UpdateAssignment assignment : node.getAssignments()) {
                builder.append("\n")
                        .append(indentString(indent + 1))
                        .append(assignment.getName().getValue())
                        .append(" = ")
                        .append(formatExpression(assignment.getValue()));
                if (setCounter > 0) {
                    builder.append(",");
                }
                setCounter--;
            }
            node.getWhere().ifPresent(where -> builder
                    .append("\n")
                    .append(indentString(indent))
                    .append("WHERE ").append(formatExpression(where)));
            return null;
        }

        @Override
        protected Void visitTruncateTable(TruncateTable node, Integer indent)
        {
            builder.append("TRUNCATE TABLE ");
            builder.append(formatName(node.getTableName()));

            return null;
        }

        @Override
        public Void visitSetSession(SetSession node, Integer indent)
        {
            builder.append("SET SESSION ")
                    .append(formatName(node.getName()))
                    .append(" = ")
                    .append(formatExpression(node.getValue()));

            return null;
        }

        @Override
        public Void visitResetSession(ResetSession node, Integer indent)
        {
            builder.append("RESET SESSION ")
                    .append(formatName(node.getName()));

            return null;
        }

        @Override
        protected Void visitSetSessionAuthorization(SetSessionAuthorization node, Integer context)
        {
            builder.append("SET SESSION AUTHORIZATION ");
            builder.append(formatExpression(node.getUser()));
            return null;
        }

        @Override
        protected Void visitResetSessionAuthorization(ResetSessionAuthorization node, Integer context)
        {
            builder.append("RESET SESSION AUTHORIZATION");
            return null;
        }

        @Override
        protected Void visitCallArgument(CallArgument node, Integer indent)
        {
            node.getName().ifPresent(name -> builder
                    .append(formatName(name))
                    .append(" => "));
            builder.append(formatExpression(node.getValue()));

            return null;
        }

        @Override
        protected Void visitCall(Call node, Integer indent)
        {
            builder.append("CALL ")
                    .append(formatName(node.getName()))
                    .append("(");
            formatCallArguments(indent, node.getArguments());
            builder.append(")");

            return null;
        }

        private void formatCallArguments(Integer indent, List<CallArgument> arguments)
        {
            Iterator<CallArgument> iterator = arguments.iterator();
            while (iterator.hasNext()) {
                process(iterator.next(), indent);
                if (iterator.hasNext()) {
                    builder.append(", ");
                }
            }
        }

        @Override
        protected Void visitRow(Row node, Integer indent)
        {
            builder.append("ROW(");
            boolean firstItem = true;
            for (Expression item : node.getItems()) {
                if (!firstItem) {
                    builder.append(", ");
                }
                process(item, indent);
                firstItem = false;
            }
            builder.append(")");
            return null;
        }

        @Override
        protected Void visitStartTransaction(StartTransaction node, Integer indent)
        {
            builder.append("START TRANSACTION");

            Iterator<TransactionMode> iterator = node.getTransactionModes().iterator();
            while (iterator.hasNext()) {
                builder.append(" ");
                process(iterator.next(), indent);
                if (iterator.hasNext()) {
                    builder.append(",");
                }
            }
            return null;
        }

        @Override
        protected Void visitIsolationLevel(Isolation node, Integer indent)
        {
            builder.append("ISOLATION LEVEL ").append(node.getLevel().getText());
            return null;
        }

        @Override
        protected Void visitTransactionAccessMode(TransactionAccessMode node, Integer indent)
        {
            builder.append(node.isReadOnly() ? "READ ONLY" : "READ WRITE");
            return null;
        }

        @Override
        protected Void visitCommit(Commit node, Integer indent)
        {
            builder.append("COMMIT");
            return null;
        }

        @Override
        protected Void visitRollback(Rollback node, Integer indent)
        {
            builder.append("ROLLBACK");
            return null;
        }

        @Override
        protected Void visitCreateRole(CreateRole node, Integer indent)
        {
            builder.append("CREATE ROLE ").append(formatName(node.getName()));
            node.getGrantor().ifPresent(grantor -> builder
                    .append(" WITH ADMIN ")
                    .append(formatGrantor(grantor)));
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(formatName(catalog)));
            return null;
        }

        @Override
        protected Void visitDropRole(DropRole node, Integer indent)
        {
            builder.append("DROP ROLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName()));
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(formatName(catalog)));
            return null;
        }

        @Override
        protected Void visitGrantRoles(GrantRoles node, Integer indent)
        {
            builder.append("GRANT ");
            builder.append(node.getRoles().stream()
                    .map(Identifier::toString)
                    .collect(joining(", ")));
            builder.append(" TO ");
            builder.append(node.getGrantees().stream()
                    .map(Formatter::formatPrincipal)
                    .collect(joining(", ")));
            if (node.isAdminOption()) {
                builder.append(" WITH ADMIN OPTION");
            }
            node.getGrantor().ifPresent(grantor -> builder
                    .append(" GRANTED BY ")
                    .append(formatGrantor(grantor)));
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(formatName(catalog)));
            return null;
        }

        @Override
        protected Void visitRevokeRoles(RevokeRoles node, Integer indent)
        {
            builder.append("REVOKE ");
            if (node.isAdminOption()) {
                builder.append("ADMIN OPTION FOR ");
            }
            builder.append(node.getRoles().stream()
                    .map(Identifier::toString)
                    .collect(joining(", ")));
            builder.append(" FROM ");
            builder.append(node.getGrantees().stream()
                    .map(Formatter::formatPrincipal)
                    .collect(joining(", ")));
            node.getGrantor().ifPresent(grantor -> builder
                    .append(" GRANTED BY ")
                    .append(formatGrantor(grantor)));
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(formatName(catalog)));
            return null;
        }

        @Override
        protected Void visitSetRole(SetRole node, Integer indent)
        {
            builder.append("SET ROLE ");
            SetRole.Type type = node.getType();
            builder.append(switch (type) {
                case ALL, NONE -> type.name();
                case ROLE -> formatName(node.getRole().get());
            });
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(formatName(catalog)));
            return null;
        }

        @Override
        public Void visitGrant(Grant node, Integer indent)
        {
            builder.append("GRANT ");

            if (node.getPrivileges().isEmpty()) {
                builder.append("ALL PRIVILEGES");
            }
            else {
                builder.append(node.getPrivileges()
                        .map(privileges -> join(", ", privileges))
                        .orElseThrow());
            }
            builder.append(" ON ")
                    .append(formatGrantScope(node.getGrantObject()))
                    .append(" TO ")
                    .append(formatPrincipal(node.getGrantee()));
            if (node.isWithGrantOption()) {
                builder.append(" WITH GRANT OPTION");
            }

            return null;
        }

        @Override
        public Void visitDeny(Deny node, Integer indent)
        {
            builder.append("DENY ");

            if (node.getPrivileges().isPresent()) {
                builder.append(join(", ", node.getPrivileges().get()));
            }
            else {
                builder.append("ALL PRIVILEGES");
            }

            builder.append(" ON ")
                    .append(formatGrantScope(node.getGrantObject()))
                    .append(" TO ")
                    .append(formatPrincipal(node.getGrantee()));

            return null;
        }

        @Override
        public Void visitRevoke(Revoke node, Integer indent)
        {
            builder.append("REVOKE ");

            if (node.isGrantOptionFor()) {
                builder.append("GRANT OPTION FOR ");
            }

            if (node.getPrivileges().isEmpty()) {
                builder.append("ALL PRIVILEGES");
            }
            else {
                builder.append(node.getPrivileges()
                        .map(privileges -> join(", ", privileges))
                        .orElseThrow());
            }

            builder.append(" ON ")
                    .append(formatGrantScope(node.getGrantObject()))
                    .append(" FROM ")
                    .append(formatPrincipal(node.getGrantee()));

            return null;
        }

        @Override
        public Void visitShowGrants(ShowGrants node, Integer indent)
        {
            builder.append("SHOW GRANTS ");

            node.getGrantObject().ifPresent(scope -> builder.append("ON ")
                    .append(formatGrantScope(scope)));

            return null;
        }

        @Override
        protected Void visitShowRoles(ShowRoles node, Integer indent)
        {
            builder.append("SHOW ");
            if (node.isCurrent()) {
                builder.append("CURRENT ");
            }
            builder.append("ROLES");
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" FROM ")
                    .append(formatName(catalog)));

            return null;
        }

        @Override
        protected Void visitShowRoleGrants(ShowRoleGrants node, Integer indent)
        {
            builder.append("SHOW ROLE GRANTS");
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" FROM ")
                    .append(formatName(catalog)));
            return null;
        }

        @Override
        public Void visitSetPath(SetPath node, Integer indent)
        {
            builder.append("SET PATH ");
            builder.append(Joiner.on(", ").join(node.getPathSpecification().getPath()));
            return null;
        }

        @Override
        public Void visitSetTimeZone(SetTimeZone node, Integer indent)
        {
            builder.append("SET TIME ZONE ");
            builder.append(node.getTimeZone().map(SqlFormatter::formatExpression).orElse("LOCAL"));
            return null;
        }

        @Override
        protected Void visitCreateFunction(CreateFunction node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            process(node.getSpecification(), indent);
            return null;
        }

        @Override
        protected Void visitDropFunction(DropFunction node, Integer indent)
        {
            builder.append("DROP FUNCTION ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName()));
            processParameters(node.getParameters(), indent);
            return null;
        }

        @Override
        protected Void visitFunctionSpecification(FunctionSpecification node, Integer indent)
        {
            append(indent, "FUNCTION ")
                    .append(formatName(node.getName()));
            processParameters(node.getParameters(), indent);
            builder.append("\n");
            process(node.getReturnsClause(), indent);
            builder.append("\n");
            for (RoutineCharacteristic characteristic : node.getRoutineCharacteristics()) {
                process(characteristic, indent);
                builder.append("\n");
            }
            node.getStatement().ifPresent(statement -> process(statement, indent));
            node.getDefinition().map(StringLiteral::getValue).ifPresent(definition -> {
                append(indent, "AS ");
                builder.append("$$\n").append(definition).append("$$");
            });
            return null;
        }

        @Override
        protected Void visitParameterDeclaration(ParameterDeclaration node, Integer indent)
        {
            node.getName().ifPresent(value ->
                    builder.append(formatName(value)).append(" "));
            builder.append(formatExpression(node.getType()));
            return null;
        }

        @Override
        protected Void visitLanguageCharacteristic(LanguageCharacteristic node, Integer indent)
        {
            append(indent, "LANGUAGE ")
                    .append(formatName(node.getLanguage()));
            return null;
        }

        @Override
        protected Void visitDeterministicCharacteristic(DeterministicCharacteristic node, Integer indent)
        {
            append(indent, (node.isDeterministic() ? "" : "NOT ") + "DETERMINISTIC");
            return null;
        }

        @Override
        protected Void visitNullInputCharacteristic(NullInputCharacteristic node, Integer indent)
        {
            if (node.isCalledOnNull()) {
                append(indent, "CALLED ON NULL INPUT");
            }
            else {
                append(indent, "RETURNS NULL ON NULL INPUT");
            }
            return null;
        }

        @Override
        protected Void visitSecurityCharacteristic(SecurityCharacteristic node, Integer indent)
        {
            append(indent, "SECURITY ")
                    .append(node.getSecurity().name());
            return null;
        }

        @Override
        protected Void visitCommentCharacteristic(CommentCharacteristic node, Integer indent)
        {
            append(indent, "COMMENT ")
                    .append(formatStringLiteral(node.getComment()));
            return null;
        }

        @Override
        protected Void visitPropertiesCharacteristic(PropertiesCharacteristic node, Integer indent)
        {
            append(indent, "WITH (\n");
            Iterator<Property> iterator = node.getProperties().iterator();
            while (iterator.hasNext()) {
                Property property = iterator.next();
                append(indent + 1, formatProperty(property));
                builder.append(iterator.hasNext() ? ",\n" : "\n");
            }
            append(indent, ")");
            return null;
        }

        @Override
        protected Void visitReturnClause(ReturnsClause node, Integer indent)
        {
            append(indent, "RETURNS ")
                    .append(formatExpression(node.getReturnType()));
            return null;
        }

        @Override
        protected Void visitReturnStatement(ReturnStatement node, Integer indent)
        {
            append(indent, "RETURN ")
                    .append(formatExpression(node.getValue()));
            return null;
        }

        @Override
        protected Void visitCompoundStatement(CompoundStatement node, Integer indent)
        {
            append(indent, "BEGIN\n");
            for (VariableDeclaration variableDeclaration : node.getVariableDeclarations()) {
                process(variableDeclaration, indent + 1);
                builder.append(";\n");
            }
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "END");
            return null;
        }

        @Override
        protected Void visitVariableDeclaration(VariableDeclaration node, Integer indent)
        {
            append(indent, "DECLARE ")
                    .append(node.getNames().stream()
                            .map(SqlFormatter::formatName)
                            .collect(joining(", ")))
                    .append(" ")
                    .append(formatExpression(node.getType()));
            if (node.getDefaultValue().isPresent()) {
                builder.append(" DEFAULT ")
                        .append(formatExpression(node.getDefaultValue().get()));
            }
            return null;
        }

        @Override
        protected Void visitAssignmentStatement(AssignmentStatement node, Integer indent)
        {
            append(indent, "SET ");
            builder.append(formatName(node.getTarget()))
                    .append(" = ")
                    .append(formatExpression(node.getValue()));
            return null;
        }

        @Override
        protected Void visitCaseStatement(CaseStatement node, Integer indent)
        {
            append(indent, "CASE");
            if (node.getExpression().isPresent()) {
                builder.append(" ")
                        .append(formatExpression(node.getExpression().get()));
            }
            builder.append("\n");
            for (CaseStatementWhenClause whenClause : node.getWhenClauses()) {
                process(whenClause, indent + 1);
            }
            if (node.getElseClause().isPresent()) {
                process(node.getElseClause().get(), indent + 1);
            }
            append(indent, "END CASE");
            return null;
        }

        @Override
        protected Void visitCaseStatementWhenClause(CaseStatementWhenClause node, Integer indent)
        {
            append(indent, "WHEN ")
                    .append(formatExpression(node.getExpression()))
                    .append(" THEN\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            return null;
        }

        @Override
        protected Void visitIfStatement(IfStatement node, Integer indent)
        {
            append(indent, "IF ")
                    .append(formatExpression(node.getExpression()))
                    .append(" THEN\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            for (ElseIfClause elseIfClause : node.getElseIfClauses()) {
                process(elseIfClause, indent);
            }
            if (node.getElseClause().isPresent()) {
                process(node.getElseClause().get(), indent);
            }
            append(indent, "END IF");
            return null;
        }

        @Override
        protected Void visitElseIfClause(ElseIfClause node, Integer indent)
        {
            append(indent, "ELSEIF ")
                    .append(formatExpression(node.getExpression()))
                    .append(" THEN\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            return null;
        }

        @Override
        protected Void visitElseClause(ElseClause node, Integer indent)
        {
            append(indent, "ELSE\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            return null;
        }

        @Override
        protected Void visitIterateStatement(IterateStatement node, Integer indent)
        {
            append(indent, "ITERATE ")
                    .append(formatName(node.getLabel()));
            return null;
        }

        @Override
        protected Void visitLeaveStatement(LeaveStatement node, Integer indent)
        {
            append(indent, "LEAVE ")
                    .append(formatName(node.getLabel()));
            return null;
        }

        @Override
        protected Void visitLoopStatement(LoopStatement node, Integer indent)
        {
            builder.append(indentString(indent));
            appendBeginLabel(node.getLabel());
            builder.append("LOOP\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "END LOOP");
            return null;
        }

        @Override
        protected Void visitWhileStatement(WhileStatement node, Integer indent)
        {
            builder.append(indentString(indent));
            appendBeginLabel(node.getLabel());
            builder.append("WHILE ")
                    .append(formatExpression(node.getExpression()))
                    .append(" DO\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "END WHILE");
            return null;
        }

        @Override
        protected Void visitRepeatStatement(RepeatStatement node, Integer indent)
        {
            builder.append(indentString(indent));
            appendBeginLabel(node.getLabel());
            builder.append("REPEAT\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "UNTIL ")
                    .append(formatExpression(node.getCondition()))
                    .append("\n");
            append(indent, "END REPEAT");
            return null;
        }

        private void appendBeginLabel(Optional<Identifier> label)
        {
            label.ifPresent(value ->
                    builder.append(formatName(value)).append(": "));
        }

        private void processRelation(Relation relation, Integer indent)
        {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(formatName(((Table) relation).getName()))
                        .append('\n');
            }
            else {
                process(relation, indent);
            }
        }

        private void processParameters(List<ParameterDeclaration> parameters, Integer indent)
        {
            builder.append("(");
            Iterator<ParameterDeclaration> iterator = parameters.iterator();
            while (iterator.hasNext()) {
                process(iterator.next(), indent);
                if (iterator.hasNext()) {
                    builder.append(", ");
                }
            }
            builder.append(")");
        }

        private SqlBuilder append(int indent, String value)
        {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private static String indentString(int indent)
        {
            return INDENT.repeat(indent);
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
                append(indent, elements.getLast())
                        .append("\n");
            }
        }
    }

    private static void appendAliasColumns(Formatter.SqlBuilder builder, List<Identifier> columns)
    {
        if ((columns != null) && !columns.isEmpty()) {
            String formattedColumns = columns.stream()
                    .map(SqlFormatter::formatName)
                    .collect(joining(", "));

            builder.append(" (")
                    .append(formattedColumns)
                    .append(')');
        }
    }

    private static String formatGrantScope(GrantObject grantObject)
    {
        return String.format("%s%s",
                grantObject.getEntityKind().isPresent() ? grantObject.getEntityKind().get() + " " : "",
                formatName(grantObject.getName()));
    }
}
