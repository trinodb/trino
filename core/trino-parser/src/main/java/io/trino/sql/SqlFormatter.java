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
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainFormat;
import io.trino.sql.tree.ExplainOption;
import io.trino.sql.tree.ExplainType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.GrantorSpecification;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.Isolation;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.Merge;
import io.trino.sql.tree.MergeCase;
import io.trino.sql.tree.MergeDelete;
import io.trino.sql.tree.MergeInsert;
import io.trino.sql.tree.MergeUpdate;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.PrincipalSpecification;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.RenameColumn;
import io.trino.sql.tree.RenameSchema;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.RenameView;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.Revoke;
import io.trino.sql.tree.RevokeRoles;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SetPath;
import io.trino.sql.tree.SetRole;
import io.trino.sql.tree.SetSchemaAuthorization;
import io.trino.sql.tree.SetSession;
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
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TransactionAccessMode;
import io.trino.sql.tree.TransactionMode;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Update;
import io.trino.sql.tree.UpdateAssignment;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.ExpressionFormatter.formatGroupBy;
import static io.trino.sql.ExpressionFormatter.formatOrderBy;
import static io.trino.sql.ExpressionFormatter.formatSkipTo;
import static io.trino.sql.ExpressionFormatter.formatStringLiteral;
import static io.trino.sql.ExpressionFormatter.formatWindowSpecification;
import static io.trino.sql.RowPatternFormatter.formatPattern;
import static java.lang.String.format;
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

    static String formatName(QualifiedName name)
    {
        return name.getOriginalParts().stream()
                .map(ExpressionFormatter::formatExpression)
                .collect(joining("."));
    }

    private static class Formatter
            extends AstVisitor<Void, Integer>
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
        protected Void visitUnnest(Unnest node, Integer indent)
        {
            builder.append("UNNEST(")
                    .append(node.getExpressions().stream()
                            .map(ExpressionFormatter::formatExpression)
                            .collect(joining(", ")))
                    .append(")");
            if (node.isWithOrdinality()) {
                builder.append(" WITH ORDINALITY");
            }
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
        protected Void visitPrepare(Prepare node, Integer indent)
        {
            append(indent, "PREPARE ");
            builder.append(node.getName());
            builder.append(" FROM");
            builder.append("\n");
            process(node.getStatement(), indent + 1);
            return null;
        }

        @Override
        protected Void visitDeallocate(Deallocate node, Integer indent)
        {
            append(indent, "DEALLOCATE PREPARE ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitExecute(Execute node, Integer indent)
        {
            append(indent, "EXECUTE ");
            builder.append(node.getName());
            List<Expression> parameters = node.getParameters();
            if (!parameters.isEmpty()) {
                builder.append(" USING ");
                Joiner.on(", ").appendTo(builder, parameters);
            }
            return null;
        }

        @Override
        protected Void visitDescribeOutput(DescribeOutput node, Integer indent)
        {
            append(indent, "DESCRIBE OUTPUT ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitDescribeInput(DescribeInput node, Integer indent)
        {
            append(indent, "DESCRIBE INPUT ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent)
        {
            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, formatExpression(query.getName()));
                    query.getColumnNames().ifPresent(columnNames -> appendAliasColumns(builder, columnNames));
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            }

            processRelation(node.getQueryBody(), indent);

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getOffset().isPresent()) {
                process(node.getOffset().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                process(node.getLimit().get(), indent);
            }
            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent)
        {
            process(node.getSelect(), indent);

            if (node.getFrom().isPresent()) {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(node.getFrom().get(), indent);
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpression(node.getWhere().get()))
                        .append('\n');
            }

            if (node.getGroupBy().isPresent()) {
                append(indent, "GROUP BY " + (node.getGroupBy().get().isDistinct() ? " DISTINCT " : "") + formatGroupBy(node.getGroupBy().get().getGroupingElements())).append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + formatExpression(node.getHaving().get()))
                        .append('\n');
            }

            if (!node.getWindows().isEmpty()) {
                append(indent, "WINDOW");
                formatDefinitionList(node.getWindows().stream()
                        .map(definition -> formatExpression(definition.getName()) + " AS " + formatWindowSpecification(definition.getWindow()))
                        .collect(toImmutableList()), indent + 1);
            }

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getOffset().isPresent()) {
                process(node.getOffset().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                process(node.getLimit().get(), indent);
            }
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
            if (node.getAlias().isPresent()) {
                builder.append(' ')
                        .append(formatExpression(node.getAlias().get()));
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer context)
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

        @Override
        protected Void visitTable(Table node, Integer indent)
        {
            builder.append(formatName(node.getName()));

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
                if (criteria instanceof JoinUsing) {
                    JoinUsing using = (JoinUsing) criteria;
                    builder.append(" USING (")
                            .append(Joiner.on(", ").join(using.getColumns()))
                            .append(")");
                }
                else if (criteria instanceof JoinOn) {
                    JoinOn on = (JoinOn) criteria;
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
                    .append(formatExpression(node.getAlias()));
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
                                .map(ExpressionFormatter::formatExpression)
                                .collect(joining(", ")))
                        .append("\n");
            }
            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent + 1);
            }
            if (!node.getMeasures().isEmpty()) {
                append(indent + 1, "MEASURES");
                formatDefinitionList(node.getMeasures().stream()
                        .map(measure -> formatExpression(measure.getExpression()) + " AS " + formatExpression(measure.getName()))
                        .collect(toImmutableList()), indent + 2);
            }
            if (node.getRowsPerMatch().isPresent()) {
                String rowsPerMatch;
                switch (node.getRowsPerMatch().get()) {
                    case ONE:
                        rowsPerMatch = "ONE ROW PER MATCH";
                        break;
                    case ALL_SHOW_EMPTY:
                        rowsPerMatch = "ALL ROWS PER MATCH SHOW EMPTY MATCHES";
                        break;
                    case ALL_OMIT_EMPTY:
                        rowsPerMatch = "ALL ROWS PER MATCH OMIT EMPTY MATCHES";
                        break;
                    case ALL_WITH_UNMATCHED:
                        rowsPerMatch = "ALL ROWS PER MATCH WITH UNMATCHED ROWS";
                        break;
                    default:
                        // RowsPerMatch of type WINDOW cannot occur in MATCH_RECOGNIZE clause
                        throw new IllegalStateException("unexpected rowsPerMatch: " + node.getRowsPerMatch().get());
                }
                append(indent + 1, rowsPerMatch)
                        .append("\n");
            }
            if (node.getAfterMatchSkipTo().isPresent()) {
                String skipTo = formatSkipTo(node.getAfterMatchSkipTo().get());
                append(indent + 1, skipTo)
                        .append("\n");
            }
            if (node.getPatternSearchMode().isPresent()) {
                append(indent + 1, node.getPatternSearchMode().get().getMode().name())
                        .append("\n");
            }
            append(indent + 1, "PATTERN (")
                    .append(formatPattern(node.getPattern()))
                    .append(")\n");
            if (!node.getSubsets().isEmpty()) {
                append(indent + 1, "SUBSET");
                formatDefinitionList(node.getSubsets().stream()
                        .map(subset -> formatExpression(subset.getName()) + " = " + subset.getIdentifiers().stream()
                                .map(ExpressionFormatter::formatExpression).collect(joining(", ", "(", ")")))
                        .collect(toImmutableList()), indent + 2);
            }
            append(indent + 1, "DEFINE");
            formatDefinitionList(node.getVariableDefinitions().stream()
                    .map(variable -> formatExpression(variable.getName()) + " AS " + formatExpression(variable.getExpression()))
                    .collect(toImmutableList()), indent + 2);

            builder.append(")");

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indent)
        {
            processRelationSuffix(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType())
                    .append(" (")
                    .append(node.getSamplePercentage())
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
                    .append(node.getTable().getName());

            node.getTargetAlias().ifPresent(value -> builder.append(" ").append(value));
            builder.append("\n");

            append(indent + 1, "USING ");

            processRelation(node.getRelation(), indent + 2);

            builder.append("\n");
            append(indent + 1, "ON ");
            builder.append(formatExpression(node.getExpression()));

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
                builder.append("(");
                Joiner.on(", ").appendTo(builder, node.getColumns());
                builder.append(")");
            }

            builder.append("VALUES (");
            Joiner.on(", ").appendTo(builder, transform(node.getValues(), ExpressionFormatter::formatExpression));
            builder.append(")");

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
                builder.append(assignment.getTarget())
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
            expression.ifPresent(value -> builder.append(" AND ").append(formatExpression(value)));
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

            node.getComment().ifPresent(comment ->
                    builder.append(" COMMENT ")
                            .append(formatStringLiteral(comment)));

            node.getSecurity().ifPresent(security ->
                    builder.append(" SECURITY ")
                            .append(security.toString()));

            builder.append(" AS\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitRenameView(RenameView node, Integer context)
        {
            builder.append("ALTER VIEW ")
                    .append(node.getSource())
                    .append(" RENAME TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitSetViewAuthorization(SetViewAuthorization node, Integer context)
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
            if (node.getComment().isPresent()) {
                builder.append("\nCOMMENT " + formatStringLiteral(node.getComment().get()));
            }
            builder.append(formatPropertiesMultiLine(node.getProperties()));
            builder.append(" AS\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitRefreshMaterializedView(RefreshMaterializedView node, Integer context)
        {
            builder.append("REFRESH MATERIALIZED VIEW ");
            builder.append(formatName(node.getName()));

            return null;
        }

        @Override
        protected Void visitDropMaterializedView(DropMaterializedView node, Integer context)
        {
            builder.append("DROP MATERIALIZED VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName()));
            return null;
        }

        @Override
        protected Void visitDropView(DropView node, Integer context)
        {
            builder.append("DROP VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getName());

            return null;
        }

        @Override
        protected Void visitExplain(Explain node, Integer indent)
        {
            builder.append("EXPLAIN ");
            if (node.isAnalyze()) {
                builder.append("ANALYZE ");
            }

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
                builder.append("(");
                Joiner.on(", ").appendTo(builder, options);
                builder.append(")");
            }

            builder.append("\n");

            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitShowCatalogs(ShowCatalogs node, Integer context)
        {
            builder.append("SHOW CATALOGS");

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent((value) ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowSchemas(ShowSchemas node, Integer context)
        {
            builder.append("SHOW SCHEMAS");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent((value) ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowTables(ShowTables node, Integer context)
        {
            builder.append("SHOW TABLES");

            node.getSchema().ifPresent(value ->
                    builder.append(" FROM ")
                            .append(formatName(value)));

            node.getLikePattern().ifPresent(value ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowCreate(ShowCreate node, Integer context)
        {
            if (node.getType() == ShowCreate.Type.TABLE) {
                builder.append("SHOW CREATE TABLE ")
                        .append(formatName(node.getName()));
            }
            else if (node.getType() == ShowCreate.Type.VIEW) {
                builder.append("SHOW CREATE VIEW ")
                        .append(formatName(node.getName()));
            }
            else if (node.getType() == ShowCreate.Type.MATERIALIZED_VIEW) {
                builder.append("SHOW CREATE MATERIALIZED VIEW ")
                        .append(formatName(node.getName()));
            }
            return null;
        }

        @Override
        protected Void visitShowColumns(ShowColumns node, Integer context)
        {
            builder.append("SHOW COLUMNS FROM ")
                    .append(formatName(node.getTable()));

            node.getLikePattern().ifPresent(value ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowStats(ShowStats node, Integer context)
        {
            builder.append("SHOW STATS FOR ");
            process(node.getRelation(), 0);

            return null;
        }

        @Override
        protected Void visitShowFunctions(ShowFunctions node, Integer context)
        {
            builder.append("SHOW FUNCTIONS");

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent((value) ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowSession(ShowSession node, Integer context)
        {
            builder.append("SHOW SESSION");

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent((value) ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitDelete(Delete node, Integer context)
        {
            builder.append("DELETE FROM ")
                    .append(formatName(node.getTable().getName()));

            if (node.getWhere().isPresent()) {
                builder.append(" WHERE ")
                        .append(formatExpression(node.getWhere().get()));
            }

            return null;
        }

        @Override
        protected Void visitCreateSchema(CreateSchema node, Integer context)
        {
            builder.append("CREATE SCHEMA ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getSchemaName()));
            if (node.getPrincipal().isPresent()) {
                builder.append("\nAUTHORIZATION ")
                        .append(formatPrincipal(node.getPrincipal().get()));
            }
            builder.append(formatPropertiesMultiLine(node.getProperties()));

            return null;
        }

        @Override
        protected Void visitDropSchema(DropSchema node, Integer context)
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
        protected Void visitRenameSchema(RenameSchema node, Integer context)
        {
            builder.append("ALTER SCHEMA ")
                    .append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatExpression(node.getTarget()));

            return null;
        }

        @Override
        protected Void visitSetSchemaAuthorization(SetSchemaAuthorization node, Integer context)
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
            builder.append("CREATE TABLE ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getName()));

            if (node.getColumnAliases().isPresent()) {
                String columnList = node.getColumnAliases().get().stream()
                        .map(ExpressionFormatter::formatExpression)
                        .collect(joining(", "));
                builder.append(format("( %s )", columnList));
            }

            if (node.getComment().isPresent()) {
                builder.append("\nCOMMENT ")
                        .append(formatStringLiteral(node.getComment().get()));
            }

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
            builder.append("CREATE TABLE ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            String tableName = formatName(node.getName());
            builder.append(tableName).append(" (\n");

            String elementIndent = indentString(indent + 1);
            String columnList = node.getElements().stream()
                    .map(element -> {
                        if (element instanceof ColumnDefinition) {
                            ColumnDefinition column = (ColumnDefinition) element;
                            return elementIndent + formatColumnDefinition(column);
                        }
                        if (element instanceof LikeClause) {
                            LikeClause likeClause = (LikeClause) element;
                            StringBuilder builder = new StringBuilder(elementIndent);
                            builder.append("LIKE ")
                                    .append(formatName(likeClause.getTableName()));
                            if (likeClause.getPropertiesOption().isPresent()) {
                                builder.append(" ")
                                        .append(likeClause.getPropertiesOption().get().name())
                                        .append(" PROPERTIES");
                            }
                            return builder.toString();
                        }
                        throw new UnsupportedOperationException("unknown table element: " + element);
                    })
                    .collect(joining(",\n"));
            builder.append(columnList);
            builder.append("\n").append(")");

            if (node.getComment().isPresent()) {
                builder.append("\nCOMMENT ")
                        .append(formatStringLiteral(node.getComment().get()));
            }

            builder.append(formatPropertiesMultiLine(node.getProperties()));

            return null;
        }

        private String formatPropertiesMultiLine(List<Property> properties)
        {
            if (properties.isEmpty()) {
                return "";
            }

            String propertyList = properties.stream()
                    .map(element -> INDENT +
                            formatExpression(element.getName()) + " = " +
                            formatExpression(element.getValue()))
                    .collect(joining(",\n"));

            return "\nWITH (\n" + propertyList + "\n)";
        }

        private String formatPropertiesSingleLine(List<Property> properties)
        {
            if (properties.isEmpty()) {
                return "";
            }

            String propertyList = properties.stream()
                    .map(element -> formatExpression(element.getName()) + " = " +
                            formatExpression(element.getValue()))
                    .collect(joining(", "));

            return " WITH ( " + propertyList + " )";
        }

        private String formatColumnDefinition(ColumnDefinition column)
        {
            StringBuilder sb = new StringBuilder()
                    .append(formatExpression(column.getName()))
                    .append(" ").append(column.getType());
            if (!column.isNullable()) {
                sb.append(" NOT NULL");
            }
            column.getComment().ifPresent(comment ->
                    sb.append(" COMMENT ").append(formatStringLiteral(comment)));
            sb.append(formatPropertiesSingleLine(column.getProperties()));
            return sb.toString();
        }

        private static String formatGrantor(GrantorSpecification grantor)
        {
            GrantorSpecification.Type type = grantor.getType();
            switch (type) {
                case CURRENT_ROLE:
                case CURRENT_USER:
                    return type.name();
                case PRINCIPAL:
                    return formatPrincipal(grantor.getPrincipal().get());
            }
            throw new IllegalArgumentException("Unsupported principal type: " + type);
        }

        private static String formatPrincipal(PrincipalSpecification principal)
        {
            PrincipalSpecification.Type type = principal.getType();
            switch (type) {
                case UNSPECIFIED:
                    return principal.getName().toString();
                case USER:
                case ROLE:
                    return format("%s %s", type.name(), principal.getName().toString());
            }
            throw new IllegalArgumentException("Unsupported principal type: " + type);
        }

        @Override
        protected Void visitDropTable(DropTable node, Integer context)
        {
            builder.append("DROP TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getTableName());

            return null;
        }

        @Override
        protected Void visitRenameTable(RenameTable node, Integer context)
        {
            builder.append("ALTER TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getSource())
                    .append(" RENAME TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitComment(Comment node, Integer context)
        {
            String comment = node.getComment().isPresent() ? formatStringLiteral(node.getComment().get()) : "NULL";

            switch (node.getType()) {
                case TABLE:
                    builder.append("COMMENT ON TABLE ")
                            .append(node.getName())
                            .append(" IS ")
                            .append(comment);
                    break;
                case COLUMN:
                    builder.append("COMMENT ON COLUMN ")
                            .append(node.getName())
                            .append(" IS ")
                            .append(comment);
                    break;
            }

            return null;
        }

        @Override
        protected Void visitRenameColumn(RenameColumn node, Integer context)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getTable())
                    .append(" RENAME COLUMN ");
            if (node.isColumnExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.getSource())
                    .append(" TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitDropColumn(DropColumn node, Integer context)
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
            builder.append(formatExpression(node.getColumn()));

            return null;
        }

        @Override
        protected Void visitAnalyze(Analyze node, Integer context)
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
            builder.append(node.getName())
                    .append(" ADD COLUMN ");
            if (node.isColumnNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatColumnDefinition(node.getColumn()));

            return null;
        }

        @Override
        protected Void visitSetTableAuthorization(SetTableAuthorization node, Integer context)
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
                    .append(node.getTarget());

            if (node.getColumns().isPresent()) {
                builder.append(" (")
                        .append(Joiner.on(", ").join(node.getColumns().get()))
                        .append(")");
            }

            builder.append("\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitUpdate(Update node, Integer indent)
        {
            builder.append("UPDATE ")
                    .append(node.getTable().getName())
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
            if (node.getWhere().isPresent()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append("WHERE ").append(formatExpression(node.getWhere().get()));
            }
            return null;
        }

        @Override
        public Void visitSetSession(SetSession node, Integer context)
        {
            builder.append("SET SESSION ")
                    .append(node.getName())
                    .append(" = ")
                    .append(formatExpression(node.getValue()));

            return null;
        }

        @Override
        public Void visitResetSession(ResetSession node, Integer context)
        {
            builder.append("RESET SESSION ")
                    .append(node.getName());

            return null;
        }

        @Override
        protected Void visitCallArgument(CallArgument node, Integer indent)
        {
            if (node.getName().isPresent()) {
                builder.append(node.getName().get())
                        .append(" => ");
            }
            builder.append(formatExpression(node.getValue()));

            return null;
        }

        @Override
        protected Void visitCall(Call node, Integer indent)
        {
            builder.append("CALL ")
                    .append(node.getName())
                    .append("(");

            Iterator<CallArgument> arguments = node.getArguments().iterator();
            while (arguments.hasNext()) {
                process(arguments.next(), indent);
                if (arguments.hasNext()) {
                    builder.append(", ");
                }
            }

            builder.append(")");

            return null;
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
        protected Void visitTransactionAccessMode(TransactionAccessMode node, Integer context)
        {
            builder.append(node.isReadOnly() ? "READ ONLY" : "READ WRITE");
            return null;
        }

        @Override
        protected Void visitCommit(Commit node, Integer context)
        {
            builder.append("COMMIT");
            return null;
        }

        @Override
        protected Void visitRollback(Rollback node, Integer context)
        {
            builder.append("ROLLBACK");
            return null;
        }

        @Override
        protected Void visitCreateRole(CreateRole node, Integer context)
        {
            builder.append("CREATE ROLE ").append(node.getName());
            if (node.getGrantor().isPresent()) {
                builder.append(" WITH ADMIN ").append(formatGrantor(node.getGrantor().get()));
            }
            return null;
        }

        @Override
        protected Void visitDropRole(DropRole node, Integer context)
        {
            builder.append("DROP ROLE ").append(node.getName());
            return null;
        }

        @Override
        protected Void visitGrantRoles(GrantRoles node, Integer context)
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
            if (node.getGrantor().isPresent()) {
                builder.append(" GRANTED BY ").append(formatGrantor(node.getGrantor().get()));
            }
            return null;
        }

        @Override
        protected Void visitRevokeRoles(RevokeRoles node, Integer context)
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
            if (node.getGrantor().isPresent()) {
                builder.append(" GRANTED BY ").append(formatGrantor(node.getGrantor().get()));
            }
            return null;
        }

        @Override
        protected Void visitSetRole(SetRole node, Integer context)
        {
            builder.append("SET ROLE ");
            SetRole.Type type = node.getType();
            switch (type) {
                case ALL:
                case NONE:
                    builder.append(type.toString());
                    return null;
                case ROLE:
                    builder.append(node.getRole().get());
                    return null;
            }
            throw new IllegalArgumentException("Unsupported type: " + type);
        }

        @Override
        public Void visitGrant(Grant node, Integer indent)
        {
            builder.append("GRANT ");

            if (node.getPrivileges().isPresent()) {
                builder.append(node.getPrivileges().get().stream()
                        .collect(joining(", ")));
            }
            else {
                builder.append("ALL PRIVILEGES");
            }

            builder.append(" ON ");
            if (node.getType().isPresent()) {
                builder.append(node.getType().get());
                builder.append(" ");
            }
            builder.append(formatName(node.getName()))
                    .append(" TO ")
                    .append(formatPrincipal(node.getGrantee()));
            if (node.isWithGrantOption()) {
                builder.append(" WITH GRANT OPTION");
            }

            return null;
        }

        @Override
        public Void visitRevoke(Revoke node, Integer indent)
        {
            builder.append("REVOKE ");

            if (node.isGrantOptionFor()) {
                builder.append("GRANT OPTION FOR ");
            }

            if (node.getPrivileges().isPresent()) {
                builder.append(node.getPrivileges().get().stream()
                        .collect(joining(", ")));
            }
            else {
                builder.append("ALL PRIVILEGES");
            }

            builder.append(" ON ");
            if (node.getType().isPresent()) {
                builder.append(node.getType().get());
                builder.append(" ");
            }
            builder.append(node.getName())
                    .append(" FROM ")
                    .append(formatPrincipal(node.getGrantee()));

            return null;
        }

        @Override
        public Void visitShowGrants(ShowGrants node, Integer indent)
        {
            builder.append("SHOW GRANTS ");

            if (node.getTableName().isPresent()) {
                builder.append("ON ");

                if (node.getTable()) {
                    builder.append("TABLE ");
                }
                builder.append(node.getTableName().get());
            }

            return null;
        }

        @Override
        protected Void visitShowRoles(ShowRoles node, Integer context)
        {
            builder.append("SHOW ");
            if (node.isCurrent()) {
                builder.append("CURRENT ");
            }
            builder.append("ROLES");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

            return null;
        }

        @Override
        protected Void visitShowRoleGrants(ShowRoleGrants node, Integer context)
        {
            builder.append("SHOW ROLE GRANTS");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

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
            builder.append(node.getTimeZone().map(ExpressionFormatter::formatExpression).orElse("LOCAL"));
            return null;
        }

        private void processRelation(Relation relation, Integer indent)
        {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(((Table) relation).getName())
                        .append('\n');
            }
            else {
                process(relation, indent);
            }
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
