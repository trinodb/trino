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
package io.prestosql.sql.rewrite;

import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataUtil;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.Except;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Insert;
import io.prestosql.sql.tree.Intersect;
import io.prestosql.sql.tree.Join;
import io.prestosql.sql.tree.Lateral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QueryBody;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.SampledRelation;
import io.prestosql.sql.tree.SetOperation;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.TableSubquery;
import io.prestosql.sql.tree.Union;
import io.prestosql.sql.tree.Unnest;
import io.prestosql.sql.tree.Values;
import io.prestosql.sql.tree.With;
import io.prestosql.sql.tree.WithQuery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.util.Objects.requireNonNull;

public class RowFilterColumnMaskingRewrite
        implements StatementRewrite.Rewrite
{
    private static final Logger log = Logger.get(RowFilterColumnMaskingRewrite.class);

    @Override
    public Statement rewrite(Session session, Metadata metadata, SqlParser parser, Optional<QueryExplainer>
            queryExplainer, Statement node, List<Expression> parameters, AccessControl accessControl, WarningCollector warningCollector)
    {
        log.info("Audit log: Input Query:" + node);
        Statement modifiedStatement = (Statement) new Visitor(metadata, parser, session, parameters, accessControl,
                queryExplainer).process(node, null);
        log.info("Audit log: Modified Query:" + node);
        return modifiedStatement;
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;
        List<Expression> parameters;
        private final AccessControl accessControl;
        private Optional<QueryExplainer> queryExplainer;

        public Visitor(Metadata metadata, SqlParser sqlParser, Session session, List<Expression> parameters,
                       AccessControl accessControl, Optional<QueryExplainer> queryExplainer)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.parameters = requireNonNull(parameters, "parameters is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        }

        @Override
        protected Node visitQueryBody(QueryBody node, Void context)
        {
            return node.accept(this, context);
        }

        @Override
        protected Node visitRelation(Relation node, Void context)
        {
            return node.accept(this, context);
        }

        protected Node visitWith(With node, Void context)
        {
            List<WithQuery> withQueries = new ArrayList<>();
            for (WithQuery withQuery : node.getQueries()) {
                withQueries.add((WithQuery) visitWithQuery(withQuery, context));
            }
            return node.getLocation().isPresent() ?
                    new With(node.getLocation().get(), node.isRecursive(), withQueries) :
                    new With(node.isRecursive(), withQueries);
        }

        protected Node visitWithQuery(WithQuery node, Void context)
        {
            Query query = (Query) visitQuery(node.getQuery(), context);

            return node.getLocation().isPresent() ?
                    new WithQuery(node.getLocation().get(), node.getName(), query, node.getColumnNames()) :
                    new WithQuery(node.getName(), query, node.getColumnNames());
        }

        @Override
        protected Node visitLateral(Lateral lateral, Void context)
        {
            Query query = (Query) visitQuery(lateral.getQuery(), context);
            return lateral.getLocation().isPresent() ?
                    new Lateral(query.getLocation().get(), query) :
                    new Lateral(query);
        }

        @Override
        protected Node visitIntersect(Intersect intersect, Void context)
        {
            List<Relation> relations = new ArrayList<>();
            for (Relation relation : intersect.getRelations()) {
                relations.add((Relation) visitRelation(relation, context));
            }
            return intersect.getLocation().isPresent() ?
                    new Intersect(intersect.getLocation().get(), relations, intersect.isDistinct()) :
                    new Intersect(relations, intersect.isDistinct());
        }

        @Override
        protected Node visitAliasedRelation(AliasedRelation aliasedRelation, Void context)
        {
            Relation relation = (Relation) visitRelation(aliasedRelation.getRelation(), context);
            if (aliasedRelation.getLocation().isPresent()) {
                return new AliasedRelation(aliasedRelation.getLocation().get(), relation, aliasedRelation.getAlias(), aliasedRelation.getColumnNames());
            }
            else {
                return new AliasedRelation(relation, aliasedRelation.getAlias(), aliasedRelation.getColumnNames());
            }
        }

        @Override
        protected Node visitExcept(Except except, Void context)
        {
            Relation relationLeft = (Relation) visitRelation(except.getLeft(), context);
            Relation relationRight = (Relation) visitRelation(except.getRight(), context);

            return except.getLocation().isPresent() ?
                    new Except(except.getLocation().get(), relationLeft, relationRight, except.isDistinct()) :
                    new Except(relationLeft, relationRight, except.isDistinct());
        }

        @Override
        protected Node visitJoin(Join join, Void context)
        {
            Relation relationLeft = (Relation) visitRelation(join.getLeft(), context);
            Relation relationRight = (Relation) visitRelation(join.getRight(), context);

            return join.getLocation().isPresent() ?
                    new Join(join.getLocation().get(), join.getType(), relationLeft, relationRight, join.getCriteria()) :
                    new Join(join.getType(), relationLeft, relationRight, join.getCriteria());
        }

        @Override
        protected Node visitSampledRelation(SampledRelation sampledRelation, Void context)
        {
            Relation relation = (Relation) visitRelation(sampledRelation.getRelation(), context);
            return sampledRelation.getLocation().isPresent() ?
                    new SampledRelation(sampledRelation.getLocation().get(), relation, sampledRelation.getType(), sampledRelation.getSamplePercentage()) :
                    new SampledRelation(relation, sampledRelation.getType(), sampledRelation.getSamplePercentage());
        }

        @Override
        protected Node visitSetOperation(SetOperation setOperation, Void context)
        {
            return visitNode(setOperation, context);
        }

        @Override
        protected Node visitTable(Table table, Void context)
        {
            log.info("Table visited:" + table.getName());

            if (!MetadataUtil.tableExists(metadata, session, table.getName().getSuffix())) {
                return table;
            }

            QualifiedObjectName qualifiedObjectName = MetadataUtil.createQualifiedObjectName(session, table, table.getName());
            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, qualifiedObjectName);
            Query query = null;
            if (tableHandle.isPresent()) {
                Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

                Set<String> columnSet = new HashSet<String>();
                for (Map.Entry<String, ColumnHandle> columnHandleEntry : columnHandles.entrySet()) {
                    ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle.get(), columnHandleEntry.getValue());
                    if (!columnMetadata.isHidden()) {
                        String columnName = columnMetadata.getName();
                        columnSet.add(columnName);
                    }
                }
                String inlineView = accessControl.applyTableInlineView(session.getTransactionId().get(), session.getIdentity(),
                        qualifiedObjectName, Optional.of(columnSet));
                if (inlineView != null) {
                    Statement stmt = sqlParser.createStatement(inlineView, new ParsingOptions(AS_DECIMAL));
                    query = (Query) stmt;
                    log.info("Aliased table relation being created:" + table.getName());
                    return new AliasedRelation(new TableSubquery(query), new Identifier(table.getName().toString()), null);
                }
            }
            log.info("Table being returned:" + table.getName());
            return table;
        }

        @Override
        protected Node visitTableSubquery(TableSubquery subquery, Void context)
        {
            Query query = (Query) visitQuery(subquery.getQuery(), context);
            return subquery.getLocation().isPresent() ?
                    new TableSubquery(subquery.getLocation().get(), query) :
                    new TableSubquery(query);
        }

        @Override
        protected Node visitUnion(Union subquery, Void context)
        {
            List<Relation> relations = new ArrayList<>();
            for (Relation relation : subquery.getRelations()) {
                relations.add((Relation) visitRelation(relation, context));
            }
            return subquery.getLocation().isPresent() ?
                    new Union(subquery.getLocation().get(), relations, subquery.isDistinct()) :
                    new Union(relations, subquery.isDistinct());
        }

        @Override
        protected Node visitValues(Values subquery, Void context)
        {
            return visitNode(subquery, context);
        }

        @Override
        protected Node visitUnnest(Unnest subquery, Void context)
        {
            return visitNode(subquery, context);
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        @Override
        protected Node visitQuery(Query node, Void context)
        {
            Optional<With> with = node.getWith();
            if (with.isPresent()) {
                with = Optional.of((With) visitWith(with.get(), context));
            }

            if (node.getLocation().isPresent()) {
                return new Query(node.getLocation().get(), with, (QueryBody) visitQueryBody(node.getQueryBody(),
                        context), node
                        .getOrderBy(), node.getLimit());
            }
            else {
                return new Query(with, (QueryBody) visitQueryBody(node.getQueryBody(),
                        context), node
                        .getOrderBy(), node.getLimit());
            }
        }

        @Override
        protected Node visitCreateTableAsSelect(CreateTableAsSelect createTableAsSelect, Void context)
        {
            Query query = (Query) visitQuery(createTableAsSelect.getQuery(), context);
            return createTableAsSelect.getLocation().isPresent() ?
                    new CreateTableAsSelect(createTableAsSelect.getLocation().get(), createTableAsSelect.getName(), query,
                            createTableAsSelect.isNotExists(), createTableAsSelect.getProperties(), createTableAsSelect.isWithData(),
                            createTableAsSelect.getColumnAliases(), createTableAsSelect.getComment()) :
                    new CreateTableAsSelect(createTableAsSelect.getName(), query, createTableAsSelect.isNotExists(),
                            createTableAsSelect.getProperties(), createTableAsSelect.isWithData(), createTableAsSelect.getColumnAliases(),
                            createTableAsSelect.getComment());
        }

        @Override
        protected Node visitInsert(Insert insert, Void context)
        {
            return new Insert(insert.getTarget(), insert.getColumns(), (Query) visitQuery(insert.getQuery(), context));
        }

        @Override
        protected Node visitQuerySpecification(QuerySpecification node, Void context)
        {
            Optional<Relation> relation = Optional.empty();
            if (node.getFrom().isPresent()) {
                relation = Optional.of((Relation) visitRelation(node.getFrom().get(), context));
            }
            return node.getLocation().isPresent() ?
                    new QuerySpecification(node.getLocation().get(), node
                            .getSelect(),
                            relation,
                            node.getWhere(),
                            node.getGroupBy(),
                            node.getHaving(),
                            node.getOrderBy(),
                            node.getLimit()) :
                    new QuerySpecification(node
                            .getSelect(),
                            relation,
                            node.getWhere(),
                            node.getGroupBy(),
                            node.getHaving(),
                            node.getOrderBy(),
                            node.getLimit());
        }
    }
}
