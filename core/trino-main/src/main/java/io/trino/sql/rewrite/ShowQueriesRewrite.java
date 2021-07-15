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
package io.trino.sql.rewrite;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.cost.StatsCalculator;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionKind;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataUtil;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.SessionPropertyManager.SessionPropertyValue;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.PrincipalSpecification;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.ShowCatalogs;
import io.trino.sql.tree.ShowColumns;
import io.trino.sql.tree.ShowCreate;
import io.trino.sql.tree.ShowFunctions;
import io.trino.sql.tree.ShowGrants;
import io.trino.sql.tree.ShowRoleGrants;
import io.trino.sql.tree.ShowRoles;
import io.trino.sql.tree.ShowSchemas;
import io.trino.sql.tree.ShowSession;
import io.trino.sql.tree.ShowTables;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.TableElement;
import io.trino.sql.tree.Values;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.connector.informationschema.InformationSchemaTable.COLUMNS;
import static io.trino.connector.informationschema.InformationSchemaTable.ENABLED_ROLES;
import static io.trino.connector.informationschema.InformationSchemaTable.ROLES;
import static io.trino.connector.informationschema.InformationSchemaTable.SCHEMATA;
import static io.trino.connector.informationschema.InformationSchemaTable.TABLES;
import static io.trino.connector.informationschema.InformationSchemaTable.TABLE_PRIVILEGES;
import static io.trino.metadata.MetadataListing.listCatalogs;
import static io.trino.metadata.MetadataListing.listSchemas;
import static io.trino.metadata.MetadataUtil.createCatalogSchemaName;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_MATERIALIZED_VIEW_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static io.trino.spi.StandardErrorCode.MISSING_CATALOG_NAME;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.QueryUtil.aliased;
import static io.trino.sql.QueryUtil.aliasedName;
import static io.trino.sql.QueryUtil.aliasedNullToEmpty;
import static io.trino.sql.QueryUtil.ascending;
import static io.trino.sql.QueryUtil.equal;
import static io.trino.sql.QueryUtil.functionCall;
import static io.trino.sql.QueryUtil.identifier;
import static io.trino.sql.QueryUtil.logicalAnd;
import static io.trino.sql.QueryUtil.ordering;
import static io.trino.sql.QueryUtil.row;
import static io.trino.sql.QueryUtil.selectAll;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.singleValueQuery;
import static io.trino.sql.QueryUtil.table;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.CreateView.Security.DEFINER;
import static io.trino.sql.tree.CreateView.Security.INVOKER;
import static io.trino.sql.tree.LogicalBinaryExpression.and;
import static io.trino.sql.tree.ShowCreate.Type.MATERIALIZED_VIEW;
import static io.trino.sql.tree.ShowCreate.Type.SCHEMA;
import static io.trino.sql.tree.ShowCreate.Type.TABLE;
import static io.trino.sql.tree.ShowCreate.Type.VIEW;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

final class ShowQueriesRewrite
        implements StatementRewrite.Rewrite
{
    @Override
    public Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            GroupProvider groupProvider,
            AccessControl accessControl,
            WarningCollector warningCollector,
            StatsCalculator statsCalculator)
    {
        return (Statement) new Visitor(metadata, parser, session, accessControl).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;
        private final AccessControl accessControl;

        public Visitor(Metadata metadata, SqlParser sqlParser, Session session, AccessControl accessControl)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
        }

        @Override
        protected Node visitExplain(Explain node, Void context)
        {
            Statement statement = (Statement) process(node.getStatement(), null);
            return new Explain(
                    node.getLocation().get(),
                    node.isAnalyze(),
                    node.isVerbose(),
                    statement,
                    node.getOptions());
        }

        @Override
        protected Node visitShowTables(ShowTables showTables, Void context)
        {
            CatalogSchemaName schema = createCatalogSchemaName(session, showTables, showTables.getSchema());

            accessControl.checkCanShowTables(session.toSecurityContext(), schema);

            if (!metadata.catalogExists(session, schema.getCatalogName())) {
                throw semanticException(CATALOG_NOT_FOUND, showTables, "Catalog '%s' does not exist", schema.getCatalogName());
            }

            if (!metadata.schemaExists(session, schema)) {
                throw semanticException(SCHEMA_NOT_FOUND, showTables, "Schema '%s' does not exist", schema.getSchemaName());
            }

            Expression predicate = equal(identifier("table_schema"), new StringLiteral(schema.getSchemaName()));

            Optional<String> likePattern = showTables.getLikePattern();
            if (likePattern.isPresent()) {
                Expression likePredicate = new LikePredicate(
                        identifier("table_name"),
                        new StringLiteral(likePattern.get()),
                        showTables.getEscape().map(StringLiteral::new));
                predicate = logicalAnd(predicate, likePredicate);
            }

            return simpleQuery(
                    selectList(aliasedName("table_name", "Table")),
                    from(schema.getCatalogName(), TABLES.getSchemaTableName()),
                    predicate,
                    ordering(ascending("table_name")));
        }

        @Override
        protected Node visitShowGrants(ShowGrants showGrants, Void context)
        {
            // TODO: make this method redirection aware
            String catalogName = session.getCatalog().orElse(null);
            Optional<Expression> predicate = Optional.empty();

            Optional<QualifiedName> tableName = showGrants.getTableName();
            if (tableName.isPresent()) {
                QualifiedObjectName qualifiedTableName = createQualifiedObjectName(session, showGrants, tableName.get());

                if (metadata.getView(session, qualifiedTableName).isEmpty() &&
                        metadata.getTableHandle(session, qualifiedTableName).isEmpty()) {
                    throw semanticException(TABLE_NOT_FOUND, showGrants, "Table '%s' does not exist", tableName);
                }

                catalogName = qualifiedTableName.getCatalogName();

                // Check is wrong here, it should be accessControl#checkCanShowGrants() which is not yet implemented
                accessControl.checkCanShowTables(
                        session.toSecurityContext(),
                        new CatalogSchemaName(catalogName, qualifiedTableName.getSchemaName()));

                predicate = Optional.of(combineConjuncts(
                        metadata,
                        equal(identifier("table_schema"), new StringLiteral(qualifiedTableName.getSchemaName())),
                        equal(identifier("table_name"), new StringLiteral(qualifiedTableName.getObjectName()))));
            }
            else {
                if (catalogName == null) {
                    throw semanticException(MISSING_CATALOG_NAME, showGrants, "Catalog must be specified when session catalog is not set");
                }

                Set<String> allowedSchemas = listSchemas(session, metadata, accessControl, catalogName);
                for (String schema : allowedSchemas) {
                    accessControl.checkCanShowTables(session.toSecurityContext(), new CatalogSchemaName(catalogName, schema));
                }
            }

            return simpleQuery(
                    selectList(
                            aliasedName("grantor", "Grantor"),
                            aliasedName("grantor_type", "Grantor Type"),
                            aliasedName("grantee", "Grantee"),
                            aliasedName("grantee_type", "Grantee Type"),
                            aliasedName("table_catalog", "Catalog"),
                            aliasedName("table_schema", "Schema"),
                            aliasedName("table_name", "Table"),
                            aliasedName("privilege_type", "Privilege"),
                            aliasedName("is_grantable", "Grantable"),
                            aliasedName("with_hierarchy", "With Hierarchy")),
                    from(catalogName, TABLE_PRIVILEGES.getSchemaTableName()),
                    predicate,
                    Optional.empty());
        }

        @Override
        protected Node visitShowRoles(ShowRoles node, Void context)
        {
            if (node.getCatalog().isEmpty() && session.getCatalog().isEmpty()) {
                throw semanticException(MISSING_CATALOG_NAME, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(c -> c.getValue().toLowerCase(ENGLISH)).orElseGet(() -> session.getCatalog().get());

            if (node.isCurrent()) {
                accessControl.checkCanShowCurrentRoles(session.toSecurityContext(), catalog);
                return simpleQuery(
                        selectList(aliasedName("role_name", "Role")),
                        from(catalog, ENABLED_ROLES.getSchemaTableName()));
            }
            else {
                accessControl.checkCanShowRoles(session.toSecurityContext(), catalog);
                return simpleQuery(
                        selectList(aliasedName("role_name", "Role")),
                        from(catalog, ROLES.getSchemaTableName()));
            }
        }

        @Override
        protected Node visitShowRoleGrants(ShowRoleGrants node, Void context)
        {
            if (node.getCatalog().isEmpty() && session.getCatalog().isEmpty()) {
                throw semanticException(MISSING_CATALOG_NAME, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(c -> c.getValue().toLowerCase(ENGLISH)).orElseGet(() -> session.getCatalog().get());
            TrinoPrincipal principal = new TrinoPrincipal(PrincipalType.USER, session.getUser());

            accessControl.checkCanShowRoleGrants(session.toSecurityContext(), catalog);
            List<Expression> rows = metadata.listRoleGrants(session, catalog, principal).stream()
                    .map(roleGrant -> row(new StringLiteral(roleGrant.getRoleName())))
                    .collect(toList());

            return simpleQuery(
                    selectList(new AllColumns()),
                    aliased(new Values(rows), "role_grants", ImmutableList.of("Role Grants")),
                    ordering(ascending("Role Grants")));
        }

        @Override
        protected Node visitShowSchemas(ShowSchemas node, Void context)
        {
            if (node.getCatalog().isEmpty() && session.getCatalog().isEmpty()) {
                throw semanticException(MISSING_CATALOG_NAME, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(Identifier::getValue).orElseGet(() -> session.getCatalog().get());
            accessControl.checkCanShowSchemas(session.toSecurityContext(), catalog);

            Optional<Expression> predicate = Optional.empty();
            Optional<String> likePattern = node.getLikePattern();
            if (likePattern.isPresent()) {
                predicate = Optional.of(new LikePredicate(
                        identifier("schema_name"),
                        new StringLiteral(likePattern.get()),
                        node.getEscape().map(StringLiteral::new)));
            }

            return simpleQuery(
                    selectList(aliasedName("schema_name", "Schema")),
                    from(catalog, SCHEMATA.getSchemaTableName()),
                    predicate,
                    Optional.of(ordering(ascending("schema_name"))));
        }

        @Override
        protected Node visitShowCatalogs(ShowCatalogs node, Void context)
        {
            List<Expression> rows = listCatalogs(session, metadata, accessControl).keySet().stream()
                    .map(name -> row(new StringLiteral(name)))
                    .collect(toImmutableList());

            Optional<Expression> predicate = Optional.empty();
            if (rows.isEmpty()) {
                rows = ImmutableList.of(new StringLiteral(""));
                predicate = Optional.of(BooleanLiteral.FALSE_LITERAL);
            }
            else if (node.getLikePattern().isPresent()) {
                predicate = Optional.of(new LikePredicate(
                        identifier("catalog"),
                        new StringLiteral(node.getLikePattern().get()),
                        node.getEscape().map(StringLiteral::new)));
            }

            return simpleQuery(
                    selectList(new AllColumns()),
                    aliased(new Values(rows), "catalogs", ImmutableList.of("Catalog")),
                    predicate,
                    Optional.of(ordering(ascending("Catalog"))));
        }

        @Override
        protected Node visitShowColumns(ShowColumns showColumns, Void context)
        {
            QualifiedObjectName tableName = createQualifiedObjectName(session, showColumns, showColumns.getTable());
            if (metadata.getCatalogHandle(session, tableName.getCatalogName()).isEmpty()) {
                throw semanticException(CATALOG_NOT_FOUND, showColumns, "Catalog '%s' does not exist", tableName.getCatalogName());
            }
            if (!metadata.schemaExists(session, new CatalogSchemaName(tableName.getCatalogName(), tableName.getSchemaName()))) {
                throw semanticException(SCHEMA_NOT_FOUND, showColumns, "Schema '%s' does not exist", tableName.getSchemaName());
            }

            boolean isMaterializedView = metadata.getMaterializedView(session, tableName).isPresent();
            boolean isView = false;
            QualifiedObjectName targetTableName = tableName;
            Optional<TableHandle> tableHandle = Optional.empty();
            // Check for view if materialized view is not present
            if (!isMaterializedView) {
                isView = metadata.getView(session, tableName).isPresent();
                // Check for table if view is not present
                if (!isView) {
                    RedirectionAwareTableHandle redirection = metadata.getRedirectionAwareTableHandle(session, tableName);
                    tableHandle = redirection.getTableHandle();
                    if (tableHandle.isEmpty()) {
                        throw semanticException(TABLE_NOT_FOUND, showColumns, "Table '%s' does not exist", tableName);
                    }
                    targetTableName = redirection.getRedirectedTableName().orElse(tableName);
                }
            }

            if (!isMaterializedView && !isView) {
                // We are using information_schema which may ignore errors when getting the list
                // of columns for a table, since listing columns is a requirement for some tools,
                // and thus failing due to a single bad table would make the system unusable.
                //
                // However, when showing columns for a single table, it is important to fail if
                // the columns are not available, rather than erroneously returning an empty list.
                // We thus ask for table metadata, which will hopefully fail for the same reasons
                // that would cause an empty list of columns.
                //
                // We still go through information_schema, even though we appear to have all the
                // needed information in the table metadata, so that we use the same code path for
                // all column listing. Connectors may have different listing logic than for metadata,
                // and we need to perform security filtering of the returned columns.
                metadata.getTableMetadata(session, tableHandle.get());
            }

            accessControl.checkCanShowColumns(session.toSecurityContext(), targetTableName.asCatalogSchemaTableName());

            Expression predicate = logicalAnd(
                    equal(identifier("table_schema"), new StringLiteral(targetTableName.getSchemaName())),
                    equal(identifier("table_name"), new StringLiteral(targetTableName.getObjectName())));
            Optional<String> likePattern = showColumns.getLikePattern();
            if (likePattern.isPresent()) {
                Expression likePredicate = new LikePredicate(
                        identifier("column_name"),
                        new StringLiteral(likePattern.get()),
                        showColumns.getEscape().map(StringLiteral::new));
                predicate = logicalAnd(predicate, likePredicate);
            }

            return simpleQuery(
                    selectList(
                            aliasedName("column_name", "Column"),
                            aliasedName("data_type", "Type"),
                            aliasedNullToEmpty("extra_info", "Extra"),
                            aliasedNullToEmpty("comment", "Comment")),
                    from(targetTableName.getCatalogName(), COLUMNS.getSchemaTableName()),
                    predicate,
                    ordering(ascending("ordinal_position")));
        }

        private static <T> Expression getExpression(PropertyMetadata<T> property, Object value)
                throws TrinoException
        {
            return toExpression(property.encode(property.getJavaType().cast(value)));
        }

        private static Expression toExpression(Object value)
                throws TrinoException
        {
            if (value instanceof String) {
                return new StringLiteral(value.toString());
            }

            if (value instanceof Boolean) {
                return new BooleanLiteral(value.toString());
            }

            if (value instanceof Long || value instanceof Integer) {
                return new LongLiteral(value.toString());
            }

            if (value instanceof Double) {
                return new DoubleLiteral(value.toString());
            }

            if (value instanceof List) {
                List<?> list = (List<?>) value;
                return new ArrayConstructor(list.stream()
                        .map(Visitor::toExpression)
                        .collect(toList()));
            }

            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Failed to convert object of type %s to expression: %s", value.getClass().getName(), value));
        }

        @Override
        protected Node visitShowCreate(ShowCreate node, Void context)
        {
            if (node.getType() == MATERIALIZED_VIEW) {
                QualifiedObjectName objectName = createQualifiedObjectName(session, node, node.getName());
                Optional<ConnectorMaterializedViewDefinition> viewDefinition = metadata.getMaterializedView(session, objectName);

                if (viewDefinition.isEmpty()) {
                    if (metadata.getView(session, objectName).isPresent()) {
                        throw semanticException(NOT_SUPPORTED, node, "Relation '%s' is a view, not a materialized view", objectName);
                    }

                    if (metadata.getTableHandle(session, objectName).isPresent()) {
                        throw semanticException(NOT_SUPPORTED, node, "Relation '%s' is a table, not a materialized view", objectName);
                    }

                    throw semanticException(TABLE_NOT_FOUND, node, "Materialized view '%s' does not exist", objectName);
                }

                Query query = parseView(viewDefinition.get().getOriginalSql(), objectName, node);
                List<Identifier> parts = Lists.reverse(node.getName().getOriginalParts());
                Identifier tableName = parts.get(0);
                Identifier schemaName = (parts.size() > 1) ? parts.get(1) : new Identifier(objectName.getSchemaName());
                Identifier catalogName = (parts.size() > 2) ? parts.get(2) : new Identifier(objectName.getCatalogName());

                accessControl.checkCanShowCreateTable(session.toSecurityContext(), new QualifiedObjectName(catalogName.getValue(), schemaName.getValue(), tableName.getValue()));

                Map<String, Object> properties = viewDefinition.get().getProperties();
                Map<String, PropertyMetadata<?>> allMaterializedViewProperties = metadata.getMaterializedViewPropertyManager().getAllProperties().get(new CatalogName(catalogName.getValue()));
                List<Property> propertyNodes = buildProperties(objectName, Optional.empty(), INVALID_MATERIALIZED_VIEW_PROPERTY, properties, allMaterializedViewProperties);

                String sql = formatSql(new CreateMaterializedView(Optional.empty(), QualifiedName.of(ImmutableList.of(catalogName, schemaName, tableName)),
                        query, false, false, propertyNodes, viewDefinition.get().getComment())).trim();
                return singleValueQuery("Create Materialized View", sql);
            }

            if (node.getType() == VIEW) {
                QualifiedObjectName objectName = createQualifiedObjectName(session, node, node.getName());

                if (metadata.getMaterializedView(session, objectName).isPresent()) {
                    throw semanticException(NOT_SUPPORTED, node, "Relation '%s' is a materialized view, not a view", objectName);
                }

                Optional<ConnectorViewDefinition> viewDefinition = metadata.getView(session, objectName);

                if (viewDefinition.isEmpty()) {
                    if (metadata.getTableHandle(session, objectName).isPresent()) {
                        throw semanticException(NOT_SUPPORTED, node, "Relation '%s' is a table, not a view", objectName);
                    }
                    throw semanticException(TABLE_NOT_FOUND, node, "View '%s' does not exist", objectName);
                }

                Query query = parseView(viewDefinition.get().getOriginalSql(), objectName, node);
                List<Identifier> parts = Lists.reverse(node.getName().getOriginalParts());
                Identifier tableName = parts.get(0);
                Identifier schemaName = (parts.size() > 1) ? parts.get(1) : new Identifier(objectName.getSchemaName());
                Identifier catalogName = (parts.size() > 2) ? parts.get(2) : new Identifier(objectName.getCatalogName());

                accessControl.checkCanShowCreateTable(session.toSecurityContext(), new QualifiedObjectName(catalogName.getValue(), schemaName.getValue(), tableName.getValue()));

                CreateView.Security security = viewDefinition.get().isRunAsInvoker() ? INVOKER : DEFINER;
                String sql = formatSql(new CreateView(QualifiedName.of(ImmutableList.of(catalogName, schemaName, tableName)), query, false, viewDefinition.get().getComment(), Optional.of(security))).trim();
                return singleValueQuery("Create View", sql);
            }

            if (node.getType() == TABLE) {
                QualifiedObjectName objectName = createQualifiedObjectName(session, node, node.getName());

                if (metadata.getMaterializedView(session, objectName).isPresent()) {
                    throw semanticException(NOT_SUPPORTED, node, "Relation '%s' is a materialized view, not a table", objectName);
                }

                if (metadata.getView(session, objectName).isPresent()) {
                    throw semanticException(NOT_SUPPORTED, node, "Relation '%s' is a view, not a table", objectName);
                }

                RedirectionAwareTableHandle redirection = metadata.getRedirectionAwareTableHandle(session, objectName);
                Optional<TableHandle> tableHandle = redirection.getTableHandle();
                if (tableHandle.isEmpty()) {
                    throw semanticException(TABLE_NOT_FOUND, node, "Table '%s' does not exist", objectName);
                }

                QualifiedObjectName targetTableName = redirection.getRedirectedTableName().orElse(objectName);
                accessControl.checkCanShowCreateTable(session.toSecurityContext(), targetTableName);
                ConnectorTableMetadata connectorTableMetadata = metadata.getTableMetadata(session, tableHandle.get()).getMetadata();

                Map<String, PropertyMetadata<?>> allColumnProperties = metadata.getColumnPropertyManager().getAllProperties().get(tableHandle.get().getCatalogName());

                List<TableElement> columns = connectorTableMetadata.getColumns().stream()
                        .filter(column -> !column.isHidden())
                        .map(column -> {
                            List<Property> propertyNodes = buildProperties(targetTableName, Optional.of(column.getName()), INVALID_COLUMN_PROPERTY, column.getProperties(), allColumnProperties);
                            return new ColumnDefinition(new Identifier(column.getName()), toSqlType(column.getType()), column.isNullable(), propertyNodes, Optional.ofNullable(column.getComment()));
                        })
                        .collect(toImmutableList());

                Map<String, Object> properties = connectorTableMetadata.getProperties();
                Map<String, PropertyMetadata<?>> allTableProperties = metadata.getTablePropertyManager().getAllProperties().get(tableHandle.get().getCatalogName());
                List<Property> propertyNodes = buildProperties(targetTableName, Optional.empty(), INVALID_TABLE_PROPERTY, properties, allTableProperties);

                CreateTable createTable = new CreateTable(
                        QualifiedName.of(objectName.getCatalogName(), objectName.getSchemaName(), objectName.getObjectName()),
                        columns,
                        false,
                        propertyNodes,
                        connectorTableMetadata.getComment());
                return singleValueQuery("Create Table", formatSql(createTable).trim());
            }

            if (node.getType() == SCHEMA) {
                CatalogSchemaName schemaName = createCatalogSchemaName(session, node, Optional.of(node.getName()));

                if (!metadata.schemaExists(session, schemaName)) {
                    throw semanticException(SCHEMA_NOT_FOUND, node, "Schema '%s' does not exist", schemaName);
                }

                accessControl.checkCanShowCreateSchema(session.toSecurityContext(), schemaName);

                Map<String, Object> properties = metadata.getSchemaProperties(session, schemaName);
                Map<String, PropertyMetadata<?>> allTableProperties = metadata.getSchemaPropertyManager().getAllProperties().get(new CatalogName(schemaName.getCatalogName()));
                QualifiedName qualifiedSchemaName = QualifiedName.of(schemaName.getCatalogName(), schemaName.getSchemaName());
                List<Property> propertyNodes = buildProperties(qualifiedSchemaName, Optional.empty(), INVALID_SCHEMA_PROPERTY, properties, allTableProperties);

                Optional<PrincipalSpecification> owner = metadata.getSchemaOwner(session, schemaName).map(MetadataUtil::createPrincipal);

                CreateSchema createSchema = new CreateSchema(
                        qualifiedSchemaName,
                        false,
                        propertyNodes,
                        owner);
                return singleValueQuery("Create Schema", formatSql(createSchema).trim());
            }

            throw new UnsupportedOperationException("SHOW CREATE only supported for schemas, tables and views");
        }

        private List<Property> buildProperties(
                Object objectName,
                Optional<String> columnName,
                StandardErrorCode errorCode,
                Map<String, Object> properties,
                Map<String, PropertyMetadata<?>> allProperties)
        {
            if (properties.isEmpty()) {
                return Collections.emptyList();
            }

            ImmutableSortedMap.Builder<String, Expression> sqlProperties = ImmutableSortedMap.naturalOrder();

            for (Map.Entry<String, Object> propertyEntry : properties.entrySet()) {
                String propertyName = propertyEntry.getKey();
                Object value = propertyEntry.getValue();
                if (value == null) {
                    throw new TrinoException(errorCode, format("Property %s for %s cannot have a null value", propertyName, toQualifiedName(objectName, columnName)));
                }

                PropertyMetadata<?> property = allProperties.get(propertyName);
                if (property == null) {
                    throw new TrinoException(errorCode, "No PropertyMetadata for property: " + propertyName);
                }
                if (!Primitives.wrap(property.getJavaType()).isInstance(value)) {
                    throw new TrinoException(errorCode, format(
                            "Property %s for %s should have value of type %s, not %s",
                            propertyName,
                            toQualifiedName(objectName, columnName),
                            property.getJavaType().getName(),
                            value.getClass().getName()));
                }

                Expression sqlExpression = getExpression(property, value);
                sqlProperties.put(propertyName, sqlExpression);
            }

            return sqlProperties.build().entrySet().stream()
                    .map(entry -> new Property(new Identifier(entry.getKey()), entry.getValue()))
                    .collect(toImmutableList());
        }

        private static String toQualifiedName(Object objectName, Optional<String> columnName)
        {
            return columnName.map(s -> format("column %s of table %s", s, objectName))
                    .orElseGet(() -> "table " + objectName);
        }

        @Override
        protected Node visitShowFunctions(ShowFunctions node, Void context)
        {
            List<Expression> rows = metadata.listFunctions().stream()
                    .filter(function -> !function.isHidden())
                    .map(function -> row(
                            new StringLiteral(function.getActualName()),
                            new StringLiteral(function.getSignature().getReturnType().toString()),
                            new StringLiteral(Joiner.on(", ").join(function.getSignature().getArgumentTypes())),
                            new StringLiteral(getFunctionType(function)),
                            function.isDeterministic() ? TRUE_LITERAL : FALSE_LITERAL,
                            new StringLiteral(nullToEmpty(function.getDescription()))))
                    .collect(toImmutableList());

            Map<String, String> columns = ImmutableMap.<String, String>builder()
                    .put("function_name", "Function")
                    .put("return_type", "Return Type")
                    .put("argument_types", "Argument Types")
                    .put("function_type", "Function Type")
                    .put("deterministic", "Deterministic")
                    .put("description", "Description")
                    .build();

            return simpleQuery(
                    selectAll(columns.entrySet().stream()
                            .map(entry -> aliasedName(entry.getKey(), entry.getValue()))
                            .collect(toImmutableList())),
                    aliased(new Values(rows), "functions", ImmutableList.copyOf(columns.keySet())),
                    node.getLikePattern().map(like ->
                            new LikePredicate(
                                    identifier("function_name"),
                                    new StringLiteral(like),
                                    node.getEscape().map(StringLiteral::new)))
                            .map(Expression.class::cast)
                            .orElse(TRUE_LITERAL),
                    ordering(
                            new SortItem(
                                    functionCall("lower", identifier("function_name")),
                                    SortItem.Ordering.ASCENDING,
                                    SortItem.NullOrdering.UNDEFINED),
                            ascending("return_type"),
                            ascending("argument_types"),
                            ascending("function_type")));
        }

        private static String getFunctionType(FunctionMetadata function)
        {
            FunctionKind kind = function.getKind();
            switch (kind) {
                case AGGREGATE:
                    return "aggregate";
                case WINDOW:
                    return "window";
                case SCALAR:
                    return "scalar";
            }
            throw new IllegalArgumentException("Unsupported function kind: " + kind);
        }

        @Override

        protected Node visitShowSession(ShowSession node, Void context)
        {
            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            SortedMap<String, CatalogName> catalogNames = listCatalogs(session, metadata, accessControl);
            List<SessionPropertyValue> sessionProperties = metadata.getSessionPropertyManager().getAllSessionProperties(session, catalogNames);
            for (SessionPropertyValue sessionProperty : sessionProperties) {
                if (sessionProperty.isHidden()) {
                    continue;
                }

                String value = sessionProperty.getValue();
                String defaultValue = sessionProperty.getDefaultValue();
                rows.add(row(
                        new StringLiteral(sessionProperty.getFullyQualifiedName()),
                        new StringLiteral(nullToEmpty(value)),
                        new StringLiteral(nullToEmpty(defaultValue)),
                        new StringLiteral(sessionProperty.getType()),
                        new StringLiteral(sessionProperty.getDescription()),
                        TRUE_LITERAL));
            }

            // add bogus row so we can support empty sessions
            rows.add(row(new StringLiteral(""), new StringLiteral(""), new StringLiteral(""), new StringLiteral(""), new StringLiteral(""), FALSE_LITERAL));

            Expression predicate = identifier("include");
            Optional<String> likePattern = node.getLikePattern();
            if (likePattern.isPresent()) {
                predicate = and(predicate, new LikePredicate(
                        identifier("name"),
                        new StringLiteral(likePattern.get()),
                        node.getEscape().map(StringLiteral::new)));
            }

            return simpleQuery(
                    selectList(
                            aliasedName("name", "Name"),
                            aliasedName("value", "Value"),
                            aliasedName("default", "Default"),
                            aliasedName("type", "Type"),
                            aliasedName("description", "Description")),
                    aliased(
                            new Values(rows.build()),
                            "session",
                            ImmutableList.of("name", "value", "default", "type", "description", "include")),
                    predicate);
        }

        private Query parseView(String view, QualifiedObjectName name, Node node)
        {
            try {
                Statement statement = sqlParser.createStatement(view, createParsingOptions(session));
                return (Query) statement;
            }
            catch (ParsingException e) {
                throw semanticException(INVALID_VIEW, node, e, "Failed parsing stored view '%s': %s", name, e.getMessage());
            }
        }

        private static Relation from(String catalog, SchemaTableName table)
        {
            return table(QualifiedName.of(catalog, table.getSchemaName(), table.getTableName()));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
