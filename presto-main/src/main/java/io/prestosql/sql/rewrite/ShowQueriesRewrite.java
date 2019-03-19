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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.SessionPropertyManager.SessionPropertyValue;
import io.prestosql.metadata.SqlFunction;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.ViewDefinition;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.ColumnDefinition;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Property;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.ShowCatalogs;
import io.prestosql.sql.tree.ShowColumns;
import io.prestosql.sql.tree.ShowCreate;
import io.prestosql.sql.tree.ShowFunctions;
import io.prestosql.sql.tree.ShowGrants;
import io.prestosql.sql.tree.ShowRoleGrants;
import io.prestosql.sql.tree.ShowRoles;
import io.prestosql.sql.tree.ShowSchemas;
import io.prestosql.sql.tree.ShowSession;
import io.prestosql.sql.tree.ShowTables;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.TableElement;
import io.prestosql.sql.tree.Values;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_COLUMNS;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_ENABLED_ROLES;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_ROLES;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_SCHEMATA;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_TABLES;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_TABLE_PRIVILEGES;
import static io.prestosql.metadata.MetadataListing.listCatalogs;
import static io.prestosql.metadata.MetadataListing.listSchemas;
import static io.prestosql.metadata.MetadataUtil.createCatalogSchemaName;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ParsingUtil.createParsingOptions;
import static io.prestosql.sql.QueryUtil.aliased;
import static io.prestosql.sql.QueryUtil.aliasedName;
import static io.prestosql.sql.QueryUtil.aliasedNullToEmpty;
import static io.prestosql.sql.QueryUtil.ascending;
import static io.prestosql.sql.QueryUtil.equal;
import static io.prestosql.sql.QueryUtil.functionCall;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.sql.QueryUtil.logicalAnd;
import static io.prestosql.sql.QueryUtil.ordering;
import static io.prestosql.sql.QueryUtil.row;
import static io.prestosql.sql.QueryUtil.selectAll;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.sql.QueryUtil.singleValueQuery;
import static io.prestosql.sql.QueryUtil.table;
import static io.prestosql.sql.SqlFormatter.formatSql;
import static io.prestosql.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.analyzer.SemanticErrorCode.VIEW_PARSE_ERROR;
import static io.prestosql.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ShowCreate.Type.TABLE;
import static io.prestosql.sql.tree.ShowCreate.Type.VIEW;
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
            AccessControl accessControl,
            WarningCollector warningCollector)
    {
        return (Statement) new Visitor(metadata, parser, session, parameters, accessControl, queryExplainer, warningCollector).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;
        final List<Expression> parameters;
        private final AccessControl accessControl;
        private Optional<QueryExplainer> queryExplainer;
        private final WarningCollector warningCollector;

        public Visitor(Metadata metadata, SqlParser sqlParser, Session session, List<Expression> parameters, AccessControl accessControl, Optional<QueryExplainer> queryExplainer, WarningCollector warningCollector)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.parameters = requireNonNull(parameters, "parameters is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
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

            accessControl.checkCanShowTablesMetadata(session.getRequiredTransactionId(), session.getIdentity(), schema);

            if (!metadata.catalogExists(session, schema.getCatalogName())) {
                throw new SemanticException(MISSING_CATALOG, showTables, "Catalog '%s' does not exist", schema.getCatalogName());
            }

            if (!metadata.schemaExists(session, schema)) {
                throw new SemanticException(MISSING_SCHEMA, showTables, "Schema '%s' does not exist", schema.getSchemaName());
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
                    from(schema.getCatalogName(), TABLE_TABLES),
                    predicate,
                    ordering(ascending("table_name")));
        }

        @Override
        protected Node visitShowGrants(ShowGrants showGrants, Void context)
        {
            String catalogName = session.getCatalog().orElse(null);
            Optional<Expression> predicate = Optional.empty();

            Optional<QualifiedName> tableName = showGrants.getTableName();
            if (tableName.isPresent()) {
                QualifiedObjectName qualifiedTableName = createQualifiedObjectName(session, showGrants, tableName.get());

                if (!metadata.getView(session, qualifiedTableName).isPresent() &&
                        !metadata.getTableHandle(session, qualifiedTableName).isPresent()) {
                    throw new SemanticException(MISSING_TABLE, showGrants, "Table '%s' does not exist", tableName);
                }

                catalogName = qualifiedTableName.getCatalogName();

                accessControl.checkCanShowTablesMetadata(
                        session.getRequiredTransactionId(),
                        session.getIdentity(),
                        new CatalogSchemaName(catalogName, qualifiedTableName.getSchemaName()));

                predicate = Optional.of(combineConjuncts(
                        equal(identifier("table_schema"), new StringLiteral(qualifiedTableName.getSchemaName())),
                        equal(identifier("table_name"), new StringLiteral(qualifiedTableName.getObjectName()))));
            }
            else {
                if (catalogName == null) {
                    throw new SemanticException(CATALOG_NOT_SPECIFIED, showGrants, "Catalog must be specified when session catalog is not set");
                }

                Set<String> allowedSchemas = listSchemas(session, metadata, accessControl, catalogName);
                for (String schema : allowedSchemas) {
                    accessControl.checkCanShowTablesMetadata(session.getRequiredTransactionId(), session.getIdentity(), new CatalogSchemaName(catalogName, schema));
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
                    from(catalogName, TABLE_TABLE_PRIVILEGES),
                    predicate,
                    Optional.empty());
        }

        @Override
        protected Node visitShowRoles(ShowRoles node, Void context)
        {
            if (!node.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(c -> c.getValue().toLowerCase(ENGLISH)).orElseGet(() -> session.getCatalog().get());

            if (node.isCurrent()) {
                accessControl.checkCanShowCurrentRoles(session.getRequiredTransactionId(), session.getIdentity(), catalog);
                return simpleQuery(
                        selectList(aliasedName("role_name", "Role")),
                        from(catalog, TABLE_ENABLED_ROLES));
            }
            else {
                accessControl.checkCanShowRoles(session.getRequiredTransactionId(), session.getIdentity(), catalog);
                return simpleQuery(
                        selectList(aliasedName("role_name", "Role")),
                        from(catalog, TABLE_ROLES));
            }
        }

        @Override
        protected Node visitShowRoleGrants(ShowRoleGrants node, Void context)
        {
            if (!node.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(c -> c.getValue().toLowerCase(ENGLISH)).orElseGet(() -> session.getCatalog().get());
            PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, session.getUser());

            accessControl.checkCanShowRoleGrants(session.getRequiredTransactionId(), session.getIdentity(), catalog);
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
            if (!node.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(Identifier::getValue).orElseGet(() -> session.getCatalog().get());
            accessControl.checkCanShowSchemas(session.getRequiredTransactionId(), session.getIdentity(), catalog);

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
                    from(catalog, TABLE_SCHEMATA),
                    predicate,
                    Optional.of(ordering(ascending("schema_name"))));
        }

        @Override
        protected Node visitShowCatalogs(ShowCatalogs node, Void context)
        {
            List<Expression> rows = listCatalogs(session, metadata, accessControl).keySet().stream()
                    .map(name -> row(new StringLiteral(name)))
                    .collect(toList());

            Optional<Expression> predicate = Optional.empty();
            Optional<String> likePattern = node.getLikePattern();
            if (likePattern.isPresent()) {
                predicate = Optional.of(new LikePredicate(identifier("Catalog"), new StringLiteral(likePattern.get()), Optional.empty()));
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

            if (!metadata.getView(session, tableName).isPresent() &&
                    !metadata.getTableHandle(session, tableName).isPresent()) {
                throw new SemanticException(MISSING_TABLE, showColumns, "Table '%s' does not exist", tableName);
            }

            accessControl.checkCanShowColumnsMetadata(session.getRequiredTransactionId(), session.getIdentity(), tableName.asCatalogSchemaTableName());

            return simpleQuery(
                    selectList(
                            aliasedName("column_name", "Column"),
                            aliasedName("data_type", "Type"),
                            aliasedNullToEmpty("extra_info", "Extra"),
                            aliasedNullToEmpty("comment", "Comment")),
                    from(tableName.getCatalogName(), TABLE_COLUMNS),
                    logicalAnd(
                            equal(identifier("table_schema"), new StringLiteral(tableName.getSchemaName())),
                            equal(identifier("table_name"), new StringLiteral(tableName.getObjectName()))),
                    ordering(ascending("ordinal_position")));
        }

        private static <T> Expression getExpression(PropertyMetadata<T> property, Object value)
                throws PrestoException
        {
            return toExpression(property.encode(property.getJavaType().cast(value)));
        }

        private static Expression toExpression(Object value)
                throws PrestoException
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

            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Failed to convert object of type %s to expression: %s", value.getClass().getName(), value));
        }

        @Override
        protected Node visitShowCreate(ShowCreate node, Void context)
        {
            QualifiedObjectName objectName = createQualifiedObjectName(session, node, node.getName());
            Optional<ViewDefinition> viewDefinition = metadata.getView(session, objectName);

            if (node.getType() == VIEW) {
                if (!viewDefinition.isPresent()) {
                    if (metadata.getTableHandle(session, objectName).isPresent()) {
                        throw new SemanticException(NOT_SUPPORTED, node, "Relation '%s' is a table, not a view", objectName);
                    }
                    throw new SemanticException(MISSING_TABLE, node, "View '%s' does not exist", objectName);
                }

                Query query = parseView(viewDefinition.get().getOriginalSql(), objectName, node);
                List<Identifier> parts = Lists.reverse(node.getName().getOriginalParts());
                Identifier tableName = parts.get(0);
                Identifier schemaName = (parts.size() > 1) ? parts.get(1) : new Identifier(objectName.getSchemaName());
                Identifier catalogName = (parts.size() > 2) ? parts.get(2) : new Identifier(objectName.getCatalogName());
                String sql = formatSql(new CreateView(QualifiedName.of(ImmutableList.of(catalogName, schemaName, tableName)), query, false, Optional.empty()), Optional.of(parameters)).trim();
                return singleValueQuery("Create View", sql);
            }

            if (node.getType() == TABLE) {
                if (viewDefinition.isPresent()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Relation '%s' is a view, not a table", objectName);
                }

                Optional<TableHandle> tableHandle = metadata.getTableHandle(session, objectName);
                if (!tableHandle.isPresent()) {
                    throw new SemanticException(MISSING_TABLE, node, "Table '%s' does not exist", objectName);
                }

                ConnectorTableMetadata connectorTableMetadata = metadata.getTableMetadata(session, tableHandle.get()).getMetadata();

                Map<String, PropertyMetadata<?>> allColumnProperties = metadata.getColumnPropertyManager().getAllProperties().get(tableHandle.get().getCatalogName());

                List<TableElement> columns = connectorTableMetadata.getColumns().stream()
                        .filter(column -> !column.isHidden())
                        .map(column -> {
                            List<Property> propertyNodes = buildProperties(objectName, Optional.of(column.getName()), INVALID_COLUMN_PROPERTY, column.getProperties(), allColumnProperties);
                            return new ColumnDefinition(new Identifier(column.getName()), column.getType().getDisplayName(), column.isNullable(), propertyNodes, Optional.ofNullable(column.getComment()));
                        })
                        .collect(toImmutableList());

                Map<String, Object> properties = connectorTableMetadata.getProperties();
                Map<String, PropertyMetadata<?>> allTableProperties = metadata.getTablePropertyManager().getAllProperties().get(tableHandle.get().getCatalogName());
                List<Property> propertyNodes = buildProperties(objectName, Optional.empty(), INVALID_TABLE_PROPERTY, properties, allTableProperties);

                CreateTable createTable = new CreateTable(
                        QualifiedName.of(objectName.getCatalogName(), objectName.getSchemaName(), objectName.getObjectName()),
                        columns,
                        false,
                        propertyNodes,
                        connectorTableMetadata.getComment());
                return singleValueQuery("Create Table", formatSql(createTable, Optional.of(parameters)).trim());
            }

            throw new UnsupportedOperationException("SHOW CREATE only supported for tables and views");
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
                    throw new PrestoException(errorCode, format("Property %s for %s cannot have a null value", propertyName, toQualifedName(objectName, columnName)));
                }

                PropertyMetadata<?> property = allProperties.get(propertyName);
                if (!Primitives.wrap(property.getJavaType()).isInstance(value)) {
                    throw new PrestoException(errorCode, format(
                            "Property %s for %s should have value of type %s, not %s",
                            propertyName,
                            toQualifedName(objectName, columnName),
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

        private static String toQualifedName(Object objectName, Optional<String> columnName)
        {
            return columnName.map(s -> format("column %s of table %s", s, objectName))
                    .orElseGet(() -> "table " + objectName);
        }

        @Override
        protected Node visitShowFunctions(ShowFunctions node, Void context)
        {
            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            for (SqlFunction function : metadata.listFunctions()) {
                rows.add(row(
                        new StringLiteral(function.getSignature().getName()),
                        new StringLiteral(function.getSignature().getReturnType().toString()),
                        new StringLiteral(Joiner.on(", ").join(function.getSignature().getArgumentTypes())),
                        new StringLiteral(getFunctionType(function)),
                        function.isDeterministic() ? TRUE_LITERAL : FALSE_LITERAL,
                        new StringLiteral(nullToEmpty(function.getDescription()))));
            }

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
                    aliased(new Values(rows.build()), "functions", ImmutableList.copyOf(columns.keySet())),
                    ordering(
                            new SortItem(
                                    functionCall("lower", identifier("function_name")),
                                    SortItem.Ordering.ASCENDING,
                                    SortItem.NullOrdering.UNDEFINED),
                            ascending("return_type"),
                            ascending("argument_types"),
                            ascending("function_type")));
        }

        private static String getFunctionType(SqlFunction function)
        {
            FunctionKind kind = function.getSignature().getKind();
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
                    identifier("include"));
        }

        private Query parseView(String view, QualifiedObjectName name, Node node)
        {
            try {
                Statement statement = sqlParser.createStatement(view, createParsingOptions(session));
                return (Query) statement;
            }
            catch (ParsingException e) {
                throw new SemanticException(VIEW_PARSE_ERROR, node, "Failed parsing stored view '%s': %s", name, e.getMessage());
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
