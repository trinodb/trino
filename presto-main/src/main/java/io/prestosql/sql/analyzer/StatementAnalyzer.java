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
package io.prestosql.sql.analyzer;

import com.google.common.base.Joiner;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.NewTableLayout;
import io.prestosql.metadata.OperatorNotFoundException;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.security.AccessControl;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.security.ViewAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeNotFoundException;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.InterpretedFunctionInvoker;
import io.prestosql.sql.SqlPath;
import io.prestosql.sql.analyzer.Analysis.GroupingSetAnalysis;
import io.prestosql.sql.analyzer.Analysis.SelectExpression;
import io.prestosql.sql.analyzer.Analysis.UnnestAnalysis;
import io.prestosql.sql.analyzer.Scope.AsteriskedIdentifierChainBasis;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.DeterminismEvaluator;
import io.prestosql.sql.planner.ExpressionInterpreter;
import io.prestosql.sql.planner.ScopeAware;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.AddColumn;
import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.AllRows;
import io.prestosql.sql.tree.Analyze;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.Call;
import io.prestosql.sql.tree.Comment;
import io.prestosql.sql.tree.Commit;
import io.prestosql.sql.tree.CreateMaterializedView;
import io.prestosql.sql.tree.CreateSchema;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.Cube;
import io.prestosql.sql.tree.Deallocate;
import io.prestosql.sql.tree.Delete;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.DropColumn;
import io.prestosql.sql.tree.DropMaterializedView;
import io.prestosql.sql.tree.DropSchema;
import io.prestosql.sql.tree.DropTable;
import io.prestosql.sql.tree.DropView;
import io.prestosql.sql.tree.Except;
import io.prestosql.sql.tree.Execute;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.ExplainType;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FetchFirst;
import io.prestosql.sql.tree.FieldReference;
import io.prestosql.sql.tree.FrameBound;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.Grant;
import io.prestosql.sql.tree.GroupBy;
import io.prestosql.sql.tree.GroupingElement;
import io.prestosql.sql.tree.GroupingOperation;
import io.prestosql.sql.tree.GroupingSets;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Insert;
import io.prestosql.sql.tree.Intersect;
import io.prestosql.sql.tree.Join;
import io.prestosql.sql.tree.JoinCriteria;
import io.prestosql.sql.tree.JoinOn;
import io.prestosql.sql.tree.JoinUsing;
import io.prestosql.sql.tree.Lateral;
import io.prestosql.sql.tree.Limit;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NaturalJoin;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.Offset;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.Parameter;
import io.prestosql.sql.tree.Prepare;
import io.prestosql.sql.tree.Property;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.RefreshMaterializedView;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.RenameColumn;
import io.prestosql.sql.tree.RenameSchema;
import io.prestosql.sql.tree.RenameTable;
import io.prestosql.sql.tree.RenameView;
import io.prestosql.sql.tree.ResetSession;
import io.prestosql.sql.tree.Revoke;
import io.prestosql.sql.tree.Rollback;
import io.prestosql.sql.tree.Rollup;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SampledRelation;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.SetOperation;
import io.prestosql.sql.tree.SetSchemaAuthorization;
import io.prestosql.sql.tree.SetSession;
import io.prestosql.sql.tree.SimpleGroupBy;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.StartTransaction;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.TableSubquery;
import io.prestosql.sql.tree.Union;
import io.prestosql.sql.tree.Unnest;
import io.prestosql.sql.tree.Use;
import io.prestosql.sql.tree.Values;
import io.prestosql.sql.tree.Window;
import io.prestosql.sql.tree.WindowFrame;
import io.prestosql.sql.tree.With;
import io.prestosql.sql.tree.WithQuery;
import io.prestosql.type.TypeCoercion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static io.prestosql.SystemSessionProperties.getMaxGroupingSets;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.metadata.FunctionKind.WINDOW;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.COLUMN_TYPE_UNKNOWN;
import static io.prestosql.spi.StandardErrorCode.DUPLICATE_COLUMN_NAME;
import static io.prestosql.spi.StandardErrorCode.DUPLICATE_NAMED_QUERY;
import static io.prestosql.spi.StandardErrorCode.DUPLICATE_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.prestosql.spi.StandardErrorCode.EXPRESSION_NOT_IN_DISTINCT;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_WINDOW;
import static io.prestosql.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.prestosql.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.INVALID_LIMIT_CLAUSE;
import static io.prestosql.spi.StandardErrorCode.INVALID_RECURSIVE_REFERENCE;
import static io.prestosql.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.prestosql.spi.StandardErrorCode.INVALID_VIEW;
import static io.prestosql.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static io.prestosql.spi.StandardErrorCode.MISMATCHED_COLUMN_ALIASES;
import static io.prestosql.spi.StandardErrorCode.MISSING_COLUMN_ALIASES;
import static io.prestosql.spi.StandardErrorCode.MISSING_COLUMN_NAME;
import static io.prestosql.spi.StandardErrorCode.MISSING_GROUP_BY;
import static io.prestosql.spi.StandardErrorCode.MISSING_ORDER_BY;
import static io.prestosql.spi.StandardErrorCode.NESTED_RECURSIVE;
import static io.prestosql.spi.StandardErrorCode.NESTED_WINDOW;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.NULL_TREATMENT_NOT_ALLOWED;
import static io.prestosql.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.prestosql.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.TOO_MANY_GROUPING_SETS;
import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.prestosql.spi.StandardErrorCode.VIEW_IS_RECURSIVE;
import static io.prestosql.spi.StandardErrorCode.VIEW_IS_STALE;
import static io.prestosql.spi.connector.StandardWarningCode.REDUNDANT_ORDER_BY;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.NodeUtils.getSortItemsFromOrderBy;
import static io.prestosql.sql.NodeUtils.mapFromProperties;
import static io.prestosql.sql.ParsingUtil.createParsingOptions;
import static io.prestosql.sql.analyzer.AggregationAnalyzer.verifyOrderByAggregations;
import static io.prestosql.sql.analyzer.AggregationAnalyzer.verifySourceAggregations;
import static io.prestosql.sql.analyzer.Analyzer.verifyNoAggregateWindowOrGroupingFunctions;
import static io.prestosql.sql.analyzer.CanonicalizationAware.canonicalizationAwareKey;
import static io.prestosql.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static io.prestosql.sql.analyzer.ExpressionTreeUtils.asQualifiedName;
import static io.prestosql.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static io.prestosql.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.prestosql.sql.analyzer.ExpressionTreeUtils.extractLocation;
import static io.prestosql.sql.analyzer.ExpressionTreeUtils.extractWindowFunctions;
import static io.prestosql.sql.analyzer.Scope.BasisType.TABLE;
import static io.prestosql.sql.analyzer.ScopeReferenceExtractor.getReferencesToScope;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.ExpressionInterpreter.expressionOptimizer;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ExplainType.Type.DISTRIBUTED;
import static io.prestosql.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.prestosql.sql.tree.FrameBound.Type.FOLLOWING;
import static io.prestosql.sql.tree.FrameBound.Type.PRECEDING;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.prestosql.sql.tree.Join.Type.FULL;
import static io.prestosql.sql.tree.Join.Type.INNER;
import static io.prestosql.sql.tree.Join.Type.LEFT;
import static io.prestosql.sql.tree.Join.Type.RIGHT;
import static io.prestosql.sql.tree.WindowFrame.Type.RANGE;
import static io.prestosql.sql.util.AstUtils.preOrder;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static io.prestosql.util.MoreLists.mappedCopy;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

class StatementAnalyzer
{
    private static final Set<String> WINDOW_VALUE_FUNCTIONS = ImmutableSet.of("lead", "lag", "first_value", "last_value", "nth_value");
    private static final String STORAGE_TABLE = "storage_table";

    private final Analysis analysis;
    private final Metadata metadata;
    private final TypeCoercion typeCoercion;
    private final Session session;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final WarningCollector warningCollector;
    private final CorrelationSupport correlationSupport;

    public StatementAnalyzer(
            Analysis analysis,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            Session session,
            WarningCollector warningCollector,
            CorrelationSupport correlationSupport)
    {
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeCoercion = new TypeCoercion(metadata::getType);
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.session = requireNonNull(session, "session is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.correlationSupport = requireNonNull(correlationSupport, "correlationAllowed is null");
    }

    public Scope analyze(Node node, Scope outerQueryScope)
    {
        return analyze(node, Optional.of(outerQueryScope));
    }

    public Scope analyze(Node node, Optional<Scope> outerQueryScope)
    {
        return new Visitor(outerQueryScope, warningCollector, false)
                .process(node, Optional.empty());
    }

    public Scope analyzeForUpdate(Table table, Optional<Scope> outerQueryScope)
    {
        return new Visitor(outerQueryScope, warningCollector, true)
                .process(table, Optional.empty());
    }

    /**
     * Visitor context represents local query scope (if exists). The invariant is
     * that the local query scopes hierarchy should always have outer query scope
     * (if provided) as ancestor.
     */
    private class Visitor
            extends AstVisitor<Scope, Optional<Scope>>
    {
        private final Optional<Scope> outerQueryScope;
        private final WarningCollector warningCollector;
        private final boolean isUpdateQuery;

        private Visitor(Optional<Scope> outerQueryScope, WarningCollector warningCollector, boolean isUpdateQuery)
        {
            this.outerQueryScope = requireNonNull(outerQueryScope, "outerQueryScope is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
            this.isUpdateQuery = isUpdateQuery;
        }

        @Override
        public Scope process(Node node, Optional<Scope> scope)
        {
            Scope returnScope = super.process(node, scope);
            checkState(returnScope.getOuterQueryParent().equals(outerQueryScope), "result scope should have outer query scope equal with parameter outer query scope");
            if (scope.isPresent()) {
                checkState(hasScopeAsLocalParent(returnScope, scope.get()), "return scope should have context scope as one of its ancestors");
            }
            return returnScope;
        }

        private Scope process(Node node, Scope scope)
        {
            return process(node, Optional.of(scope));
        }

        @Override
        protected Scope visitNode(Node node, Optional<Scope> context)
        {
            throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
        }

        @Override
        protected Scope visitUse(Use node, Optional<Scope> scope)
        {
            throw semanticException(NOT_SUPPORTED, node, "USE statement is not supported");
        }

        @Override
        protected Scope visitInsert(Insert insert, Optional<Scope> scope)
        {
            QualifiedObjectName targetTable = createQualifiedObjectName(session, insert, insert.getTarget());

            if (metadata.getMaterializedView(session, targetTable).isPresent()) {
                throw semanticException(NOT_SUPPORTED, insert, "Inserting into materialized views is not supported");
            }

            if (metadata.getView(session, targetTable).isPresent()) {
                throw semanticException(NOT_SUPPORTED, insert, "Inserting into views is not supported");
            }

            // analyze the query that creates the data
            Scope queryScope = analyze(insert.getQuery(), createScope(scope));

            analysis.setUpdateType("INSERT", targetTable);

            // verify the insert destination columns match the query
            Optional<TableHandle> targetTableHandle = metadata.getTableHandle(session, targetTable);
            if (targetTableHandle.isEmpty()) {
                throw semanticException(TABLE_NOT_FOUND, insert, "Table '%s' does not exist", targetTable);
            }
            accessControl.checkCanInsertIntoTable(session.toSecurityContext(), targetTable);

            TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTableHandle.get());

            List<ColumnMetadata> columns = tableMetadata.getColumns().stream()
                    .filter(column -> !column.isHidden())
                    .collect(toImmutableList());

            for (ColumnMetadata column : columns) {
                if (!accessControl.getColumnMasks(session.toSecurityContext(), targetTable, column.getName(), column.getType()).isEmpty()) {
                    throw semanticException(NOT_SUPPORTED, insert, "Insert into table with column masks");
                }
            }

            List<String> tableColumns = columns.stream()
                    .map(ColumnMetadata::getName)
                    .collect(toImmutableList());

            // analyze target table layout, table columns should contain all partition columns
            Optional<NewTableLayout> newTableLayout = metadata.getInsertLayout(session, targetTableHandle.get());
            newTableLayout.ifPresent(layout -> {
                if (!ImmutableSet.copyOf(tableColumns).containsAll(layout.getPartitionColumns())) {
                    throw new PrestoException(NOT_SUPPORTED, "INSERT must write all distribution columns: " + layout.getPartitionColumns());
                }
            });

            List<String> insertColumns;
            if (insert.getColumns().isPresent()) {
                insertColumns = insert.getColumns().get().stream()
                        .map(Identifier::getValue)
                        .map(column -> column.toLowerCase(ENGLISH))
                        .collect(toImmutableList());

                Set<String> columnNames = new HashSet<>();
                for (String insertColumn : insertColumns) {
                    if (!tableColumns.contains(insertColumn)) {
                        throw semanticException(COLUMN_NOT_FOUND, insert, "Insert column name does not exist in target table: %s", insertColumn);
                    }
                    if (!columnNames.add(insertColumn)) {
                        throw semanticException(DUPLICATE_COLUMN_NAME, insert, "Insert column name is specified more than once: %s", insertColumn);
                    }
                }
            }
            else {
                insertColumns = tableColumns;
            }

            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTableHandle.get());
            analysis.setInsert(new Analysis.Insert(
                    targetTableHandle.get(),
                    insertColumns.stream().map(columnHandles::get).collect(toImmutableList()),
                    newTableLayout));

            List<Type> tableTypes = insertColumns.stream()
                    .map(insertColumn -> tableMetadata.getColumn(insertColumn).getType())
                    .collect(toImmutableList());

            List<Type> queryTypes = queryScope.getRelationType().getVisibleFields().stream()
                    .map(Field::getType)
                    .collect(toImmutableList());

            if (!typesMatchForInsert(tableTypes, queryTypes)) {
                throw semanticException(TYPE_MISMATCH,
                        insert,
                        "Insert query has mismatched column types: Table: [%s], Query: [%s]",
                        Joiner.on(", ").join(tableTypes),
                        Joiner.on(", ").join(queryTypes));
            }

            return createAndAssignScope(insert, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitRefreshMaterializedView(RefreshMaterializedView refreshMaterializedView, Optional<Scope> scope)
        {
            QualifiedObjectName name = createQualifiedObjectName(session, refreshMaterializedView, refreshMaterializedView.getName());
            Optional<ConnectorMaterializedViewDefinition> optionalView = metadata.getMaterializedView(session, name);

            if (optionalView.isEmpty()) {
                throw semanticException(TABLE_NOT_FOUND, refreshMaterializedView, "Materialized view '%s' does not exist", name);
            }

            Optional<QualifiedName> storageName = getMaterializedViewStorageTableName(name);

            if (storageName.isEmpty()) {
                throw semanticException(TABLE_NOT_FOUND, refreshMaterializedView, "Storage Table '%s' for materialized view '%s' does not exist", storageName, name);
            }

            QualifiedObjectName targetTable = createQualifiedObjectName(session, refreshMaterializedView, storageName.get());

            // analyze the query that creates the data
            Query query = parseView(optionalView.get().getOriginalSql(), name, refreshMaterializedView);
            Scope queryScope = process(query, scope);

            analysis.setUpdateType("INSERT", targetTable);

            // verify the insert destination columns match the query
            Optional<TableHandle> targetTableHandle = metadata.getTableHandle(session, targetTable);
            if (!targetTableHandle.isPresent()) {
                throw semanticException(TABLE_NOT_FOUND, refreshMaterializedView, "Table '%s' does not exist", targetTable);
            }

            Optional<TableHandle> materializedViewHandle = metadata.getTableHandle(session, name);
            if (targetTableHandle.isPresent() && metadata.getMaterializedViewFreshness(session, materializedViewHandle.get()).isMaterializedViewFresh()) {
                analysis.setSkipMaterializedViewRefresh(true);
            }
            else {
                analysis.setSkipMaterializedViewRefresh(false);
            }

            TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTableHandle.get());
            List<String> insertColumns = tableMetadata.getColumns().stream()
                    .filter(column -> !column.isHidden())
                    .map(ColumnMetadata::getName)
                    .collect(toImmutableList());

            accessControl.checkCanInsertIntoTable(session.toSecurityContext(), targetTable);

            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTableHandle.get());
            checkState(materializedViewHandle.isPresent());
            analysis.setRefreshMaterializedView(new Analysis.RefreshMaterializedViewAnalysis(
                    materializedViewHandle.get(),
                    targetTableHandle.get(), query,
                    insertColumns.stream().map(columnHandles::get).collect(toImmutableList())));

            List<Type> tableTypes = insertColumns.stream()
                    .map(insertColumn -> tableMetadata.getColumn(insertColumn).getType())
                    .collect(toImmutableList());

            List<Type> queryTypes = queryScope.getRelationType().getVisibleFields().stream()
                    .map(Field::getType)
                    .collect(toImmutableList());

            if (!typesMatchForInsert(tableTypes, queryTypes)) {
                throw semanticException(TYPE_MISMATCH, refreshMaterializedView, "Insert query has mismatched column types: " +
                        "Table: [" + Joiner.on(", ").join(tableTypes) + "], " +
                        "Query: [" + Joiner.on(", ").join(queryTypes) + "]");
            }

            return createAndAssignScope(refreshMaterializedView, scope, Field.newUnqualified("rows", BIGINT));
        }

        private boolean typesMatchForInsert(List<Type> tableTypes, List<Type> queryTypes)
        {
            if (tableTypes.size() != queryTypes.size()) {
                return false;
            }

            /*
            TODO enable coercions based on type compatibility for INSERT of structural types containing nested bounded character types.
            It might require defining a new range of cast operators and changes in FunctionRegistry to ensure proper handling
            of nested types.
            Currently, INSERT for such structural types is only allowed in the case of strict type coercibility.
            INSERT for other types is allowed in all cases described by the Standard. It is obtained
            by emulating a "guarded cast" in LogicalPlanner, and without any changes to the actual operators.
            */
            for (int i = 0; i < tableTypes.size(); i++) {
                if (hasNestedBoundedCharacterType(tableTypes.get(i))) {
                    if (!typeCoercion.canCoerce(queryTypes.get(i), tableTypes.get(i))) {
                        return false;
                    }
                }
                else if (!typeCoercion.isCompatible(queryTypes.get(i), tableTypes.get(i))) {
                    return false;
                }
            }

            return true;
        }

        private boolean hasNestedBoundedCharacterType(Type type)
        {
            if (type instanceof ArrayType) {
                return hasBoundedCharacterType(((ArrayType) type).getElementType());
            }

            if (type instanceof MapType) {
                return hasBoundedCharacterType(((MapType) type).getKeyType()) || hasBoundedCharacterType(((MapType) type).getValueType());
            }

            if (type instanceof RowType) {
                for (Type fieldType : type.getTypeParameters()) {
                    if (hasBoundedCharacterType(fieldType)) {
                        return true;
                    }
                }
            }

            return false;
        }

        private boolean hasBoundedCharacterType(Type type)
        {
            return type instanceof CharType || (type instanceof VarcharType && !((VarcharType) type).isUnbounded()) || hasNestedBoundedCharacterType(type);
        }

        @Override
        protected Scope visitDelete(Delete node, Optional<Scope> scope)
        {
            Table table = node.getTable();
            QualifiedObjectName tableName = createQualifiedObjectName(session, table, table.getName());
            if (metadata.getView(session, tableName).isPresent()) {
                throw semanticException(NOT_SUPPORTED, node, "Deleting from views is not supported");
            }

            TableHandle handle = metadata.getTableHandle(session, tableName)
                    .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, table, "Table '%s' does not exist", tableName));

            accessControl.checkCanDeleteFromTable(session.toSecurityContext(), tableName);

            if (!accessControl.getRowFilters(session.toSecurityContext(), tableName).isEmpty()) {
                throw semanticException(NOT_SUPPORTED, node, "Delete from table with row filter");
            }

            TableMetadata tableMetadata = metadata.getTableMetadata(session, handle);
            for (ColumnMetadata tableColumn : tableMetadata.getColumns()) {
                if (!accessControl.getColumnMasks(session.toSecurityContext(), tableName, tableColumn.getName(), tableColumn.getType()).isEmpty()) {
                    throw semanticException(NOT_SUPPORTED, node, "Delete from table with column mask");
                }
            }

            // Analyzer checks for select permissions but DELETE has a separate permission, so disable access checks
            // TODO: we shouldn't need to create a new analyzer. The access control should be carried in the context object
            StatementAnalyzer analyzer = new StatementAnalyzer(
                    analysis,
                    metadata,
                    sqlParser,
                    new AllowAllAccessControl(),
                    session,
                    warningCollector,
                    CorrelationSupport.ALLOWED);

            Scope tableScope = analyzer.analyzeForUpdate(table, scope);
            node.getWhere().ifPresent(where -> analyzeWhere(node, tableScope, where));

            analysis.setUpdateType("DELETE", tableName);

            return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitAnalyze(Analyze node, Optional<Scope> scope)
        {
            QualifiedObjectName tableName = createQualifiedObjectName(session, node, node.getTableName());
            analysis.setUpdateType("ANALYZE", tableName);

            // verify the target table exists and it's not a view
            if (metadata.getView(session, tableName).isPresent()) {
                throw semanticException(NOT_SUPPORTED, node, "Analyzing views is not supported");
            }

            validateProperties(node.getProperties(), scope);
            CatalogName catalogName = metadata.getCatalogHandle(session, tableName.getCatalogName())
                    .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog not found: " + tableName.getCatalogName()));

            Map<String, Object> analyzeProperties = metadata.getAnalyzePropertyManager().getProperties(
                    catalogName,
                    catalogName.getCatalogName(),
                    mapFromProperties(node.getProperties()),
                    session,
                    metadata,
                    accessControl,
                    analysis.getParameters());
            TableHandle tableHandle = metadata.getTableHandleForStatisticsCollection(session, tableName, analyzeProperties)
                    .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, node, "Table '%s' does not exist", tableName));

            // user must have read and insert permission in order to analyze stats of a table
            analysis.addTableColumnReferences(
                    accessControl,
                    session.getIdentity(),
                    ImmutableMultimap.<QualifiedObjectName, String>builder()
                            .putAll(tableName, metadata.getColumnHandles(session, tableHandle).keySet())
                            .build());
            try {
                accessControl.checkCanInsertIntoTable(session.toSecurityContext(), tableName);
            }
            catch (AccessDeniedException exception) {
                throw new AccessDeniedException(format("Cannot ANALYZE (missing insert privilege) table %s", tableName));
            }

            analysis.setAnalyzeTarget(tableHandle);
            return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitCreateTableAsSelect(CreateTableAsSelect node, Optional<Scope> scope)
        {
            // turn this into a query that has a new table writer node on top.
            QualifiedObjectName targetTable = createQualifiedObjectName(session, node, node.getName());
            analysis.setUpdateType("CREATE TABLE", targetTable);

            Optional<TableHandle> targetTableHandle = metadata.getTableHandle(session, targetTable);
            if (targetTableHandle.isPresent()) {
                if (node.isNotExists()) {
                    analysis.setCreate(new Analysis.Create(
                            Optional.of(targetTable),
                            Optional.empty(),
                            Optional.empty(),
                            node.isWithData(),
                            true));
                    return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
                }
                throw semanticException(TABLE_ALREADY_EXISTS, node, "Destination table '%s' already exists", targetTable);
            }

            validateProperties(node.getProperties(), scope);

            accessControl.checkCanCreateTable(session.toSecurityContext(), targetTable);

            // analyze the query that creates the table
            Scope queryScope = analyze(node.getQuery(), createScope(scope));

            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();

            // analyze target table columns and column aliases
            if (node.getColumnAliases().isPresent()) {
                validateColumnAliases(node.getColumnAliases().get(), queryScope.getRelationType().getVisibleFieldCount());

                int aliasPosition = 0;
                for (Field field : queryScope.getRelationType().getVisibleFields()) {
                    if (field.getType().equals(UNKNOWN)) {
                        throw semanticException(COLUMN_TYPE_UNKNOWN, node, "Column type is unknown at position %s", queryScope.getRelationType().indexOf(field) + 1);
                    }
                    columns.add(new ColumnMetadata(node.getColumnAliases().get().get(aliasPosition).getValue(), field.getType()));
                    aliasPosition++;
                }
            }
            else {
                validateColumns(node, queryScope.getRelationType());
                columns.addAll(queryScope.getRelationType().getVisibleFields().stream()
                        .map(field -> new ColumnMetadata(field.getName().get(), field.getType()))
                        .collect(toImmutableList()));
            }

            // create target table metadata
            CatalogName catalogName = metadata.getCatalogHandle(session, targetTable.getCatalogName())
                    .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + targetTable.getCatalogName()));

            Map<String, Object> properties = metadata.getTablePropertyManager().getProperties(
                    catalogName,
                    targetTable.getCatalogName(),
                    mapFromProperties(node.getProperties()),
                    session,
                    metadata,
                    accessControl,
                    analysis.getParameters());

            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(targetTable.asSchemaTableName(), columns.build(), properties, node.getComment());

            // analyze target table layout
            Optional<NewTableLayout> newTableLayout = metadata.getNewTableLayout(session, targetTable.getCatalogName(), tableMetadata);

            Set<String> columnNames = columns.build().stream()
                    .map(ColumnMetadata::getName)
                    .collect(toImmutableSet());

            if (newTableLayout.isPresent()) {
                NewTableLayout layout = newTableLayout.get();
                if (!columnNames.containsAll(layout.getPartitionColumns())) {
                    if (layout.getLayout().getPartitioning().isPresent()) {
                        throw new PrestoException(NOT_SUPPORTED, "INSERT must write all distribution columns: " + layout.getPartitionColumns());
                    }
                    else {
                        // created table does not contain all columns required by preferred layout
                        newTableLayout = Optional.empty();
                    }
                }
            }

            analysis.setCreate(new Analysis.Create(
                    Optional.of(targetTable),
                    Optional.of(tableMetadata),
                    newTableLayout,
                    node.isWithData(),
                    false));

            return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitCreateView(CreateView node, Optional<Scope> scope)
        {
            QualifiedObjectName viewName = createQualifiedObjectName(session, node, node.getName());
            analysis.setUpdateType("CREATE VIEW", viewName);

            // analyze the query that creates the view
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector, CorrelationSupport.ALLOWED);

            Scope queryScope = analyzer.analyze(node.getQuery(), scope);

            accessControl.checkCanCreateView(session.toSecurityContext(), viewName);

            validateColumns(node, queryScope.getRelationType());

            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitSetSession(SetSession node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitResetSession(ResetSession node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitAddColumn(AddColumn node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCreateSchema(CreateSchema node, Optional<Scope> scope)
        {
            validateProperties(node.getProperties(), scope);
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropSchema(DropSchema node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameSchema(RenameSchema node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitSetSchemaAuthorization(SetSchemaAuthorization node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCreateTable(CreateTable node, Optional<Scope> scope)
        {
            validateProperties(node.getProperties(), scope);
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitProperty(Property node, Optional<Scope> scope)
        {
            // Property value expressions must be constant
            createConstantAnalyzer(metadata, accessControl, session, analysis.getParameters(), WarningCollector.NOOP, analysis.isDescribe())
                    .analyze(node.getValue(), createScope(scope));
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropTable(DropTable node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameTable(RenameTable node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitComment(Comment node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameColumn(RenameColumn node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropColumn(DropColumn node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameView(RenameView node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropView(DropView node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitStartTransaction(StartTransaction node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCommit(Commit node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRollback(Rollback node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitPrepare(Prepare node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDeallocate(Deallocate node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitExecute(Execute node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitGrant(Grant node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRevoke(Revoke node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCall(Call node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCreateMaterializedView(CreateMaterializedView node, Optional<Scope> scope)
        {
            QualifiedObjectName viewName = createQualifiedObjectName(session, node, node.getName());
            analysis.setUpdateType("CREATE MATERIALIZED VIEW", viewName);

            // analyze the query that creates the view
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector, CorrelationSupport.ALLOWED);

            Scope queryScope = analyzer.analyze(node.getQuery(), scope);

            // Materialized view access control is implemented as a combination of access control check for creation of view and creation of storage table.
            // When the storage table name is generated by the connector, the table creation check is skipped since the table created by the connector
            // should be accessible to the user. When the user specifies the name of the storage table (future extension), create table acccess check
            // must be made on the storage table.
            accessControl.checkCanCreateView(session.toSecurityContext(), viewName);

            validateColumns(node, queryScope.getRelationType());

            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropMaterializedView(DropMaterializedView node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        private void validateProperties(List<Property> properties, Optional<Scope> scope)
        {
            Set<String> propertyNames = new HashSet<>();
            for (Property property : properties) {
                if (!propertyNames.add(property.getName().getValue())) {
                    throw semanticException(DUPLICATE_PROPERTY, property, "Duplicate property: %s", property.getName().getValue());
                }
            }
            for (Property property : properties) {
                process(property, scope);
            }
        }

        private void validateColumns(Statement node, RelationType descriptor)
        {
            // verify that all column names are specified and unique
            // TODO: collect errors and return them all at once
            Set<String> names = new HashSet<>();
            for (Field field : descriptor.getVisibleFields()) {
                Optional<String> fieldName = field.getName();
                if (fieldName.isEmpty()) {
                    throw semanticException(MISSING_COLUMN_NAME, node, "Column name not specified at position %s", descriptor.indexOf(field) + 1);
                }
                if (!names.add(fieldName.get())) {
                    throw semanticException(DUPLICATE_COLUMN_NAME, node, "Column name '%s' specified more than once", fieldName.get());
                }
                if (field.getType().equals(UNKNOWN)) {
                    throw semanticException(COLUMN_TYPE_UNKNOWN, node, "Column type is unknown: %s", fieldName.get());
                }
            }
        }

        private void validateColumnAliases(List<Identifier> columnAliases, int sourceColumnSize)
        {
            validateColumnAliasesCount(columnAliases, sourceColumnSize);
            Set<String> names = new HashSet<>();
            for (Identifier identifier : columnAliases) {
                if (names.contains(identifier.getValue().toLowerCase(ENGLISH))) {
                    throw semanticException(DUPLICATE_COLUMN_NAME, identifier, "Column name '%s' specified more than once", identifier.getValue());
                }
                names.add(identifier.getValue().toLowerCase(ENGLISH));
            }
        }

        private void validateColumnAliasesCount(List<Identifier> columnAliases, int sourceColumnSize)
        {
            if (columnAliases.size() != sourceColumnSize) {
                throw semanticException(
                        MISMATCHED_COLUMN_ALIASES,
                        columnAliases.get(0),
                        "Column alias list has %s entries but relation has %s columns",
                        columnAliases.size(),
                        sourceColumnSize);
            }
        }

        @Override
        protected Scope visitExplain(Explain node, Optional<Scope> scope)
        {
            checkState(node.isAnalyze(), "Non analyze explain should be rewritten to Query");
            if (node.getOptions().stream().anyMatch(option -> !option.equals(new ExplainType(DISTRIBUTED)))) {
                throw semanticException(NOT_SUPPORTED, node, "EXPLAIN ANALYZE only supports TYPE DISTRIBUTED option");
            }
            process(node.getStatement(), scope);
            analysis.resetUpdateType();
            return createAndAssignScope(node, scope, Field.newUnqualified("Query Plan", VARCHAR));
        }

        @Override
        protected Scope visitQuery(Query node, Optional<Scope> scope)
        {
            Scope withScope = analyzeWith(node, scope);
            Scope queryBodyScope = process(node.getQueryBody(), withScope);

            List<Expression> orderByExpressions = emptyList();
            if (node.getOrderBy().isPresent()) {
                orderByExpressions = analyzeOrderBy(node, getSortItemsFromOrderBy(node.getOrderBy()), queryBodyScope);

                if (queryBodyScope.getOuterQueryParent().isPresent() && node.getLimit().isEmpty() && node.getOffset().isEmpty()) {
                    // not the root scope and ORDER BY is ineffective
                    analysis.markRedundantOrderBy(node.getOrderBy().get());
                    warningCollector.add(new PrestoWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
                }
            }
            analysis.setOrderByExpressions(node, orderByExpressions);

            if (node.getOffset().isPresent()) {
                analyzeOffset(node.getOffset().get(), queryBodyScope);
            }

            if (node.getLimit().isPresent()) {
                boolean requiresOrderBy = analyzeLimit(node.getLimit().get(), queryBodyScope);
                if (requiresOrderBy && node.getOrderBy().isEmpty()) {
                    throw semanticException(MISSING_ORDER_BY, node.getLimit().get(), "FETCH FIRST WITH TIES clause requires ORDER BY");
                }
            }

            // Input fields == Output fields
            analysis.setSelectExpressions(
                    node,
                    descriptorToFields(queryBodyScope).stream()
                            .map(expression -> new SelectExpression(expression, Optional.empty()))
                            .collect(toImmutableList()));

            Scope queryScope = Scope.builder()
                    .withParent(withScope)
                    .withRelationType(RelationId.of(node), queryBodyScope.getRelationType())
                    .build();

            analysis.setScope(node, queryScope);
            return queryScope;
        }

        @Override
        protected Scope visitUnnest(Unnest node, Optional<Scope> scope)
        {
            ImmutableMap.Builder<NodeRef<Expression>, List<Field>> mappings = ImmutableMap.<NodeRef<Expression>, List<Field>>builder();

            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
            for (Expression expression : node.getExpressions()) {
                List<Field> expressionOutputs = new ArrayList<>();

                ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, createScope(scope));
                Type expressionType = expressionAnalysis.getType(expression);
                if (expressionType instanceof ArrayType) {
                    Type elementType = ((ArrayType) expressionType).getElementType();
                    if (elementType instanceof RowType) {
                        ((RowType) elementType).getFields().stream()
                                .map(field -> Field.newUnqualified(field.getName(), field.getType()))
                                .forEach(expressionOutputs::add);
                    }
                    else {
                        expressionOutputs.add(Field.newUnqualified(Optional.empty(), elementType));
                    }
                }
                else if (expressionType instanceof MapType) {
                    expressionOutputs.add(Field.newUnqualified(Optional.empty(), ((MapType) expressionType).getKeyType()));
                    expressionOutputs.add(Field.newUnqualified(Optional.empty(), ((MapType) expressionType).getValueType()));
                }
                else {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot unnest type: " + expressionType);
                }

                outputFields.addAll(expressionOutputs);
                mappings.put(NodeRef.of(expression), expressionOutputs);
            }

            Optional<Field> ordinalityField = Optional.empty();
            if (node.isWithOrdinality()) {
                ordinalityField = Optional.of(Field.newUnqualified(Optional.empty(), BIGINT));
            }

            ordinalityField.ifPresent(outputFields::add);

            analysis.setUnnest(node, new UnnestAnalysis(mappings.build(), ordinalityField));

            return createAndAssignScope(node, scope, outputFields.build());
        }

        @Override
        protected Scope visitLateral(Lateral node, Optional<Scope> scope)
        {
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector, CorrelationSupport.ALLOWED);
            Scope queryScope = analyzer.analyze(node.getQuery(), scope);
            return createAndAssignScope(node, scope, queryScope.getRelationType());
        }

        private Optional<QualifiedName> getMaterializedViewStorageTableName(QualifiedObjectName name)
        {
            Optional<ConnectorMaterializedViewDefinition> optionalView = metadata.getMaterializedView(session, name);
            if (optionalView.isEmpty()) {
                return Optional.empty();
            }

            String storageTable = String.valueOf(optionalView.get().getProperties().getOrDefault(STORAGE_TABLE, ""));
            if (storageTable == null || storageTable.isEmpty()) {
                return Optional.empty();
            }
            Identifier catalogName = new Identifier(name.getCatalogName(), true);
            Identifier schemaName = new Identifier(name.getSchemaName(), true);
            Identifier tableName = new Identifier(storageTable, true);
            return Optional.of(QualifiedName.of(ImmutableList.of(catalogName, schemaName, tableName)));
        }

        @Override
        protected Scope visitTable(Table table, Optional<Scope> scope)
        {
            if (table.getName().getPrefix().isEmpty()) {
                // is this a reference to a WITH query?
                Optional<WithQuery> withQuery = createScope(scope).getNamedQuery(table.getName().getSuffix());
                if (withQuery.isPresent()) {
                    return createScopeForCommonTableExpression(table, scope, withQuery.get());
                }
                // is this a recursive reference in expandable WITH query? If so, there's base scope recorded.
                Optional<Scope> expandableBaseScope = analysis.getExpandableBaseScope(table);
                if (expandableBaseScope.isPresent()) {
                    Scope baseScope = expandableBaseScope.get();
                    // adjust local and outer parent scopes accordingly to the local context of the recursive reference
                    Scope resultScope = scopeBuilder(scope)
                            .withRelationType(baseScope.getRelationId(), baseScope.getRelationType())
                            .build();
                    analysis.setScope(table, resultScope);
                    return resultScope;
                }
            }

            QualifiedObjectName name = createQualifiedObjectName(session, table, table.getName());
            analysis.addEmptyColumnReferencesForTable(accessControl, session.getIdentity(), name);
            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, name);

            // If this is a materialized view, get the name of the storage table
            Optional<QualifiedName> storageName = getMaterializedViewStorageTableName(name);
            Optional<TableHandle> storageHandle = Optional.empty();
            if (storageName.isPresent()) {
                storageHandle = metadata.getTableHandle(session, createQualifiedObjectName(session, table, storageName.get()));
            }

            // If materialized view is current, answer the query using the storage table
            Identifier catalogName = new Identifier(name.getCatalogName());
            Identifier schemaName = new Identifier(name.getSchemaName());
            Identifier tableName = new Identifier(name.getObjectName());
            QualifiedName materializedViewName = QualifiedName.of(ImmutableList.of(catalogName, schemaName, tableName));
            Optional<TableHandle> materializedViewHandle = metadata.getTableHandle(session, createQualifiedObjectName(session, table, materializedViewName));
            if (storageHandle.isPresent() && metadata.getMaterializedViewFreshness(session, materializedViewHandle.get()).isMaterializedViewFresh()) {
                tableHandle = storageHandle;
            }
            else {
                // This is a stale materialized view and should be expanded like a logical view
                if (storageHandle.isPresent()) {
                    Optional<ConnectorMaterializedViewDefinition> optionalMaterializedView = metadata.getMaterializedView(session, name);
                    if (optionalMaterializedView.isPresent()) {
                        return createScopeForMaterializedView(table, name, scope, optionalMaterializedView.get());
                    }
                }
                // This is a reference to a logical view
                Optional<ConnectorViewDefinition> optionalView = metadata.getView(session, name);
                if (optionalView.isPresent()) {
                    return createScopeForView(table, name, scope, optionalView.get());
                }
            }

            if (tableHandle.isEmpty()) {
                if (metadata.getCatalogHandle(session, name.getCatalogName()).isEmpty()) {
                    throw semanticException(CATALOG_NOT_FOUND, table, "Catalog '%s' does not exist", name.getCatalogName());
                }
                if (!metadata.schemaExists(session, new CatalogSchemaName(name.getCatalogName(), name.getSchemaName()))) {
                    throw semanticException(SCHEMA_NOT_FOUND, table, "Schema '%s' does not exist", name.getSchemaName());
                }
                throw semanticException(TABLE_NOT_FOUND, table, "Table '%s' does not exist", name);
            }
            TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get());
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

            // TODO: discover columns lazily based on where they are needed (to support connectors that can't enumerate all tables)
            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                Field field = Field.newQualified(
                        table.getName(),
                        Optional.of(column.getName()),
                        column.getType(),
                        column.isHidden(),
                        Optional.of(name),
                        Optional.of(column.getName()),
                        false);
                fields.add(field);
                ColumnHandle columnHandle = columnHandles.get(column.getName());
                checkArgument(columnHandle != null, "Unknown field %s", field);
                analysis.setColumn(field, columnHandle);
            }

            if (isUpdateQuery) {
                // Add the row id field
                ColumnHandle column = metadata.getUpdateRowIdColumnHandle(session, tableHandle.get());
                Type type = metadata.getColumnMetadata(session, tableHandle.get(), column).getType();
                Field field = Field.newUnqualified(Optional.empty(), type);
                fields.add(field);
                analysis.setColumn(field, column);
            }

            List<Field> outputFields = fields.build();

            analyzeFiltersAndMasks(table, name, tableHandle, outputFields, session.getIdentity().getUser());

            Scope tableScope = createAndAssignScope(table, scope, outputFields);

            if (isUpdateQuery) {
                FieldReference reference = new FieldReference(outputFields.size() - 1);
                analyzeExpression(reference, tableScope);
                analysis.setRowIdField(table, reference);
            }

            return tableScope;
        }

        private void analyzeFiltersAndMasks(Table table, QualifiedObjectName name, Optional<TableHandle> tableHandle, List<Field> fields, String authorization)
        {
            Scope accessControlScope = Scope.builder()
                    .withRelationType(RelationId.anonymous(), new RelationType(fields))
                    .build();

            ImmutableMap.Builder<Field, List<ViewExpression>> columnMasks = ImmutableMap.builder();
            for (Field field : fields) {
                if (field.getName().isPresent()) {
                    List<ViewExpression> masks = accessControl.getColumnMasks(session.toSecurityContext(), name, field.getName().get(), field.getType());
                    columnMasks.put(field, masks);

                    masks.forEach(mask -> analyzeColumnMask(session.getIdentity().getUser(), table, name, field, accessControlScope, mask));
                }
            }

            List<ViewExpression> filters = accessControl.getRowFilters(session.toSecurityContext(), name);
            filters.forEach(filter -> analyzeRowFilter(session.getIdentity().getUser(), table, name, accessControlScope, filter));

            analysis.registerTable(table, tableHandle, name, filters, columnMasks.build(), authorization, accessControlScope);
        }

        private Scope createScopeForCommonTableExpression(Table table, Optional<Scope> scope, WithQuery withQuery)
        {
            Query query = withQuery.getQuery();
            analysis.registerNamedQuery(table, query);

            // re-alias the fields with the name assigned to the query in the WITH declaration
            RelationType queryDescriptor = analysis.getOutputDescriptor(query);

            List<Field> fields;
            Optional<List<Identifier>> columnNames = withQuery.getColumnNames();
            if (columnNames.isPresent()) {
                // if columns are explicitly aliased -> WITH cte(alias1, alias2 ...)
                ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();

                int field = 0;
                for (Identifier columnName : columnNames.get()) {
                    Field inputField = queryDescriptor.getFieldByIndex(field);
                    fieldBuilder.add(Field.newQualified(
                            QualifiedName.of(table.getName().getSuffix()),
                            Optional.of(columnName.getValue()),
                            inputField.getType(),
                            false,
                            inputField.getOriginTable(),
                            inputField.getOriginColumnName(),
                            inputField.isAliased()));

                    field++;
                }

                fields = fieldBuilder.build();
            }
            else {
                fields = queryDescriptor.getAllFields().stream()
                        .map(field -> Field.newQualified(
                                QualifiedName.of(table.getName().getSuffix()),
                                field.getName(),
                                field.getType(),
                                field.isHidden(),
                                field.getOriginTable(),
                                field.getOriginColumnName(),
                                field.isAliased()))
                        .collect(toImmutableList());
            }

            return createAndAssignScope(table, scope, fields);
        }

        private Scope createScopeForView(Table table, QualifiedObjectName name, Optional<Scope> scope, ConnectorViewDefinition view)
        {
            Statement statement = analysis.getStatement();
            if (statement instanceof CreateView) {
                CreateView viewStatement = (CreateView) statement;
                QualifiedObjectName viewNameFromStatement = createQualifiedObjectName(session, viewStatement, viewStatement.getName());
                if (viewStatement.isReplace() && viewNameFromStatement.equals(name)) {
                    throw semanticException(VIEW_IS_RECURSIVE, table, "Statement would create a recursive view");
                }
            }
            if (analysis.hasTableInView(table)) {
                throw semanticException(VIEW_IS_RECURSIVE, table, "View is recursive");
            }

            Query query = parseView(view.getOriginalSql(), name, table);
            analysis.registerNamedQuery(table, query);
            analysis.registerTableForView(table);
            RelationType descriptor = analyzeView(query, name, view.getCatalog(), view.getSchema(), view.getOwner(), table);
            analysis.unregisterTableForView();

            if (isViewStale(view.getColumns(), descriptor.getVisibleFields(), name, table)) {
                throw semanticException(VIEW_IS_STALE, table, "View '%s' is stale; it must be re-created", name);
            }

            // Derive the type of the view from the stored definition, not from the analysis of the underlying query.
            // This is needed in case the underlying table(s) changed and the query in the view now produces types that
            // are implicitly coercible to the declared view types.
            List<Field> outputFields = view.getColumns().stream()
                    .map(column -> Field.newQualified(
                            table.getName(),
                            Optional.of(column.getName()),
                            getViewColumnType(column, name, table),
                            false,
                            Optional.of(name),
                            Optional.of(column.getName()),
                            false))
                    .collect(toImmutableList());

            analysis.addRelationCoercion(table, outputFields.stream().map(Field::getType).toArray(Type[]::new));

            analyzeFiltersAndMasks(table, name, Optional.empty(), outputFields, session.getIdentity().getUser());

            return createAndAssignScope(table, scope, outputFields);
        }

        private List<ConnectorViewDefinition.ViewColumn> translateMaterializedViewColumns(List<ConnectorMaterializedViewDefinition.Column> materializedViewColumns)
        {
            List<ConnectorViewDefinition.ViewColumn> viewColumns = new ArrayList<>();
            for (ConnectorMaterializedViewDefinition.Column column : materializedViewColumns) {
                viewColumns.add(new ViewColumn(column.getName(), column.getType()));
            }
            return viewColumns;
        }

        private Scope createScopeForMaterializedView(Table table, QualifiedObjectName name, Optional<Scope> scope, ConnectorMaterializedViewDefinition view)
        {
            Statement statement = analysis.getStatement();
            if (statement instanceof CreateMaterializedView) {
                CreateMaterializedView viewStatement = (CreateMaterializedView) statement;
                QualifiedObjectName viewNameFromStatement = createQualifiedObjectName(session, viewStatement, viewStatement.getName());
                if (viewStatement.isReplace() && viewNameFromStatement.equals(name)) {
                    throw semanticException(VIEW_IS_RECURSIVE, table, "Statement would create a recursive materialized view");
                }
            }
            if (analysis.hasTableInView(table)) {
                throw semanticException(VIEW_IS_RECURSIVE, table, "Materialized View is recursive");
            }

            Query query = parseView(view.getOriginalSql(), name, table);
            analysis.registerNamedQuery(table, query);
            analysis.registerTableForView(table);
            RelationType descriptor = analyzeView(query, name, view.getCatalog(), view.getSchema(), view.getOwner(), table);
            analysis.unregisterTableForView();

            List<ConnectorViewDefinition.ViewColumn> viewColumns = translateMaterializedViewColumns(view.getColumns());
            if (isViewStale(viewColumns, descriptor.getVisibleFields(), name, table)) {
                throw semanticException(VIEW_IS_STALE, table, "Materialized View '%s' is stale; it must be re-created", name);
            }

            // Derive the type of the materialized view from the stored definition, not from the analysis of the underlying query.
            // This is needed in case the underlying table(s) changed and the query in the materialized view now produces types that
            // are implicitly coercible to the declared materialized view types.
            List<Field> outputFields = viewColumns.stream()
                    .map(column -> Field.newQualified(
                            table.getName(),
                            Optional.of(column.getName()),
                            getViewColumnType(column, name, table),
                            false,
                            Optional.of(name),
                            Optional.of(column.getName()),
                            false))
                    .collect(toImmutableList());

            analysis.addRelationCoercion(table, outputFields.stream().map(Field::getType).toArray(Type[]::new));

            analyzeFiltersAndMasks(table, name, Optional.empty(), outputFields, session.getIdentity().getUser());

            return createAndAssignScope(table, scope, outputFields);
        }

        @Override
        protected Scope visitAliasedRelation(AliasedRelation relation, Optional<Scope> scope)
        {
            Scope relationScope = process(relation.getRelation(), scope);

            // todo this check should be inside of TupleDescriptor.withAlias, but the exception needs the node object
            RelationType relationType = relationScope.getRelationType();
            if (relation.getColumnNames() != null) {
                int totalColumns = relationType.getVisibleFieldCount();
                if (totalColumns != relation.getColumnNames().size()) {
                    throw semanticException(MISMATCHED_COLUMN_ALIASES, relation, "Column alias list has %s entries but '%s' has %s columns available", relation.getColumnNames().size(), relation.getAlias(), totalColumns);
                }
            }

            List<String> aliases = null;
            if (relation.getColumnNames() != null) {
                aliases = relation.getColumnNames().stream()
                        .map(Identifier::getValue)
                        .collect(Collectors.toList());
            }

            RelationType descriptor = relationType.withAlias(relation.getAlias().getValue(), aliases);

            return createAndAssignScope(relation, scope, descriptor);
        }

        @Override
        protected Scope visitSampledRelation(SampledRelation relation, Optional<Scope> scope)
        {
            Expression samplePercentage = relation.getSamplePercentage();
            if (!SymbolsExtractor.extractNames(samplePercentage, analysis.getColumnReferences()).isEmpty()) {
                throw semanticException(EXPRESSION_NOT_CONSTANT, samplePercentage, "Sample percentage cannot contain column references");
            }

            Map<NodeRef<Expression>, Type> expressionTypes = ExpressionAnalyzer.analyzeExpressions(
                    session,
                    metadata,
                    accessControl,
                    sqlParser,
                    TypeProvider.empty(),
                    ImmutableList.of(samplePercentage),
                    analysis.getParameters(),
                    WarningCollector.NOOP,
                    analysis.isDescribe())
                    .getExpressionTypes();

            Type samplePercentageType = expressionTypes.get(NodeRef.of(samplePercentage));
            if (!typeCoercion.canCoerce(samplePercentageType, DOUBLE)) {
                throw semanticException(TYPE_MISMATCH, samplePercentage, "Sample percentage should be a numeric expression");
            }

            ExpressionInterpreter samplePercentageEval = expressionOptimizer(samplePercentage, metadata, session, expressionTypes);

            Object samplePercentageObject = samplePercentageEval.optimize(symbol -> {
                throw semanticException(EXPRESSION_NOT_CONSTANT, samplePercentage, "Sample percentage cannot contain column references");
            });

            if (samplePercentageObject == null) {
                throw semanticException(INVALID_ARGUMENTS, samplePercentage, "Sample percentage cannot be NULL");
            }

            if (samplePercentageType != DOUBLE) {
                ResolvedFunction coercion = metadata.getCoercion(samplePercentageType, DOUBLE);
                InterpretedFunctionInvoker functionInvoker = new InterpretedFunctionInvoker(metadata);
                samplePercentageObject = functionInvoker.invoke(coercion, session.toConnectorSession(), samplePercentageObject);
                verify(samplePercentageObject != null, "Coercion from %s to %s returned null", samplePercentageType, DOUBLE);
            }

            double samplePercentageValue = (double) samplePercentageObject;

            if (samplePercentageValue < 0.0) {
                throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, samplePercentage, "Sample percentage must be greater than or equal to 0");
            }
            if ((samplePercentageValue > 100.0)) {
                throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, samplePercentage, "Sample percentage must be less than or equal to 100");
            }

            analysis.setSampleRatio(relation, samplePercentageValue / 100);
            Scope relationScope = process(relation.getRelation(), scope);
            return createAndAssignScope(relation, scope, relationScope.getRelationType());
        }

        @Override
        protected Scope visitTableSubquery(TableSubquery node, Optional<Scope> scope)
        {
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector, CorrelationSupport.ALLOWED);
            Scope queryScope = analyzer.analyze(node.getQuery(), scope);
            return createAndAssignScope(node, scope, queryScope.getRelationType());
        }

        @Override
        protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
            // to pass down to analyzeFrom

            Scope sourceScope = analyzeFrom(node, scope);

            node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));

            List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
            GroupingSetAnalysis groupByAnalysis = analyzeGroupBy(node, sourceScope, outputExpressions);
            analyzeHaving(node, sourceScope);

            Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);

            List<Expression> orderByExpressions = emptyList();
            Optional<Scope> orderByScope = Optional.empty();
            if (node.getOrderBy().isPresent()) {
                OrderBy orderBy = node.getOrderBy().get();
                orderByScope = Optional.of(computeAndAssignOrderByScope(orderBy, sourceScope, outputScope));

                orderByExpressions = analyzeOrderBy(node, orderBy.getSortItems(), orderByScope.get());

                if (sourceScope.getOuterQueryParent().isPresent() && node.getLimit().isEmpty() && node.getOffset().isEmpty()) {
                    // not the root scope and ORDER BY is ineffective
                    analysis.markRedundantOrderBy(orderBy);
                    warningCollector.add(new PrestoWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
                }
            }
            analysis.setOrderByExpressions(node, orderByExpressions);

            if (node.getOffset().isPresent()) {
                analyzeOffset(node.getOffset().get(), outputScope);
            }

            if (node.getLimit().isPresent()) {
                boolean requiresOrderBy = analyzeLimit(node.getLimit().get(), outputScope);
                if (requiresOrderBy && node.getOrderBy().isEmpty()) {
                    throw semanticException(MISSING_ORDER_BY, node.getLimit().get(), "FETCH FIRST WITH TIES clause requires ORDER BY");
                }
            }

            List<Expression> sourceExpressions = new ArrayList<>();
            analysis.getSelectExpressions(node).stream()
                    .map(SelectExpression::getExpression)
                    .forEach(sourceExpressions::add);
            node.getHaving().ifPresent(sourceExpressions::add);

            analyzeGroupingOperations(node, sourceExpressions, orderByExpressions);
            analyzeAggregations(node, sourceScope, orderByScope, groupByAnalysis, sourceExpressions, orderByExpressions);
            analyzeWindowFunctions(node, outputExpressions, orderByExpressions);

            if (analysis.isAggregation(node) && node.getOrderBy().isPresent()) {
                ImmutableList.Builder<Expression> aggregates = ImmutableList.<Expression>builder()
                        .addAll(groupByAnalysis.getOriginalExpressions())
                        .addAll(extractAggregateFunctions(orderByExpressions, metadata))
                        .addAll(extractExpressions(orderByExpressions, GroupingOperation.class));

                analysis.setOrderByAggregates(node.getOrderBy().get(), aggregates.build());
            }

            if (node.getOrderBy().isPresent() && node.getSelect().isDistinct()) {
                verifySelectDistinct(node, orderByExpressions, outputExpressions, sourceScope, orderByScope.get());
            }

            return outputScope;
        }

        @Override
        protected Scope visitSubqueryExpression(SubqueryExpression node, Optional<Scope> context)
        {
            return process(node.getQuery(), context);
        }

        @Override
        protected Scope visitSetOperation(SetOperation node, Optional<Scope> scope)
        {
            checkState(node.getRelations().size() >= 2);

            List<RelationType> childrenTypes = node.getRelations().stream()
                    .map(relation -> process(relation, scope).getRelationType().withOnlyVisibleFields())
                    .collect(toImmutableList());

            Type[] outputFieldTypes = childrenTypes.get(0).getVisibleFields().stream()
                    .map(Field::getType)
                    .toArray(Type[]::new);
            for (RelationType relationType : childrenTypes) {
                int outputFieldSize = outputFieldTypes.length;
                int descFieldSize = relationType.getVisibleFields().size();
                String setOperationName = node.getClass().getSimpleName().toUpperCase(ENGLISH);
                if (outputFieldSize != descFieldSize) {
                    throw semanticException(
                            TYPE_MISMATCH,
                            node,
                            "%s query has different number of fields: %d, %d",
                            setOperationName,
                            outputFieldSize,
                            descFieldSize);
                }
                for (int i = 0; i < descFieldSize; i++) {
                    Type descFieldType = relationType.getFieldByIndex(i).getType();
                    Optional<Type> commonSuperType = typeCoercion.getCommonSuperType(outputFieldTypes[i], descFieldType);
                    if (commonSuperType.isEmpty()) {
                        throw semanticException(
                                TYPE_MISMATCH,
                                node,
                                "column %d in %s query has incompatible types: %s, %s",
                                i + 1,
                                setOperationName,
                                outputFieldTypes[i].getDisplayName(),
                                descFieldType.getDisplayName());
                    }
                    outputFieldTypes[i] = commonSuperType.get();
                }
            }

            Field[] outputDescriptorFields = new Field[outputFieldTypes.length];
            RelationType firstDescriptor = childrenTypes.get(0);
            for (int i = 0; i < outputFieldTypes.length; i++) {
                Field oldField = firstDescriptor.getFieldByIndex(i);
                outputDescriptorFields[i] = new Field(
                        oldField.getRelationAlias(),
                        oldField.getName(),
                        outputFieldTypes[i],
                        oldField.isHidden(),
                        oldField.getOriginTable(),
                        oldField.getOriginColumnName(),
                        oldField.isAliased());
            }

            for (int i = 0; i < node.getRelations().size(); i++) {
                Relation relation = node.getRelations().get(i);
                RelationType relationType = childrenTypes.get(i);
                for (int j = 0; j < relationType.getVisibleFields().size(); j++) {
                    Type outputFieldType = outputFieldTypes[j];
                    Type descFieldType = relationType.getFieldByIndex(j).getType();
                    if (!outputFieldType.equals(descFieldType)) {
                        analysis.addRelationCoercion(relation, outputFieldTypes);
                        break;
                    }
                }
            }
            return createAndAssignScope(node, scope, outputDescriptorFields);
        }

        @Override
        protected Scope visitIntersect(Intersect node, Optional<Scope> scope)
        {
            if (!node.isDistinct()) {
                throw semanticException(NOT_SUPPORTED, node, "INTERSECT ALL not yet implemented");
            }

            return visitSetOperation(node, scope);
        }

        @Override
        protected Scope visitExcept(Except node, Optional<Scope> scope)
        {
            if (!node.isDistinct()) {
                throw semanticException(NOT_SUPPORTED, node, "EXCEPT ALL not yet implemented");
            }

            return visitSetOperation(node, scope);
        }

        @Override
        protected Scope visitJoin(Join node, Optional<Scope> scope)
        {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            if (criteria instanceof NaturalJoin) {
                throw semanticException(NOT_SUPPORTED, node, "Natural join not supported");
            }

            Scope left = process(node.getLeft(), scope);
            Scope right = process(node.getRight(), isLateralRelation(node.getRight()) ? Optional.of(left) : scope);

            if (isLateralRelation(node.getRight())) {
                if (node.getType() == RIGHT || node.getType() == FULL) {
                    Stream<Expression> leftScopeReferences = getReferencesToScope(node.getRight(), analysis, left);
                    leftScopeReferences.findFirst().ifPresent(reference -> {
                        throw semanticException(INVALID_COLUMN_REFERENCE, reference, "LATERAL reference not allowed in %s JOIN", node.getType().name());
                    });
                }
                if (isUnnestRelation(node.getRight())) {
                    if (criteria != null) {
                        if (!(criteria instanceof JoinOn) || !((JoinOn) criteria).getExpression().equals(TRUE_LITERAL)) {
                            throw semanticException(
                                    NOT_SUPPORTED,
                                    criteria instanceof JoinOn ? ((JoinOn) criteria).getExpression() : node,
                                    "%s JOIN involving UNNEST is only supported with condition ON TRUE",
                                    node.getType().name());
                        }
                    }
                }
                else if (node.getType() == FULL) {
                    if (!(criteria instanceof JoinOn) || !((JoinOn) criteria).getExpression().equals(TRUE_LITERAL)) {
                        throw semanticException(
                                NOT_SUPPORTED,
                                criteria instanceof JoinOn ? ((JoinOn) criteria).getExpression() : node,
                                "FULL JOIN involving LATERAL relation is only supported with condition ON TRUE");
                    }
                }
            }

            if (criteria instanceof JoinUsing) {
                return analyzeJoinUsing(node, ((JoinUsing) criteria).getColumns(), scope, left, right);
            }

            Scope output = createAndAssignScope(node, scope, left.getRelationType().joinWith(right.getRelationType()));

            if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT) {
                return output;
            }
            if (criteria instanceof JoinOn) {
                Expression expression = ((JoinOn) criteria).getExpression();

                // Need to register coercions in case when join criteria requires coercion (e.g. join on char(1) = char(2))
                // Correlations are only currently support in the join criteria for INNER joins
                ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, output, node.getType() == INNER ? CorrelationSupport.ALLOWED : CorrelationSupport.DISALLOWED);
                Type clauseType = expressionAnalysis.getType(expression);
                if (!clauseType.equals(BOOLEAN)) {
                    if (!clauseType.equals(UNKNOWN)) {
                        throw semanticException(TYPE_MISMATCH, expression, "JOIN ON clause must evaluate to a boolean: actual type %s", clauseType);
                    }
                    // coerce null to boolean
                    analysis.addCoercion(expression, BOOLEAN, false);
                }

                verifyNoAggregateWindowOrGroupingFunctions(metadata, expression, "JOIN clause");

                analysis.recordSubqueries(node, expressionAnalysis);
                analysis.setJoinCriteria(node, expression);
            }
            else {
                throw new UnsupportedOperationException("Unsupported join criteria: " + criteria.getClass().getName());
            }

            return output;
        }

        private Scope analyzeJoinUsing(Join node, List<Identifier> columns, Optional<Scope> scope, Scope left, Scope right)
        {
            List<Field> joinFields = new ArrayList<>();

            List<Integer> leftJoinFields = new ArrayList<>();
            List<Integer> rightJoinFields = new ArrayList<>();

            Set<Identifier> seen = new HashSet<>();
            for (Identifier column : columns) {
                if (!seen.add(column)) {
                    throw semanticException(DUPLICATE_COLUMN_NAME, column, "Column '%s' appears multiple times in USING clause", column.getValue());
                }

                Optional<ResolvedField> leftField = left.tryResolveField(column);
                Optional<ResolvedField> rightField = right.tryResolveField(column);

                if (leftField.isEmpty()) {
                    throw semanticException(COLUMN_NOT_FOUND, column, "Column '%s' is missing from left side of join", column.getValue());
                }
                if (rightField.isEmpty()) {
                    throw semanticException(COLUMN_NOT_FOUND, column, "Column '%s' is missing from right side of join", column.getValue());
                }

                // ensure a comparison operator exists for the given types (applying coercions if necessary)
                try {
                    metadata.resolveOperator(OperatorType.EQUAL, ImmutableList.of(
                            leftField.get().getType(), rightField.get().getType()));
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, column, "%s", e.getMessage());
                }

                Optional<Type> type = typeCoercion.getCommonSuperType(leftField.get().getType(), rightField.get().getType());
                analysis.addTypes(ImmutableMap.of(NodeRef.of(column), type.get()));

                joinFields.add(Field.newUnqualified(column.getValue(), type.get()));

                leftJoinFields.add(leftField.get().getRelationFieldIndex());
                rightJoinFields.add(rightField.get().getRelationFieldIndex());
            }

            ImmutableList.Builder<Field> outputs = ImmutableList.builder();
            outputs.addAll(joinFields);

            ImmutableList.Builder<Integer> leftFields = ImmutableList.builder();
            for (int i = 0; i < left.getRelationType().getAllFieldCount(); i++) {
                if (!leftJoinFields.contains(i)) {
                    outputs.add(left.getRelationType().getFieldByIndex(i));
                    leftFields.add(i);
                }
            }

            ImmutableList.Builder<Integer> rightFields = ImmutableList.builder();
            for (int i = 0; i < right.getRelationType().getAllFieldCount(); i++) {
                if (!rightJoinFields.contains(i)) {
                    outputs.add(right.getRelationType().getFieldByIndex(i));
                    rightFields.add(i);
                }
            }

            analysis.setJoinUsing(node, new Analysis.JoinUsingAnalysis(leftJoinFields, rightJoinFields, leftFields.build(), rightFields.build()));

            return createAndAssignScope(node, scope, new RelationType(outputs.build()));
        }

        private boolean isLateralRelation(Relation node)
        {
            if (node instanceof AliasedRelation) {
                return isLateralRelation(((AliasedRelation) node).getRelation());
            }
            return node instanceof Unnest || node instanceof Lateral;
        }

        private boolean isUnnestRelation(Relation node)
        {
            if (node instanceof AliasedRelation) {
                return isUnnestRelation(((AliasedRelation) node).getRelation());
            }
            return node instanceof Unnest;
        }

        @Override
        protected Scope visitValues(Values node, Optional<Scope> scope)
        {
            checkState(node.getRows().size() >= 1);

            List<List<Type>> rowTypes = node.getRows().stream()
                    .map(row -> analyzeExpression(row, createScope(scope)).getType(row))
                    .map(type -> {
                        if (type instanceof RowType) {
                            return type.getTypeParameters();
                        }
                        return ImmutableList.of(type);
                    })
                    .collect(toImmutableList());

            // determine common super type of the rows
            List<Type> fieldTypes = new ArrayList<>(rowTypes.iterator().next());
            for (List<Type> rowType : rowTypes) {
                // check field count consistency for rows
                if (rowType.size() != fieldTypes.size()) {
                    throw semanticException(TYPE_MISMATCH,
                            node,
                            "Values rows have mismatched types: %s vs %s",
                            rowTypes.get(0),
                            rowType);
                }

                for (int i = 0; i < rowType.size(); i++) {
                    Type fieldType = rowType.get(i);
                    Type superType = fieldTypes.get(i);

                    Optional<Type> commonSuperType = typeCoercion.getCommonSuperType(fieldType, superType);
                    if (commonSuperType.isEmpty()) {
                        throw semanticException(TYPE_MISMATCH,
                                node,
                                "Values rows have mismatched types: %s vs %s",
                                rowTypes.get(0),
                                rowType);
                    }
                    fieldTypes.set(i, commonSuperType.get());
                }
            }

            // add coercions for the rows
            for (Expression row : node.getRows()) {
                if (row instanceof Row) {
                    List<Expression> items = ((Row) row).getItems();
                    for (int i = 0; i < items.size(); i++) {
                        Type expectedType = fieldTypes.get(i);
                        Expression item = items.get(i);
                        Type actualType = analysis.getType(item);
                        if (!actualType.equals(expectedType)) {
                            analysis.addCoercion(item, expectedType, typeCoercion.isTypeOnlyCoercion(actualType, expectedType));
                        }
                    }
                }
                else {
                    Type actualType = analysis.getType(row);
                    Type expectedType = fieldTypes.get(0);
                    if (!actualType.equals(expectedType)) {
                        analysis.addCoercion(row, expectedType, typeCoercion.isTypeOnlyCoercion(actualType, expectedType));
                    }
                }
            }

            List<Field> fields = fieldTypes.stream()
                    .map(valueType -> Field.newUnqualified(Optional.empty(), valueType))
                    .collect(toImmutableList());

            return createAndAssignScope(node, scope, fields);
        }

        private void analyzeWindowFunctions(QuerySpecification node, List<Expression> outputExpressions, List<Expression> orderByExpressions)
        {
            analysis.setWindowFunctions(node, analyzeWindowFunctions(node, outputExpressions));
            if (node.getOrderBy().isPresent()) {
                analysis.setOrderByWindowFunctions(node.getOrderBy().get(), analyzeWindowFunctions(node, orderByExpressions));
            }
        }

        private List<FunctionCall> analyzeWindowFunctions(QuerySpecification node, List<Expression> expressions)
        {
            for (Expression expression : expressions) {
                new WindowFunctionValidator(metadata).process(expression, analysis);
            }

            List<FunctionCall> windowFunctions = extractWindowFunctions(expressions);

            for (FunctionCall windowFunction : windowFunctions) {
                // filter with window function is not supported yet
                if (windowFunction.getFilter().isPresent()) {
                    throw semanticException(NOT_SUPPORTED, node, "FILTER is not yet supported for window functions");
                }

                if (windowFunction.getOrderBy().isPresent()) {
                    throw semanticException(NOT_SUPPORTED, windowFunction, "Window function with ORDER BY is not supported");
                }

                Window window = windowFunction.getWindow().get();

                ImmutableList.Builder<Node> toExtract = ImmutableList.builder();
                toExtract.addAll(windowFunction.getArguments());
                toExtract.addAll(window.getPartitionBy());
                window.getOrderBy().ifPresent(orderBy -> toExtract.addAll(orderBy.getSortItems()));
                window.getFrame().ifPresent(toExtract::add);

                List<FunctionCall> nestedWindowFunctions = extractWindowFunctions(toExtract.build());

                if (!nestedWindowFunctions.isEmpty()) {
                    throw semanticException(NESTED_WINDOW, node, "Cannot nest window functions inside window function '%s': %s",
                            windowFunction,
                            windowFunctions);
                }

                if (windowFunction.isDistinct()) {
                    throw semanticException(NOT_SUPPORTED, node, "DISTINCT in window function parameters not yet supported: %s", windowFunction);
                }

                // TODO get function requirements from window function metadata when we have it
                String name = windowFunction.getName().toString().toLowerCase(ENGLISH);
                if (name.equals("lag") || name.equals("lead")) {
                    if (window.getOrderBy().isEmpty()) {
                        throw semanticException(MISSING_ORDER_BY, window, "%s function requires an ORDER BY window clause", windowFunction.getName());
                    }
                    if (window.getFrame().isPresent()) {
                        throw semanticException(INVALID_WINDOW_FRAME, window.getFrame().get(), "Cannot specify window frame for %s function", windowFunction.getName());
                    }
                }

                if (!WINDOW_VALUE_FUNCTIONS.contains(name) && windowFunction.getNullTreatment().isPresent()) {
                    throw semanticException(NULL_TREATMENT_NOT_ALLOWED, windowFunction, "Cannot specify null treatment clause for %s function", windowFunction.getName());
                }

                if (window.getFrame().isPresent()) {
                    analyzeWindowFrame(window.getFrame().get());
                }

                List<Type> argumentTypes = mappedCopy(windowFunction.getArguments(), analysis::getType);

                ResolvedFunction resolvedFunction = metadata.resolveFunction(windowFunction.getName(), fromTypes(argumentTypes));
                FunctionKind kind = metadata.getFunctionMetadata(resolvedFunction).getKind();
                if (kind != AGGREGATE && kind != WINDOW) {
                    throw semanticException(FUNCTION_NOT_WINDOW, node, "Not a window function: %s", windowFunction.getName());
                }
            }

            return windowFunctions;
        }

        private void analyzeWindowFrame(WindowFrame frame)
        {
            FrameBound.Type startType = frame.getStart().getType();
            FrameBound.Type endType = frame.getEnd().orElse(new FrameBound(CURRENT_ROW)).getType();

            if (startType == UNBOUNDED_FOLLOWING) {
                throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame start cannot be UNBOUNDED FOLLOWING");
            }
            if (endType == UNBOUNDED_PRECEDING) {
                throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame end cannot be UNBOUNDED PRECEDING");
            }
            if ((startType == CURRENT_ROW) && (endType == PRECEDING)) {
                throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from CURRENT ROW cannot end with PRECEDING");
            }
            if ((startType == FOLLOWING) && (endType == PRECEDING)) {
                throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from FOLLOWING cannot end with PRECEDING");
            }
            if ((startType == FOLLOWING) && (endType == CURRENT_ROW)) {
                throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame starting from FOLLOWING cannot end with CURRENT ROW");
            }
            if ((frame.getType() == RANGE) && ((startType == PRECEDING) || (endType == PRECEDING))) {
                throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame RANGE PRECEDING is only supported with UNBOUNDED");
            }
            if ((frame.getType() == RANGE) && ((startType == FOLLOWING) || (endType == FOLLOWING))) {
                throw semanticException(INVALID_WINDOW_FRAME, frame, "Window frame RANGE FOLLOWING is only supported with UNBOUNDED");
            }
        }

        private void analyzeHaving(QuerySpecification node, Scope scope)
        {
            if (node.getHaving().isPresent()) {
                Expression predicate = node.getHaving().get();

                ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);

                expressionAnalysis.getWindowFunctions().stream()
                        .findFirst()
                        .ifPresent(function -> {
                            throw semanticException(NESTED_WINDOW, function.getNode(), "HAVING clause cannot contain window functions");
                        });

                analysis.recordSubqueries(node, expressionAnalysis);

                Type predicateType = expressionAnalysis.getType(predicate);
                if (!predicateType.equals(BOOLEAN) && !predicateType.equals(UNKNOWN)) {
                    throw semanticException(TYPE_MISMATCH, predicate, "HAVING clause must evaluate to a boolean: actual type %s", predicateType);
                }

                analysis.setHaving(node, predicate);
            }
        }

        private void checkGroupingSetsCount(GroupBy node)
        {
            // If groupBy is distinct then crossProduct will be overestimated if there are duplicate grouping sets.
            int crossProduct = 1;
            for (GroupingElement element : node.getGroupingElements()) {
                try {
                    int product;
                    if (element instanceof SimpleGroupBy) {
                        product = 1;
                    }
                    else if (element instanceof Cube) {
                        int exponent = element.getExpressions().size();
                        if (exponent > 30) {
                            throw new ArithmeticException();
                        }
                        product = 1 << exponent;
                    }
                    else if (element instanceof Rollup) {
                        product = element.getExpressions().size() + 1;
                    }
                    else if (element instanceof GroupingSets) {
                        product = ((GroupingSets) element).getSets().size();
                    }
                    else {
                        throw new UnsupportedOperationException("Unsupported grouping element type: " + element.getClass().getName());
                    }
                    crossProduct = Math.multiplyExact(crossProduct, product);
                }
                catch (ArithmeticException e) {
                    throw semanticException(TOO_MANY_GROUPING_SETS, node,
                            "GROUP BY has more than %s grouping sets but can contain at most %s", Integer.MAX_VALUE, getMaxGroupingSets(session));
                }
                if (crossProduct > getMaxGroupingSets(session)) {
                    throw semanticException(TOO_MANY_GROUPING_SETS, node,
                            "GROUP BY has %s grouping sets but can contain at most %s", crossProduct, getMaxGroupingSets(session));
                }
            }
        }

        private GroupingSetAnalysis analyzeGroupBy(QuerySpecification node, Scope scope, List<Expression> outputExpressions)
        {
            if (node.getGroupBy().isPresent()) {
                ImmutableList.Builder<Set<FieldId>> cubes = ImmutableList.builder();
                ImmutableList.Builder<List<FieldId>> rollups = ImmutableList.builder();
                ImmutableList.Builder<List<Set<FieldId>>> sets = ImmutableList.builder();
                ImmutableList.Builder<Expression> complexExpressions = ImmutableList.builder();
                ImmutableList.Builder<Expression> groupingExpressions = ImmutableList.builder();

                checkGroupingSetsCount(node.getGroupBy().get());
                for (GroupingElement groupingElement : node.getGroupBy().get().getGroupingElements()) {
                    if (groupingElement instanceof SimpleGroupBy) {
                        for (Expression column : groupingElement.getExpressions()) {
                            // simple GROUP BY expressions allow ordinals or arbitrary expressions
                            if (column instanceof LongLiteral) {
                                long ordinal = ((LongLiteral) column).getValue();
                                if (ordinal < 1 || ordinal > outputExpressions.size()) {
                                    throw semanticException(INVALID_COLUMN_REFERENCE, column, "GROUP BY position %s is not in select list", ordinal);
                                }

                                column = outputExpressions.get(toIntExact(ordinal - 1));
                            }
                            else {
                                analyzeExpression(column, scope);
                            }

                            ResolvedField field = analysis.getColumnReferenceFields().get(NodeRef.of(column));
                            if (field != null) {
                                sets.add(ImmutableList.of(ImmutableSet.of(field.getFieldId())));
                            }
                            else {
                                verifyNoAggregateWindowOrGroupingFunctions(metadata, column, "GROUP BY clause");
                                analysis.recordSubqueries(node, analyzeExpression(column, scope));
                                complexExpressions.add(column);
                            }

                            groupingExpressions.add(column);
                        }
                    }
                    else {
                        for (Expression column : groupingElement.getExpressions()) {
                            analyzeExpression(column, scope);
                            if (!analysis.getColumnReferences().contains(NodeRef.of(column))) {
                                throw semanticException(INVALID_COLUMN_REFERENCE, column, "GROUP BY expression must be a column reference: %s", column);
                            }

                            groupingExpressions.add(column);
                        }

                        if (groupingElement instanceof Cube) {
                            Set<FieldId> cube = groupingElement.getExpressions().stream()
                                    .map(NodeRef::of)
                                    .map(analysis.getColumnReferenceFields()::get)
                                    .map(ResolvedField::getFieldId)
                                    .collect(toImmutableSet());

                            cubes.add(cube);
                        }
                        else if (groupingElement instanceof Rollup) {
                            List<FieldId> rollup = groupingElement.getExpressions().stream()
                                    .map(NodeRef::of)
                                    .map(analysis.getColumnReferenceFields()::get)
                                    .map(ResolvedField::getFieldId)
                                    .collect(toImmutableList());

                            rollups.add(rollup);
                        }
                        else if (groupingElement instanceof GroupingSets) {
                            List<Set<FieldId>> groupingSets = ((GroupingSets) groupingElement).getSets().stream()
                                    .map(set -> set.stream()
                                            .map(NodeRef::of)
                                            .map(analysis.getColumnReferenceFields()::get)
                                            .map(ResolvedField::getFieldId)
                                            .collect(toImmutableSet()))
                                    .collect(toImmutableList());

                            sets.add(groupingSets);
                        }
                    }
                }

                List<Expression> expressions = groupingExpressions.build();
                for (Expression expression : expressions) {
                    Type type = analysis.getType(expression);
                    if (!type.isComparable()) {
                        throw semanticException(TYPE_MISMATCH, node, "%s is not comparable, and therefore cannot be used in GROUP BY", type);
                    }
                }

                GroupingSetAnalysis groupingSets = new GroupingSetAnalysis(expressions, cubes.build(), rollups.build(), sets.build(), complexExpressions.build());
                analysis.setGroupingSets(node, groupingSets);

                return groupingSets;
            }

            GroupingSetAnalysis result = new GroupingSetAnalysis(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());

            if (hasAggregates(node) || node.getHaving().isPresent()) {
                analysis.setGroupingSets(node, result);
            }

            return result;
        }

        private boolean hasAggregates(QuerySpecification node)
        {
            List<Node> toExtract = ImmutableList.<Node>builder()
                    .addAll(node.getSelect().getSelectItems())
                    .addAll(getSortItemsFromOrderBy(node.getOrderBy()))
                    .build();

            List<FunctionCall> aggregates = extractAggregateFunctions(toExtract, metadata);

            return !aggregates.isEmpty();
        }

        private Scope computeAndAssignOutputScope(QuerySpecification node, Optional<Scope> scope, Scope sourceScope)
        {
            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    List<Field> itemOutputFields = analysis.getSelectAllResultFields((AllColumns) item);
                    checkNotNull(itemOutputFields, "output fields is null for select item %s", item);
                    outputFields.addAll(itemOutputFields);
                }
                else if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;

                    Expression expression = column.getExpression();
                    Optional<Identifier> field = column.getAlias();

                    Optional<QualifiedObjectName> originTable = Optional.empty();
                    Optional<String> originColumn = Optional.empty();
                    QualifiedName name = null;

                    if (expression instanceof Identifier) {
                        name = QualifiedName.of(((Identifier) expression).getValue());
                    }
                    else if (expression instanceof DereferenceExpression) {
                        name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
                    }

                    if (name != null) {
                        List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
                        if (!matchingFields.isEmpty()) {
                            originTable = matchingFields.get(0).getOriginTable();
                            originColumn = matchingFields.get(0).getOriginColumnName();
                        }
                    }

                    if (field.isEmpty()) {
                        if (name != null) {
                            field = Optional.of(getLast(name.getOriginalParts()));
                        }
                    }

                    outputFields.add(Field.newUnqualified(field.map(Identifier::getValue), analysis.getType(expression), originTable, originColumn, column.getAlias().isPresent())); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }

            return createAndAssignScope(node, scope, outputFields.build());
        }

        private Scope computeAndAssignOrderByScope(OrderBy node, Scope sourceScope, Scope outputScope)
        {
            // ORDER BY should "see" both output and FROM fields during initial analysis and non-aggregation query planning
            Scope orderByScope = Scope.builder()
                    .withParent(sourceScope)
                    .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
                    .build();
            analysis.setScope(node, orderByScope);
            return orderByScope;
        }

        private List<Expression> analyzeSelect(QuerySpecification node, Scope scope)
        {
            ImmutableList.Builder<Expression> outputExpressionBuilder = ImmutableList.builder();
            ImmutableList.Builder<SelectExpression> selectExpressionBuilder = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    analyzeSelectAllColumns((AllColumns) item, node, scope, outputExpressionBuilder, selectExpressionBuilder);
                }
                else if (item instanceof SingleColumn) {
                    analyzeSelectSingleColumn((SingleColumn) item, node, scope, outputExpressionBuilder, selectExpressionBuilder);
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }
            analysis.setSelectExpressions(node, selectExpressionBuilder.build());

            return outputExpressionBuilder.build();
        }

        private void analyzeSelectAllColumns(
                AllColumns allColumns,
                QuerySpecification node,
                Scope scope,
                ImmutableList.Builder<Expression> outputExpressionBuilder,
                ImmutableList.Builder<SelectExpression> selectExpressionBuilder)
        {
            // expand * and expression.*
            if (allColumns.getTarget().isPresent()) {
                // analyze AllColumns with target expression (expression.*)
                Expression expression = allColumns.getTarget().get();

                QualifiedName prefix = asQualifiedName(expression);
                if (prefix != null) {
                    // analyze prefix as an 'asterisked identifier chain'
                    Optional<AsteriskedIdentifierChainBasis> identifierChainBasis = scope.resolveAsteriskedIdentifierChainBasis(prefix, allColumns);
                    if (identifierChainBasis.isEmpty()) {
                        throw semanticException(TABLE_NOT_FOUND, allColumns, "Unable to resolve reference %s", prefix);
                    }
                    if (identifierChainBasis.get().getBasisType() == TABLE) {
                        RelationType relationType = identifierChainBasis.get().getRelationType().get();
                        List<Field> fields = relationType.resolveVisibleFieldsWithRelationPrefix(Optional.of(prefix));
                        if (fields.isEmpty()) {
                            throw semanticException(COLUMN_NOT_FOUND, allColumns, "SELECT * not allowed from relation that has no columns");
                        }
                        boolean local = scope.isLocalScope(identifierChainBasis.get().getScope().get());
                        analyzeAllColumnsFromTable(
                                fields,
                                allColumns,
                                node,
                                local ? scope : identifierChainBasis.get().getScope().get(),
                                outputExpressionBuilder,
                                selectExpressionBuilder,
                                relationType,
                                local);
                        return;
                    }
                }
                // identifierChainBasis.get().getBasisType == FIELD or target expression isn't a QualifiedName
                analyzeAllFieldsFromRowTypeExpression(expression, allColumns, node, scope, outputExpressionBuilder, selectExpressionBuilder);
            }
            else {
                // analyze AllColumns without target expression ('*')
                if (!allColumns.getAliases().isEmpty()) {
                    throw semanticException(NOT_SUPPORTED, allColumns, "Column aliases not supported");
                }

                List<Field> fields = (List<Field>) scope.getRelationType().getVisibleFields();
                if (fields.isEmpty()) {
                    if (node.getFrom().isEmpty()) {
                        throw semanticException(COLUMN_NOT_FOUND, allColumns, "SELECT * not allowed in queries without FROM clause");
                    }
                    throw semanticException(COLUMN_NOT_FOUND, allColumns, "SELECT * not allowed from relation that has no columns");
                }

                analyzeAllColumnsFromTable(fields, allColumns, node, scope, outputExpressionBuilder, selectExpressionBuilder, scope.getRelationType(), true);
            }
        }

        private void analyzeAllColumnsFromTable(
                List<Field> fields,
                AllColumns allColumns,
                QuerySpecification node,
                Scope scope,
                ImmutableList.Builder<Expression> outputExpressionBuilder,
                ImmutableList.Builder<SelectExpression> selectExpressionBuilder,
                RelationType relationType,
                boolean local)
        {
            if (!allColumns.getAliases().isEmpty()) {
                validateColumnAliasesCount(allColumns.getAliases(), fields.size());
            }

            ImmutableList.Builder<Field> itemOutputFieldBuilder = ImmutableList.builder();

            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                Expression fieldExpression;
                if (local) {
                    fieldExpression = new FieldReference(relationType.indexOf(field));
                }
                else {
                    if (field.getName().isEmpty()) {
                        throw semanticException(NOT_SUPPORTED, node.getSelect(), "SELECT * from outer scope table not supported with anonymous columns");
                    }
                    checkState(field.getRelationAlias().isPresent(), "missing relation alias");
                    fieldExpression = new DereferenceExpression(DereferenceExpression.from(field.getRelationAlias().get()), new Identifier(field.getName().get()));
                }
                analyzeExpression(fieldExpression, scope);
                outputExpressionBuilder.add(fieldExpression);
                selectExpressionBuilder.add(new SelectExpression(fieldExpression, Optional.empty()));

                Optional<String> alias = field.getName();
                if (!allColumns.getAliases().isEmpty()) {
                    alias = Optional.of((allColumns.getAliases().get(i)).getValue());
                }

                itemOutputFieldBuilder.add(new Field(
                        field.getRelationAlias(),
                        alias,
                        field.getType(),
                        false,
                        field.getOriginTable(),
                        field.getOriginColumnName(),
                        !allColumns.getAliases().isEmpty() || field.isAliased()));

                Type type = field.getType();
                if (node.getSelect().isDistinct() && !type.isComparable()) {
                    throw semanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s)", type);
                }
            }
            analysis.setSelectAllResultFields(allColumns, itemOutputFieldBuilder.build());
        }

        private void analyzeAllFieldsFromRowTypeExpression(
                Expression expression,
                AllColumns allColumns,
                QuerySpecification node,
                Scope scope,
                ImmutableList.Builder<Expression> outputExpressionBuilder,
                ImmutableList.Builder<SelectExpression> selectExpressionBuilder)
        {
            ImmutableList.Builder<Field> itemOutputFieldBuilder = ImmutableList.builder();

            ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);
            Type type = expressionAnalysis.getType(expression);
            if (!(type instanceof RowType)) {
                throw semanticException(TYPE_MISMATCH, node.getSelect(), "expected expression of type Row");
            }
            int referencedFieldsCount = ((RowType) type).getFields().size();
            if (!allColumns.getAliases().isEmpty()) {
                validateColumnAliasesCount(allColumns.getAliases(), referencedFieldsCount);
            }
            analysis.recordSubqueries(node, expressionAnalysis);

            ImmutableList.Builder<Expression> unfoldedExpressionsBuilder = ImmutableList.builder();
            for (int i = 0; i < referencedFieldsCount; i++) {
                Expression outputExpression = new SubscriptExpression(expression, new LongLiteral("" + (i + 1)));
                outputExpressionBuilder.add(outputExpression);
                analyzeExpression(outputExpression, scope);
                unfoldedExpressionsBuilder.add(outputExpression);

                Type outputExpressionType = type.getTypeParameters().get(i);
                if (node.getSelect().isDistinct() && !(outputExpressionType.isComparable())) {
                    throw semanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s)", type.getTypeParameters().get(i));
                }

                Optional<String> name = ((RowType) type).getFields().get(i).getName();
                if (!allColumns.getAliases().isEmpty()) {
                    name = Optional.of((allColumns.getAliases().get(i)).getValue());
                }
                itemOutputFieldBuilder.add(Field.newUnqualified(name, outputExpressionType));
            }
            selectExpressionBuilder.add(new SelectExpression(expression, Optional.of(unfoldedExpressionsBuilder.build())));
            analysis.setSelectAllResultFields(allColumns, itemOutputFieldBuilder.build());
        }

        private void analyzeSelectSingleColumn(
                SingleColumn singleColumn,
                QuerySpecification node,
                Scope scope,
                ImmutableList.Builder<Expression> outputExpressionBuilder,
                ImmutableList.Builder<SelectExpression> selectExpressionBuilder)
        {
            Expression expression = singleColumn.getExpression();
            ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);
            analysis.recordSubqueries(node, expressionAnalysis);
            outputExpressionBuilder.add(expression);
            selectExpressionBuilder.add(new SelectExpression(expression, Optional.empty()));

            Type type = expressionAnalysis.getType(expression);
            if (node.getSelect().isDistinct() && !type.isComparable()) {
                throw semanticException(
                        TYPE_MISMATCH, node.getSelect(),
                        "DISTINCT can only be applied to comparable types (actual: %s): %s",
                        type,
                        expression);
            }
        }

        public void analyzeWhere(Node node, Scope scope, Expression predicate)
        {
            verifyNoAggregateWindowOrGroupingFunctions(metadata, predicate, "WHERE clause");

            ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);
            analysis.recordSubqueries(node, expressionAnalysis);

            Type predicateType = expressionAnalysis.getType(predicate);
            if (!predicateType.equals(BOOLEAN)) {
                if (!predicateType.equals(UNKNOWN)) {
                    throw semanticException(TYPE_MISMATCH, predicate, "WHERE clause must evaluate to a boolean: actual type %s", predicateType);
                }
                // coerce null to boolean
                analysis.addCoercion(predicate, BOOLEAN, false);
            }

            analysis.setWhere(node, predicate);
        }

        private Scope analyzeFrom(QuerySpecification node, Optional<Scope> scope)
        {
            if (node.getFrom().isPresent()) {
                return process(node.getFrom().get(), scope);
            }

            Scope result = createScope(scope);
            analysis.setImplicitFromScope(node, result);
            return result;
        }

        private void analyzeGroupingOperations(QuerySpecification node, List<Expression> outputExpressions, List<Expression> orderByExpressions)
        {
            List<GroupingOperation> groupingOperations = extractExpressions(Iterables.concat(outputExpressions, orderByExpressions), GroupingOperation.class);
            boolean isGroupingOperationPresent = !groupingOperations.isEmpty();

            if (isGroupingOperationPresent && node.getGroupBy().isEmpty()) {
                throw semanticException(
                        MISSING_GROUP_BY,
                        node,
                        "A GROUPING() operation can only be used with a corresponding GROUPING SET/CUBE/ROLLUP/GROUP BY clause");
            }

            analysis.setGroupingOperations(node, groupingOperations);
        }

        private void analyzeAggregations(
                QuerySpecification node,
                Scope sourceScope,
                Optional<Scope> orderByScope,
                GroupingSetAnalysis groupByAnalysis,
                List<Expression> outputExpressions,
                List<Expression> orderByExpressions)
        {
            checkState(orderByExpressions.isEmpty() || orderByScope.isPresent(), "non-empty orderByExpressions list without orderByScope provided");

            List<FunctionCall> aggregates = extractAggregateFunctions(Iterables.concat(outputExpressions, orderByExpressions), metadata);
            analysis.setAggregates(node, aggregates);

            if (analysis.isAggregation(node)) {
                // ensure SELECT, ORDER BY and HAVING are constant with respect to group
                // e.g, these are all valid expressions:
                //     SELECT f(a) GROUP BY a
                //     SELECT f(a + 1) GROUP BY a + 1
                //     SELECT a + sum(b) GROUP BY a
                List<Expression> distinctGroupingColumns = ImmutableSet.copyOf(groupByAnalysis.getOriginalExpressions()).asList();

                for (Expression expression : outputExpressions) {
                    verifySourceAggregations(distinctGroupingColumns, sourceScope, expression, metadata, analysis);
                }

                for (Expression expression : orderByExpressions) {
                    verifyOrderByAggregations(distinctGroupingColumns, sourceScope, orderByScope.get(), expression, metadata, analysis);
                }
            }
        }

        private RelationType analyzeView(Query query, QualifiedObjectName name, Optional<String> catalog, Optional<String> schema, Optional<String> owner, Table node)
        {
            try {
                // run view as view owner if set; otherwise, run as session user
                Identity identity;
                AccessControl viewAccessControl;
                if (owner.isPresent() && !owner.get().equals(session.getIdentity().getUser())) {
                    identity = Identity.ofUser(owner.get());
                    viewAccessControl = new ViewAccessControl(accessControl, session.getIdentity());
                }
                else {
                    identity = session.getIdentity();
                    viewAccessControl = accessControl;
                }

                // TODO: record path in view definition (?) (check spec) and feed it into the session object we use to evaluate the query defined by the view
                Session viewSession = createViewSession(catalog, schema, identity, session.getPath());

                StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, viewAccessControl, viewSession, warningCollector, CorrelationSupport.ALLOWED);
                Scope queryScope = analyzer.analyze(query, Scope.create());
                return queryScope.getRelationType().withAlias(name.getObjectName(), null);
            }
            catch (RuntimeException e) {
                throw semanticException(INVALID_VIEW, node, e, "Failed analyzing stored view '%s': %s", name, e.getMessage());
            }
        }

        private Query parseView(String view, QualifiedObjectName name, Node node)
        {
            try {
                return (Query) sqlParser.createStatement(view, createParsingOptions(session));
            }
            catch (ParsingException e) {
                throw semanticException(INVALID_VIEW, node, e, "Failed parsing stored view '%s': %s", name, e.getMessage());
            }
        }

        private boolean isViewStale(List<ViewColumn> columns, Collection<Field> fields, QualifiedObjectName name, Node node)
        {
            if (columns.size() != fields.size()) {
                return true;
            }

            List<Field> fieldList = ImmutableList.copyOf(fields);
            for (int i = 0; i < columns.size(); i++) {
                ViewColumn column = columns.get(i);
                Type type = getViewColumnType(column, name, node);
                Field field = fieldList.get(i);
                if (!column.getName().equalsIgnoreCase(field.getName().orElse(null)) ||
                        !typeCoercion.canCoerce(field.getType(), type)) {
                    return true;
                }
            }

            return false;
        }

        private Type getViewColumnType(ViewColumn column, QualifiedObjectName name, Node node)
        {
            try {
                return metadata.getType(column.getType());
            }
            catch (TypeNotFoundException e) {
                throw semanticException(INVALID_VIEW, node, e, "Unknown type '%s' for column '%s' in view: %s", column.getType(), column.getName(), name);
            }
        }

        private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope)
        {
            return ExpressionAnalyzer.analyzeExpression(
                    session,
                    metadata,
                    accessControl,
                    sqlParser,
                    scope,
                    analysis,
                    expression,
                    warningCollector,
                    correlationSupport);
        }

        private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope, CorrelationSupport correlationSupport)
        {
            return ExpressionAnalyzer.analyzeExpression(
                    session,
                    metadata,
                    accessControl,
                    sqlParser,
                    scope,
                    analysis,
                    expression,
                    warningCollector,
                    correlationSupport);
        }

        private void analyzeRowFilter(String currentIdentity, Table table, QualifiedObjectName name, Scope scope, ViewExpression filter)
        {
            if (analysis.hasRowFilter(name, currentIdentity)) {
                throw new PrestoException(INVALID_ROW_FILTER, extractLocation(table), format("Row filter for '%s' is recursive", name), null);
            }

            Expression expression;
            try {
                expression = sqlParser.createExpression(filter.getExpression(), createParsingOptions(session));
            }
            catch (ParsingException e) {
                throw new PrestoException(INVALID_ROW_FILTER, extractLocation(table), format("Invalid row filter for '%s': %s", name, e.getErrorMessage()), e);
            }

            analysis.registerTableForRowFiltering(name, currentIdentity);
            ExpressionAnalysis expressionAnalysis;
            try {
                expressionAnalysis = ExpressionAnalyzer.analyzeExpression(
                        createViewSession(filter.getCatalog(), filter.getSchema(), Identity.forUser(filter.getIdentity()).build(), session.getPath()), // TODO: path should be included in row filter
                        metadata,
                        accessControl,
                        sqlParser,
                        scope,
                        analysis,
                        expression,
                        warningCollector,
                        correlationSupport);
            }
            catch (PrestoException e) {
                throw new PrestoException(e::getErrorCode, extractLocation(table), format("Invalid row filter for '%s': %s", name, e.getRawMessage()), e);
            }
            finally {
                analysis.unregisterTableForRowFiltering(name, currentIdentity);
            }

            verifyNoAggregateWindowOrGroupingFunctions(metadata, expression, format("Row filter for '%s'", name));

            analysis.recordSubqueries(expression, expressionAnalysis);

            Type actualType = expressionAnalysis.getType(expression);
            if (!actualType.equals(BOOLEAN)) {
                TypeCoercion coercion = new TypeCoercion(metadata::getType);

                if (!coercion.canCoerce(actualType, BOOLEAN)) {
                    throw new PrestoException(TYPE_MISMATCH, extractLocation(table), format("Expected row filter for '%s' to be of type BOOLEAN, but was %s", name, actualType), null);
                }

                analysis.addCoercion(expression, BOOLEAN, coercion.isTypeOnlyCoercion(actualType, BOOLEAN));
            }

            analysis.addRowFilter(table, expression);
        }

        private void analyzeColumnMask(String currentIdentity, Table table, QualifiedObjectName tableName, Field field, Scope scope, ViewExpression mask)
        {
            String column = field.getName().get();
            if (analysis.hasColumnMask(tableName, column, currentIdentity)) {
                throw new PrestoException(INVALID_ROW_FILTER, extractLocation(table), format("Column mask for '%s.%s' is recursive", tableName, column), null);
            }

            Expression expression;
            try {
                expression = sqlParser.createExpression(mask.getExpression(), createParsingOptions(session));
            }
            catch (ParsingException e) {
                throw new PrestoException(INVALID_ROW_FILTER, extractLocation(table), format("Invalid column mask for '%s.%s': %s", tableName, column, e.getErrorMessage()), e);
            }

            ExpressionAnalysis expressionAnalysis;
            analysis.registerTableForColumnMasking(tableName, column, currentIdentity);
            try {
                expressionAnalysis = ExpressionAnalyzer.analyzeExpression(
                        createViewSession(mask.getCatalog(), mask.getSchema(), Identity.forUser(mask.getIdentity()).build(), session.getPath()), // TODO: path should be included in row filter
                        metadata,
                        accessControl,
                        sqlParser,
                        scope,
                        analysis,
                        expression,
                        warningCollector,
                        correlationSupport);
            }
            catch (PrestoException e) {
                throw new PrestoException(e::getErrorCode, extractLocation(table), format("Invalid column mask for '%s.%s': %s", tableName, column, e.getRawMessage()), e);
            }
            finally {
                analysis.unregisterTableForColumnMasking(tableName, column, currentIdentity);
            }

            verifyNoAggregateWindowOrGroupingFunctions(metadata, expression, format("Column mask for '%s.%s'", table.getName(), column));

            analysis.recordSubqueries(expression, expressionAnalysis);

            Type expectedType = field.getType();
            Type actualType = expressionAnalysis.getType(expression);
            if (!actualType.equals(expectedType)) {
                TypeCoercion coercion = new TypeCoercion(metadata::getType);

                if (!coercion.canCoerce(actualType, field.getType())) {
                    throw new PrestoException(TYPE_MISMATCH, extractLocation(table), format("Expected column mask for '%s.%s' to be of type %s, but was %s", tableName, column, field.getType(), actualType), null);
                }

                // TODO: this should be "coercion.isTypeOnlyCoercion(actualType, expectedType)", but type-only coercions are broken
                // due to the line "changeType(value, returnType)" in SqlToRowExpressionTranslator.visitCast. If there's an expression
                // like CAST(CAST(x AS VARCHAR(1)) AS VARCHAR(2)), it determines that the outer cast is type-only and converts the expression
                // to CAST(x AS VARCHAR(2)) by changing the type of the inner cast.
                analysis.addCoercion(expression, expectedType, false);
            }

            analysis.addColumnMask(table, column, expression);
        }

        private List<Expression> descriptorToFields(Scope scope)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (int fieldIndex = 0; fieldIndex < scope.getRelationType().getAllFieldCount(); fieldIndex++) {
                FieldReference expression = new FieldReference(fieldIndex);
                builder.add(expression);
                analyzeExpression(expression, scope);
            }
            return builder.build();
        }

        private Scope analyzeWith(Query node, Optional<Scope> scope)
        {
            if (node.getWith().isEmpty()) {
                return createScope(scope);
            }

            // analyze WITH clause
            With with = node.getWith().get();
            Scope.Builder withScopeBuilder = scopeBuilder(scope);

            for (WithQuery withQuery : with.getQueries()) {
                String name = withQuery.getName().getValue().toLowerCase(ENGLISH);
                if (withScopeBuilder.containsNamedQuery(name)) {
                    throw semanticException(DUPLICATE_NAMED_QUERY, withQuery, "WITH query name '%s' specified more than once", name);
                }

                boolean isRecursive = false;
                if (with.isRecursive()) {
                    isRecursive = tryProcessRecursiveQuery(withQuery, name, withScopeBuilder);
                    // WITH query is not shaped accordingly to the rules for expandable query and will be processed like a plain WITH query.
                    // Since RECURSIVE is specified, any reference to WITH query name is considered a recursive reference and is not allowed.
                    if (!isRecursive) {
                        List<Node> recursiveReferences = findReferences(withQuery.getQuery(), withQuery.getName());
                        if (!recursiveReferences.isEmpty()) {
                            throw semanticException(INVALID_RECURSIVE_REFERENCE, recursiveReferences.get(0), "recursive reference not allowed in this context");
                        }
                    }
                }

                if (!isRecursive) {
                    Query query = withQuery.getQuery();
                    process(query, withScopeBuilder.build());

                    // check if all or none of the columns are explicitly alias
                    if (withQuery.getColumnNames().isPresent()) {
                        validateColumnAliases(withQuery.getColumnNames().get(), analysis.getOutputDescriptor(query).getVisibleFieldCount());
                    }

                    withScopeBuilder.withNamedQuery(name, withQuery);
                }
            }
            Scope withScope = withScopeBuilder.build();
            analysis.setScope(with, withScope);
            return withScope;
        }

        private boolean tryProcessRecursiveQuery(WithQuery withQuery, String name, Scope.Builder withScopeBuilder)
        {
            if (withQuery.getColumnNames().isEmpty()) {
                throw semanticException(MISSING_COLUMN_ALIASES, withQuery, "missing column aliases in recursive WITH query");
            }
            preOrder(withQuery.getQuery())
                    .filter(child -> child instanceof With && ((With) child).isRecursive())
                    .findFirst()
                    .ifPresent(child -> {
                        throw semanticException(NESTED_RECURSIVE, child, "nested recursive WITH query");
                    });
            // if RECURSIVE is specified, all queries in the WITH list are considered potentially recursive
            // try resolve WITH query as expandable query
            // a) validate shape of the query and location of recursive reference
            if (!(withQuery.getQuery().getQueryBody() instanceof Union)) {
                return false;
            }
            Union union = (Union) withQuery.getQuery().getQueryBody();
            if (union.getRelations().size() != 2) {
                return false;
            }
            Relation anchor = union.getRelations().get(0);
            Relation step = union.getRelations().get(1);
            List<Node> anchorReferences = findReferences(anchor, withQuery.getName());
            if (!anchorReferences.isEmpty()) {
                throw semanticException(INVALID_RECURSIVE_REFERENCE, anchorReferences.get(0), "WITH table name is referenced in the base relation of recursion");
            }
            // a WITH query is linearly recursive if it has a single recursive reference
            List<Node> stepReferences = findReferences(step, withQuery.getName());
            if (stepReferences.size() > 1) {
                throw semanticException(INVALID_RECURSIVE_REFERENCE, stepReferences.get(1), "multiple recursive references in the step relation of recursion");
            }
            if (stepReferences.size() != 1) {
                return false;
            }
            // search for QuerySpecification in parenthesized subquery
            Relation specification = step;
            while (specification instanceof TableSubquery) {
                Query query = ((TableSubquery) specification).getQuery();
                query.getLimit().ifPresent(limit -> {
                    throw semanticException(INVALID_LIMIT_CLAUSE, limit, "FETCH FIRST / LIMIT clause in the step relation of recursion");
                });
                specification = query.getQueryBody();
            }
            if (!(specification instanceof QuerySpecification) || ((QuerySpecification) specification).getFrom().isEmpty()) {
                throw semanticException(INVALID_RECURSIVE_REFERENCE, stepReferences.get(0), "recursive reference outside of FROM clause of the step relation of recursion");
            }
            Relation from = ((QuerySpecification) specification).getFrom().get();
            List<Node> fromReferences = findReferences(from, withQuery.getName());
            if (fromReferences.size() == 0) {
                throw semanticException(INVALID_RECURSIVE_REFERENCE, stepReferences.get(0), "recursive reference outside of FROM clause of the step relation of recursion");
            }

            // b) validate top-level shape of recursive query
            withQuery.getQuery().getWith().ifPresent(innerWith -> {
                throw semanticException(NOT_SUPPORTED, innerWith, "immediate WITH clause in recursive query");
            });
            withQuery.getQuery().getOrderBy().ifPresent(orderBy -> {
                throw semanticException(NOT_SUPPORTED, orderBy, "immediate ORDER BY clause in recursive query");
            });
            withQuery.getQuery().getOffset().ifPresent(offset -> {
                throw semanticException(NOT_SUPPORTED, offset, "immediate OFFSET clause in recursive query");
            });
            withQuery.getQuery().getLimit().ifPresent(limit -> {
                throw semanticException(INVALID_LIMIT_CLAUSE, limit, "immediate FETCH FIRST / LIMIT clause in recursive query");
            });

            // c) validate recursion step has no illegal clauses
            validateFromClauseOfRecursiveTerm(from, withQuery.getName());

            // shape validation complete - process query as expandable query
            Scope parentScope = withScopeBuilder.build();
            // process expandable query -- anchor
            Scope anchorScope = process(anchor, parentScope);
            // set aliases in anchor scope as defined for WITH query. Recursion step will refer to anchor fields by aliases.
            Scope aliasedAnchorScope = setAliases(anchorScope, withQuery.getName(), withQuery.getColumnNames().get());
            // record expandable query base scope for recursion step analysis
            Node recursiveReference = fromReferences.get(0);
            analysis.setExpandableBaseScope(recursiveReference, aliasedAnchorScope);
            // process expandable query -- recursion step
            Scope stepScope = process(step, parentScope);

            // verify anchor and step have matching descriptors
            RelationType anchorType = aliasedAnchorScope.getRelationType().withOnlyVisibleFields();
            RelationType stepType = stepScope.getRelationType().withOnlyVisibleFields();
            if (anchorType.getVisibleFieldCount() != stepType.getVisibleFieldCount()) {
                throw semanticException(TYPE_MISMATCH, step, "base and step relations of recursion have different number of fields: %s, %s", anchorType.getVisibleFieldCount(), stepType.getVisibleFieldCount());
            }

            List<Type> anchorFieldTypes = anchorType.getVisibleFields().stream()
                    .map(Field::getType)
                    .collect(toImmutableList());
            List<Type> stepFieldTypes = stepType.getVisibleFields().stream()
                    .map(Field::getType)
                    .collect(toImmutableList());

            for (int i = 0; i < anchorFieldTypes.size(); i++) {
                if (!typeCoercion.canCoerce(stepFieldTypes.get(i), anchorFieldTypes.get(i))) {
                    // TODO for more precise error location, pass the mismatching select expression instead of `step`
                    throw semanticException(
                            TYPE_MISMATCH,
                            step,
                            "recursion step relation output type (%s) is not coercible to recursion base relation output type (%s) at column %s",
                            stepFieldTypes.get(i),
                            anchorFieldTypes.get(i),
                            i + 1);
                }
            }

            if (!anchorFieldTypes.equals(stepFieldTypes)) {
                analysis.addRelationCoercion(step, anchorFieldTypes.toArray(Type[]::new));
            }

            analysis.setScope(withQuery.getQuery(), aliasedAnchorScope);
            analysis.registerExpandableQuery(withQuery.getQuery(), recursiveReference);
            withScopeBuilder.withNamedQuery(name, withQuery);
            return true;
        }

        private List<Node> findReferences(Node node, Identifier name)
        {
            Stream<Node> allReferences = preOrder(node)
                    .filter(isTableWithName(name));

            // TODO: recursive references could be supported in subquery before the point of shadowing.
            //currently, the recursive query name is considered shadowed in the whole subquery if the subquery defines a common table with the same name
            Set<Node> shadowedReferences = preOrder(node)
                    .filter(isQueryWithNameShadowed(name))
                    .flatMap(query -> preOrder(query)
                            .filter(isTableWithName(name)))
                    .collect(toImmutableSet());

            return allReferences
                    .filter(reference -> !shadowedReferences.contains(reference))
                    .collect(toImmutableList());
        }

        private Predicate<Node> isTableWithName(Identifier name)
        {
            return node -> {
                if (!(node instanceof Table)) {
                    return false;
                }
                Table table = (Table) node;
                QualifiedName tableName = table.getName();
                return tableName.getPrefix().isEmpty() && tableName.hasSuffix(QualifiedName.of(name.getValue()));
            };
        }

        private Predicate<Node> isQueryWithNameShadowed(Identifier name)
        {
            return node -> {
                if (!(node instanceof Query)) {
                    return false;
                }
                Query query = (Query) node;
                if (query.getWith().isEmpty()) {
                    return false;
                }
                return query.getWith().get().getQueries().stream()
                        .map(WithQuery::getName)
                        .map(Identifier::getValue)
                        .anyMatch(withQueryName -> withQueryName.equalsIgnoreCase(name.getValue()));
            };
        }

        private void validateFromClauseOfRecursiveTerm(Relation from, Identifier name)
        {
            preOrder(from)
                    .filter(node -> node instanceof Join)
                    .forEach(node -> {
                        Join join = (Join) node;
                        Join.Type type = join.getType();
                        if (type == LEFT || type == RIGHT || type == FULL) {
                            List<Node> leftRecursiveReferences = findReferences(join.getLeft(), name);
                            List<Node> rightRecursiveReferences = findReferences(join.getRight(), name);
                            if (!leftRecursiveReferences.isEmpty() && (type == RIGHT || type == FULL)) {
                                throw semanticException(INVALID_RECURSIVE_REFERENCE, leftRecursiveReferences.get(0), "recursive reference in left source of %s join", type);
                            }
                            if (!rightRecursiveReferences.isEmpty() && (type == LEFT || type == FULL)) {
                                throw semanticException(INVALID_RECURSIVE_REFERENCE, rightRecursiveReferences.get(0), "recursive reference in right source of %s join", type);
                            }
                        }
                    });

            preOrder(from)
                    .filter(node -> node instanceof Intersect && !((Intersect) node).isDistinct())
                    .forEach(node -> {
                        Intersect intersect = (Intersect) node;
                        intersect.getRelations().stream()
                                .flatMap(relation -> findReferences(relation, name).stream())
                                .findFirst()
                                .ifPresent(reference -> {
                                    throw semanticException(INVALID_RECURSIVE_REFERENCE, reference, "recursive reference in INTERSECT ALL");
                                });
                    });

            preOrder(from)
                    .filter(node -> node instanceof Except)
                    .forEach(node -> {
                        Except except = (Except) node;
                        List<Node> rightRecursiveReferences = findReferences(except.getRight(), name);
                        if (!rightRecursiveReferences.isEmpty()) {
                            throw semanticException(
                                    INVALID_RECURSIVE_REFERENCE,
                                    rightRecursiveReferences.get(0),
                                    "recursive reference in right relation of EXCEPT %s",
                                    except.isDistinct() ? "DISTINCT" : "ALL");
                        }
                        if (!except.isDistinct()) {
                            List<Node> leftRecursiveReferences = findReferences(except.getLeft(), name);
                            if (!leftRecursiveReferences.isEmpty()) {
                                throw semanticException(INVALID_RECURSIVE_REFERENCE, leftRecursiveReferences.get(0), "recursive reference in left relation of EXCEPT ALL");
                            }
                        }
                    });
        }

        private Scope setAliases(Scope scope, Identifier tableName, List<Identifier> columnNames)
        {
            RelationType oldDescriptor = scope.getRelationType();
            validateColumnAliases(columnNames, oldDescriptor.getVisibleFieldCount());
            RelationType newDescriptor = oldDescriptor.withAlias(tableName.getValue(), columnNames.stream().map(Identifier::getValue).collect(toImmutableList()));
            return scope.withRelationType(newDescriptor);
        }

        private void verifySelectDistinct(QuerySpecification node, List<Expression> orderByExpressions, List<Expression> outputExpressions, Scope sourceScope, Scope orderByScope)
        {
            Set<CanonicalizationAware<Identifier>> aliases = getAliases(node.getSelect());

            Set<ScopeAware<Expression>> expressions = outputExpressions.stream()
                    .map(e -> ScopeAware.scopeAwareKey(e, analysis, sourceScope))
                    .collect(Collectors.toSet());

            for (Expression expression : orderByExpressions) {
                if (expression instanceof FieldReference) {
                    continue;
                }

                // In a query such as
                //    SELECT a FROM t ORDER BY a
                // the "a" in the SELECT clause is bound to the FROM scope, while the "a" in ORDER BY clause is bound
                // to the "a" from the SELECT clause, so we can't compare by field id / relation id.
                if (expression instanceof Identifier && aliases.contains(canonicalizationAwareKey(expression))) {
                    continue;
                }

                if (!expressions.contains(ScopeAware.scopeAwareKey(expression, analysis, orderByScope))) {
                    throw semanticException(EXPRESSION_NOT_IN_DISTINCT, node.getSelect(), "For SELECT DISTINCT, ORDER BY expressions must appear in select list");
                }
            }

            for (Expression expression : orderByExpressions) {
                if (!DeterminismEvaluator.isDeterministic(expression, this::getFunctionMetadata)) {
                    throw semanticException(EXPRESSION_NOT_IN_DISTINCT, expression, "Non deterministic ORDER BY expression is not supported with SELECT DISTINCT");
                }
            }
        }

        private Set<CanonicalizationAware<Identifier>> getAliases(Select node)
        {
            ImmutableSet.Builder<CanonicalizationAware<Identifier>> aliases = ImmutableSet.builder();
            for (SelectItem item : node.getSelectItems()) {
                if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;
                    Optional<Identifier> alias = column.getAlias();
                    if (alias.isPresent()) {
                        aliases.add(canonicalizationAwareKey(alias.get()));
                    }
                    else if (column.getExpression() instanceof Identifier) {
                        aliases.add(canonicalizationAwareKey((Identifier) column.getExpression()));
                    }
                    else if (column.getExpression() instanceof DereferenceExpression) {
                        aliases.add(canonicalizationAwareKey(((DereferenceExpression) column.getExpression()).getField()));
                    }
                }
                else if (item instanceof AllColumns) {
                    ((AllColumns) item).getAliases().stream()
                            .map(CanonicalizationAware::canonicalizationAwareKey)
                            .forEach(aliases::add);
                }
            }

            return aliases.build();
        }

        private FunctionMetadata getFunctionMetadata(FunctionCall functionCall)
        {
            ResolvedFunction resolvedFunction = analysis.getResolvedFunction(functionCall);
            verify(resolvedFunction != null, "function has not been analyzed yet: %s", functionCall);
            return metadata.getFunctionMetadata(resolvedFunction);
        }

        private List<Expression> analyzeOrderBy(Node node, List<SortItem> sortItems, Scope orderByScope)
        {
            ImmutableList.Builder<Expression> orderByFieldsBuilder = ImmutableList.builder();

            for (SortItem item : sortItems) {
                Expression expression = item.getSortKey();

                if (expression instanceof LongLiteral) {
                    // this is an ordinal in the output tuple

                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > orderByScope.getRelationType().getVisibleFieldCount()) {
                        throw semanticException(INVALID_COLUMN_REFERENCE, expression, "ORDER BY position %s is not in select list", ordinal);
                    }

                    expression = new FieldReference(toIntExact(ordinal - 1));
                }

                ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(session,
                        metadata,
                        accessControl,
                        sqlParser,
                        orderByScope,
                        analysis,
                        expression,
                        WarningCollector.NOOP,
                        correlationSupport);
                analysis.recordSubqueries(node, expressionAnalysis);

                Type type = analysis.getType(expression);
                if (!type.isOrderable()) {
                    throw semanticException(TYPE_MISMATCH, node, "Type %s is not orderable, and therefore cannot be used in ORDER BY: %s", type, expression);
                }

                orderByFieldsBuilder.add(expression);
            }

            return orderByFieldsBuilder.build();
        }

        private void analyzeOffset(Offset node, Scope scope)
        {
            long rowCount;
            if (node.getRowCount() instanceof LongLiteral) {
                rowCount = ((LongLiteral) node.getRowCount()).getValue();
            }
            else {
                checkState(node.getRowCount() instanceof Parameter, "unexpected OFFSET rowCount: " + node.getRowCount().getClass().getSimpleName());
                OptionalLong providedValue = analyzeParameterAsRowCount((Parameter) node.getRowCount(), scope, "OFFSET");
                rowCount = providedValue.orElse(0);
            }
            if (rowCount < 0) {
                throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, node, "OFFSET row count must be greater or equal to 0 (actual value: %s)", rowCount);
            }
            analysis.setOffset(node, rowCount);
        }

        /**
         * @return true if the Query / QuerySpecification containing the analyzed
         * Limit or FetchFirst, must contain orderBy (i.e., for FetchFirst with ties).
         */
        private boolean analyzeLimit(Node node, Scope scope)
        {
            checkState(
                    node instanceof FetchFirst || node instanceof Limit,
                    "Invalid limit node type. Expected: FetchFirst or Limit. Actual: %s", node.getClass().getName());
            if (node instanceof FetchFirst) {
                return analyzeLimit((FetchFirst) node, scope);
            }
            else {
                return analyzeLimit((Limit) node, scope);
            }
        }

        private boolean analyzeLimit(FetchFirst node, Scope scope)
        {
            long rowCount = 1;
            if (node.getRowCount().isPresent()) {
                Expression count = node.getRowCount().get();
                if (count instanceof LongLiteral) {
                    rowCount = ((LongLiteral) count).getValue();
                }
                else {
                    checkState(count instanceof Parameter, "unexpected FETCH FIRST rowCount: " + count.getClass().getSimpleName());
                    OptionalLong providedValue = analyzeParameterAsRowCount((Parameter) count, scope, "FETCH FIRST");
                    if (providedValue.isPresent()) {
                        rowCount = providedValue.getAsLong();
                    }
                }
            }
            if (rowCount <= 0) {
                throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, node, "FETCH FIRST row count must be positive (actual value: %s)", rowCount);
            }
            analysis.setLimit(node, rowCount);

            return node.isWithTies();
        }

        private boolean analyzeLimit(Limit node, Scope scope)
        {
            OptionalLong rowCount;
            if (node.getRowCount() instanceof AllRows) {
                rowCount = OptionalLong.empty();
            }
            else if (node.getRowCount() instanceof LongLiteral) {
                rowCount = OptionalLong.of(((LongLiteral) node.getRowCount()).getValue());
            }
            else {
                checkState(node.getRowCount() instanceof Parameter, "unexpected LIMIT rowCount: " + node.getRowCount().getClass().getSimpleName());
                rowCount = analyzeParameterAsRowCount((Parameter) node.getRowCount(), scope, "LIMIT");
            }
            rowCount.ifPresent(count -> {
                if (count < 0) {
                    throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, node, "LIMIT row count must be greater or equal to 0 (actual value: %s)", count);
                }
            });

            analysis.setLimit(node, rowCount);

            return false;
        }

        private OptionalLong analyzeParameterAsRowCount(Parameter parameter, Scope scope, String context)
        {
            if (analysis.isDescribe()) {
                analyzeExpression(parameter, scope);
                analysis.addCoercion(parameter, BIGINT, false);
                return OptionalLong.empty();
            }
            else {
                // validate parameter index
                analyzeExpression(parameter, scope);
                Expression providedValue = analysis.getParameters().get(NodeRef.of(parameter));
                Object value;
                try {
                    value = ExpressionInterpreter.evaluateConstantExpression(
                            providedValue,
                            BIGINT,
                            metadata,
                            session,
                            accessControl,
                            analysis.getParameters());
                }
                catch (VerifyException e) {
                    throw semanticException(INVALID_ARGUMENTS, parameter, "Non constant parameter value for %s: %s", context, providedValue);
                }
                if (value == null) {
                    throw semanticException(INVALID_ARGUMENTS, parameter, "Parameter value provided for %s is NULL: %s", context, providedValue);
                }
                return OptionalLong.of((long) value);
            }
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope)
        {
            return createAndAssignScope(node, parentScope, emptyList());
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, Field... fields)
        {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, List<Field> fields)
        {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, RelationType relationType)
        {
            Scope scope = scopeBuilder(parentScope)
                    .withRelationType(RelationId.of(node), relationType)
                    .build();

            analysis.setScope(node, scope);
            return scope;
        }

        private Scope createScope(Optional<Scope> parentScope)
        {
            return scopeBuilder(parentScope).build();
        }

        private Scope.Builder scopeBuilder(Optional<Scope> parentScope)
        {
            Scope.Builder scopeBuilder = Scope.builder();

            if (parentScope.isPresent()) {
                // parent scope represents local query scope hierarchy. Local query scope
                // hierarchy should have outer query scope as ancestor already.
                scopeBuilder.withParent(parentScope.get());
            }
            else {
                outerQueryScope.ifPresent(scopeBuilder::withOuterQueryParent);
            }

            return scopeBuilder;
        }
    }

    private Session createViewSession(Optional<String> catalog, Optional<String> schema, Identity identity, SqlPath path)
    {
        return Session.builder(metadata.getSessionPropertyManager())
                .setQueryId(session.getQueryId())
                .setTransactionId(session.getTransactionId().orElse(null))
                .setIdentity(identity)
                .setSource(session.getSource().orElse(null))
                .setCatalog(catalog.orElse(null))
                .setSchema(schema.orElse(null))
                .setPath(path)
                .setTimeZoneKey(session.getTimeZoneKey())
                .setLocale(session.getLocale())
                .setRemoteUserAddress(session.getRemoteUserAddress().orElse(null))
                .setUserAgent(session.getUserAgent().orElse(null))
                .setClientInfo(session.getClientInfo().orElse(null))
                .setStart(session.getStart())
                .build();
    }

    private static boolean hasScopeAsLocalParent(Scope root, Scope parent)
    {
        Scope scope = root;
        while (scope.getLocalParent().isPresent()) {
            scope = scope.getLocalParent().get();
            if (scope.equals(parent)) {
                return true;
            }
        }

        return false;
    }
}
