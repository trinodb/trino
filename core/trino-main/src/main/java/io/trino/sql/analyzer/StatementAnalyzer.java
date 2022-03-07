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
package io.trino.sql.analyzer;

import com.google.common.base.Joiner;
import com.google.common.base.VerifyException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.connector.CatalogName;
import io.trino.execution.Column;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.FunctionKind;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.Metadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableLayout;
import io.trino.metadata.TableMetadata;
import io.trino.metadata.TableProceduresPropertyManager;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.metadata.TablePropertyManager;
import io.trino.metadata.TableSchema;
import io.trino.metadata.TableVersion;
import io.trino.metadata.ViewColumn;
import io.trino.metadata.ViewDefinition;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.security.ViewAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.TrinoWarning;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.function.OperatorType;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.Identity;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.VarcharType;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.SqlPath;
import io.trino.sql.analyzer.Analysis.GroupingSetAnalysis;
import io.trino.sql.analyzer.Analysis.ResolvedWindow;
import io.trino.sql.analyzer.Analysis.SelectExpression;
import io.trino.sql.analyzer.Analysis.SourceColumn;
import io.trino.sql.analyzer.Analysis.UnnestAnalysis;
import io.trino.sql.analyzer.PatternRecognitionAnalyzer.PatternRecognitionAnalysis;
import io.trino.sql.analyzer.Scope.AsteriskedIdentifierChainBasis;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.DeterminismEvaluator;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.ScopeAware;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AllRows;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.Deny;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.Merge;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryPeriod;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.RenameColumn;
import io.trino.sql.tree.RenameMaterializedView;
import io.trino.sql.tree.RenameSchema;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.RenameView;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.Revoke;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.Rollup;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SetOperation;
import io.trino.sql.tree.SetProperties;
import io.trino.sql.tree.SetSchemaAuthorization;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.SetTableAuthorization;
import io.trino.sql.tree.SetTimeZone;
import io.trino.sql.tree.SetViewAuthorization;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableExecute;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TruncateTable;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Update;
import io.trino.sql.tree.UpdateAssignment;
import io.trino.sql.tree.Use;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.VariableDefinition;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowDefinition;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowOperation;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;
import io.trino.type.TypeCoercion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.getMaxGroupingSets;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.FunctionKind.WINDOW;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.COLUMN_TYPE_UNKNOWN;
import static io.trino.spi.StandardErrorCode.DUPLICATE_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.DUPLICATE_NAMED_QUERY;
import static io.trino.spi.StandardErrorCode.DUPLICATE_PROPERTY;
import static io.trino.spi.StandardErrorCode.DUPLICATE_WINDOW_NAME;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_IN_DISTINCT;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_WINDOW;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_LIMIT_CLAUSE;
import static io.trino.spi.StandardErrorCode.INVALID_ORDER_BY;
import static io.trino.spi.StandardErrorCode.INVALID_PARTITION_BY;
import static io.trino.spi.StandardErrorCode.INVALID_RECURSIVE_REFERENCE;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static io.trino.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static io.trino.spi.StandardErrorCode.INVALID_WINDOW_REFERENCE;
import static io.trino.spi.StandardErrorCode.MISMATCHED_COLUMN_ALIASES;
import static io.trino.spi.StandardErrorCode.MISSING_COLUMN_ALIASES;
import static io.trino.spi.StandardErrorCode.MISSING_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.MISSING_GROUP_BY;
import static io.trino.spi.StandardErrorCode.MISSING_ORDER_BY;
import static io.trino.spi.StandardErrorCode.NESTED_RECURSIVE;
import static io.trino.spi.StandardErrorCode.NESTED_ROW_PATTERN_RECOGNITION;
import static io.trino.spi.StandardErrorCode.NESTED_WINDOW;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NULL_TREATMENT_NOT_ALLOWED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_HAS_NO_COLUMNS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TOO_MANY_GROUPING_SETS;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.StandardErrorCode.VIEW_IS_RECURSIVE;
import static io.trino.spi.StandardErrorCode.VIEW_IS_STALE;
import static io.trino.spi.connector.StandardWarningCode.REDUNDANT_ORDER_BY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.NodeUtils.getSortItemsFromOrderBy;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.analyzer.AggregationAnalyzer.verifyOrderByAggregations;
import static io.trino.sql.analyzer.AggregationAnalyzer.verifySourceAggregations;
import static io.trino.sql.analyzer.Analyzer.verifyNoAggregateWindowOrGroupingFunctions;
import static io.trino.sql.analyzer.CanonicalizationAware.canonicalizationAwareKey;
import static io.trino.sql.analyzer.ExpressionAnalyzer.createConstantAnalyzer;
import static io.trino.sql.analyzer.ExpressionTreeUtils.asQualifiedName;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractLocation;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractWindowExpressions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractWindowFunctions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractWindowMeasures;
import static io.trino.sql.analyzer.Scope.BasisType.TABLE;
import static io.trino.sql.analyzer.ScopeReferenceExtractor.getReferencesToScope;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static io.trino.sql.tree.Join.Type.FULL;
import static io.trino.sql.tree.Join.Type.INNER;
import static io.trino.sql.tree.Join.Type.LEFT;
import static io.trino.sql.tree.Join.Type.RIGHT;
import static io.trino.sql.util.AstUtils.preOrder;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.MoreLists.mappedCopy;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

class StatementAnalyzer
{
    private static final Set<String> WINDOW_VALUE_FUNCTIONS = ImmutableSet.of("lead", "lag", "first_value", "last_value", "nth_value");

    private final StatementAnalyzerFactory statementAnalyzerFactory;
    private final Analysis analysis;
    private final Metadata metadata;
    private final PlannerContext plannerContext;
    private final TypeCoercion typeCoercion;
    private final Session session;
    private final SqlParser sqlParser;
    private final GroupProvider groupProvider;
    private final AccessControl accessControl;
    private final TableProceduresRegistry tableProceduresRegistry;
    private final SessionPropertyManager sessionPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final AnalyzePropertyManager analyzePropertyManager;
    private final TableProceduresPropertyManager tableProceduresPropertyManager;

    private final WarningCollector warningCollector;
    private final CorrelationSupport correlationSupport;

    StatementAnalyzer(
            StatementAnalyzerFactory statementAnalyzerFactory,
            Analysis analysis,
            PlannerContext plannerContext,
            SqlParser sqlParser,
            GroupProvider groupProvider,
            AccessControl accessControl,
            Session session,
            TableProceduresRegistry tableProceduresRegistry,
            SessionPropertyManager sessionPropertyManager,
            TablePropertyManager tablePropertyManager,
            AnalyzePropertyManager analyzePropertyManager,
            TableProceduresPropertyManager tableProceduresPropertyManager,
            WarningCollector warningCollector,
            CorrelationSupport correlationSupport)
    {
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
        this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.session = requireNonNull(session, "session is null");
        this.tableProceduresRegistry = requireNonNull(tableProceduresRegistry, "tableProceduresRegistry is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
        this.analyzePropertyManager = requireNonNull(analyzePropertyManager, "analyzePropertyManager is null");
        this.tableProceduresPropertyManager = tableProceduresPropertyManager;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.correlationSupport = requireNonNull(correlationSupport, "correlationSupport is null");
    }

    public Scope analyze(Node node, Scope outerQueryScope)
    {
        return analyze(node, Optional.of(outerQueryScope));
    }

    public Scope analyze(Node node, Optional<Scope> outerQueryScope)
    {
        return new Visitor(outerQueryScope, warningCollector, Optional.empty())
                .process(node, Optional.empty());
    }

    private Scope analyzeForUpdate(Table table, Optional<Scope> outerQueryScope, UpdateKind updateKind)
    {
        return new Visitor(outerQueryScope, warningCollector, Optional.of(updateKind))
                .process(table, Optional.empty());
    }

    private enum UpdateKind
    {
        DELETE,
        UPDATE,
    }

    /**
     * Visitor context represents local query scope (if exists). The invariant is
     * that the local query scopes hierarchy should always have outer query scope
     * (if provided) as ancestor.
     */
    private final class Visitor
            extends AstVisitor<Scope, Optional<Scope>>
    {
        private final Optional<Scope> outerQueryScope;
        private final WarningCollector warningCollector;
        private final Optional<UpdateKind> updateKind;

        private Visitor(Optional<Scope> outerQueryScope, WarningCollector warningCollector, Optional<UpdateKind> updateKind)
        {
            this.outerQueryScope = requireNonNull(outerQueryScope, "outerQueryScope is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
            this.updateKind = requireNonNull(updateKind, "updateKind is null");
        }

        @Override
        public Scope process(Node node, Optional<Scope> scope)
        {
            Scope returnScope = super.process(node, scope);
            checkState(returnScope.getOuterQueryParent().equals(outerQueryScope), "result scope should have outer query scope equal with parameter outer query scope");
            scope.ifPresent(value -> checkState(hasScopeAsLocalParent(returnScope, value), "return scope should have context scope as one of its ancestors"));
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

            if (metadata.isMaterializedView(session, targetTable)) {
                throw semanticException(NOT_SUPPORTED, insert, "Inserting into materialized views is not supported");
            }

            if (metadata.isView(session, targetTable)) {
                throw semanticException(NOT_SUPPORTED, insert, "Inserting into views is not supported");
            }

            // analyze the query that creates the data
            Scope queryScope = analyze(insert.getQuery(), createScope(scope));

            // verify the insert destination columns match the query
            RedirectionAwareTableHandle redirection = metadata.getRedirectionAwareTableHandle(session, targetTable);
            Optional<TableHandle> targetTableHandle = redirection.getTableHandle();
            targetTable = redirection.getRedirectedTableName().orElse(targetTable);
            if (targetTableHandle.isEmpty()) {
                throw semanticException(TABLE_NOT_FOUND, insert, "Table '%s' does not exist", targetTable);
            }
            accessControl.checkCanInsertIntoTable(session.toSecurityContext(), targetTable);

            TableSchema tableSchema = metadata.getTableSchema(session, targetTableHandle.get());

            List<ColumnSchema> columns = tableSchema.getColumns().stream()
                    .filter(column -> !column.isHidden())
                    .collect(toImmutableList());

            for (ColumnSchema column : columns) {
                if (!accessControl.getColumnMasks(session.toSecurityContext(), targetTable, column.getName(), column.getType()).isEmpty()) {
                    throw semanticException(NOT_SUPPORTED, insert, "Insert into table with column masks is not supported");
                }
            }

            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTableHandle.get());
            List<Field> tableFields = analyzeTableOutputFields(insert.getTable(), targetTable, tableSchema, columnHandles);
            analyzeFiltersAndMasks(insert.getTable(), targetTable, targetTableHandle, tableFields, session.getIdentity().getUser());

            List<String> tableColumns = columns.stream()
                    .map(ColumnSchema::getName)
                    .collect(toImmutableList());

            // analyze target table layout, table columns should contain all partition columns
            Optional<TableLayout> newTableLayout = metadata.getInsertLayout(session, targetTableHandle.get());
            newTableLayout.ifPresent(layout -> {
                if (!ImmutableSet.copyOf(tableColumns).containsAll(layout.getPartitionColumns())) {
                    throw new TrinoException(NOT_SUPPORTED, "INSERT must write all distribution columns: " + layout.getPartitionColumns());
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

            analysis.setInsert(new Analysis.Insert(
                    insert.getTable(),
                    targetTableHandle.get(),
                    insertColumns.stream().map(columnHandles::get).collect(toImmutableList()),
                    newTableLayout));

            List<Type> tableTypes = insertColumns.stream()
                    .map(insertColumn -> tableSchema.getColumn(insertColumn).getType())
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

            Stream<Column> columnStream = Streams.zip(
                    insertColumns.stream(),
                    tableTypes.stream()
                            .map(Type::toString),
                    Column::new);

            analysis.setUpdateType("INSERT");
            analysis.setUpdateTarget(
                    targetTable,
                    Optional.empty(),
                    Optional.of(Streams.zip(
                            columnStream,
                            queryScope.getRelationType().getVisibleFields().stream(),
                            (column, field) -> new OutputColumn(column, analysis.getSourceColumns(field)))
                            .collect(toImmutableList())));

            return createAndAssignScope(insert, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitRefreshMaterializedView(RefreshMaterializedView refreshMaterializedView, Optional<Scope> scope)
        {
            QualifiedObjectName name = createQualifiedObjectName(session, refreshMaterializedView, refreshMaterializedView.getName());
            Optional<MaterializedViewDefinition> optionalView = metadata.getMaterializedView(session, name);

            if (optionalView.isEmpty()) {
                throw semanticException(TABLE_NOT_FOUND, refreshMaterializedView, "Materialized view '%s' does not exist", name);
            }

            accessControl.checkCanRefreshMaterializedView(session.toSecurityContext(), name);
            analysis.setUpdateType("REFRESH MATERIALIZED VIEW");

            if (metadata.delegateMaterializedViewRefreshToConnector(session, name)) {
                analysis.setDelegatedRefreshMaterializedView(name);
                analysis.setUpdateTarget(
                        name,
                        Optional.empty(),
                        Optional.empty());
                return createAndAssignScope(refreshMaterializedView, scope);
            }

            Optional<QualifiedName> storageName = getMaterializedViewStorageTableName(optionalView.get());

            if (storageName.isEmpty()) {
                throw semanticException(TABLE_NOT_FOUND, refreshMaterializedView, "Storage Table '%s' for materialized view '%s' does not exist", storageName, name);
            }

            QualifiedObjectName targetTable = createQualifiedObjectName(session, refreshMaterializedView, storageName.get());
            checkStorageTableNotRedirected(targetTable);

            // analyze the query that creates the data
            Query query = parseView(optionalView.get().getOriginalSql(), name, refreshMaterializedView);
            Scope queryScope = process(query, scope);

            // verify the insert destination columns match the query
            Optional<TableHandle> targetTableHandle = metadata.getTableHandle(session, targetTable);
            if (targetTableHandle.isEmpty()) {
                throw semanticException(TABLE_NOT_FOUND, refreshMaterializedView, "Table '%s' does not exist", targetTable);
            }

            analysis.setSkipMaterializedViewRefresh(metadata.getMaterializedViewFreshness(session, name).isMaterializedViewFresh());

            TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTableHandle.get());
            List<String> insertColumns = tableMetadata.getColumns().stream()
                    .filter(column -> !column.isHidden())
                    .map(ColumnMetadata::getName)
                    .collect(toImmutableList());

            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTableHandle.get());
            analysis.setRefreshMaterializedView(new Analysis.RefreshMaterializedViewAnalysis(
                    refreshMaterializedView.getTable(),
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

            Stream<Column> columns = Streams.zip(
                    insertColumns.stream(),
                    tableTypes.stream()
                            .map(Type::toString),
                    Column::new);

            analysis.setUpdateTarget(
                    targetTable,
                    Optional.empty(),
                    Optional.of(Streams.zip(
                            columns,
                            queryScope.getRelationType().getVisibleFields().stream(),
                            (column, field) -> new OutputColumn(column, analysis.getSourceColumns(field)))
                            .collect(toImmutableList())));

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
            QualifiedObjectName originalName = createQualifiedObjectName(session, table, table.getName());
            if (metadata.isView(session, originalName)) {
                throw semanticException(NOT_SUPPORTED, node, "Deleting from views is not supported");
            }

            RedirectionAwareTableHandle redirection = metadata.getRedirectionAwareTableHandle(session, originalName);
            QualifiedObjectName tableName = redirection.getRedirectedTableName().orElse(originalName);
            TableHandle handle = redirection.getTableHandle()
                    .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, table, "Table '%s' does not exist", tableName));

            accessControl.checkCanDeleteFromTable(session.toSecurityContext(), tableName);

            TableSchema tableSchema = metadata.getTableSchema(session, handle);
            for (ColumnSchema tableColumn : tableSchema.getColumns()) {
                if (!accessControl.getColumnMasks(session.toSecurityContext(), tableName, tableColumn.getName(), tableColumn.getType()).isEmpty()) {
                    throw semanticException(NOT_SUPPORTED, node, "Delete from table with column mask");
                }
            }

            // Analyzer checks for select permissions but DELETE has a separate permission, so disable access checks
            // TODO: we shouldn't need to create a new analyzer. The access control should be carried in the context object
            StatementAnalyzer analyzer = statementAnalyzerFactory
                    .withSpecializedAccessControl(new AllowAllAccessControl())
                    .createStatementAnalyzer(analysis, session, warningCollector, CorrelationSupport.ALLOWED);

            Scope tableScope = analyzer.analyzeForUpdate(table, scope, UpdateKind.DELETE);
            node.getWhere().ifPresent(where -> analyzeWhere(node, tableScope, where));

            analysis.setUpdateType("DELETE");
            analysis.setUpdateTarget(tableName, Optional.of(table), Optional.empty());
            analyzeFiltersAndMasks(table, tableName, Optional.of(handle), analysis.getScope(table).getRelationType(), session.getIdentity().getUser());

            return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitAnalyze(Analyze node, Optional<Scope> scope)
        {
            QualifiedObjectName tableName = createQualifiedObjectName(session, node, node.getTableName());
            analysis.setUpdateType("ANALYZE");
            analysis.setUpdateTarget(tableName, Optional.empty(), Optional.empty());

            // verify the target table exists and it's not a view
            if (metadata.isView(session, tableName)) {
                throw semanticException(NOT_SUPPORTED, node, "Analyzing views is not supported");
            }

            validateProperties(node.getProperties(), scope);
            CatalogName catalogName = getRequiredCatalogHandle(metadata, session, node, tableName.getCatalogName());

            Map<String, Object> analyzeProperties = analyzePropertyManager.getProperties(
                    catalogName,
                    node.getProperties(),
                    session,
                    plannerContext,
                    accessControl,
                    analysis.getParameters(),
                    true);
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

            Optional<TableHandle> targetTableHandle = metadata.getTableHandle(session, targetTable);
            if (targetTableHandle.isPresent()) {
                if (node.isNotExists()) {
                    analysis.setCreate(new Analysis.Create(
                            Optional.of(targetTable),
                            Optional.empty(),
                            Optional.empty(),
                            node.isWithData(),
                            true));
                    analysis.setUpdateType("CREATE TABLE");
                    analysis.setUpdateTarget(targetTable, Optional.empty(), Optional.of(ImmutableList.of()));
                    return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
                }
                throw semanticException(TABLE_ALREADY_EXISTS, node, "Destination table '%s' already exists", targetTable);
            }

            validateProperties(node.getProperties(), scope);

            CatalogName catalogName = getRequiredCatalogHandle(metadata, session, node, targetTable.getCatalogName());
            Map<String, Object> properties = tablePropertyManager.getProperties(
                    catalogName,
                    node.getProperties(),
                    session,
                    plannerContext,
                    accessControl,
                    analysis.getParameters(),
                    true);
            accessControl.checkCanCreateTable(session.toSecurityContext(), targetTable, properties);

            // analyze the query that creates the table
            Scope queryScope = analyze(node.getQuery(), createScope(scope));

            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();

            // analyze target table columns and column aliases
            ImmutableList.Builder<OutputColumn> outputColumns = ImmutableList.builder();
            if (node.getColumnAliases().isPresent()) {
                validateColumnAliases(node.getColumnAliases().get(), queryScope.getRelationType().getVisibleFieldCount());

                int aliasPosition = 0;
                for (Field field : queryScope.getRelationType().getVisibleFields()) {
                    if (field.getType().equals(UNKNOWN)) {
                        throw semanticException(COLUMN_TYPE_UNKNOWN, node, "Column type is unknown at position %s", queryScope.getRelationType().indexOf(field) + 1);
                    }
                    String columnName = node.getColumnAliases().get().get(aliasPosition).getValue();
                    columns.add(new ColumnMetadata(columnName, field.getType()));
                    outputColumns.add(new OutputColumn(new Column(columnName, field.getType().toString()), analysis.getSourceColumns(field)));
                    aliasPosition++;
                }
            }
            else {
                validateColumns(node, queryScope.getRelationType());
                columns.addAll(queryScope.getRelationType().getVisibleFields().stream()
                        .map(field -> new ColumnMetadata(field.getName().orElseThrow(), field.getType()))
                        .collect(toImmutableList()));
                queryScope.getRelationType().getVisibleFields().stream()
                        .map(this::createOutputColumn)
                        .forEach(outputColumns::add);
            }

            // create target table metadata
            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(targetTable.asSchemaTableName(), columns.build(), properties, node.getComment());

            // analyze target table layout
            Optional<TableLayout> newTableLayout = metadata.getNewTableLayout(session, targetTable.getCatalogName(), tableMetadata);

            Set<String> columnNames = columns.build().stream()
                    .map(ColumnMetadata::getName)
                    .collect(toImmutableSet());

            if (newTableLayout.isPresent()) {
                TableLayout layout = newTableLayout.get();
                if (!columnNames.containsAll(layout.getPartitionColumns())) {
                    if (layout.getLayout().getPartitioning().isPresent()) {
                        throw new TrinoException(NOT_SUPPORTED, "INSERT must write all distribution columns: " + layout.getPartitionColumns());
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

            analysis.setUpdateType("CREATE TABLE");
            analysis.setUpdateTarget(
                    targetTable,
                    Optional.empty(),
                    Optional.of(outputColumns.build()));

            return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitCreateView(CreateView node, Optional<Scope> scope)
        {
            QualifiedObjectName viewName = createQualifiedObjectName(session, node, node.getName());

            // analyze the query that creates the view
            StatementAnalyzer analyzer = statementAnalyzerFactory.createStatementAnalyzer(analysis, session, warningCollector, CorrelationSupport.ALLOWED);

            Scope queryScope = analyzer.analyze(node.getQuery(), scope);

            accessControl.checkCanCreateView(session.toSecurityContext(), viewName);

            validateColumns(node, queryScope.getRelationType());

            analysis.setUpdateType("CREATE VIEW");
            analysis.setUpdateTarget(
                    viewName,
                    Optional.empty(),
                    Optional.of(queryScope.getRelationType().getVisibleFields().stream()
                            .map(this::createOutputColumn)
                            .collect(toImmutableList())));

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
            if (node.isSetToDefault()) {
                return createAndAssignScope(node, scope);
            }
            // Property value expressions must be constant
            createConstantAnalyzer(plannerContext, accessControl, session, analysis.getParameters(), WarningCollector.NOOP, analysis.isDescribe())
                    .analyze(node.getNonDefaultValue(), createScope(scope));
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitCallArgument(CallArgument node, Optional<Scope> scope)
        {
            // CallArgument value expressions must be constant
            createConstantAnalyzer(plannerContext, accessControl, session, analysis.getParameters(), WarningCollector.NOOP, analysis.isDescribe())
                    .analyze(node.getValue(), createScope(scope));
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitTruncateTable(TruncateTable node, Optional<Scope> scope)
        {
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
        protected Scope visitSetProperties(SetProperties node, Optional<Scope> context)
        {
            return createAndAssignScope(node, context);
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
        protected Scope visitSetTableAuthorization(SetTableAuthorization node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitTableExecute(TableExecute node, Optional<Scope> scope)
        {
            Table table = node.getTable();
            QualifiedObjectName originalName = createQualifiedObjectName(session, table, table.getName());
            String procedureName = node.getProcedureName().getCanonicalValue();

            if (metadata.isMaterializedView(session, originalName)) {
                throw semanticException(NOT_SUPPORTED, node, "ALTER TABLE EXECUTE is not supported for materialized views");
            }

            if (metadata.isView(session, originalName)) {
                throw semanticException(NOT_SUPPORTED, node, "ALTER TABLE EXECUTE is not supported for views");
            }

            RedirectionAwareTableHandle redirection = metadata.getRedirectionAwareTableHandle(session, originalName);
            QualifiedObjectName tableName = redirection.getRedirectedTableName().orElse(originalName);
            TableHandle tableHandle = redirection.getTableHandle()
                    .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, table, "Table '%s' does not exist", tableName));

            accessControl.checkCanExecuteTableProcedure(
                    session.toSecurityContext(),
                    tableName,
                    procedureName);

            if (!accessControl.getRowFilters(session.toSecurityContext(), tableName).isEmpty()) {
                throw semanticException(NOT_SUPPORTED, node, "ALTER TABLE EXECUTE is not supported for table with row filter");
            }

            TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
            for (ColumnMetadata tableColumn : tableMetadata.getColumns()) {
                if (!accessControl.getColumnMasks(session.toSecurityContext(), tableName, tableColumn.getName(), tableColumn.getType()).isEmpty()) {
                    throw semanticException(NOT_SUPPORTED, node, "ALTER TABLE EXECUTE is not supported for table with column masks");
                }
            }

            Scope tableScope = analyze(table, scope);

            CatalogName catalogName = getRequiredCatalogHandle(metadata, session, node, tableName.getCatalogName());
            TableProcedureMetadata procedureMetadata = tableProceduresRegistry.resolve(catalogName, procedureName);

            // analyze WHERE
            if (!procedureMetadata.getExecutionMode().supportsFilter() && node.getWhere().isPresent()) {
                throw semanticException(NOT_SUPPORTED, node, "WHERE not supported for procedure " + procedureName);
            }
            node.getWhere().ifPresent(where -> analyzeWhere(node, tableScope, where));

            // analyze arguments

            Map<String, Expression> propertiesMap = processTableExecuteArguments(node, procedureMetadata, scope);
            Map<String, Object> tableProperties = tableProceduresPropertyManager.getProperties(
                    catalogName,
                    procedureName,
                    propertiesMap,
                    session,
                    plannerContext,
                    accessControl,
                    analysis.getParameters());

            TableExecuteHandle executeHandle =
                    metadata.getTableHandleForExecute(
                                    session,
                                    tableHandle,
                                    procedureName,
                                    tableProperties)
                            .orElseThrow(() -> semanticException(NOT_SUPPORTED, node, "Procedure '%s' cannot be executed on table '%s'", procedureName, tableName));

            analysis.setTableExecuteHandle(executeHandle);

            analysis.setUpdateType("ALTER TABLE EXECUTE");
            analysis.setUpdateTarget(tableName, Optional.of(table), Optional.empty());

            return createAndAssignScope(node, scope, Field.newUnqualified("rows", BIGINT));
        }

        private Map<String, Expression> processTableExecuteArguments(TableExecute node, TableProcedureMetadata procedureMetadata, Optional<Scope> scope)
        {
            List<CallArgument> arguments = node.getArguments();
            Predicate<CallArgument> hasName = argument -> argument.getName().isPresent();
            boolean anyNamed = arguments.stream().anyMatch(hasName);
            boolean allNamed = arguments.stream().allMatch(hasName);
            if (anyNamed && !allNamed) {
                throw semanticException(INVALID_ARGUMENTS, node, "Named and positional arguments cannot be mixed");
            }

            if (!anyNamed && arguments.size() > procedureMetadata.getProperties().size()) {
                throw semanticException(INVALID_ARGUMENTS, node, "Too many positional arguments");
            }

            for (CallArgument argument : arguments) {
                process(argument, scope);
            }

            Map<String, Expression> argumentsMap = new HashMap<>();

            if (anyNamed) {
                // all properties named
                for (CallArgument argument : arguments) {
                    if (argumentsMap.put(argument.getName().get().getCanonicalValue(), argument.getValue()) != null) {
                        throw semanticException(DUPLICATE_PROPERTY, argument, "Duplicate named argument: %s", argument.getName());
                    }
                }
            }
            else {
                // all properties unnamed
                int pos = 0;
                for (CallArgument argument : arguments) {
                    argumentsMap.put(procedureMetadata.getProperties().get(pos).getName(), argument.getValue());
                    pos++;
                }
            }
            return ImmutableMap.copyOf(argumentsMap);
        }

        @Override
        protected Scope visitRenameView(RenameView node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitRenameMaterializedView(RenameMaterializedView node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitSetViewAuthorization(SetViewAuthorization node, Optional<Scope> scope)
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
        protected Scope visitDeny(Deny node, Optional<Scope> scope)
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

            if (node.isReplace() && node.isNotExists()) {
                throw semanticException(NOT_SUPPORTED, node, "'CREATE OR REPLACE' and 'IF NOT EXISTS' clauses can not be used together");
            }

            // analyze the query that creates the view
            StatementAnalyzer analyzer = statementAnalyzerFactory.createStatementAnalyzer(analysis, session, warningCollector, CorrelationSupport.ALLOWED);

            Scope queryScope = analyzer.analyze(node.getQuery(), scope);

            validateColumns(node, queryScope.getRelationType());

            analysis.setUpdateType("CREATE MATERIALIZED VIEW");
            analysis.setUpdateTarget(
                    viewName,
                    Optional.empty(),
                    Optional.of(
                            queryScope.getRelationType().getVisibleFields().stream()
                                    .map(this::createOutputColumn)
                                    .collect(toImmutableList())));

            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitDropMaterializedView(DropMaterializedView node, Optional<Scope> scope)
        {
            return createAndAssignScope(node, scope);
        }

        @Override
        protected Scope visitSetTimeZone(SetTimeZone node, Optional<Scope> scope)
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
            process(node.getStatement(), scope);
            return createAndAssignScope(node, scope, Field.newUnqualified("Query Plan", VARCHAR));
        }

        @Override
        protected Scope visitExplainAnalyze(ExplainAnalyze node, Optional<Scope> scope)
        {
            process(node.getStatement(), scope);
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
                    warningCollector.add(new TrinoWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
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
            ImmutableMap.Builder<NodeRef<Expression>, List<Field>> mappings = ImmutableMap.builder();

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
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot unnest type: " + expressionType);
                }

                outputFields.addAll(expressionOutputs);
                mappings.put(NodeRef.of(expression), expressionOutputs);
            }

            Optional<Field> ordinalityField = Optional.empty();
            if (node.isWithOrdinality()) {
                ordinalityField = Optional.of(Field.newUnqualified(Optional.empty(), BIGINT));
            }

            ordinalityField.ifPresent(outputFields::add);

            analysis.setUnnest(node, new UnnestAnalysis(mappings.buildOrThrow(), ordinalityField));

            return createAndAssignScope(node, scope, outputFields.build());
        }

        @Override
        protected Scope visitLateral(Lateral node, Optional<Scope> scope)
        {
            StatementAnalyzer analyzer = statementAnalyzerFactory.createStatementAnalyzer(analysis, session, warningCollector, CorrelationSupport.ALLOWED);
            Scope queryScope = analyzer.analyze(node.getQuery(), scope);
            return createAndAssignScope(node, scope, queryScope.getRelationType());
        }

        private Optional<QualifiedName> getMaterializedViewStorageTableName(MaterializedViewDefinition viewDefinition)
        {
            if (viewDefinition.getStorageTable().isEmpty()) {
                return Optional.empty();
            }
            CatalogSchemaTableName catalogSchemaTableName = viewDefinition.getStorageTable().get();
            SchemaTableName schemaTableName = catalogSchemaTableName.getSchemaTableName();
            return Optional.of(QualifiedName.of(ImmutableList.of(
                    new Identifier(catalogSchemaTableName.getCatalogName(), true),
                    new Identifier(schemaTableName.getSchemaName(), true),
                    new Identifier(schemaTableName.getTableName(), true))));
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

            Optional<MaterializedViewDefinition> optionalMaterializedView = metadata.getMaterializedView(session, name);
            if (optionalMaterializedView.isPresent()) {
                if (metadata.getMaterializedViewFreshness(session, name).isMaterializedViewFresh()) {
                    // If materialized view is current, answer the query using the storage table
                    Optional<QualifiedName> storageName = getMaterializedViewStorageTableName(optionalMaterializedView.get());
                    if (storageName.isEmpty()) {
                        throw semanticException(INVALID_VIEW, table, "Materialized view '%s' is fresh but does not have storage table name", name);
                    }
                    QualifiedObjectName storageTableName = createQualifiedObjectName(session, table, storageName.get());
                    checkStorageTableNotRedirected(storageTableName);
                    Optional<TableHandle> tableHandle = metadata.getTableHandle(session, storageTableName);
                    if (tableHandle.isEmpty()) {
                        throw semanticException(INVALID_VIEW, table, "Storage table '%s' does not exist", storageTableName);
                    }

                    return createScopeForMaterializedView(table, name, scope, optionalMaterializedView.get(), tableHandle);
                }
                else {
                    // This is a stale materialized view and should be expanded like a logical view
                    return createScopeForMaterializedView(table, name, scope, optionalMaterializedView.get(), Optional.empty());
                }
            }

            // This could be a reference to a logical view or a table
            Optional<ViewDefinition> optionalView = metadata.getView(session, name);
            if (optionalView.isPresent()) {
                analysis.addEmptyColumnReferencesForTable(accessControl, session.getIdentity(), name);
                return createScopeForView(table, name, scope, optionalView.get());
            }

            // This can only be a table
            RedirectionAwareTableHandle redirection = getTableHandle(table, name, scope);
            Optional<TableHandle> tableHandle = redirection.getTableHandle();
            QualifiedObjectName targetTableName = redirection.getRedirectedTableName().orElse(name);
            analysis.addEmptyColumnReferencesForTable(accessControl, session.getIdentity(), targetTableName);

            if (tableHandle.isEmpty()) {
                getRequiredCatalogHandle(metadata, session, table, targetTableName.getCatalogName());
                if (!metadata.schemaExists(session, new CatalogSchemaName(targetTableName.getCatalogName(), targetTableName.getSchemaName()))) {
                    throw semanticException(SCHEMA_NOT_FOUND, table, "Schema '%s' does not exist", targetTableName.getSchemaName());
                }
                throw semanticException(TABLE_NOT_FOUND, table, "Table '%s' does not exist", targetTableName);
            }
            TableSchema tableSchema = metadata.getTableSchema(session, tableHandle.get());
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            fields.addAll(analyzeTableOutputFields(table, targetTableName, tableSchema, columnHandles));

            if (updateKind.isPresent()) {
                // Add the row id field
                ColumnHandle rowIdColumnHandle;
                switch (updateKind.get()) {
                    case DELETE:
                        rowIdColumnHandle = metadata.getDeleteRowIdColumnHandle(session, tableHandle.get());
                        break;
                    case UPDATE:
                        List<ColumnSchema> updatedColumnMetadata = analysis.getUpdatedColumns()
                                .orElseThrow(() -> new VerifyException("updated columns not set"));
                        Set<String> updatedColumnNames = updatedColumnMetadata.stream()
                                .map(ColumnSchema::getName)
                                .collect(toImmutableSet());
                        List<ColumnHandle> updatedColumns = columnHandles.entrySet().stream()
                                .filter(entry -> updatedColumnNames.contains(entry.getKey()))
                                .map(Map.Entry::getValue)
                                .collect(toImmutableList());
                        rowIdColumnHandle = metadata.getUpdateRowIdColumnHandle(session, tableHandle.get(), updatedColumns);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unknown UpdateKind " + updateKind.get());
                }

                Type type = metadata.getColumnMetadata(session, tableHandle.get(), rowIdColumnHandle).getType();
                Field field = Field.newUnqualified(Optional.empty(), type);
                fields.add(field);
                analysis.setColumn(field, rowIdColumnHandle);
            }

            List<Field> outputFields = fields.build();

            analyzeFiltersAndMasks(table, targetTableName, tableHandle, outputFields, session.getIdentity().getUser());

            Scope tableScope = createAndAssignScope(table, scope, outputFields);

            if (updateKind.isPresent()) {
                FieldReference reference = new FieldReference(outputFields.size() - 1);
                analyzeExpression(reference, tableScope);
                analysis.setRowIdField(table, reference);
            }

            return tableScope;
        }

        private void checkStorageTableNotRedirected(QualifiedObjectName source)
        {
            metadata.getRedirectionAwareTableHandle(session, source).getRedirectedTableName().ifPresent(name -> {
                throw new TrinoException(NOT_SUPPORTED, format("Redirection of materialized view storage table '%s' to '%s' is not supported", source, name));
            });
        }

        private void analyzeFiltersAndMasks(Table table, QualifiedObjectName name, Optional<TableHandle> tableHandle, List<Field> fields, String authorization)
        {
            analyzeFiltersAndMasks(table, name, tableHandle, new RelationType(fields), authorization);
        }

        private void analyzeFiltersAndMasks(Table table, QualifiedObjectName name, Optional<TableHandle> tableHandle, RelationType relationType, String authorization)
        {
            Scope accessControlScope = Scope.builder()
                    .withRelationType(RelationId.anonymous(), relationType)
                    .build();

            for (int index = 0; index < relationType.getAllFieldCount(); index++) {
                Field field = relationType.getFieldByIndex(index);
                if (field.getName().isPresent()) {
                    List<ViewExpression> masks = accessControl.getColumnMasks(session.toSecurityContext(), name, field.getName().get(), field.getType());

                    if (!masks.isEmpty() && checkCanSelectFromColumn(name, field.getName().orElseThrow())) {
                        masks.forEach(mask -> analyzeColumnMask(session.getIdentity().getUser(), table, name, field, accessControlScope, mask));
                    }
                }
            }

            accessControl.getRowFilters(session.toSecurityContext(), name)
                    .forEach(filter -> analyzeRowFilter(session.getIdentity().getUser(), table, name, accessControlScope, filter));

            analysis.registerTable(table, tableHandle, name, authorization, accessControlScope);
        }

        private boolean checkCanSelectFromColumn(QualifiedObjectName name, String column)
        {
            try {
                accessControl.checkCanSelectFromColumns(session.toSecurityContext(), name, ImmutableSet.of(column));
                return true;
            }
            catch (AccessDeniedException e) {
                return false;
            }
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
                checkState(columnNames.get().size() == queryDescriptor.getVisibleFieldCount(), "mismatched aliases");
                ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
                Iterator<Identifier> aliases = columnNames.get().iterator();
                for (int i = 0; i < queryDescriptor.getAllFieldCount(); i++) {
                    Field inputField = queryDescriptor.getFieldByIndex(i);
                    if (!inputField.isHidden()) {
                        Field field = Field.newQualified(
                                QualifiedName.of(table.getName().getSuffix()),
                                Optional.of(aliases.next().getValue()),
                                inputField.getType(),
                                false,
                                inputField.getOriginTable(),
                                inputField.getOriginColumnName(),
                                inputField.isAliased());
                        fieldBuilder.add(field);
                        analysis.addSourceColumns(field, analysis.getSourceColumns(inputField));
                    }
                }
                fields = fieldBuilder.build();
            }
            else {
                ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
                for (int i = 0; i < queryDescriptor.getAllFieldCount(); i++) {
                    Field inputField = queryDescriptor.getFieldByIndex(i);
                    if (!inputField.isHidden()) {
                        Field field = Field.newQualified(
                                QualifiedName.of(table.getName().getSuffix()),
                                inputField.getName(),
                                inputField.getType(),
                                false,
                                inputField.getOriginTable(),
                                inputField.getOriginColumnName(),
                                inputField.isAliased());
                        fieldBuilder.add(field);
                        analysis.addSourceColumns(field, analysis.getSourceColumns(inputField));
                    }
                }
                fields = fieldBuilder.build();
            }

            return createAndAssignScope(table, scope, fields);
        }

        private Scope createScopeForMaterializedView(Table table, QualifiedObjectName name, Optional<Scope> scope, MaterializedViewDefinition view, Optional<TableHandle> storageTable)
        {
            return createScopeForView(
                    table,
                    name,
                    scope,
                    view.getOriginalSql(),
                    view.getCatalog(),
                    view.getSchema(),
                    view.getRunAsIdentity(),
                    view.getColumns(),
                    storageTable);
        }

        private Scope createScopeForView(Table table, QualifiedObjectName name, Optional<Scope> scope, ViewDefinition view)
        {
            return createScopeForView(table, name, scope, view.getOriginalSql(), view.getCatalog(), view.getSchema(), view.getRunAsIdentity(), view.getColumns(), Optional.empty());
        }

        private Scope createScopeForView(
                Table table,
                QualifiedObjectName name,
                Optional<Scope> scope,
                String originalSql,
                Optional<String> catalog,
                Optional<String> schema,
                Optional<Identity> owner,
                List<ViewColumn> columns,
                Optional<TableHandle> storageTable)
        {
            Statement statement = analysis.getStatement();
            if (statement instanceof CreateView) {
                CreateView viewStatement = (CreateView) statement;
                QualifiedObjectName viewNameFromStatement = createQualifiedObjectName(session, viewStatement, viewStatement.getName());
                if (viewStatement.isReplace() && viewNameFromStatement.equals(name)) {
                    throw semanticException(VIEW_IS_RECURSIVE, table, "Statement would create a recursive view");
                }
            }
            if (statement instanceof CreateMaterializedView) {
                CreateMaterializedView viewStatement = (CreateMaterializedView) statement;
                QualifiedObjectName viewNameFromStatement = createQualifiedObjectName(session, viewStatement, viewStatement.getName());
                if (viewStatement.isReplace() && viewNameFromStatement.equals(name)) {
                    throw semanticException(VIEW_IS_RECURSIVE, table, "Statement would create a recursive materialized view");
                }
            }
            if (analysis.hasTableInView(table)) {
                throw semanticException(VIEW_IS_RECURSIVE, table, "View is recursive");
            }

            Query query = parseView(originalSql, name, table);
            analysis.registerTableForView(table);
            RelationType descriptor = analyzeView(query, name, catalog, schema, owner, table);
            analysis.unregisterTableForView();

            checkViewStaleness(columns, descriptor.getVisibleFields(), name, table)
                    .ifPresent(explanation -> { throw semanticException(VIEW_IS_STALE, table, "View '%s' is stale or in invalid state: %s", name, explanation); });

            // Derive the type of the view from the stored definition, not from the analysis of the underlying query.
            // This is needed in case the underlying table(s) changed and the query in the view now produces types that
            // are implicitly coercible to the declared view types.
            List<Field> viewFields = columns.stream()
                    .map(column -> Field.newQualified(
                            table.getName(),
                            Optional.of(column.getName()),
                            getViewColumnType(column, name, table),
                            false,
                            Optional.of(name),
                            Optional.of(column.getName()),
                            false))
                    .collect(toImmutableList());

            if (storageTable.isPresent()) {
                List<Field> storageTableFields = analyzeStorageTable(table, viewFields, storageTable.get());
                analyzeFiltersAndMasks(table, name, storageTable, viewFields, session.getIdentity().getUser());
                analysis.addRelationCoercion(table, viewFields.stream().map(Field::getType).toArray(Type[]::new));
                // use storage table output fields as they contain ColumnHandles
                return createAndAssignScope(table, scope, storageTableFields);
            }

            analyzeFiltersAndMasks(table, name, storageTable, viewFields, session.getIdentity().getUser());
            viewFields.forEach(field -> analysis.addSourceColumns(field, ImmutableSet.of(new SourceColumn(name, field.getName().orElseThrow()))));
            analysis.registerNamedQuery(table, query);
            return createAndAssignScope(table, scope, viewFields);
        }

        private List<Field> analyzeStorageTable(Table table, List<Field> viewFields, TableHandle storageTable)
        {
            TableSchema tableSchema = metadata.getTableSchema(session, storageTable);
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, storageTable);
            QualifiedObjectName tableName = createQualifiedObjectName(session, table, table.getName());
            checkStorageTableNotRedirected(tableName);
            List<Field> tableFields = analyzeTableOutputFields(table, tableName, tableSchema, columnHandles)
                    .stream()
                    .filter(field -> !field.isHidden())
                    .collect(toImmutableList());

            // make sure storage table fields match view fields
            if (tableFields.size() != viewFields.size()) {
                throw semanticException(
                        INVALID_VIEW,
                        table,
                        "storage table column count (%s) does not match column count derived from the materialized view query analysis (%s)",
                        tableFields.size(),
                        viewFields.size());
            }

            for (int index = 0; index < tableFields.size(); index++) {
                Field tableField = tableFields.get(index);
                Field viewField = viewFields.get(index);

                if (tableField.getName().isEmpty()) {
                    throw semanticException(
                            INVALID_VIEW,
                            table,
                            "a column of type %s projected from query view at position %s has no name",
                            tableField.getType(),
                            index);
                }

                String tableFieldName = tableField.getName().orElseThrow();
                String viewFieldName = viewField.getName().orElseThrow();
                if (!viewFieldName.equalsIgnoreCase(tableFieldName)) {
                    throw semanticException(
                            INVALID_VIEW,
                            table,
                            "column [%s] of type %s projected from storage table at position %s has a different name from column [%s] of type %s stored in materialized view definition",
                            tableFieldName,
                            tableField.getType(),
                            index,
                            viewFieldName,
                            viewField.getType());
                }

                if (!tableField.getType().equals(viewField.getType())) {
                    try {
                        metadata.getCoercion(session, viewField.getType(), tableField.getType());
                    }
                    catch (TrinoException e) {
                        throw semanticException(
                                INVALID_VIEW,
                                table,
                                "cannot cast column [%s] of type %s projected from storage table at position %s into column [%s] of type %s stored in view definition",
                                tableFieldName,
                                tableField.getType(),
                                index,
                                viewFieldName,
                                viewField.getType());
                    }
                }
            }

            return tableFields;
        }

        private List<Field> analyzeTableOutputFields(Table table, QualifiedObjectName tableName, TableSchema tableSchema, Map<String, ColumnHandle> columnHandles)
        {
            // TODO: discover columns lazily based on where they are needed (to support connectors that can't enumerate all tables)
            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (ColumnSchema column : tableSchema.getColumns()) {
                Field field = Field.newQualified(
                        table.getName(),
                        Optional.of(column.getName()),
                        column.getType(),
                        column.isHidden(),
                        Optional.of(tableName),
                        Optional.of(column.getName()),
                        false);
                fields.add(field);
                ColumnHandle columnHandle = columnHandles.get(column.getName());
                checkArgument(columnHandle != null, "Unknown field %s", field);
                analysis.setColumn(field, columnHandle);
                analysis.addSourceColumns(field, ImmutableSet.of(new SourceColumn(tableName, column.getName())));
            }
            return fields.build();
        }

        @Override
        protected Scope visitPatternRecognitionRelation(PatternRecognitionRelation relation, Optional<Scope> scope)
        {
            Scope inputScope = process(relation.getInput(), scope);

            // check that input table column names are not ambiguous
            // Note: This check is not compliant with SQL identifier semantics. Quoted identifiers should have different comparison rules than unquoted identifiers.
            // However, field names do not contain the information about quotation, and so every comparison is case-insensitive. For example, if there are fields named
            // 'a' and 'A' (quoted), they should be considered non-ambiguous. However, their names will be compared case-insensitive and will cause failure as ambiguous.
            Set<String> inputNames = new HashSet<>();
            for (Field field : inputScope.getRelationType().getAllFields()) {
                field.getName().ifPresent(name -> {
                    if (!inputNames.add(name.toUpperCase(ENGLISH))) {
                        throw semanticException(AMBIGUOUS_NAME, relation.getInput(), "ambiguous column: %s in row pattern input relation", name);
                    }
                });
            }

            // analyze PARTITION BY
            for (Expression expression : relation.getPartitionBy()) {
                // The PARTITION BY clause is a list of columns of the row pattern input table.
                validateAndGetInputField(expression, inputScope);
                Type type = analyzeExpression(expression, inputScope).getType(expression);
                if (!type.isComparable()) {
                    throw semanticException(TYPE_MISMATCH, expression, "%s is not comparable, and therefore cannot be used in PARTITION BY", type);
                }
            }

            // analyze ORDER BY
            for (SortItem sortItem : getSortItemsFromOrderBy(relation.getOrderBy())) {
                // The ORDER BY clause is a list of columns of the row pattern input table.
                Expression expression = sortItem.getSortKey();
                validateAndGetInputField(expression, inputScope);
                Type type = analyzeExpression(expression, inputScope).getType(sortItem.getSortKey());
                if (!type.isOrderable()) {
                    throw semanticException(TYPE_MISMATCH, sortItem, "%s is not orderable, and therefore cannot be used in ORDER BY", type);
                }
            }

            // analyze pattern recognition clauses
            PatternRecognitionAnalysis patternRecognitionAnalysis = PatternRecognitionAnalyzer.analyze(
                    relation.getSubsets(),
                    relation.getVariableDefinitions(),
                    relation.getMeasures(),
                    relation.getPattern(),
                    relation.getAfterMatchSkipTo());

            analysis.setUndefinedLabels(relation.getPattern(), patternRecognitionAnalysis.getUndefinedLabels());
            analysis.setRanges(patternRecognitionAnalysis.getRanges());

            PatternRecognitionAnalyzer.validateNoPatternSearchMode(relation.getPatternSearchMode());
            PatternRecognitionAnalyzer.validatePatternExclusions(relation.getRowsPerMatch(), relation.getPattern());

            // Notes on potential name ambiguity between pattern labels and other identifiers:
            // Labels are allowed in expressions of MEASURES and DEFINE clauses. In those expressions, qualifying column names with table name is not allowed.
            // Theoretically, user might define pattern label "T" where input table name was "T". Then a dereference "T.column" would refer to:
            // - input table's column, if it was in PARTITION BY or ORDER BY clause,
            // - subset of rows matched with label "T", if it was in MEASURES or DEFINE clause.
            // There could be a check to catch such non-intuitive situation and produce a warning.
            // Similarly, it is possible to define pattern label with the same name as some input column. However, this causes no ambiguity, as labels can only
            // appear as column name's prefix, and column names in pattern recognition context cannot be dereferenced.

            // analyze expressions in MEASURES and DEFINE (with set of all labels passed as context)
            for (VariableDefinition variableDefinition : relation.getVariableDefinitions()) {
                Expression expression = variableDefinition.getExpression();
                ExpressionAnalysis expressionAnalysis = analyzePatternRecognitionExpression(expression, inputScope, patternRecognitionAnalysis.getAllLabels());
                analysis.recordSubqueries(relation, expressionAnalysis);
                Type type = expressionAnalysis.getType(expression);
                if (!type.equals(BOOLEAN)) {
                    throw semanticException(TYPE_MISMATCH, expression, "Expression defining a label must be boolean (actual type: %s)", type);
                }
            }
            ImmutableMap.Builder<NodeRef<Node>, Type> measureTypesBuilder = ImmutableMap.builder();
            for (MeasureDefinition measureDefinition : relation.getMeasures()) {
                Expression expression = measureDefinition.getExpression();
                ExpressionAnalysis expressionAnalysis = analyzePatternRecognitionExpression(expression, inputScope, patternRecognitionAnalysis.getAllLabels());
                analysis.recordSubqueries(relation, expressionAnalysis);
                measureTypesBuilder.put(NodeRef.of(expression), expressionAnalysis.getType(expression));
            }
            Map<NodeRef<Node>, Type> measureTypes = measureTypesBuilder.buildOrThrow();

            // create output scope
            // ONE ROW PER MATCH: PARTITION BY columns, then MEASURES columns in order of declaration
            // ALL ROWS PER MATCH: PARTITION BY columns, ORDER BY columns, MEASURES columns, then any remaining input table columns in order of declaration
            // Note: row pattern input table name should not be exposed on output
            boolean oneRowPerMatch = relation.getRowsPerMatch().isEmpty() || relation.getRowsPerMatch().get().isOneRow();
            ImmutableSet.Builder<Field> inputFieldsOnOutputBuilder = ImmutableSet.builder();
            ImmutableList.Builder<Field> outputFieldsBuilder = ImmutableList.builder();

            for (Expression expression : relation.getPartitionBy()) {
                Field inputField = validateAndGetInputField(expression, inputScope);
                outputFieldsBuilder.add(unqualifiedVisible(inputField));
                inputFieldsOnOutputBuilder.add(inputField);
            }
            if (!oneRowPerMatch) {
                for (SortItem sortItem : getSortItemsFromOrderBy(relation.getOrderBy())) {
                    Field inputField = validateAndGetInputField(sortItem.getSortKey(), inputScope);
                    outputFieldsBuilder.add(unqualifiedVisible(inputField));
                    inputFieldsOnOutputBuilder.add(inputField); // might have duplicates (ORDER BY a ASC, a DESC)
                }
            }
            for (MeasureDefinition measureDefinition : relation.getMeasures()) {
                outputFieldsBuilder.add(Field.newUnqualified(
                        measureDefinition.getName().getValue(),
                        measureTypes.get(NodeRef.of(measureDefinition.getExpression()))));
            }
            if (!oneRowPerMatch) {
                Set<Field> inputFieldsOnOutput = inputFieldsOnOutputBuilder.build();
                for (Field inputField : inputScope.getRelationType().getAllFields()) {
                    if (!inputFieldsOnOutput.contains(inputField)) {
                        outputFieldsBuilder.add(unqualified(inputField));
                    }
                }
            }
            // pattern recognition output must have at least 1 column
            List<Field> outputFields = outputFieldsBuilder.build();
            if (outputFields.isEmpty()) {
                throw semanticException(TABLE_HAS_NO_COLUMNS, relation, "pattern recognition output table has no columns");
            }

            return createAndAssignScope(relation, scope, outputFields);
        }

        private Field validateAndGetInputField(Expression expression, Scope inputScope)
        {
            QualifiedName qualifiedName;
            if (expression instanceof Identifier) {
                qualifiedName = QualifiedName.of(ImmutableList.of((Identifier) expression));
            }
            else if (expression instanceof DereferenceExpression) {
                qualifiedName = getQualifiedName((DereferenceExpression) expression);
            }
            else {
                throw semanticException(INVALID_COLUMN_REFERENCE, expression, "Expected column reference. Actual: " + expression);
            }
            Optional<ResolvedField> field = inputScope.tryResolveField(expression, qualifiedName);
            if (field.isEmpty() || !field.get().isLocal()) {
                throw semanticException(COLUMN_NOT_FOUND, expression, "Column %s is not present in the input relation", expression);
            }

            return field.get().getField();
        }

        private Field unqualifiedVisible(Field field)
        {
            return new Field(
                    Optional.empty(),
                    field.getName(),
                    field.getType(),
                    false,
                    field.getOriginTable(),
                    field.getOriginColumnName(),
                    field.isAliased());
        }

        private Field unqualified(Field field)
        {
            return new Field(
                    Optional.empty(),
                    field.getName(),
                    field.getType(),
                    field.isHidden(),
                    field.getOriginTable(),
                    field.getOriginColumnName(),
                    field.isAliased());
        }

        private ExpressionAnalysis analyzePatternRecognitionExpression(Expression expression, Scope scope, Set<String> labels)
        {
            List<Expression> nestedWindowExpressions = extractWindowExpressions(ImmutableList.of(expression));
            if (!nestedWindowExpressions.isEmpty()) {
                throw semanticException(NESTED_WINDOW, nestedWindowExpressions.get(0), "Cannot nest window functions or row pattern measures inside pattern recognition expressions");
            }

            return ExpressionAnalyzer.analyzePatternRecognitionExpression(
                    session,
                    plannerContext,
                    statementAnalyzerFactory,
                    accessControl,
                    scope,
                    analysis,
                    expression,
                    warningCollector,
                    labels);
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
            Collection<Field> inputFields = relationType.getAllFields();
            if (relation.getColumnNames() != null) {
                aliases = relation.getColumnNames().stream()
                        .map(Identifier::getValue)
                        .collect(Collectors.toList());
                // hidden fields are not exposed when there are column aliases
                inputFields = relationType.getVisibleFields();
            }

            RelationType descriptor = relationType.withAlias(relation.getAlias().getValue(), aliases);

            checkArgument(inputFields.size() == descriptor.getAllFieldCount(),
                    "Expected %s fields, got %s",
                    descriptor.getAllFieldCount(),
                    inputFields.size());

            Streams.forEachPair(
                    descriptor.getAllFields().stream(),
                    inputFields.stream(),
                    (newField, field) -> analysis.addSourceColumns(newField, analysis.getSourceColumns(field)));

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
                    plannerContext,
                    statementAnalyzerFactory,
                    accessControl,
                    TypeProvider.empty(),
                    ImmutableList.of(samplePercentage),
                    analysis.getParameters(),
                    WarningCollector.NOOP,
                    analysis.getQueryType())
                    .getExpressionTypes();

            Type samplePercentageType = expressionTypes.get(NodeRef.of(samplePercentage));
            if (!typeCoercion.canCoerce(samplePercentageType, DOUBLE)) {
                throw semanticException(TYPE_MISMATCH, samplePercentage, "Sample percentage should be a numeric expression");
            }

            ExpressionInterpreter samplePercentageEval = new ExpressionInterpreter(samplePercentage, plannerContext, session, expressionTypes);

            Object samplePercentageObject = samplePercentageEval.optimize(symbol -> {
                throw semanticException(EXPRESSION_NOT_CONSTANT, samplePercentage, "Sample percentage cannot contain column references");
            });

            if (samplePercentageObject == null) {
                throw semanticException(INVALID_ARGUMENTS, samplePercentage, "Sample percentage cannot be NULL");
            }

            if (samplePercentageType != DOUBLE) {
                ResolvedFunction coercion = metadata.getCoercion(session, samplePercentageType, DOUBLE);
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
            StatementAnalyzer analyzer = statementAnalyzerFactory.createStatementAnalyzer(analysis, session, warningCollector, CorrelationSupport.ALLOWED);
            Scope queryScope = analyzer.analyze(node.getQuery(), scope);
            return createAndAssignScope(node, scope, queryScope.getRelationType());
        }

        @Override
        protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
            // to pass down to analyzeFrom

            Scope sourceScope = analyzeFrom(node, scope);

            analyzeWindowDefinitions(node, sourceScope);
            resolveFunctionCallAndMeasureWindows(node);

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
                    warningCollector.add(new TrinoWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
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
            for (WindowDefinition windowDefinition : node.getWindows()) {
                WindowSpecification window = windowDefinition.getWindow();
                sourceExpressions.addAll(window.getPartitionBy());
                getSortItemsFromOrderBy(window.getOrderBy()).stream()
                        .map(SortItem::getSortKey)
                        .forEach(sourceExpressions::add);
                if (window.getFrame().isPresent()) {
                    WindowFrame frame = window.getFrame().get();
                    frame.getStart().getValue()
                            .ifPresent(sourceExpressions::add);
                    frame.getEnd()
                            .flatMap(FrameBound::getValue)
                            .ifPresent(sourceExpressions::add);
                    frame.getMeasures().stream()
                            .map(MeasureDefinition::getExpression)
                            .forEach(sourceExpressions::add);
                    frame.getVariableDefinitions().stream()
                            .map(VariableDefinition::getExpression)
                            .forEach(sourceExpressions::add);
                }
            }

            analyzeGroupingOperations(node, sourceExpressions, orderByExpressions);
            analyzeAggregations(node, sourceScope, orderByScope, groupByAnalysis, sourceExpressions, orderByExpressions);
            analyzeWindowFunctionsAndMeasures(node, outputExpressions, orderByExpressions);

            if (analysis.isAggregation(node) && node.getOrderBy().isPresent()) {
                ImmutableList.Builder<Expression> aggregates = ImmutableList.<Expression>builder()
                        .addAll(groupByAnalysis.getOriginalExpressions())
                        .addAll(extractAggregateFunctions(orderByExpressions, metadata))
                        .addAll(extractExpressions(orderByExpressions, GroupingOperation.class));

                analysis.setOrderByAggregates(node.getOrderBy().get(), aggregates.build());
            }

            if (node.getOrderBy().isPresent() && node.getSelect().isDistinct()) {
                verifySelectDistinct(node, orderByExpressions, outputExpressions, sourceScope, orderByScope.orElseThrow());
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

            String setOperationName = node.getClass().getSimpleName().toUpperCase(ENGLISH);
            Type[] outputFieldTypes = childrenTypes.get(0).getVisibleFields().stream()
                    .map(Field::getType)
                    .toArray(Type[]::new);
            for (RelationType relationType : childrenTypes) {
                int outputFieldSize = outputFieldTypes.length;
                int descFieldSize = relationType.getVisibleFields().size();
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

            if (node instanceof Intersect || node instanceof Except || node instanceof Union && node.isDistinct()) {
                for (Type type : outputFieldTypes) {
                    if (!type.isComparable()) {
                        StringBuilder message = new StringBuilder(format("Type %s is not comparable and therefore cannot be used in ", type));
                        message.append(setOperationName);
                        if (node instanceof Union) {
                            message.append(" DISTINCT");
                        }
                        throw semanticException(TYPE_MISMATCH, node, message.toString());
                    }
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

                int index = i; // Variable used in Lambda should be final
                analysis.addSourceColumns(
                        outputDescriptorFields[index],
                        childrenTypes.stream()
                                .map(relationType -> relationType.getFieldByIndex(index))
                                .flatMap(field -> analysis.getSourceColumns(field).stream())
                                .collect(toImmutableSet()));
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
                verifyNoAggregateWindowOrGroupingFunctions(metadata, expression, "JOIN clause");

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

                analysis.recordSubqueries(node, expressionAnalysis);
                analysis.setJoinCriteria(node, expression);
            }
            else {
                throw new UnsupportedOperationException("Unsupported join criteria: " + criteria.getClass().getName());
            }

            return output;
        }

        @Override
        protected Scope visitUpdate(Update update, Optional<Scope> scope)
        {
            Table table = update.getTable();
            QualifiedObjectName originalName = createQualifiedObjectName(session, table, table.getName());
            if (metadata.isView(session, originalName)) {
                throw semanticException(NOT_SUPPORTED, update, "Updating through views is not supported");
            }

            RedirectionAwareTableHandle redirection = metadata.getRedirectionAwareTableHandle(session, originalName);
            QualifiedObjectName tableName = redirection.getRedirectedTableName().orElse(originalName);
            TableHandle handle = redirection.getTableHandle()
                    .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, table, "Table '%s' does not exist", tableName));

            TableSchema tableSchema = metadata.getTableSchema(session, handle);

            List<ColumnSchema> allColumns = tableSchema.getColumns();
            Map<String, ColumnSchema> columns = allColumns.stream()
                    .collect(toImmutableMap(ColumnSchema::getName, Function.identity()));

            for (UpdateAssignment assignment : update.getAssignments()) {
                String columnName = assignment.getName().getValue();
                if (!columns.containsKey(columnName)) {
                    throw semanticException(COLUMN_NOT_FOUND, assignment.getName(), "The UPDATE SET target column %s doesn't exist", columnName);
                }
            }

            Set<String> assignmentTargets = update.getAssignments().stream()
                    .map(assignment -> assignment.getName().getValue())
                    .collect(toImmutableSet());
            accessControl.checkCanUpdateTableColumns(session.toSecurityContext(), tableName, assignmentTargets);

            if (!accessControl.getRowFilters(session.toSecurityContext(), tableName).isEmpty()) {
                throw semanticException(NOT_SUPPORTED, update, "Updating a table with a row filter is not supported");
            }

            // TODO: how to deal with connectors that need to see the pre-image of rows to perform the update without
            //       flowing that data through the masking logic
            for (ColumnSchema tableColumn : allColumns) {
                if (!accessControl.getColumnMasks(session.toSecurityContext(), tableName, tableColumn.getName(), tableColumn.getType()).isEmpty()) {
                    throw semanticException(NOT_SUPPORTED, update, "Updating a table with column masks is not supported");
                }
            }

            List<ColumnSchema> updatedColumns = allColumns.stream()
                    .filter(column -> assignmentTargets.contains(column.getName()))
                    .collect(toImmutableList());
            analysis.setUpdatedColumns(updatedColumns);

            // Analyzer checks for select permissions but UPDATE has a separate permission, so disable access checks
            StatementAnalyzer analyzer = statementAnalyzerFactory
                    .withSpecializedAccessControl(new AllowAllAccessControl())
                    .createStatementAnalyzer(analysis, session, warningCollector, CorrelationSupport.ALLOWED);

            Scope tableScope = analyzer.analyzeForUpdate(table, scope, UpdateKind.UPDATE);
            update.getWhere().ifPresent(where -> analyzeWhere(update, tableScope, where));

            ImmutableList.Builder<ExpressionAnalysis> analysesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> expressionTypesBuilder = ImmutableList.builder();
            for (UpdateAssignment assignment : update.getAssignments()) {
                Expression expression = assignment.getValue();
                ExpressionAnalysis analysis = analyzeExpression(expression, tableScope);
                analysesBuilder.add(analysis);
                expressionTypesBuilder.add(analysis.getType(expression));
            }
            List<ExpressionAnalysis> analyses = analysesBuilder.build();
            List<Type> expressionTypes = expressionTypesBuilder.build();

            List<Type> tableTypes = update.getAssignments().stream()
                    .map(assignment -> requireNonNull(columns.get(assignment.getName().getValue())))
                    .map(ColumnSchema::getType)
                    .collect(toImmutableList());

            if (!typesMatchForInsert(tableTypes, expressionTypes)) {
                throw semanticException(TYPE_MISMATCH,
                        update,
                        "UPDATE table column types don't match SET expressions: Table: [%s], Expressions: [%s]",
                        Joiner.on(", ").join(tableTypes),
                        Joiner.on(", ").join(expressionTypes));
            }

            for (int index = 0; index < expressionTypes.size(); index++) {
                Expression expression = update.getAssignments().get(index).getValue();
                Type expressionType = expressionTypes.get(index);
                Type targetType = tableTypes.get(index);
                if (!targetType.equals(expressionType)) {
                    analysis.addCoercion(expression, targetType, typeCoercion.isTypeOnlyCoercion(expressionType, targetType));
                }
                analysis.recordSubqueries(update, analyses.get(index));
            }

            analysis.setUpdateType("UPDATE");
            analysis.setUpdateTarget(
                    tableName,
                    Optional.of(table),
                    Optional.of(updatedColumns.stream()
                            .map(column -> new OutputColumn(new Column(column.getName(), column.getType().toString()), ImmutableSet.of()))
                            .collect(toImmutableList())));

            return createAndAssignScope(update, scope, Field.newUnqualified("rows", BIGINT));
        }

        @Override
        protected Scope visitMerge(Merge merge, Optional<Scope> scope)
        {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support merge");
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
                    metadata.resolveOperator(session, OperatorType.EQUAL, ImmutableList.of(
                            leftField.get().getType(), rightField.get().getType()));
                }
                catch (OperatorNotFoundException e) {
                    throw semanticException(TYPE_MISMATCH, column, e, "%s", e.getMessage());
                }

                Optional<Type> type = typeCoercion.getCommonSuperType(leftField.get().getType(), rightField.get().getType());
                analysis.addTypes(ImmutableMap.of(NodeRef.of(column), type.orElseThrow()));

                joinFields.add(Field.newUnqualified(column.getValue(), type.get()));

                leftJoinFields.add(leftField.get().getRelationFieldIndex());
                rightJoinFields.add(rightField.get().getRelationFieldIndex());

                recordColumnAccess(leftField.get().getField());
                recordColumnAccess(rightField.get().getField());
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

        private void recordColumnAccess(Field field)
        {
            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                analysis.addTableColumnReferences(
                        accessControl,
                        session.getIdentity(),
                        ImmutableMultimap.of(field.getOriginTable().get(), field.getOriginColumnName().get()));
            }
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

            List<Type> rowTypes = node.getRows().stream()
                    .map(row -> analyzeExpression(row, createScope(scope)).getType(row))
                    .map(type -> {
                        if (type instanceof RowType) {
                            return type;
                        }
                        return RowType.anonymousRow(type);
                    })
                    .collect(toImmutableList());

            int fieldCount = rowTypes.get(0).getTypeParameters().size();
            Type commonSuperType = rowTypes.get(0);
            for (Type rowType : rowTypes) {
                // check field count consistency for rows
                if (rowType.getTypeParameters().size() != fieldCount) {
                    throw semanticException(TYPE_MISMATCH,
                            node,
                            "Values rows have mismatched sizes: %s vs %s",
                            fieldCount,
                            rowType.getTypeParameters().size());
                }

                // determine common super type of the rows
                Optional<Type> partialSuperType = typeCoercion.getCommonSuperType(rowType, commonSuperType);
                if (partialSuperType.isEmpty()) {
                    throw semanticException(TYPE_MISMATCH,
                            node,
                            "Values rows have mismatched types: %s vs %s",
                            rowTypes.get(0),
                            rowType);
                }
                commonSuperType = partialSuperType.get();
            }

            // add coercions
            for (Expression row : node.getRows()) {
                Type actualType = analysis.getType(row);
                if (row instanceof Row) {
                    // coerce Row by fields to preserve Row structure and enable optimizations based on this structure, e.g. pruning, predicate extraction
                    // TODO coerce the whole Row and add an Optimizer rule that converts CAST(ROW(...) AS ...) into ROW(CAST(...), CAST(...), ...).
                    //  The rule would also handle Row-type expressions that were specified as CAST(ROW). It should support multiple casts over a ROW.
                    for (int i = 0; i < actualType.getTypeParameters().size(); i++) {
                        Expression item = ((Row) row).getItems().get(i);
                        Type actualItemType = actualType.getTypeParameters().get(i);
                        Type expectedItemType = commonSuperType.getTypeParameters().get(i);
                        if (!actualItemType.equals(expectedItemType)) {
                            analysis.addCoercion(item, expectedItemType, typeCoercion.isTypeOnlyCoercion(actualItemType, expectedItemType));
                        }
                    }
                }
                else if (actualType instanceof RowType) {
                    // coerce row-type expression as a whole
                    if (!actualType.equals(commonSuperType)) {
                        analysis.addCoercion(row, commonSuperType, typeCoercion.isTypeOnlyCoercion(actualType, commonSuperType));
                    }
                }
                else {
                    // coerce field. it will be wrapped in Row by Planner
                    Type superType = getOnlyElement(commonSuperType.getTypeParameters());
                    if (!actualType.equals(superType)) {
                        analysis.addCoercion(row, superType, typeCoercion.isTypeOnlyCoercion(actualType, superType));
                    }
                }
            }

            List<Field> fields = commonSuperType.getTypeParameters().stream()
                    .map(valueType -> Field.newUnqualified(Optional.empty(), valueType))
                    .collect(toImmutableList());

            return createAndAssignScope(node, scope, fields);
        }

        private void analyzeWindowDefinitions(QuerySpecification node, Scope scope)
        {
            for (WindowDefinition windowDefinition : node.getWindows()) {
                CanonicalizationAware<Identifier> canonicalName = canonicalizationAwareKey(windowDefinition.getName());

                if (analysis.getWindowDefinition(node, canonicalName) != null) {
                    throw semanticException(DUPLICATE_WINDOW_NAME, windowDefinition, "WINDOW name '%s' specified more than once", windowDefinition.getName());
                }

                ResolvedWindow resolvedWindow = resolveWindowSpecification(node, windowDefinition.getWindow());

                // Analyze window after it is resolved, because resolving might provide necessary information, e.g. ORDER BY necessary for frame analysis.
                // Analyze only newly introduced window properties. Properties of the referenced window have been already analyzed.
                analyzeWindow(node, resolvedWindow, scope, windowDefinition.getWindow());

                analysis.addWindowDefinition(node, canonicalName, resolvedWindow);
            }
        }

        private ResolvedWindow resolveWindowSpecification(QuerySpecification querySpecification, Window window)
        {
            if (window instanceof WindowReference) {
                WindowReference windowReference = (WindowReference) window;
                CanonicalizationAware<Identifier> canonicalName = canonicalizationAwareKey(windowReference.getName());
                ResolvedWindow referencedWindow = analysis.getWindowDefinition(querySpecification, canonicalName);
                if (referencedWindow == null) {
                    throw semanticException(INVALID_WINDOW_REFERENCE, windowReference.getName(), "Cannot resolve WINDOW name " + windowReference.getName());
                }

                return new ResolvedWindow(
                        referencedWindow.getPartitionBy(),
                        referencedWindow.getOrderBy(),
                        referencedWindow.getFrame(),
                        !referencedWindow.getPartitionBy().isEmpty(),
                        referencedWindow.getOrderBy().isPresent(),
                        referencedWindow.getFrame().isPresent());
            }

            WindowSpecification windowSpecification = (WindowSpecification) window;

            if (windowSpecification.getExistingWindowName().isPresent()) {
                Identifier referencedName = windowSpecification.getExistingWindowName().get();
                CanonicalizationAware<Identifier> canonicalName = canonicalizationAwareKey(referencedName);
                ResolvedWindow referencedWindow = analysis.getWindowDefinition(querySpecification, canonicalName);
                if (referencedWindow == null) {
                    throw semanticException(INVALID_WINDOW_REFERENCE, referencedName, "Cannot resolve WINDOW name " + referencedName);
                }

                // analyze dependencies between this window specification and referenced window specification
                if (!windowSpecification.getPartitionBy().isEmpty()) {
                    throw semanticException(INVALID_PARTITION_BY, windowSpecification.getPartitionBy().get(0), "WINDOW specification with named WINDOW reference cannot specify PARTITION BY");
                }
                if (windowSpecification.getOrderBy().isPresent() && referencedWindow.getOrderBy().isPresent()) {
                    throw semanticException(INVALID_ORDER_BY, windowSpecification.getOrderBy().get(), "Cannot specify ORDER BY if referenced named WINDOW specifies ORDER BY");
                }
                if (referencedWindow.getFrame().isPresent()) {
                    throw semanticException(INVALID_WINDOW_REFERENCE, windowSpecification.getExistingWindowName().get(), "Cannot reference named WINDOW containing frame specification");
                }

                // resolve window
                Optional<OrderBy> orderBy = windowSpecification.getOrderBy();
                boolean orderByInherited = false;
                if (orderBy.isEmpty() && referencedWindow.getOrderBy().isPresent()) {
                    orderBy = referencedWindow.getOrderBy();
                    orderByInherited = true;
                }

                List<Expression> partitionBy = windowSpecification.getPartitionBy();
                boolean partitionByInherited = false;
                if (!referencedWindow.getPartitionBy().isEmpty()) {
                    partitionBy = referencedWindow.getPartitionBy();
                    partitionByInherited = true;
                }

                Optional<WindowFrame> windowFrame = windowSpecification.getFrame();
                boolean frameInherited = false;
                if (windowFrame.isEmpty() && referencedWindow.getFrame().isPresent()) {
                    windowFrame = referencedWindow.getFrame();
                    frameInherited = true;
                }

                return new ResolvedWindow(partitionBy, orderBy, windowFrame, partitionByInherited, orderByInherited, frameInherited);
            }

            return new ResolvedWindow(windowSpecification.getPartitionBy(), windowSpecification.getOrderBy(), windowSpecification.getFrame(), false, false, false);
        }

        private void analyzeWindow(QuerySpecification querySpecification, ResolvedWindow window, Scope scope, Node originalNode)
        {
            ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeWindow(
                    session,
                    plannerContext,
                    statementAnalyzerFactory,
                    accessControl,
                    scope,
                    analysis,
                    WarningCollector.NOOP,
                    correlationSupport,
                    window,
                    originalNode);
            analysis.recordSubqueries(querySpecification, expressionAnalysis);
        }

        private void resolveFunctionCallAndMeasureWindows(QuerySpecification querySpecification)
        {
            ImmutableList.Builder<Expression> expressions = ImmutableList.builder();

            // SELECT expressions and ORDER BY expressions can contain window functions
            for (SelectItem item : querySpecification.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    ((AllColumns) item).getTarget().ifPresent(expressions::add);
                }
                else if (item instanceof SingleColumn) {
                    expressions.add(((SingleColumn) item).getExpression());
                }
            }
            for (SortItem sortItem : getSortItemsFromOrderBy(querySpecification.getOrderBy())) {
                expressions.add(sortItem.getSortKey());
            }

            for (FunctionCall windowFunction : extractWindowFunctions(expressions.build())) {
                ResolvedWindow resolvedWindow = resolveWindowSpecification(querySpecification, windowFunction.getWindow().orElseThrow());
                analysis.setWindow(windowFunction, resolvedWindow);
            }

            for (WindowOperation measure : extractWindowMeasures(expressions.build())) {
                ResolvedWindow resolvedWindow = resolveWindowSpecification(querySpecification, measure.getWindow());
                analysis.setWindow(measure, resolvedWindow);
            }
        }

        private void analyzeWindowFunctionsAndMeasures(QuerySpecification node, List<Expression> outputExpressions, List<Expression> orderByExpressions)
        {
            analysis.setWindowFunctions(node, analyzeWindowFunctions(node, outputExpressions));
            analysis.setWindowMeasures(node, extractWindowMeasures(outputExpressions));
            if (node.getOrderBy().isPresent()) {
                OrderBy orderBy = node.getOrderBy().get();
                analysis.setOrderByWindowFunctions(orderBy, analyzeWindowFunctions(node, orderByExpressions));
                analysis.setOrderByWindowMeasures(orderBy, extractWindowMeasures(orderByExpressions));
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

                List<Expression> nestedWindowExpressions = extractWindowExpressions(windowFunction.getArguments());
                if (!nestedWindowExpressions.isEmpty()) {
                    throw semanticException(NESTED_WINDOW, nestedWindowExpressions.get(0), "Cannot nest window functions or row pattern measures inside window function arguments");
                }

                if (windowFunction.isDistinct()) {
                    throw semanticException(NOT_SUPPORTED, node, "DISTINCT in window function parameters not yet supported: %s", windowFunction);
                }

                ResolvedWindow window = analysis.getWindow(windowFunction);
                // TODO get function requirements from window function metadata when we have it
                String name = windowFunction.getName().toString().toLowerCase(ENGLISH);
                if (name.equals("lag") || name.equals("lead")) {
                    if (window.getOrderBy().isEmpty()) {
                        throw semanticException(MISSING_ORDER_BY, (Node) windowFunction.getWindow().orElseThrow(), "%s function requires an ORDER BY window clause", windowFunction.getName());
                    }
                    if (window.getFrame().isPresent()) {
                        throw semanticException(INVALID_WINDOW_FRAME, window.getFrame().get(), "Cannot specify window frame for %s function", windowFunction.getName());
                    }
                }

                if (!WINDOW_VALUE_FUNCTIONS.contains(name) && windowFunction.getNullTreatment().isPresent()) {
                    throw semanticException(NULL_TREATMENT_NOT_ALLOWED, windowFunction, "Cannot specify null treatment clause for %s function", windowFunction.getName());
                }

                List<Type> argumentTypes = mappedCopy(windowFunction.getArguments(), analysis::getType);

                ResolvedFunction resolvedFunction = metadata.resolveFunction(session, windowFunction.getName(), fromTypes(argumentTypes));
                FunctionKind kind = metadata.getFunctionMetadata(resolvedFunction).getKind();
                if (kind != AGGREGATE && kind != WINDOW) {
                    throw semanticException(FUNCTION_NOT_WINDOW, node, "Not a window function: %s", windowFunction.getName());
                }
            }

            return windowFunctions;
        }

        private void analyzeHaving(QuerySpecification node, Scope scope)
        {
            if (node.getHaving().isPresent()) {
                Expression predicate = node.getHaving().get();

                List<Expression> windowExpressions = extractWindowExpressions(ImmutableList.of(predicate));
                if (!windowExpressions.isEmpty()) {
                    throw semanticException(NESTED_WINDOW, windowExpressions.get(0), "HAVING clause cannot contain window functions or row pattern measures");
                }

                ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);
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
                                verifyNoAggregateWindowOrGroupingFunctions(metadata, column, "GROUP BY clause");
                            }
                            else {
                                verifyNoAggregateWindowOrGroupingFunctions(metadata, column, "GROUP BY clause");
                                analyzeExpression(column, scope);
                            }

                            ResolvedField field = analysis.getColumnReferenceFields().get(NodeRef.of(column));
                            if (field != null) {
                                sets.add(ImmutableList.of(ImmutableSet.of(field.getFieldId())));
                            }
                            else {
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
                    AllColumns allColumns = (AllColumns) item;

                    List<Field> fields = analysis.getSelectAllResultFields(allColumns);
                    checkNotNull(fields, "output fields is null for select item %s", item);
                    for (int i = 0; i < fields.size(); i++) {
                        Field field = fields.get(i);

                        Optional<String> name;
                        if (!allColumns.getAliases().isEmpty()) {
                            name = Optional.of((allColumns.getAliases().get(i)).getCanonicalValue());
                        }
                        else {
                            name = field.getName();
                        }

                        Field newField = Field.newUnqualified(name, field.getType(), field.getOriginTable(), field.getOriginColumnName(), false);
                        analysis.addSourceColumns(newField, analysis.getSourceColumns(field));
                        outputFields.add(newField);
                    }
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

                    Field newField = Field.newUnqualified(field.map(Identifier::getValue), analysis.getType(expression), originTable, originColumn, column.getAlias().isPresent()); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type
                    if (originTable.isPresent()) {
                        analysis.addSourceColumns(newField, ImmutableSet.of(new SourceColumn(originTable.get(), originColumn.orElseThrow())));
                    }
                    else {
                        analysis.addSourceColumns(newField, analysis.getExpressionSourceColumns(expression));
                    }
                    outputFields.add(newField);
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
                        RelationType relationType = identifierChainBasis.get().getRelationType().orElseThrow();
                        List<Field> fields = filterInaccessibleFields(relationType.resolveVisibleFieldsWithRelationPrefix(Optional.of(prefix)));
                        if (fields.isEmpty()) {
                            throw semanticException(COLUMN_NOT_FOUND, allColumns, "SELECT * not allowed from relation that has no columns");
                        }
                        boolean local = scope.isLocalScope(identifierChainBasis.get().getScope().orElseThrow());
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

                List<Field> fields = filterInaccessibleFields((List<Field>) scope.getRelationType().getVisibleFields());
                if (fields.isEmpty()) {
                    if (node.getFrom().isEmpty()) {
                        throw semanticException(COLUMN_NOT_FOUND, allColumns, "SELECT * not allowed in queries without FROM clause");
                    }
                    throw semanticException(COLUMN_NOT_FOUND, allColumns, "SELECT * not allowed from relation that has no columns");
                }

                analyzeAllColumnsFromTable(fields, allColumns, node, scope, outputExpressionBuilder, selectExpressionBuilder, scope.getRelationType(), true);
            }
        }

        private List<Field> filterInaccessibleFields(List<Field> fields)
        {
            if (!SystemSessionProperties.isHideInaccessibleColumns(session)) {
                return fields;
            }

            List<Field> accessibleFields = new ArrayList<>();

            //collect fields by table
            ListMultimap<QualifiedObjectName, Field> tableFieldsMap = ArrayListMultimap.create();
            fields.forEach(field -> {
                Optional<QualifiedObjectName> originTable = field.getOriginTable();
                if (originTable.isPresent()) {
                    tableFieldsMap.put(originTable.get(), field);
                }
                else {
                    // keep anonymous fields accessible
                    accessibleFields.add(field);
                }
            });

            tableFieldsMap.asMap().forEach((table, tableFields) -> {
                Set<String> accessibleColumns = accessControl.filterColumns(
                        session.toSecurityContext(),
                        table.asCatalogSchemaTableName(),
                        tableFields.stream()
                                .map(field -> field.getOriginColumnName().get())
                                .collect(toImmutableSet()));
                accessibleFields.addAll(tableFields.stream()
                        .filter(field -> accessibleColumns.contains(field.getOriginColumnName().get()))
                        .collect(toImmutableList()));
            });

            return fields.stream()
                .filter(field -> accessibleFields.contains(field))
                .collect(toImmutableList());
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

                Field newField = new Field(
                        field.getRelationAlias(),
                        alias,
                        field.getType(),
                        false,
                        field.getOriginTable(),
                        field.getOriginColumnName(),
                        !allColumns.getAliases().isEmpty() || field.isAliased());
                itemOutputFieldBuilder.add(newField);
                analysis.addSourceColumns(newField, analysis.getSourceColumns(field));

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

        private void analyzeWhere(Node node, Scope scope, Expression predicate)
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
                    verifyOrderByAggregations(distinctGroupingColumns, sourceScope, orderByScope.orElseThrow(), expression, metadata, analysis);
                }
            }
        }

        private RelationType analyzeView(Query query, QualifiedObjectName name, Optional<String> catalog, Optional<String> schema, Optional<Identity> owner, Table node)
        {
            try {
                // run view as view owner if set; otherwise, run as session user
                Identity identity;
                AccessControl viewAccessControl;
                if (owner.isPresent() && !owner.get().getUser().equals(session.getIdentity().getUser())) {
                    identity = Identity.from(owner.get())
                            .withGroups(groupProvider.getGroups(owner.get().getUser()))
                            .build();
                    viewAccessControl = new ViewAccessControl(accessControl, session.getIdentity());
                }
                else {
                    identity = session.getIdentity();
                    viewAccessControl = accessControl;
                }

                // TODO: record path in view definition (?) (check spec) and feed it into the session object we use to evaluate the query defined by the view
                Session viewSession = createViewSession(catalog, schema, identity, session.getPath());

                StatementAnalyzer analyzer = statementAnalyzerFactory
                        .withSpecializedAccessControl(viewAccessControl)
                        .createStatementAnalyzer(analysis, viewSession, warningCollector, CorrelationSupport.ALLOWED);
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

        private Optional<String> checkViewStaleness(List<ViewColumn> columns, Collection<Field> fields, QualifiedObjectName name, Node node)
        {
            if (columns.size() != fields.size()) {
                return Optional.of(format(
                        "stored view column count (%s) does not match column count derived from the view query analysis (%s)",
                        columns.size(),
                        fields.size()));
            }

            List<Field> fieldList = ImmutableList.copyOf(fields);
            for (int i = 0; i < columns.size(); i++) {
                ViewColumn column = columns.get(i);
                Type type = getViewColumnType(column, name, node);
                Field field = fieldList.get(i);
                if (field.getName().isEmpty()) {
                    return Optional.of(format(
                            "a column of type %s projected from query view at position %s has no name",
                            field.getType(),
                            i));
                }
                String fieldName = field.getName().orElseThrow();
                if (!column.getName().equalsIgnoreCase(fieldName)) {
                    return Optional.of(format(
                            "column [%s] of type %s projected from query view at position %s has a different name from column [%s] of type %s stored in view definition",
                            fieldName,
                            field.getType(),
                            i,
                            column.getName(),
                            type));
                }
                if (!typeCoercion.canCoerce(field.getType(), type)) {
                    return Optional.of(format(
                            "column [%s] of type %s projected from query view at position %s cannot be coerced to column [%s] of type %s stored in view definition",
                            fieldName,
                            field.getType(),
                            i,
                            column.getName(),
                            type));
                }
            }

            return Optional.empty();
        }

        private Type getViewColumnType(ViewColumn column, QualifiedObjectName name, Node node)
        {
            try {
                return plannerContext.getTypeManager().getType(column.getType());
            }
            catch (TypeNotFoundException e) {
                throw semanticException(INVALID_VIEW, node, e, "Unknown type '%s' for column '%s' in view: %s", column.getType(), column.getName(), name);
            }
        }

        private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope)
        {
            return ExpressionAnalyzer.analyzeExpression(
                    session,
                    plannerContext,
                    statementAnalyzerFactory,
                    accessControl,
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
                    plannerContext,
                    statementAnalyzerFactory,
                    accessControl,
                    scope,
                    analysis,
                    expression,
                    warningCollector,
                    correlationSupport);
        }

        private void analyzeRowFilter(String currentIdentity, Table table, QualifiedObjectName name, Scope scope, ViewExpression filter)
        {
            if (analysis.hasRowFilter(name, currentIdentity)) {
                throw new TrinoException(INVALID_ROW_FILTER, extractLocation(table), format("Row filter for '%s' is recursive", name), null);
            }

            Expression expression;
            try {
                expression = sqlParser.createExpression(filter.getExpression(), createParsingOptions(session));
            }
            catch (ParsingException e) {
                throw new TrinoException(INVALID_ROW_FILTER, extractLocation(table), format("Invalid row filter for '%s': %s", name, e.getErrorMessage()), e);
            }

            analysis.registerTableForRowFiltering(name, currentIdentity);

            verifyNoAggregateWindowOrGroupingFunctions(metadata, expression, format("Row filter for '%s'", name));

            ExpressionAnalysis expressionAnalysis;
            try {
                expressionAnalysis = ExpressionAnalyzer.analyzeExpression(
                        createViewSession(filter.getCatalog(), filter.getSchema(), Identity.forUser(filter.getIdentity()).build(), session.getPath()), // TODO: path should be included in row filter
                        plannerContext,
                        statementAnalyzerFactory,
                        accessControl,
                        scope,
                        analysis,
                        expression,
                        warningCollector,
                        correlationSupport);
            }
            catch (TrinoException e) {
                throw new TrinoException(e::getErrorCode, extractLocation(table), format("Invalid row filter for '%s': %s", name, e.getRawMessage()), e);
            }
            finally {
                analysis.unregisterTableForRowFiltering(name, currentIdentity);
            }

            analysis.recordSubqueries(expression, expressionAnalysis);

            Type actualType = expressionAnalysis.getType(expression);
            if (!actualType.equals(BOOLEAN)) {
                TypeCoercion coercion = new TypeCoercion(plannerContext.getTypeManager()::getType);

                if (!coercion.canCoerce(actualType, BOOLEAN)) {
                    throw new TrinoException(TYPE_MISMATCH, extractLocation(table), format("Expected row filter for '%s' to be of type BOOLEAN, but was %s", name, actualType), null);
                }

                analysis.addCoercion(expression, BOOLEAN, coercion.isTypeOnlyCoercion(actualType, BOOLEAN));
            }

            analysis.addRowFilter(table, expression);
        }

        private void analyzeColumnMask(String currentIdentity, Table table, QualifiedObjectName tableName, Field field, Scope scope, ViewExpression mask)
        {
            String column = field.getName().orElseThrow();
            if (analysis.hasColumnMask(tableName, column, currentIdentity)) {
                throw new TrinoException(INVALID_ROW_FILTER, extractLocation(table), format("Column mask for '%s.%s' is recursive", tableName, column), null);
            }

            Expression expression;
            try {
                expression = sqlParser.createExpression(mask.getExpression(), createParsingOptions(session));
            }
            catch (ParsingException e) {
                throw new TrinoException(INVALID_ROW_FILTER, extractLocation(table), format("Invalid column mask for '%s.%s': %s", tableName, column, e.getErrorMessage()), e);
            }

            ExpressionAnalysis expressionAnalysis;
            analysis.registerTableForColumnMasking(tableName, column, currentIdentity);

            verifyNoAggregateWindowOrGroupingFunctions(metadata, expression, format("Column mask for '%s.%s'", table.getName(), column));

            try {
                expressionAnalysis = ExpressionAnalyzer.analyzeExpression(
                        createViewSession(mask.getCatalog(), mask.getSchema(), Identity.forUser(mask.getIdentity()).build(), session.getPath()), // TODO: path should be included in row filter
                        plannerContext,
                        statementAnalyzerFactory,
                        accessControl,
                        scope,
                        analysis,
                        expression,
                        warningCollector,
                        correlationSupport);
            }
            catch (TrinoException e) {
                throw new TrinoException(e::getErrorCode, extractLocation(table), format("Invalid column mask for '%s.%s': %s", tableName, column, e.getRawMessage()), e);
            }
            finally {
                analysis.unregisterTableForColumnMasking(tableName, column, currentIdentity);
            }

            analysis.recordSubqueries(expression, expressionAnalysis);

            Type expectedType = field.getType();
            Type actualType = expressionAnalysis.getType(expression);
            if (!actualType.equals(expectedType)) {
                TypeCoercion coercion = new TypeCoercion(plannerContext.getTypeManager()::getType);

                if (!coercion.canCoerce(actualType, field.getType())) {
                    throw new TrinoException(TYPE_MISMATCH, extractLocation(table), format("Expected column mask for '%s.%s' to be of type %s, but was %s", tableName, column, field.getType(), actualType), null);
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
                    // cannot nest pattern recognition within recursive query
                    preOrder(withQuery.getQuery())
                            .filter(child -> child instanceof PatternRecognitionRelation || child instanceof RowPattern)
                            .findFirst()
                            .ifPresent(nested -> {
                                throw semanticException(NESTED_ROW_PATTERN_RECOGNITION, nested, "nested row pattern recognition in recursive query");
                            });
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
            if (fromReferences.isEmpty()) {
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
                    .filter(Join.class::isInstance)
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
                    .filter(Except.class::isInstance)
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

            Streams.forEachPair(
                    oldDescriptor.getAllFields().stream(),
                    newDescriptor.getAllFields().stream(),
                    (newField, field) -> analysis.addSourceColumns(newField, analysis.getSourceColumns(field)));
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
                if (!DeterminismEvaluator.isDeterministic(expression, this::getResolvedFunction)) {
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
                        aliases.add(canonicalizationAwareKey(((DereferenceExpression) column.getExpression()).getField().orElseThrow()));
                    }
                }
                else if (item instanceof AllColumns) {
                    AllColumns allColumns = (AllColumns) item;

                    List<Field> fields = analysis.getSelectAllResultFields(allColumns);
                    checkNotNull(fields, "output fields is null for select item %s", item);
                    for (int i = 0; i < fields.size(); i++) {
                        Field field = fields.get(i);

                        if (!allColumns.getAliases().isEmpty()) {
                            aliases.add(canonicalizationAwareKey(allColumns.getAliases().get(i)));
                        }
                        else if (field.getName().isPresent()) {
                            aliases.add(canonicalizationAwareKey(new Identifier(field.getName().get())));
                        }
                    }
                }
            }

            return aliases.build();
        }

        private ResolvedFunction getResolvedFunction(FunctionCall functionCall)
        {
            ResolvedFunction resolvedFunction = analysis.getResolvedFunction(functionCall);
            verify(resolvedFunction != null, "function has not been analyzed yet: %s", functionCall);
            return resolvedFunction;
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

                ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(
                        session,
                        plannerContext,
                        statementAnalyzerFactory,
                        accessControl,
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
                    value = evaluateConstantExpression(
                            providedValue,
                            BIGINT,
                            plannerContext,
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

        private OutputColumn createOutputColumn(Field field)
        {
            return new OutputColumn(new Column(field.getName().orElseThrow(), field.getType().toString()), analysis.getSourceColumns(field));
        }

        /**
         * Helper function that analyzes any versioning and returns the appropriate table handle.
         * If no for clause exists, this is just a wrapper around getRedirectionAwareTableHandle in MetadataManager.
         */
        private RedirectionAwareTableHandle getTableHandle(Table table, QualifiedObjectName name, Optional<Scope> scope)
        {
            if (table.getQueryPeriod().isPresent()) {
                Optional<TableVersion> startVersion = extractTableVersion(table, name, table.getQueryPeriod().get().getStart(), scope);
                Optional<TableVersion> endVersion = extractTableVersion(table, name, table.getQueryPeriod().get().getEnd(), scope);
                return metadata.getRedirectionAwareTableHandle(session, name, startVersion, endVersion);
            }
            return metadata.getRedirectionAwareTableHandle(session, name);
        }

        /**
         * Analyzes the version pointer in a query period and extracts an evaluated version value
         */
        private Optional<TableVersion> extractTableVersion(Table table, QualifiedObjectName tableName, Optional<Expression> version, Optional<Scope> scope)
        {
            Optional<TableVersion> tableVersion = Optional.empty();
            if (version.isEmpty()) {
                return tableVersion;
            }
            ExpressionAnalysis expressionAnalysis = analyzeExpression(version.get(), scope.get());
            analysis.recordSubqueries(table, expressionAnalysis);

            // Once the range value is analyzed, we can evaluate it
            Type versionType = expressionAnalysis.getType(version.get());
            PointerType pointerType = toPointerType(table.getQueryPeriod().get().getRangeType());
            Object evaluatedVersion = evaluateConstantExpression(version.get(), versionType, plannerContext, session, accessControl, ImmutableMap.of());
            TableVersion extractedVersion = new TableVersion(pointerType, versionType, evaluatedVersion);

            // Before checking if the connector supports the version type, verify that version is a valid time-based type
            if (extractedVersion.getPointerType() == PointerType.TEMPORAL) {
                if (!isValidTemporalType(extractedVersion.getObjectType())) {
                    throw semanticException(
                            TYPE_MISMATCH,
                            table.getQueryPeriod().get(),
                            format(
                                    "Type %s invalid. Temporal pointers must be of type Timestamp, Timestamp with Time Zone, or Date.",
                                    extractedVersion.getObjectType().getDisplayName()));
                }
            }

            if (!metadata.isValidTableVersion(session, tableName, extractedVersion)) {
                throw semanticException(
                        TYPE_MISMATCH,
                        table.getQueryPeriod().get(),
                        format("Type %s not supported by this connector.", extractedVersion.getObjectType().getDisplayName()));
            }

            return Optional.of(extractedVersion);
        }

        private boolean isValidTemporalType(Type type)
        {
            return (type instanceof TimestampWithTimeZoneType ||
                    type instanceof TimestampType ||
                    type instanceof DateType);
        }

        private PointerType toPointerType(QueryPeriod.RangeType type)
        {
            switch (type) {
                case TIMESTAMP:
                    return PointerType.TEMPORAL;
                case VERSION:
                    return PointerType.TARGET_ID;
            }
            throw new UnsupportedOperationException("Unsupported range type: " + type);
        }
    }

    private Session createViewSession(Optional<String> catalog, Optional<String> schema, Identity identity, SqlPath path)
    {
        return Session.builder(sessionPropertyManager)
                .setQueryId(session.getQueryId())
                .setTransactionId(session.getTransactionId().orElse(null))
                .setIdentity(identity)
                .setSource(session.getSource().orElse(null))
                .setCatalog(catalog)
                .setSchema(schema)
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
