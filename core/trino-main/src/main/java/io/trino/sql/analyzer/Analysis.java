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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Streams;
import io.trino.metadata.NewTableLayout;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.eventlistener.ColumnDetail;
import io.trino.spi.eventlistener.ColumnInfo;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.ExpressionAnalyzer.LabelPrefixedReference;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.RangeQuantifier;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowOperation;
import io.trino.transaction.TransactionId;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.analyzer.QueryType.DESCRIBE;
import static io.trino.sql.analyzer.QueryType.EXPLAIN;
import static java.lang.Boolean.FALSE;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class Analysis
{
    @Nullable
    private final Statement root;
    private final Map<NodeRef<Parameter>, Expression> parameters;
    private String updateType;
    private Optional<UpdateTarget> target = Optional.empty();
    private boolean skipMaterializedViewRefresh;

    private final Map<NodeRef<Table>, Query> namedQueries = new LinkedHashMap<>();

    // map expandable query to the node being the inner recursive reference
    private final Map<NodeRef<Query>, Node> expandableNamedQueries = new LinkedHashMap<>();

    // map inner recursive reference in the expandable query to the recursion base scope
    private final Map<NodeRef<Node>, Scope> expandableBaseScopes = new LinkedHashMap<>();

    // Synthetic scope when a query does not have a FROM clause
    // We need to track this separately because there's no node we can attach it to.
    private final Map<NodeRef<QuerySpecification>, Scope> implicitFromScopes = new LinkedHashMap<>();

    private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, ResolvedField> columnReferences = new LinkedHashMap<>();

    // a map of users to the columns per table that they access
    private final Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> tableColumnReferences = new LinkedHashMap<>();

    // Record fields prefixed with labels in row pattern recognition context
    private final Map<NodeRef<DereferenceExpression>, LabelPrefixedReference> labelDereferences = new LinkedHashMap<>();

    private final Set<NodeRef<FunctionCall>> patternRecognitionFunctions = new LinkedHashSet<>();

    private final Map<NodeRef<RangeQuantifier>, Range> ranges = new LinkedHashMap<>();

    private final Map<NodeRef<RowPattern>, Set<String>> undefinedLabels = new LinkedHashMap<>();

    private final Map<NodeRef<WindowOperation>, MeasureDefinition> measureDefinitions = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, List<FunctionCall>> aggregates = new LinkedHashMap<>();
    private final Map<NodeRef<OrderBy>, List<Expression>> orderByAggregates = new LinkedHashMap<>();
    private final Map<NodeRef<QuerySpecification>, GroupingSetAnalysis> groupingSets = new LinkedHashMap<>();

    private final Map<NodeRef<Node>, Expression> where = new LinkedHashMap<>();
    private final Map<NodeRef<QuerySpecification>, Expression> having = new LinkedHashMap<>();
    private final Map<NodeRef<Node>, List<Expression>> orderByExpressions = new LinkedHashMap<>();
    private final Set<NodeRef<OrderBy>> redundantOrderBy = new HashSet<>();
    private final Map<NodeRef<Node>, List<SelectExpression>> selectExpressions = new LinkedHashMap<>();

    // Store resolved window specifications defined in WINDOW clause
    private final Map<NodeRef<QuerySpecification>, Map<CanonicalizationAware<Identifier>, ResolvedWindow>> windowDefinitions = new LinkedHashMap<>();

    // Store resolved window specifications for window functions and row pattern measures
    private final Map<NodeRef<Node>, ResolvedWindow> windows = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, List<FunctionCall>> windowFunctions = new LinkedHashMap<>();
    private final Map<NodeRef<OrderBy>, List<FunctionCall>> orderByWindowFunctions = new LinkedHashMap<>();
    private final Map<NodeRef<QuerySpecification>, List<WindowOperation>> windowMeasures = new LinkedHashMap<>();
    private final Map<NodeRef<OrderBy>, List<WindowOperation>> orderByWindowMeasures = new LinkedHashMap<>();
    private final Map<NodeRef<Offset>, Long> offset = new LinkedHashMap<>();
    private final Map<NodeRef<Node>, OptionalLong> limit = new LinkedHashMap<>();
    private final Map<NodeRef<AllColumns>, List<Field>> selectAllResultFields = new LinkedHashMap<>();

    private final Map<NodeRef<Join>, Expression> joins = new LinkedHashMap<>();
    private final Map<NodeRef<Join>, JoinUsingAnalysis> joinUsing = new LinkedHashMap<>();
    private final Map<NodeRef<Node>, SubqueryAnalysis> subqueries = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, PredicateCoercions> predicateCoercions = new LinkedHashMap<>();

    private final Map<NodeRef<Table>, TableEntry> tables = new LinkedHashMap<>();

    private final Map<NodeRef<Expression>, Type> types = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, Type> coercions = new LinkedHashMap<>();
    private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();

    private final Map<NodeRef<Expression>, Type> sortKeyCoercionsForFrameBoundCalculation = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, Type> sortKeyCoercionsForFrameBoundComparison = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, ResolvedFunction> frameBoundCalculations = new LinkedHashMap<>();
    private final Map<NodeRef<Relation>, List<Type>> relationCoercions = new LinkedHashMap<>();
    private final Map<NodeRef<FunctionCall>, RoutineEntry> resolvedFunctions = new LinkedHashMap<>();
    private final Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences = new LinkedHashMap<>();

    private final Map<Field, ColumnHandle> columns = new LinkedHashMap<>();

    private final Map<NodeRef<SampledRelation>, Double> sampleRatios = new LinkedHashMap<>();

    private final Map<NodeRef<QuerySpecification>, List<GroupingOperation>> groupingOperations = new LinkedHashMap<>();

    private final Multiset<RowFilterScopeEntry> rowFilterScopes = HashMultiset.create();
    private final Map<NodeRef<Table>, List<Expression>> rowFilters = new LinkedHashMap<>();

    private final Multiset<ColumnMaskScopeEntry> columnMaskScopes = HashMultiset.create();
    private final Map<NodeRef<Table>, Map<String, List<Expression>>> columnMasks = new LinkedHashMap<>();

    private final Map<NodeRef<Unnest>, UnnestAnalysis> unnestAnalysis = new LinkedHashMap<>();
    private Optional<Create> create = Optional.empty();
    private Optional<Insert> insert = Optional.empty();
    private Optional<RefreshMaterializedViewAnalysis> refreshMaterializedView = Optional.empty();
    private Optional<QualifiedObjectName> delegatedRefreshMaterializedView = Optional.empty();
    private Optional<TableHandle> analyzeTarget = Optional.empty();
    private Optional<List<ColumnMetadata>> updatedColumns = Optional.empty();

    private final QueryType queryType;

    // for recursive view detection
    private final Deque<Table> tablesForView = new ArrayDeque<>();

    // row id field for update/delete queries
    private final Map<NodeRef<Table>, FieldReference> rowIdField = new LinkedHashMap<>();
    private final Multimap<Field, SourceColumn> originColumnDetails = ArrayListMultimap.create();
    private final Multimap<NodeRef<Expression>, Field> fieldLineage = ArrayListMultimap.create();

    public Analysis(@Nullable Statement root, Map<NodeRef<Parameter>, Expression> parameters, QueryType queryType)
    {
        this.root = root;
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
        this.queryType = requireNonNull(queryType, "queryType is null");
    }

    public Statement getStatement()
    {
        return root;
    }

    public String getUpdateType()
    {
        return updateType;
    }

    public Optional<Output> getTarget()
    {
        return target.map(target -> {
            QualifiedObjectName name = target.getName();
            return new Output(name.getCatalogName(), name.getSchemaName(), name.getObjectName(), target.getColumns());
        });
    }

    public void setUpdateType(String updateType)
    {
        if (queryType != EXPLAIN) {
            this.updateType = updateType;
        }
    }

    public void setUpdateTarget(QualifiedObjectName targetName, Optional<Table> targetTable, Optional<List<OutputColumn>> targetColumns)
    {
        this.target = Optional.of(new UpdateTarget(targetName, targetTable, targetColumns));
    }

    public boolean isUpdateTarget(Table table)
    {
        requireNonNull(table, "table is null");
        return target
                .flatMap(UpdateTarget::getTable)
                .map(tableReference -> tableReference == table) // intentional comparison by reference
                .orElse(FALSE);
    }

    public boolean isSkipMaterializedViewRefresh()
    {
        return skipMaterializedViewRefresh;
    }

    public void setSkipMaterializedViewRefresh(boolean skipMaterializedViewRefresh)
    {
        this.skipMaterializedViewRefresh = skipMaterializedViewRefresh;
    }

    public void setAggregates(QuerySpecification node, List<FunctionCall> aggregates)
    {
        this.aggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
    }

    public List<FunctionCall> getAggregates(QuerySpecification query)
    {
        return aggregates.get(NodeRef.of(query));
    }

    public void setOrderByAggregates(OrderBy node, List<Expression> aggregates)
    {
        this.orderByAggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
    }

    public List<Expression> getOrderByAggregates(OrderBy node)
    {
        return orderByAggregates.get(NodeRef.of(node));
    }

    public Map<NodeRef<Expression>, Type> getTypes()
    {
        return unmodifiableMap(types);
    }

    public Type getType(Expression expression)
    {
        Type type = types.get(NodeRef.of(expression));
        checkArgument(type != null, "Expression not analyzed: %s", expression);
        return type;
    }

    public List<Type> getRelationCoercion(Relation relation)
    {
        return relationCoercions.get(NodeRef.of(relation));
    }

    public void addRelationCoercion(Relation relation, Type[] types)
    {
        relationCoercions.put(NodeRef.of(relation), ImmutableList.copyOf(types));
    }

    public Map<NodeRef<Expression>, Type> getCoercions()
    {
        return unmodifiableMap(coercions);
    }

    public Set<NodeRef<Expression>> getTypeOnlyCoercions()
    {
        return unmodifiableSet(typeOnlyCoercions);
    }

    public Type getCoercion(Expression expression)
    {
        return coercions.get(NodeRef.of(expression));
    }

    public void addLambdaArgumentReferences(Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences)
    {
        this.lambdaArgumentReferences.putAll(lambdaArgumentReferences);
    }

    public LambdaArgumentDeclaration getLambdaArgumentReference(Identifier identifier)
    {
        return lambdaArgumentReferences.get(NodeRef.of(identifier));
    }

    public Map<NodeRef<Identifier>, LambdaArgumentDeclaration> getLambdaArgumentReferences()
    {
        return unmodifiableMap(lambdaArgumentReferences);
    }

    public void setGroupingSets(QuerySpecification node, GroupingSetAnalysis groupingSets)
    {
        this.groupingSets.put(NodeRef.of(node), groupingSets);
    }

    public boolean isAggregation(QuerySpecification node)
    {
        return groupingSets.containsKey(NodeRef.of(node));
    }

    public boolean isTypeOnlyCoercion(Expression expression)
    {
        return typeOnlyCoercions.contains(NodeRef.of(expression));
    }

    public GroupingSetAnalysis getGroupingSets(QuerySpecification node)
    {
        return groupingSets.get(NodeRef.of(node));
    }

    public void setWhere(Node node, Expression expression)
    {
        where.put(NodeRef.of(node), expression);
    }

    public Expression getWhere(QuerySpecification node)
    {
        return where.get(NodeRef.<Node>of(node));
    }

    public void setOrderByExpressions(Node node, List<Expression> items)
    {
        orderByExpressions.put(NodeRef.of(node), ImmutableList.copyOf(items));
    }

    public List<Expression> getOrderByExpressions(Node node)
    {
        return orderByExpressions.get(NodeRef.of(node));
    }

    public void setOffset(Offset node, long rowCount)
    {
        offset.put(NodeRef.of(node), rowCount);
    }

    public long getOffset(Offset node)
    {
        checkState(offset.containsKey(NodeRef.of(node)), "missing OFFSET value for node %s", node);
        return offset.get(NodeRef.of(node));
    }

    public void setLimit(Node node, OptionalLong rowCount)
    {
        limit.put(NodeRef.of(node), rowCount);
    }

    public void setLimit(Node node, long rowCount)
    {
        limit.put(NodeRef.of(node), OptionalLong.of(rowCount));
    }

    public OptionalLong getLimit(Node node)
    {
        checkState(limit.containsKey(NodeRef.of(node)), "missing LIMIT value for node %s", node);
        return limit.get(NodeRef.of(node));
    }

    public void setSelectAllResultFields(AllColumns node, List<Field> expressions)
    {
        selectAllResultFields.put(NodeRef.of(node), ImmutableList.copyOf(expressions));
    }

    public List<Field> getSelectAllResultFields(AllColumns node)
    {
        return selectAllResultFields.get(NodeRef.of(node));
    }

    public void setSelectExpressions(Node node, List<SelectExpression> expressions)
    {
        selectExpressions.put(NodeRef.of(node), ImmutableList.copyOf(expressions));
    }

    public List<SelectExpression> getSelectExpressions(Node node)
    {
        return selectExpressions.get(NodeRef.of(node));
    }

    public void setHaving(QuerySpecification node, Expression expression)
    {
        having.put(NodeRef.of(node), expression);
    }

    public void setJoinCriteria(Join node, Expression criteria)
    {
        joins.put(NodeRef.of(node), criteria);
    }

    public Expression getJoinCriteria(Join join)
    {
        return joins.get(NodeRef.of(join));
    }

    public void recordSubqueries(Node node, ExpressionAnalysis expressionAnalysis)
    {
        SubqueryAnalysis subqueries = this.subqueries.computeIfAbsent(NodeRef.of(node), key -> new SubqueryAnalysis());
        subqueries.addInPredicates(dereference(expressionAnalysis.getSubqueryInPredicates()));
        subqueries.addSubqueries(dereference(expressionAnalysis.getSubqueries()));
        subqueries.addExistsSubqueries(dereference(expressionAnalysis.getExistsSubqueries()));
        subqueries.addQuantifiedComparisons(dereference(expressionAnalysis.getQuantifiedComparisons()));
    }

    private <T extends Node> List<T> dereference(Collection<NodeRef<T>> nodeRefs)
    {
        return nodeRefs.stream()
                .map(NodeRef::getNode)
                .collect(toImmutableList());
    }

    public SubqueryAnalysis getSubqueries(Node node)
    {
        return subqueries.computeIfAbsent(NodeRef.of(node), key -> new SubqueryAnalysis());
    }

    public void addWindowDefinition(QuerySpecification query, CanonicalizationAware<Identifier> name, ResolvedWindow window)
    {
        windowDefinitions.computeIfAbsent(NodeRef.of(query), key -> new LinkedHashMap<>())
                .put(name, window);
    }

    public ResolvedWindow getWindowDefinition(QuerySpecification query, CanonicalizationAware<Identifier> name)
    {
        Map<CanonicalizationAware<Identifier>, ResolvedWindow> windows = windowDefinitions.get(NodeRef.of(query));
        if (windows != null) {
            return windows.get(name);
        }

        return null;
    }

    public void setWindow(Node node, ResolvedWindow window)
    {
        windows.put(NodeRef.of(node), window);
    }

    public ResolvedWindow getWindow(Node node)
    {
        return windows.get(NodeRef.of(node));
    }

    public void setWindowFunctions(QuerySpecification node, List<FunctionCall> functions)
    {
        windowFunctions.put(NodeRef.of(node), ImmutableList.copyOf(functions));
    }

    public List<FunctionCall> getWindowFunctions(QuerySpecification query)
    {
        return windowFunctions.get(NodeRef.of(query));
    }

    public void setOrderByWindowFunctions(OrderBy node, List<FunctionCall> functions)
    {
        orderByWindowFunctions.put(NodeRef.of(node), ImmutableList.copyOf(functions));
    }

    public List<FunctionCall> getOrderByWindowFunctions(OrderBy query)
    {
        return orderByWindowFunctions.get(NodeRef.of(query));
    }

    public void setWindowMeasures(QuerySpecification node, List<WindowOperation> measures)
    {
        windowMeasures.put(NodeRef.of(node), ImmutableList.copyOf(measures));
    }

    public List<WindowOperation> getWindowMeasures(QuerySpecification node)
    {
        return windowMeasures.get(NodeRef.of(node));
    }

    public void setOrderByWindowMeasures(OrderBy node, List<WindowOperation> measures)
    {
        orderByWindowMeasures.put(NodeRef.of(node), ImmutableList.copyOf(measures));
    }

    public List<WindowOperation> getOrderByWindowMeasures(OrderBy node)
    {
        return orderByWindowMeasures.get(NodeRef.of(node));
    }

    public void addColumnReferences(Map<NodeRef<Expression>, ResolvedField> columnReferences)
    {
        this.columnReferences.putAll(columnReferences);
    }

    public Scope getScope(Node node)
    {
        return tryGetScope(node).orElseThrow(() -> new IllegalArgumentException(format("Analysis does not contain information for node: %s", node)));
    }

    public Optional<Scope> tryGetScope(Node node)
    {
        NodeRef<Node> key = NodeRef.of(node);
        if (scopes.containsKey(key)) {
            return Optional.of(scopes.get(key));
        }

        return Optional.empty();
    }

    public Scope getRootScope()
    {
        return getScope(root);
    }

    public void setScope(Node node, Scope scope)
    {
        scopes.put(NodeRef.of(node), scope);
    }

    public RelationType getOutputDescriptor()
    {
        return getOutputDescriptor(root);
    }

    public RelationType getOutputDescriptor(Node node)
    {
        return getScope(node).getRelationType();
    }

    public TableHandle getTableHandle(Table table)
    {
        return tables.get(NodeRef.of(table))
                .getHandle()
                .orElseThrow(() -> new IllegalArgumentException(format("%s is not a table reference", table)));
    }

    public Collection<TableHandle> getTables()
    {
        return tables.values().stream()
                .map(TableEntry::getHandle)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
    }

    public void registerTable(
            Table table,
            Optional<TableHandle> handle,
            QualifiedObjectName name,
            String authorization,
            Scope accessControlScope)
    {
        tables.put(
                NodeRef.of(table),
                new TableEntry(
                        handle,
                        name,
                        authorization,
                        accessControlScope,
                        tablesForView.isEmpty() &&
                                rowFilterScopes.isEmpty() &&
                                columnMaskScopes.isEmpty()));
    }

    public ResolvedFunction getResolvedFunction(FunctionCall function)
    {
        return resolvedFunctions.get(NodeRef.of(function)).getFunction();
    }

    public void addResolvedFunction(FunctionCall node, ResolvedFunction function, String authorization)
    {
        resolvedFunctions.put(NodeRef.of(node), new RoutineEntry(function, authorization));
    }

    public Set<NodeRef<Expression>> getColumnReferences()
    {
        return unmodifiableSet(columnReferences.keySet());
    }

    public Map<NodeRef<Expression>, ResolvedField> getColumnReferenceFields()
    {
        return unmodifiableMap(columnReferences);
    }

    public ResolvedField getResolvedField(Expression expression)
    {
        checkArgument(isColumnReference(expression), "Expression is not a column reference: %s", expression);
        return columnReferences.get(NodeRef.of(expression));
    }

    public boolean isColumnReference(Expression expression)
    {
        requireNonNull(expression, "expression is null");
        return columnReferences.containsKey(NodeRef.of(expression));
    }

    public void addTypes(Map<NodeRef<Expression>, Type> types)
    {
        this.types.putAll(types);
    }

    public void addCoercion(Expression expression, Type type, boolean isTypeOnlyCoercion)
    {
        this.coercions.put(NodeRef.of(expression), type);
        if (isTypeOnlyCoercion) {
            this.typeOnlyCoercions.add(NodeRef.of(expression));
        }
    }

    public void addCoercions(
            Map<NodeRef<Expression>, Type> coercions,
            Set<NodeRef<Expression>> typeOnlyCoercions,
            Map<NodeRef<Expression>, Type> sortKeyCoercionsForFrameBoundCalculation,
            Map<NodeRef<Expression>, Type> sortKeyCoercionsForFrameBoundComparison)
    {
        this.coercions.putAll(coercions);
        this.typeOnlyCoercions.addAll(typeOnlyCoercions);
        this.sortKeyCoercionsForFrameBoundCalculation.putAll(sortKeyCoercionsForFrameBoundCalculation);
        this.sortKeyCoercionsForFrameBoundComparison.putAll(sortKeyCoercionsForFrameBoundComparison);
    }

    public Type getSortKeyCoercionForFrameBoundCalculation(Expression frameOffset)
    {
        return sortKeyCoercionsForFrameBoundCalculation.get(NodeRef.of(frameOffset));
    }

    public Type getSortKeyCoercionForFrameBoundComparison(Expression frameOffset)
    {
        return sortKeyCoercionsForFrameBoundComparison.get(NodeRef.of(frameOffset));
    }

    public void addFrameBoundCalculations(Map<NodeRef<Expression>, ResolvedFunction> frameBoundCalculations)
    {
        this.frameBoundCalculations.putAll(frameBoundCalculations);
    }

    public ResolvedFunction getFrameBoundCalculation(Expression frameOffset)
    {
        return frameBoundCalculations.get(NodeRef.of(frameOffset));
    }

    public Expression getHaving(QuerySpecification query)
    {
        return having.get(NodeRef.of(query));
    }

    public void setColumn(Field field, ColumnHandle handle)
    {
        columns.put(field, handle);
    }

    public ColumnHandle getColumn(Field field)
    {
        return columns.get(field);
    }

    public Optional<TableHandle> getAnalyzeTarget()
    {
        return analyzeTarget;
    }

    public void setAnalyzeTarget(TableHandle analyzeTarget)
    {
        this.analyzeTarget = Optional.of(analyzeTarget);
    }

    public void setCreate(Create create)
    {
        this.create = Optional.of(create);
    }

    public Optional<Create> getCreate()
    {
        return create;
    }

    public void setInsert(Insert insert)
    {
        this.insert = Optional.of(insert);
    }

    public Optional<Insert> getInsert()
    {
        return insert;
    }

    public void setUpdatedColumns(List<ColumnMetadata> updatedColumns)
    {
        this.updatedColumns = Optional.of(updatedColumns);
    }

    public Optional<List<ColumnMetadata>> getUpdatedColumns()
    {
        return updatedColumns;
    }

    public void setRefreshMaterializedView(RefreshMaterializedViewAnalysis refreshMaterializedView)
    {
        this.refreshMaterializedView = Optional.of(refreshMaterializedView);
    }

    public Optional<RefreshMaterializedViewAnalysis> getRefreshMaterializedView()
    {
        return refreshMaterializedView;
    }

    public void setDelegatedRefreshMaterializedView(QualifiedObjectName viewName)
    {
        this.delegatedRefreshMaterializedView = Optional.of(viewName);
    }

    public Optional<QualifiedObjectName> getDelegatedRefreshMaterializedView()
    {
        return delegatedRefreshMaterializedView;
    }

    public Query getNamedQuery(Table table)
    {
        return namedQueries.get(NodeRef.of(table));
    }

    public void registerNamedQuery(Table tableReference, Query query)
    {
        requireNonNull(tableReference, "tableReference is null");
        requireNonNull(query, "query is null");

        namedQueries.put(NodeRef.of(tableReference), query);
    }

    public void registerExpandableQuery(Query query, Node recursiveReference)
    {
        requireNonNull(query, "query is null");
        requireNonNull(recursiveReference, "recursiveReference is null");

        expandableNamedQueries.put(NodeRef.of(query), recursiveReference);
    }

    public boolean isExpandableQuery(Query query)
    {
        return expandableNamedQueries.containsKey(NodeRef.of(query));
    }

    public Node getRecursiveReference(Query query)
    {
        checkArgument(isExpandableQuery(query), "query is not registered as expandable");
        return expandableNamedQueries.get(NodeRef.of(query));
    }

    public void setExpandableBaseScope(Node node, Scope scope)
    {
        expandableBaseScopes.put(NodeRef.of(node), scope);
    }

    public Optional<Scope> getExpandableBaseScope(Node node)
    {
        return Optional.ofNullable(expandableBaseScopes.get(NodeRef.of(node)));
    }

    public void registerTableForView(Table tableReference)
    {
        tablesForView.push(requireNonNull(tableReference, "tableReference is null"));
    }

    public void unregisterTableForView()
    {
        tablesForView.pop();
    }

    public boolean hasTableInView(Table tableReference)
    {
        return tablesForView.contains(tableReference);
    }

    public void setSampleRatio(SampledRelation relation, double ratio)
    {
        sampleRatios.put(NodeRef.of(relation), ratio);
    }

    public double getSampleRatio(SampledRelation relation)
    {
        NodeRef<SampledRelation> key = NodeRef.of(relation);
        checkState(sampleRatios.containsKey(key), "Sample ratio missing for %s. Broken analysis?", relation);
        return sampleRatios.get(key);
    }

    public void setGroupingOperations(QuerySpecification querySpecification, List<GroupingOperation> groupingOperations)
    {
        this.groupingOperations.put(NodeRef.of(querySpecification), ImmutableList.copyOf(groupingOperations));
    }

    public List<GroupingOperation> getGroupingOperations(QuerySpecification querySpecification)
    {
        return Optional.ofNullable(groupingOperations.get(NodeRef.of(querySpecification)))
                .orElse(emptyList());
    }

    public Map<NodeRef<Parameter>, Expression> getParameters()
    {
        return parameters;
    }

    public QueryType getQueryType()
    {
        return queryType;
    }

    public boolean isDescribe()
    {
        return queryType == DESCRIBE;
    }

    public void setJoinUsing(Join node, JoinUsingAnalysis analysis)
    {
        joinUsing.put(NodeRef.of(node), analysis);
    }

    public JoinUsingAnalysis getJoinUsing(Join node)
    {
        return joinUsing.get(NodeRef.of(node));
    }

    public void setUnnest(Unnest node, UnnestAnalysis analysis)
    {
        unnestAnalysis.put(NodeRef.of(node), analysis);
    }

    public UnnestAnalysis getUnnest(Unnest node)
    {
        return unnestAnalysis.get(NodeRef.of(node));
    }

    public void addTableColumnReferences(AccessControl accessControl, Identity identity, Multimap<QualifiedObjectName, String> tableColumnMap)
    {
        AccessControlInfo accessControlInfo = new AccessControlInfo(accessControl, identity);
        Map<QualifiedObjectName, Set<String>> references = tableColumnReferences.computeIfAbsent(accessControlInfo, k -> new LinkedHashMap<>());
        tableColumnMap.asMap()
                .forEach((key, value) -> references.computeIfAbsent(key, k -> new HashSet<>()).addAll(value));
    }

    public void addEmptyColumnReferencesForTable(AccessControl accessControl, Identity identity, QualifiedObjectName table)
    {
        AccessControlInfo accessControlInfo = new AccessControlInfo(accessControl, identity);
        tableColumnReferences.computeIfAbsent(accessControlInfo, k -> new LinkedHashMap<>()).computeIfAbsent(table, k -> new HashSet<>());
    }

    public void addLabelDereferences(Map<NodeRef<DereferenceExpression>, LabelPrefixedReference> dereferences)
    {
        labelDereferences.putAll(dereferences);
    }

    public LabelPrefixedReference getLabelDereference(DereferenceExpression expression)
    {
        return labelDereferences.get(NodeRef.of(expression));
    }

    public void addPatternRecognitionFunctions(Set<NodeRef<FunctionCall>> functions)
    {
        patternRecognitionFunctions.addAll(functions);
    }

    public boolean isPatternRecognitionFunction(FunctionCall functionCall)
    {
        return patternRecognitionFunctions.contains(NodeRef.of(functionCall));
    }

    public void setRanges(Map<NodeRef<RangeQuantifier>, Range> quantifierRanges)
    {
        ranges.putAll(quantifierRanges);
    }

    public Range getRange(RangeQuantifier quantifier)
    {
        Range range = ranges.get(NodeRef.of(quantifier));
        checkNotNull(range, "missing range for quantifier ", quantifier);
        return range;
    }

    public void setUndefinedLabels(RowPattern pattern, Set<String> labels)
    {
        undefinedLabels.put(NodeRef.of(pattern), labels);
    }

    public void setUndefinedLabels(Map<NodeRef<RowPattern>, Set<String>> labels)
    {
        undefinedLabels.putAll(labels);
    }

    public Set<String> getUndefinedLabels(RowPattern pattern)
    {
        Set<String> labels = undefinedLabels.get(NodeRef.of(pattern));
        checkNotNull(labels, "missing undefined labels for ", pattern);
        return labels;
    }

    public void setMeasureDefinitions(Map<NodeRef<WindowOperation>, MeasureDefinition> definitions)
    {
        measureDefinitions.putAll(definitions);
    }

    public MeasureDefinition getMeasureDefinition(WindowOperation measure)
    {
        return measureDefinitions.get(NodeRef.of(measure));
    }

    public Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> getTableColumnReferences()
    {
        return tableColumnReferences;
    }

    public void markRedundantOrderBy(OrderBy orderBy)
    {
        redundantOrderBy.add(NodeRef.of(orderBy));
    }

    public boolean isOrderByRedundant(OrderBy orderBy)
    {
        return redundantOrderBy.contains(NodeRef.of(orderBy));
    }

    public boolean hasRowFilter(QualifiedObjectName table, String identity)
    {
        return rowFilterScopes.contains(new RowFilterScopeEntry(table, identity));
    }

    public void registerTableForRowFiltering(QualifiedObjectName table, String identity)
    {
        rowFilterScopes.add(new RowFilterScopeEntry(table, identity));
    }

    public void unregisterTableForRowFiltering(QualifiedObjectName table, String identity)
    {
        rowFilterScopes.remove(new RowFilterScopeEntry(table, identity));
    }

    public void addRowFilter(Table table, Expression filter)
    {
        rowFilters.computeIfAbsent(NodeRef.of(table), node -> new ArrayList<>())
                .add(filter);
    }

    public List<Expression> getRowFilters(Table node)
    {
        return rowFilters.getOrDefault(NodeRef.of(node), ImmutableList.of());
    }

    public boolean hasColumnMask(QualifiedObjectName table, String column, String identity)
    {
        return columnMaskScopes.contains(new ColumnMaskScopeEntry(table, column, identity));
    }

    public void registerTableForColumnMasking(QualifiedObjectName table, String column, String identity)
    {
        columnMaskScopes.add(new ColumnMaskScopeEntry(table, column, identity));
    }

    public void unregisterTableForColumnMasking(QualifiedObjectName table, String column, String identity)
    {
        columnMaskScopes.remove(new ColumnMaskScopeEntry(table, column, identity));
    }

    public void addColumnMask(Table table, String column, Expression mask)
    {
        Map<String, List<Expression>> masks = columnMasks.computeIfAbsent(NodeRef.of(table), node -> new LinkedHashMap<>());
        masks.computeIfAbsent(column, name -> new ArrayList<>())
                .add(mask);
    }

    public Map<String, List<Expression>> getColumnMasks(Table table)
    {
        return columnMasks.getOrDefault(NodeRef.of(table), ImmutableMap.of());
    }

    public List<TableInfo> getReferencedTables()
    {
        return tables.entrySet().stream()
                .map(entry -> {
                    NodeRef<Table> table = entry.getKey();

                    QualifiedObjectName tableName = entry.getValue().getName();
                    List<ColumnInfo> columns = tableColumnReferences.values().stream()
                            .map(tablesToColumns -> tablesToColumns.get(tableName))
                            .filter(Objects::nonNull)
                            .flatMap(Collection::stream)
                            .distinct()
                            .map(fieldName -> new ColumnInfo(
                                    fieldName,
                                    columnMasks.getOrDefault(table, ImmutableMap.of())
                                            .getOrDefault(fieldName, ImmutableList.of()).stream()
                                            .map(Expression::toString)
                                            .collect(toImmutableList())))
                            .collect(toImmutableList());

                    TableEntry info = entry.getValue();
                    return new TableInfo(
                            info.getName().getCatalogName(),
                            info.getName().getSchemaName(),
                            info.getName().getObjectName(),
                            info.getAuthorization(),
                            rowFilters.getOrDefault(table, ImmutableList.of()).stream()
                                    .map(Expression::toString)
                                    .collect(toImmutableList()),
                            columns,
                            info.isDirectlyReferenced());
                })
                .collect(toImmutableList());
    }

    public List<RoutineInfo> getRoutines()
    {
        return resolvedFunctions.entrySet().stream()
                .map(entry -> new RoutineInfo(entry.getValue().function.getSignature().getName(), entry.getValue().getAuthorization()))
                .collect(toImmutableList());
    }

    public void addSourceColumns(Field field, Set<SourceColumn> sourceColumn)
    {
        originColumnDetails.putAll(field, sourceColumn);
    }

    public Set<SourceColumn> getSourceColumns(Field field)
    {
        return ImmutableSet.copyOf(originColumnDetails.get(field));
    }

    public void addExpressionFields(Expression expression, Collection<Field> fields)
    {
        fieldLineage.putAll(NodeRef.of(expression), fields);
    }

    public Set<SourceColumn> getExpressionSourceColumns(Expression expression)
    {
        return fieldLineage.get(NodeRef.of(expression)).stream()
                .flatMap(field -> getSourceColumns(field).stream())
                .collect(toImmutableSet());
    }

    public void setRowIdField(Table table, FieldReference field)
    {
        rowIdField.put(NodeRef.of(table), field);
    }

    public FieldReference getRowIdField(Table table)
    {
        return rowIdField.get(NodeRef.of(table));
    }

    public Scope getAccessControlScope(Table node)
    {
        return tables.get(NodeRef.of(node)).getAccessControlScope();
    }

    public void setImplicitFromScope(QuerySpecification node, Scope scope)
    {
        implicitFromScopes.put(NodeRef.of(node), scope);
    }

    public Scope getImplicitFromScope(QuerySpecification node)
    {
        return implicitFromScopes.get(NodeRef.of(node));
    }

    public void addPredicateCoercions(Map<NodeRef<Expression>, PredicateCoercions> coercions)
    {
        predicateCoercions.putAll(coercions);
    }

    public PredicateCoercions getPredicateCoercions(Expression expression)
    {
        return predicateCoercions.get(NodeRef.of(expression));
    }

    @Immutable
    public static final class SelectExpression
    {
        // expression refers to a select item, either to be returned directly, or unfolded by all-fields reference
        // unfoldedExpressions applies to the latter case, and is a list of subscript expressions
        // referencing each field of the row.
        private final Expression expression;
        private final Optional<List<Expression>> unfoldedExpressions;

        public SelectExpression(Expression expression, Optional<List<Expression>> unfoldedExpressions)
        {
            this.expression = requireNonNull(expression, "expression is null");
            this.unfoldedExpressions = requireNonNull(unfoldedExpressions);
        }

        public Expression getExpression()
        {
            return expression;
        }

        public Optional<List<Expression>> getUnfoldedExpressions()
        {
            return unfoldedExpressions;
        }
    }

    @Immutable
    public static final class Create
    {
        private final Optional<QualifiedObjectName> destination;
        private final Optional<ConnectorTableMetadata> metadata;
        private final Optional<NewTableLayout> layout;
        private final boolean createTableAsSelectWithData;
        private final boolean createTableAsSelectNoOp;

        public Create(
                Optional<QualifiedObjectName> destination,
                Optional<ConnectorTableMetadata> metadata,
                Optional<NewTableLayout> layout,
                boolean createTableAsSelectWithData,
                boolean createTableAsSelectNoOp)
        {
            this.destination = requireNonNull(destination, "destination is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.layout = requireNonNull(layout, "layout is null");
            this.createTableAsSelectWithData = createTableAsSelectWithData;
            this.createTableAsSelectNoOp = createTableAsSelectNoOp;
        }

        public Optional<QualifiedObjectName> getDestination()
        {
            return destination;
        }

        public Optional<ConnectorTableMetadata> getMetadata()
        {
            return metadata;
        }

        public Optional<NewTableLayout> getLayout()
        {
            return layout;
        }

        public boolean isCreateTableAsSelectWithData()
        {
            return createTableAsSelectWithData;
        }

        public boolean isCreateTableAsSelectNoOp()
        {
            return createTableAsSelectNoOp;
        }
    }

    @Immutable
    public static final class Insert
    {
        private final TableHandle target;
        private final List<ColumnHandle> columns;
        private final Optional<NewTableLayout> newTableLayout;

        public Insert(TableHandle target, List<ColumnHandle> columns, Optional<NewTableLayout> newTableLayout)
        {
            this.target = requireNonNull(target, "target is null");
            this.columns = requireNonNull(columns, "columns is null");
            checkArgument(columns.size() > 0, "No columns given to insert");
            this.newTableLayout = requireNonNull(newTableLayout, "newTableLayout is null");
        }

        public List<ColumnHandle> getColumns()
        {
            return columns;
        }

        public TableHandle getTarget()
        {
            return target;
        }

        public Optional<NewTableLayout> getNewTableLayout()
        {
            return newTableLayout;
        }
    }

    @Immutable
    public static final class RefreshMaterializedViewAnalysis
    {
        private final QualifiedObjectName materializedViewName;
        private final TableHandle target;
        private final Query query;
        private final List<ColumnHandle> columns;

        public RefreshMaterializedViewAnalysis(QualifiedObjectName materializedViewName, TableHandle target, Query query, List<ColumnHandle> columns)
        {
            this.materializedViewName = requireNonNull(materializedViewName, "materializedViewName is null");
            this.target = requireNonNull(target, "target is null");
            this.query = query;
            this.columns = requireNonNull(columns, "columns is null");
            checkArgument(columns.size() > 0, "No columns given to refresh materialized view");
        }

        public Query getQuery()
        {
            return query;
        }

        public List<ColumnHandle> getColumns()
        {
            return columns;
        }

        public TableHandle getTarget()
        {
            return target;
        }

        public QualifiedObjectName getMaterializedViewName()
        {
            return materializedViewName;
        }
    }

    public static final class JoinUsingAnalysis
    {
        private final List<Integer> leftJoinFields;
        private final List<Integer> rightJoinFields;
        private final List<Integer> otherLeftFields;
        private final List<Integer> otherRightFields;

        JoinUsingAnalysis(List<Integer> leftJoinFields, List<Integer> rightJoinFields, List<Integer> otherLeftFields, List<Integer> otherRightFields)
        {
            this.leftJoinFields = ImmutableList.copyOf(leftJoinFields);
            this.rightJoinFields = ImmutableList.copyOf(rightJoinFields);
            this.otherLeftFields = ImmutableList.copyOf(otherLeftFields);
            this.otherRightFields = ImmutableList.copyOf(otherRightFields);

            checkArgument(leftJoinFields.size() == rightJoinFields.size(), "Expected join fields for left and right to have the same size");
        }

        public List<Integer> getLeftJoinFields()
        {
            return leftJoinFields;
        }

        public List<Integer> getRightJoinFields()
        {
            return rightJoinFields;
        }

        public List<Integer> getOtherLeftFields()
        {
            return otherLeftFields;
        }

        public List<Integer> getOtherRightFields()
        {
            return otherRightFields;
        }
    }

    public static class GroupingSetAnalysis
    {
        private final List<Expression> originalExpressions;

        private final List<Set<FieldId>> cubes;
        private final List<List<FieldId>> rollups;
        private final List<List<Set<FieldId>>> ordinarySets;
        private final List<Expression> complexExpressions;

        public GroupingSetAnalysis(
                List<Expression> originalExpressions,
                List<Set<FieldId>> cubes,
                List<List<FieldId>> rollups,
                List<List<Set<FieldId>>> ordinarySets,
                List<Expression> complexExpressions)
        {
            this.originalExpressions = ImmutableList.copyOf(originalExpressions);
            this.cubes = ImmutableList.copyOf(cubes);
            this.rollups = ImmutableList.copyOf(rollups);
            this.ordinarySets = ImmutableList.copyOf(ordinarySets);
            this.complexExpressions = ImmutableList.copyOf(complexExpressions);
        }

        public List<Expression> getOriginalExpressions()
        {
            return originalExpressions;
        }

        public List<Set<FieldId>> getCubes()
        {
            return cubes;
        }

        public List<List<FieldId>> getRollups()
        {
            return rollups;
        }

        public List<List<Set<FieldId>>> getOrdinarySets()
        {
            return ordinarySets;
        }

        public List<Expression> getComplexExpressions()
        {
            return complexExpressions;
        }

        public Set<FieldId> getAllFields()
        {
            return Streams.concat(
                    cubes.stream().flatMap(Collection::stream),
                    rollups.stream().flatMap(Collection::stream),
                    ordinarySets.stream()
                            .flatMap(Collection::stream)
                            .flatMap(Collection::stream))
                    .collect(toImmutableSet());
        }
    }

    public static class UnnestAnalysis
    {
        private final Map<NodeRef<Expression>, List<Field>> mappings;
        private final Optional<Field> ordinalityField;

        public UnnestAnalysis(Map<NodeRef<Expression>, List<Field>> mappings, Optional<Field> ordinalityField)
        {
            requireNonNull(mappings, "mappings is null");
            this.mappings = mappings.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue())));

            this.ordinalityField = requireNonNull(ordinalityField, "ordinalityField is null");
        }

        public Map<NodeRef<Expression>, List<Field>> getMappings()
        {
            return mappings;
        }

        public Optional<Field> getOrdinalityField()
        {
            return ordinalityField;
        }
    }

    public static class SubqueryAnalysis
    {
        private final List<InPredicate> inPredicatesSubqueries = new ArrayList<>();
        private final List<SubqueryExpression> subqueries = new ArrayList<>();
        private final List<ExistsPredicate> existsSubqueries = new ArrayList<>();
        private final List<QuantifiedComparisonExpression> quantifiedComparisonSubqueries = new ArrayList<>();

        public void addInPredicates(List<InPredicate> expressions)
        {
            inPredicatesSubqueries.addAll(expressions);
        }

        public void addSubqueries(List<SubqueryExpression> expressions)
        {
            subqueries.addAll(expressions);
        }

        public void addExistsSubqueries(List<ExistsPredicate> expressions)
        {
            existsSubqueries.addAll(expressions);
        }

        public void addQuantifiedComparisons(List<QuantifiedComparisonExpression> expressions)
        {
            quantifiedComparisonSubqueries.addAll(expressions);
        }

        public List<InPredicate> getInPredicatesSubqueries()
        {
            return Collections.unmodifiableList(inPredicatesSubqueries);
        }

        public List<SubqueryExpression> getSubqueries()
        {
            return Collections.unmodifiableList(subqueries);
        }

        public List<ExistsPredicate> getExistsSubqueries()
        {
            return Collections.unmodifiableList(existsSubqueries);
        }

        public List<QuantifiedComparisonExpression> getQuantifiedComparisonSubqueries()
        {
            return Collections.unmodifiableList(quantifiedComparisonSubqueries);
        }
    }

    /**
     * Analysis for predicates such as <code>x IN (subquery)</code> or <code>x = SOME (subquery)</code>
     */
    public static class PredicateCoercions
    {
        private final Type valueType;
        private final Optional<Type> valueCoercion;
        private final Optional<Type> subqueryCoercion;

        public PredicateCoercions(Type valueType, Optional<Type> valueCoercion, Optional<Type> subqueryCoercion)
        {
            this.valueType = requireNonNull(valueType, "valueType is null");
            this.valueCoercion = requireNonNull(valueCoercion, "valueCoercion is null");
            this.subqueryCoercion = requireNonNull(subqueryCoercion, "subqueryCoercion is null");
        }

        public Type getValueType()
        {
            return valueType;
        }

        public Optional<Type> getValueCoercion()
        {
            return valueCoercion;
        }

        public Optional<Type> getSubqueryCoercion()
        {
            return subqueryCoercion;
        }
    }

    public static class ResolvedWindow
    {
        private final List<Expression> partitionBy;
        private final Optional<OrderBy> orderBy;
        private final Optional<WindowFrame> frame;
        private final boolean partitionByInherited;
        private final boolean orderByInherited;
        private final boolean frameInherited;

        public ResolvedWindow(List<Expression> partitionBy, Optional<OrderBy> orderBy, Optional<WindowFrame> frame, boolean partitionByInherited, boolean orderByInherited, boolean frameInherited)
        {
            this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
            this.orderBy = requireNonNull(orderBy, "orderBy is null");
            this.frame = requireNonNull(frame, "frame is null");
            this.partitionByInherited = partitionByInherited;
            this.orderByInherited = orderByInherited;
            this.frameInherited = frameInherited;
        }

        public List<Expression> getPartitionBy()
        {
            return partitionBy;
        }

        public Optional<OrderBy> getOrderBy()
        {
            return orderBy;
        }

        public Optional<WindowFrame> getFrame()
        {
            return frame;
        }

        public boolean isPartitionByInherited()
        {
            return partitionByInherited;
        }

        public boolean isOrderByInherited()
        {
            return orderByInherited;
        }

        public boolean isFrameInherited()
        {
            return frameInherited;
        }
    }

    public static final class AccessControlInfo
    {
        private final AccessControl accessControl;
        private final Identity identity;

        public AccessControlInfo(AccessControl accessControl, Identity identity)
        {
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.identity = requireNonNull(identity, "identity is null");
        }

        public AccessControl getAccessControl()
        {
            return accessControl;
        }

        public SecurityContext getSecurityContext(TransactionId transactionId, QueryId queryId)
        {
            return new SecurityContext(transactionId, identity, queryId);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AccessControlInfo that = (AccessControlInfo) o;
            return Objects.equals(accessControl, that.accessControl) &&
                    Objects.equals(identity, that.identity);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(accessControl, identity);
        }

        @Override
        public String toString()
        {
            return format("AccessControl: %s, Identity: %s", accessControl.getClass(), identity);
        }
    }

    private static class RowFilterScopeEntry
    {
        private final QualifiedObjectName table;
        private final String identity;

        public RowFilterScopeEntry(QualifiedObjectName table, String identity)
        {
            this.table = requireNonNull(table, "table is null");
            this.identity = requireNonNull(identity, "identity is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RowFilterScopeEntry that = (RowFilterScopeEntry) o;
            return table.equals(that.table) &&
                    identity.equals(that.identity);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, identity);
        }
    }

    private static class ColumnMaskScopeEntry
    {
        private final QualifiedObjectName table;
        private final String column;
        private final String identity;

        public ColumnMaskScopeEntry(QualifiedObjectName table, String column, String identity)
        {
            this.table = requireNonNull(table, "table is null");
            this.column = requireNonNull(column, "column is null");
            this.identity = requireNonNull(identity, "identity is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnMaskScopeEntry that = (ColumnMaskScopeEntry) o;
            return table.equals(that.table) &&
                    column.equals(that.column) &&
                    identity.equals(that.identity);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, column, identity);
        }
    }

    private static class TableEntry
    {
        private final Optional<TableHandle> handle;
        private final QualifiedObjectName name;
        private final String authorization;
        private final Scope accessControlScope; // synthetic scope for analysis of row filters and masks
        private final boolean directlyReferenced;

        public TableEntry(
                Optional<TableHandle> handle,
                QualifiedObjectName name,
                String authorization,
                Scope accessControlScope,
                boolean directlyReferenced)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.name = requireNonNull(name, "name is null");
            this.authorization = requireNonNull(authorization, "authorization is null");
            this.accessControlScope = requireNonNull(accessControlScope, "accessControlScope is null");
            this.directlyReferenced = directlyReferenced;
        }

        public Optional<TableHandle> getHandle()
        {
            return handle;
        }

        public QualifiedObjectName getName()
        {
            return name;
        }

        public boolean isDirectlyReferenced()
        {
            return directlyReferenced;
        }

        public String getAuthorization()
        {
            return authorization;
        }

        public Scope getAccessControlScope()
        {
            return accessControlScope;
        }
    }

    public static class SourceColumn
    {
        private final QualifiedObjectName tableName;
        private final String columnName;

        @JsonCreator
        public SourceColumn(@JsonProperty("tableName") QualifiedObjectName tableName, @JsonProperty("columnName") String columnName)
        {
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.columnName = requireNonNull(columnName, "columnName is null");
        }

        @JsonProperty
        public QualifiedObjectName getTableName()
        {
            return tableName;
        }

        @JsonProperty
        public String getColumnName()
        {
            return columnName;
        }

        public ColumnDetail getColumnDetail()
        {
            return new ColumnDetail(tableName.getCatalogName(), tableName.getSchemaName(), tableName.getObjectName(), columnName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName, columnName);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            SourceColumn entry = (SourceColumn) obj;
            return Objects.equals(tableName, entry.tableName) &&
                    Objects.equals(columnName, entry.columnName);
        }
    }

    private static class RoutineEntry
    {
        private final ResolvedFunction function;
        private final String authorization;

        public RoutineEntry(ResolvedFunction function, String authorization)
        {
            this.function = requireNonNull(function, "function is null");
            this.authorization = requireNonNull(authorization, "authorization is null");
        }

        public ResolvedFunction getFunction()
        {
            return function;
        }

        public String getAuthorization()
        {
            return authorization;
        }
    }

    private static class UpdateTarget
    {
        private final QualifiedObjectName name;
        private final Optional<Table> table;
        private final Optional<List<OutputColumn>> columns;

        public UpdateTarget(QualifiedObjectName name, Optional<Table> table, Optional<List<OutputColumn>> columns)
        {
            this.name = requireNonNull(name, "name is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = requireNonNull(columns, "columns is null").map(ImmutableList::copyOf);
        }

        public QualifiedObjectName getName()
        {
            return name;
        }

        public Optional<Table> getTable()
        {
            return table;
        }

        public Optional<List<OutputColumn>> getColumns()
        {
            return columns;
        }
    }

    public static class Range
    {
        private final Optional<Integer> atLeast;
        private final Optional<Integer> atMost;

        public Range(Optional<Integer> atLeast, Optional<Integer> atMost)
        {
            this.atLeast = requireNonNull(atLeast, "atLeast is null");
            this.atMost = requireNonNull(atMost, "atMost is null");
        }

        public Optional<Integer> getAtLeast()
        {
            return atLeast;
        }

        public Optional<Integer> getAtMost()
        {
            return atMost;
        }
    }
}
