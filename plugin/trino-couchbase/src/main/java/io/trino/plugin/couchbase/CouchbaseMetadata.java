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
package io.trino.plugin.couchbase;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.couchbase.client.core.deps.com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.couchbase.translations.TrinoToCbType.isPushdownSupportedType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class CouchbaseMetadata
        implements ConnectorMetadata
{
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseMetadata.class);
    private final TypeManager typeManager;
    private final CouchbaseClient client;
    private final CouchbaseConfig config;
    private final Map<String, Long> mtimeCache = new ConcurrentHashMap<>();
    private final Map<String, ConnectorTableMetadata> metaCache = new ConcurrentHashMap<>();

    @Inject
    public CouchbaseMetadata(TypeManager typeManager, CouchbaseClient client, CouchbaseConfig config)
    {
        this.typeManager = typeManager;
        this.client = client;
        this.config = config;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return client.getBucket().collections().getAllScopes().stream()
                .filter(scope -> scope.name().equals(schemaName))
                .findAny().isPresent();
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        CollectionManager cm = client.getBucket().collections();
        return cm.getAllScopes().stream()
                .filter(scopeSpec -> scopeSpec.name().equals(tableName.getSchemaName()))
                .flatMap(scopeSpec -> scopeSpec.collections().stream())
                .filter(collectionSpec -> collectionSpec.name().equals(tableName.getTableName()))
                .findFirst().map(collectionSpec -> CouchbaseTableHandle.fromSchemaAndName(tableName.getSchemaName(), tableName.getTableName()))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        requireNonNull(config.getSchemaFolder(), "Couchbase schema folder is not set");
        if (!(table instanceof CouchbaseTableHandle)) {
            throw new RuntimeException("Couchbase table handle is not an instance of CouchbaseTableHandle");
        }

        CouchbaseTableHandle handle = (CouchbaseTableHandle) table;
        String tablePath = handle.path();
        checkMtime(handle);
        if (!metaCache.containsKey(tablePath)) {
            File schemaFile = getSchemaFile(handle);
            if (!schemaFile.exists()) {
                throw new RuntimeException(String.format("Couchbase schema file '%s' does not exist", schemaFile.getAbsolutePath()));
            }
            List<ColumnMetadata> columns = new LinkedList<>();
            try (Reader schemaReader = Files.newBufferedReader(Path.of(schemaFile.toURI()))) {
                JsonObject schema = JsonObject.fromJson(schemaReader.readAllAsString());
                JsonObject properties = schema.getObject("properties");

                ColumnMetadata[] orderedColumns = new ColumnMetadata[properties.size()];
                boolean unordered = false;
                boolean ordered = false;
                for (String propertyName : properties.getNames()) {
                    if (propertyName.equals("~meta")) {
                        // skip the meta column
                        continue;
                    }
                    try {
                        JsonObject property = properties.getObject(propertyName);
                        if (property.containsKey("order")) {
                            if (unordered) {
                                throw new RuntimeException(String.format("unable to mix ordered and unordered properties: %s", tablePath));
                            }
                            int order = property.getInt("order");
                            orderedColumns[order] = new ColumnMetadata(propertyName, deductType(property));
                        }
                        else {
                            if (ordered) {
                                unordered = true;
                            }
                            columns.add(new ColumnMetadata(propertyName, deductType(property)));
                        }
                    }
                    catch (Exception e) {
                        throw new RuntimeException(String.format("Failed to read schema for column '%s'", propertyName), e);
                    }
                }

                if (!unordered) {
                    columns = Arrays.asList(orderedColumns);
                }

                ConnectorTableMetadata result = new ConnectorTableMetadata(new SchemaTableName(handle.schema(), handle.name()), columns);
                mtimeCache.put(tablePath, Files.getLastModifiedTime(schemaFile.toPath()).toMillis());
                metaCache.put(tablePath, result);
            }
            catch (Exception e) {
                throw new RuntimeException(String.format("Failed to read schema for collection '%s.%s'", ((CouchbaseTableHandle) table).schema(), ((CouchbaseTableHandle) table).name()), e);
            }
        }

        return metaCache.get(tablePath);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CouchbaseTableHandle handle = (CouchbaseTableHandle) tableHandle;
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        return tableMetadata.getColumns().stream()
                .collect(Collectors.toMap(ColumnMetadata::getName,
                        column -> new CouchbaseColumnHandle(
                                Arrays.asList(handle.schema(), handle.name(), column.getName()),
                                new ArrayList<>(),
                                column.getType())));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorTableMetadata tableMeta = getTableMetadata(session, tableHandle);
        return tableMeta.getColumns().stream()
                .filter(column -> column.getName().equals(((CouchbaseColumnHandle) columnHandle).name()))
                .findFirst()
                .orElse(null);
    }

    private File getSchemaFile(CouchbaseTableHandle handle)
    {
        return new File(new File(config.getSchemaFolder()), String.format("%s.%s.%s.json", client.getBucket().name(), handle.schema(), handle.name()));
    }

    private void checkMtime(CouchbaseTableHandle handle)
    {
        final String path = handle.path();
        try {
            long cached = mtimeCache.getOrDefault(path, 0L);
            long actual = Files.getLastModifiedTime(getSchemaFile(handle).toPath()).toMillis();
            if (actual - cached > 1000) {
                metaCache.remove(path);
            }
        }
        catch (Exception e) {
            metaCache.remove(path);
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        if (!takeOrReject(constraint.getAssignments())) {
            LOG.info("rejecting constraint {} for table {}: unsupported assignments", constraint, handle);
            return Optional.empty();
        }

        if (handle instanceof CouchbaseTableHandle cbHandle) {
            if (cbHandle.containsConstraint(constraint)) {
                return Optional.empty();
            }
            TupleDomain<ColumnHandle> oldDomain = cbHandle.constraint();
            TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
            TupleDomain<ColumnHandle> remainingFilter;
            if (newDomain.isNone()) {
                remainingFilter = TupleDomain.all();
            }
            else {
                Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();

                Map<ColumnHandle, Domain> supported = new HashMap<>();
                Map<ColumnHandle, Domain> unsupported = new HashMap<>();

                for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                    CouchbaseColumnHandle columnHandle = (CouchbaseColumnHandle) entry.getKey();
                    Domain domain = entry.getValue();
                    Type columnType = columnHandle.type();
                    if (isPushdownSupportedType(columnType)) {
                        supported.put(entry.getKey(), entry.getValue());
                    }
                    else {
                        unsupported.put(columnHandle, domain);
                    }
                }
                newDomain = TupleDomain.withColumnDomains(supported);
                remainingFilter = TupleDomain.withColumnDomains(unsupported);
            }
            if (oldDomain.equals(newDomain)) {
                return Optional.empty();
            }

            cbHandle = cbHandle.withConstraint(newDomain);
            handle = cbHandle;

            return Optional.of(new ConstraintApplicationResult<>(handle, remainingFilter, constraint.getExpression(), false));
        }
        return ConnectorMetadata.super.applyFilter(session, handle, constraint);
    }

    private Type deductType(JsonObject property)
    {
        Object type = property.get("type");
        if (type instanceof String) {
            type = JsonArray.from(type);
        }

        JsonArray types = (JsonArray) type;
        List<Type> deductedTypes = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            String cbType = types.getString(i);
            if (cbType.equals("null")) {
                continue;
            }
            else if (cbType.equals("string")) {
                deductedTypes.add(VarcharType.VARCHAR);
            }
            else if (cbType.startsWith("varchar(")) {
                int size = Integer.valueOf(cbType.replace("varchar(", "").replace(")", ""));
                deductedTypes.add(VarcharType.createVarcharType(size));
            }
            else if (cbType.equals("boolean")) {
                deductedTypes.add(BooleanType.BOOLEAN);
            }
            else if (cbType.equals("number")) {
                deductedTypes.add(DecimalType.createDecimalType());
            }
            else if (cbType.equals("integer")) {
                deductedTypes.add(IntegerType.INTEGER);
            }
            else if (cbType.equals("date")) {
                deductedTypes.add(DateType.DATE);
            }
            else if (cbType.equals("bigint")) {
                deductedTypes.add(BigintType.BIGINT);
            }
            else if (cbType.equals("double")) {
                deductedTypes.add(DoubleType.DOUBLE);
            }
            else if (cbType.equals("array")) {
                deductedTypes.add(new ArrayType(deductType(property.getObject("items"))));
            }
            else if (cbType.equals("object")) {
                List<RowType.Field> fields = new ArrayList<>();
                JsonObject properties = property.getObject("properties");
                for (String propertyName : properties.getNames()) {
                    JsonObject subProperty = properties.getObject(propertyName);
                    fields.add(new RowType.Field(Optional.of(propertyName), deductType(subProperty)));
                }
                deductedTypes.add(RowType.from(fields));
            }
            else {
                throw new RuntimeException("Unsupported couchbase type: " + cbType);
            }
        }
        if (deductedTypes.size() != 1) {
            throw new RuntimeException("Ambiguous couchbase type: " + deductedTypes);
        }
        return deductedTypes.get(0);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        CollectionManager cm = client.getBucket().collections();
        return cm.getAllScopes().stream()
                .filter(scopeSpec -> scopeSpec.name().equals(client.getScope().name()))
                .flatMap(scopeSpec -> scopeSpec.collections().stream())
                .map(collectionSpec -> new SchemaTableName(client.getScope().name(), collectionSpec.name()))
                .toList();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return Arrays.asList(client.getScope().name());
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(ConnectorSession session, ConnectorTableHandle handle, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        if (!takeOrReject(assignments)) {
            LOG.info("Rejecting topN assignments: {}", assignments);
            return Optional.empty();
        }

        if (handle instanceof CouchbaseTableHandle cbHandle) {
            if (cbHandle.topNCount().longValue() != -1 || !cbHandle.orderClauses().isEmpty()) {
                if (cbHandle.topNCount().longValue() == topNCount && cbHandle.compareSortItems(sortItems, assignments)) {
                    LOG.info("Rejecting topN: no effect");
                    return Optional.empty();
                }
                LOG.info("Wrapping table handle: already got topN or non-matching order");
                cbHandle = cbHandle.wrap();
                handle = cbHandle;
            }

            cbHandle.setTopNCount(topNCount);
            cbHandle.addSortItems(sortItems, assignments);
        }
        else {
            LOG.warn("Rejecting topN assignments: handle is not couchbase");
            return Optional.empty();
        }

        LOG.info("Accepted topN assignments: {}", handle);
        return Optional.of(new TopNApplicationResult<ConnectorTableHandle>(handle, true, true));
    }

    private boolean takeOrReject(Map<String, ColumnHandle> assignments)
    {
        return assignments.keySet().stream()
                .allMatch(key -> assignments.get(key) instanceof CouchbaseColumnHandle);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        if (handle instanceof CouchbaseTableHandle cbHandle) {
            if (cbHandle.topNCount().longValue() != -1) {
                if (cbHandle.topNCount().longValue() <= limit) {
                    return Optional.empty();
                }
            }
            cbHandle.setTopNCount(limit);
        }
        else {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(handle, true, false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        if (!(handle instanceof CouchbaseTableHandle)) {
            return Optional.empty();
        }

        CouchbaseTableHandle cbTable = (CouchbaseTableHandle) handle;

        try {
            if (cbTable.containsProjections(projections, assignments)) {
                return Optional.empty();
            }
        }
        catch (IllegalArgumentException e) {
            LOG.warn(String.format("Exception while applying projections to couchbase table: %s", cbTable), e);
            return Optional.empty();
        }

        Set<ConnectorExpression> projectedExpressions = projections.stream()
                .flatMap(expression -> extractSupportedProjectedColumns(expression, ex -> true).stream())
                .collect(toImmutableSet());

        Map<ConnectorExpression, ApplyProjectionUtil.ProjectedColumnRepresentation> columnProjections = projectedExpressions.stream()
                .collect(toImmutableMap(identity(), ApplyProjectionUtil::createProjectedColumnRepresentation));

//            cbTable.addColumns(columnProjections, assignments);

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<CouchbaseColumnHandle> projectedColumnsBuilder = ImmutableSet.builder();

        for (Map.Entry<ConnectorExpression, ApplyProjectionUtil.ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ApplyProjectionUtil.ProjectedColumnRepresentation projectedColumn = entry.getValue();

            CouchbaseColumnHandle baseColumnHandle = (CouchbaseColumnHandle) assignments.get(projectedColumn.getVariable().getName());
            CouchbaseColumnHandle projectedColumnHandle = projectColumn(baseColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType());
            String projectedColumnName = projectedColumnHandle.name();

            Variable projectedColumnVariable = new Variable(projectedColumnName, expression.getType());
            Assignment newAssignment = new Assignment(projectedColumnName, projectedColumnHandle, expression.getType());
            newAssignments.putIfAbsent(projectedColumnName, newAssignment);

            newVariablesBuilder.put(expression, projectedColumnVariable);
            projectedColumnsBuilder.add(projectedColumnHandle);
        }

        Map<ConnectorExpression, Variable> newVariables = newVariablesBuilder.buildOrThrow();
        List<ConnectorExpression> newProjections = projections.stream()
                .map(expression -> replaceWithNewVariables(expression, newVariables))
                .collect(toImmutableList());

        if (cbTable.containsProjections(newProjections, assignments)) {
            return Optional.empty();
        }

        cbTable.selectClauses().clear();
        cbTable.selectTypes().clear();
        cbTable.selectNames().clear();

        cbTable.addProjections(newProjections, assignments);

        List<Assignment> outputAssignments = newAssignments.values().stream().collect(toImmutableList());
        return Optional.of(new ProjectionApplicationResult<>(
                cbTable,
                newProjections,
                outputAssignments,
                false));
    }

    private static CouchbaseColumnHandle projectColumn(CouchbaseColumnHandle baseColumn, List<Integer> indices, Type projectedColumnType)
    {
        if (indices.isEmpty()) {
            return baseColumn;
        }
        ImmutableList.Builder<String> dereferenceNamesBuilder = ImmutableList.builder();
        dereferenceNamesBuilder.addAll(baseColumn.dereferenceNames());

        Type type = baseColumn.type();
        for (int index : indices) {
            checkArgument(type instanceof RowType, "type should be Row type");
            RowType rowType = (RowType) type;
            RowType.Field field = rowType.getFields().get(index);
            dereferenceNamesBuilder.add(field.getName()
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "ROW type does not have field names declared: " + rowType)));
            type = field.getType();
        }
        return new CouchbaseColumnHandle(
                baseColumn.path(),
                dereferenceNamesBuilder.build(),
                projectedColumnType);
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
    {
        return ConnectorMetadata.super.applySample(session, handle, sampleType, sampleRatio);
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session, ConnectorTableHandle handle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return Optional.empty();
    }

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(ConnectorSession session, JoinType joinType, ConnectorTableHandle left, ConnectorTableHandle right, ConnectorExpression joinCondition, Map<String, ColumnHandle> leftAssignments, Map<String, ColumnHandle> rightAssignments, JoinStatistics statistics)
    {
        return ConnectorMetadata.super.applyJoin(session, joinType, left, right, joinCondition, leftAssignments, rightAssignments, statistics);
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        return ConnectorMetadata.super.applyTableFunction(session, handle);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return ConnectorMetadata.super.applyTableScanRedirect(session, tableHandle);
    }
}
