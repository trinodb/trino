package io.trino.plugin.couchbase;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionManager;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.*;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class CouchbaseMetadata
                implements ConnectorMetadata {

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
    public boolean schemaExists(ConnectorSession session, String schemaName) {
        return client.getBucket().collections().getAllScopes().stream()
                .filter(scope -> scope.name().equals(schemaName))
                .findAny().isPresent();
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
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
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
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
            try (FileReader schemaReader = new FileReader(schemaFile)) {
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
                        } else {
                            if (ordered)
                            unordered = true;
                            columns.add(new ColumnMetadata(propertyName, deductType(property)));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(String.format("Failed to read schema for column '%s'", propertyName), e);
                    }
                }

                if (!unordered) {
                    columns = Arrays.asList(orderedColumns);
                }

                ConnectorTableMetadata result = new ConnectorTableMetadata(new SchemaTableName(handle.schema(), handle.name()), columns);
                mtimeCache.put(tablePath, Files.getLastModifiedTime(schemaFile.toPath()).toMillis());
                metaCache.put(tablePath, result);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to read schema for collection '%s.%s'", ((CouchbaseTableHandle) table).schema(), ((CouchbaseTableHandle) table).name()), e);
            }
        }

        return metaCache.get(tablePath);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        CouchbaseTableHandle handle = (CouchbaseTableHandle) tableHandle;
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        return tableMetadata.getColumns().stream()
                .collect(Collectors.toMap(ColumnMetadata::getName,
                        column -> new CouchbaseColumnHandle(
                                Arrays.asList(handle.schema(), handle.name(), column.getName()),
                                column.getType()
                        )));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        ConnectorTableMetadata tableMeta =  getTableMetadata(session, tableHandle);
        return tableMeta.getColumns().stream()
                .filter(column -> column.getName().equals(((CouchbaseColumnHandle) columnHandle).name()))
                .findFirst()
                .orElse(null);
    }

    private File getSchemaFile(CouchbaseTableHandle handle) {
        return new File(new File(config.getSchemaFolder()), String.format("%s.%s.%s.json", client.getBucket().name(), handle.schema(), handle.name()));
    }

    private void checkMtime(CouchbaseTableHandle handle) {
        final String path = handle.path();
        try {
            long cached = mtimeCache.getOrDefault(path, 0L);
            long actual = Files.getLastModifiedTime(getSchemaFile(handle).toPath()).toMillis();
            if (actual - cached > 1000) {
                metaCache.remove(path);
            }
        } catch (Exception e) {
            metaCache.remove(path);
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        if (!takeOrReject(constraint.getAssignments())) {
            LOG.info("rejecting constraint {} for table {}: unsupported assignments", constraint, handle);
            return Optional.empty();
        }

        if (handle instanceof CouchbaseTableHandle cbHandle) {
            if (cbHandle.compareConstraint(constraint)) {
                return Optional.empty();
            } else {
                handle = cbHandle = new CouchbaseTableHandle(
                        cbHandle.schema(),
                        cbHandle.name(),
                        Arrays.asList(cbHandle),
                        new HashMap<>(),
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new AtomicLong(-1L)
                );
            }
            cbHandle.addConstraint(constraint);
            return Optional.of(new ConstraintApplicationResult<>(handle, TupleDomain.withColumnDomains(Collections.EMPTY_MAP), constraint.getExpression(), false));
        }
        return ConnectorMetadata.super.applyFilter(session, handle, constraint);
    }

    private Type deductType(JsonObject property) {
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
            } else if (cbType.equals("string")) {
                deductedTypes.add(VarcharType.VARCHAR);
            } else if (cbType.startsWith("varchar(")) {
                int size = Integer.valueOf(cbType.replace("varchar(", "").replace(")", ""));
                deductedTypes.add(VarcharType.createVarcharType(size));
            } else if (cbType.equals("boolean")) {
                deductedTypes.add(BooleanType.BOOLEAN);
            } else if (cbType.equals("number")) {
                deductedTypes.add(DecimalType.createDecimalType());
            } else if (cbType.equals("integer")) {
                deductedTypes.add(IntegerType.INTEGER);
            } else if (cbType.equals("date")) {
                deductedTypes.add(DateType.DATE);
            } else if (cbType.equals("bigint")) {
                deductedTypes.add(BigintType.BIGINT);
            } else if (cbType.equals("double")) {
                deductedTypes.add(DoubleType.DOUBLE);
            } else if (cbType.equals("array")) {
                deductedTypes.add(new ArrayType(deductType(property.getObject("items"))));
            } else if (cbType.equals("object")) {
                List<RowType.Field> fields = new ArrayList<>();
                JsonObject properties = property.getObject("properties");
                for (String propertyName: properties.getNames()) {
                    JsonObject subProperty = properties.getObject(propertyName);
                    fields.add(new RowType.Field(Optional.of(propertyName), deductType(subProperty)));
                }
                deductedTypes.add(RowType.from(fields));
            } else {
                throw new RuntimeException("Unsupported couchbase type: " + cbType);
            }
        }
        if (deductedTypes.size() != 1) {
            throw new RuntimeException("Ambiguous couchbase type: " + deductedTypes);
        }
        return deductedTypes.get(0);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        CollectionManager cm = client.getBucket().collections();
        return cm.getAllScopes().stream()
                .filter(scopeSpec -> scopeSpec.name().equals(client.getScope().name()))
                .flatMap(scopeSpec -> scopeSpec.collections().stream())
                .map(collectionSpec -> new SchemaTableName(client.getScope().name(), collectionSpec.name()))
                .toList();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return Arrays.asList(client.getScope().name());
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(ConnectorSession session, ConnectorTableHandle handle, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments) {
        if (!takeOrReject(assignments)) {
            LOG.info("Rejecting topN assignments: {}", assignments);
            return Optional.empty();
        }

        if (handle instanceof CouchbaseTableHandle cbHandle) {
            if (cbHandle.topNCount().longValue() != -1 || !cbHandle.orderClauses().isEmpty()){
                if (cbHandle.topNCount().longValue() == topNCount && cbHandle.compareSortItems(sortItems)) {
                    LOG.info("Rejecting topN: no effect");
                    return Optional.empty();
                }
                LOG.info("Wrapping table handle: already got topN or non-matching order");
                handle = cbHandle = new CouchbaseTableHandle(
                        cbHandle.schema(),
                        cbHandle.name(),
                        Arrays.asList(cbHandle),
                        new HashMap<>(),
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new AtomicLong(-1)
                );
            }

            cbHandle.setTopNCount(topNCount);
            cbHandle.addSortItems(sortItems);
        } else {
            LOG.warn("Rejecting topN assignments: handle is not couchbase");
            return Optional.empty();
        }

        LOG.info("Accepted topN assignments: {}", handle);
        return Optional.of(new TopNApplicationResult<ConnectorTableHandle>(handle, true, true));
    }

    private boolean takeOrReject(Map<String, ColumnHandle> assignments) {
        return assignments.keySet().stream()
                .allMatch(key -> assignments.get(key) instanceof CouchbaseColumnHandle);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit) {
        if (handle instanceof CouchbaseTableHandle cbHandle) {
            if (cbHandle.topNCount().longValue() != -1){
                if (cbHandle.topNCount().longValue() == limit) {
                    return Optional.empty();
                }
                handle = cbHandle = new CouchbaseTableHandle(
                        cbHandle.schema(),
                        cbHandle.name(),
                        Arrays.asList(cbHandle),
                        new HashMap<>(),
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new AtomicLong(limit)
                );
            } else {
                cbHandle.setTopNCount(limit);
            }
        } else {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(handle, true, false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments) {

        Map<String, Assignment> resultAssignments = new HashMap<>();
        Map<String, CouchbaseColumnHandle> columns = new HashMap<>();
        if (!(handle instanceof  CouchbaseTableHandle)) {
            return Optional.empty();
        }

        CouchbaseTableHandle cbTable = (CouchbaseTableHandle) handle;
        List<ConnectorExpression> acceptedProjections = projections.stream()
                .filter(projection -> takeOrReject(projection, assignments, resultAssignments))
                .toList();

        if (acceptedProjections.isEmpty()) {
            return Optional.empty();
        }

        if (!cbTable.addAssignments(assignments, projections)) {
            return Optional.empty();
        }


        return Optional.of(new ProjectionApplicationResult<ConnectorTableHandle>(
                handle, acceptedProjections, resultAssignments.values().stream().toList(), false
        ));
    }

    private boolean takeOrReject(ConnectorExpression projection, Map<String, ColumnHandle> assignments, Map<String, Assignment> acceptedAssignments) {
        for (int i = 0; i < projection.getChildren().size(); i++) {
            ConnectorExpression child = projection.getChildren().get(i);
            if (!takeOrReject(child, assignments, acceptedAssignments)) {
                return false;
            }
        }

        if (projection instanceof Variable) {
            String name =  ((Variable) projection).getName();
            if (acceptedAssignments.containsKey(name)) {
                return true;
            } else if (assignments.containsKey(name)) {
                if (assignments.get(name) instanceof CouchbaseColumnHandle cbColumn) {
                    acceptedAssignments.put(name, new Assignment(name, cbColumn, projection.getType()));
                    return true;
                } else {
                    return false;
                }
            }
            throw new IllegalStateException("Variable projection is not found in assignments");
        } else {
            throw new UnsupportedOperationException("Unsupported base projection type: " + projection.getClass().getName());
        }
    }



    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio) {
        return ConnectorMetadata.super.applySample(session, handle, sampleType, sampleRatio);
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session, ConnectorTableHandle handle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets) {
        return ConnectorMetadata.super.applyAggregation(session, handle, aggregates, assignments, groupingSets);
    }

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(ConnectorSession session, JoinType joinType, ConnectorTableHandle left, ConnectorTableHandle right, ConnectorExpression joinCondition, Map<String, ColumnHandle> leftAssignments, Map<String, ColumnHandle> rightAssignments, JoinStatistics statistics) {
        return ConnectorMetadata.super.applyJoin(session, joinType, left, right, joinCondition, leftAssignments, rightAssignments, statistics);
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle) {
        return ConnectorMetadata.super.applyTableFunction(session, handle);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return ConnectorMetadata.super.applyTableScanRedirect(session, tableHandle);
    }
}
