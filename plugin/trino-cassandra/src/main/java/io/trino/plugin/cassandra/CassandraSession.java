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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.cassandra.util.CassandraCqlUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.transform;
import static io.trino.plugin.cassandra.CassandraErrorCode.CASSANDRA_VERSION_ERROR;
import static io.trino.plugin.cassandra.CassandraMetadata.PRESTO_COMMENT_METADATA;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.selectDistinctFrom;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.validSchemaName;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.validTableName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class CassandraSession
        implements Closeable
{
    private static final Logger log = Logger.get(CassandraSession.class);

    private static final String SYSTEM = "system";
    private static final String SIZE_ESTIMATES = "size_estimates";
    private static final Version PARTITION_FETCH_WITH_IN_PREDICATE_VERSION = Version.parse("2.2");

    private final CassandraTypeManager cassandraTypeManager;
    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;
    private final Supplier<CqlSession> session;
    private final Duration noHostAvailableRetryTimeout;

    public CassandraSession(
            CassandraTypeManager cassandraTypeManager,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec,
            Supplier<CqlSession> sessionSupplier,
            Duration noHostAvailableRetryTimeout)
    {
        this.cassandraTypeManager = requireNonNull(cassandraTypeManager, "cassandraTypeManager is null");
        this.extraColumnMetadataCodec = requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");
        this.noHostAvailableRetryTimeout = requireNonNull(noHostAvailableRetryTimeout, "noHostAvailableRetryTimeout is null");
        this.session = memoize(sessionSupplier::get);
    }

    public Version getCassandraVersion()
    {
        ResultSet result = executeWithSession(session -> session.execute("select release_version from system.local"));
        Row versionRow = result.one();
        if (versionRow == null) {
            throw new TrinoException(CASSANDRA_VERSION_ERROR, "The cluster version is not available. " +
                    "Please make sure that the Cassandra cluster is up and running, " +
                    "and that the contact points are specified correctly.");
        }
        return Version.parse(versionRow.getString("release_version"));
    }

    public ProtocolVersion getProtocolVersion()
    {
        return executeWithSession(session -> session.getContext().getProtocolVersion());
    }

    public String getPartitioner()
    {
        return executeWithSession(session -> session.getMetadata().getTokenMap()
                .orElseThrow()
                .getPartitionerName());
    }

    public Set<TokenRange> getTokenRanges()
    {
        return executeWithSession(session -> session.getMetadata().getTokenMap()
                .orElseThrow()
                .getTokenRanges());
    }

    public Set<Node> getReplicas(String caseSensitiveSchemaName, TokenRange tokenRange)
    {
        requireNonNull(caseSensitiveSchemaName, "caseSensitiveSchemaName is null");
        requireNonNull(tokenRange, "tokenRange is null");
        return executeWithSession(session ->
                session.getMetadata()
                        .getTokenMap()
                        .map(tokenMap -> tokenMap.getReplicas(validSchemaName(caseSensitiveSchemaName), tokenRange))
                        .orElse(ImmutableSet.of()));
    }

    public Set<Node> getReplicas(String caseSensitiveSchemaName, ByteBuffer partitionKey)
    {
        requireNonNull(caseSensitiveSchemaName, "caseSensitiveSchemaName is null");
        requireNonNull(partitionKey, "partitionKey is null");
        return executeWithSession(session ->
                session.getMetadata().getTokenMap()
                        .map(tokenMap -> tokenMap.getReplicas(validSchemaName(caseSensitiveSchemaName), partitionKey))
                        .orElse(ImmutableSet.of()));
    }

    public String getCaseSensitiveSchemaName(String caseInsensitiveSchemaName)
    {
        return getKeyspaceByCaseInsensitiveName(caseInsensitiveSchemaName).getName().asInternal();
    }

    public List<String> getCaseSensitiveSchemaNames()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        Map<CqlIdentifier, KeyspaceMetadata> keyspaces = executeWithSession(session -> session.getMetadata().getKeyspaces());
        for (KeyspaceMetadata meta : keyspaces.values()) {
            builder.add(meta.getName().asInternal());
        }
        return builder.build();
    }

    public List<String> getCaseSensitiveTableNames(String caseInsensitiveSchemaName)
            throws SchemaNotFoundException
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseInsensitiveName(caseInsensitiveSchemaName);
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (TableMetadata table : keyspace.getTables().values()) {
            builder.add(table.getName().asInternal());
        }
        for (ViewMetadata materializedView : keyspace.getViews().values()) {
            builder.add(materializedView.getName().asInternal());
        }
        return builder.build();
    }

    public CassandraTable getTable(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseInsensitiveName(schemaTableName.getSchemaName());
        RelationMetadata tableMeta = getTableMetadata(keyspace, schemaTableName.getTableName());

        List<String> columnNames = new ArrayList<>();
        Collection<ColumnMetadata> columns = tableMeta.getColumns().values();
        checkColumnNames(columns);
        for (ColumnMetadata columnMetadata : columns) {
            columnNames.add(columnMetadata.getName().asInternal());
        }

        // check if there is a comment to establish column ordering
        Object comment = tableMeta.getOptions().get(CqlIdentifier.fromInternal("comment"));
        Set<String> hiddenColumns = ImmutableSet.of();
        if (comment instanceof String && ((String) comment).startsWith(PRESTO_COMMENT_METADATA)) {
            String columnOrderingString = ((String) comment).substring(PRESTO_COMMENT_METADATA.length());

            // column ordering
            List<ExtraColumnMetadata> extras = extraColumnMetadataCodec.fromJson(columnOrderingString);
            List<String> explicitColumnOrder = new ArrayList<>(ImmutableList.copyOf(transform(extras, ExtraColumnMetadata::getName)));
            hiddenColumns = extras.stream()
                    .filter(ExtraColumnMetadata::isHidden)
                    .map(ExtraColumnMetadata::getName)
                    .collect(toImmutableSet());

            // add columns not in the comment to the ordering
            List<String> remaining = columnNames.stream()
                    .filter(name -> !explicitColumnOrder.contains(name))
                    .collect(toList());
            explicitColumnOrder.addAll(remaining);

            // sort the actual columns names using the explicit column order (this allows for missing columns)
            columnNames = Ordering.explicit(explicitColumnOrder).sortedCopy(columnNames);
        }

        ImmutableList.Builder<CassandraColumnHandle> columnHandles = ImmutableList.builder();

        // add primary keys first
        Set<CqlIdentifier> primaryKeySet = new HashSet<>();
        for (ColumnMetadata columnMeta : tableMeta.getPartitionKey()) {
            primaryKeySet.add(columnMeta.getName());
            boolean hidden = hiddenColumns.contains(columnMeta.getName().asInternal());
            CassandraColumnHandle columnHandle = buildColumnHandle(tableMeta, columnMeta, true, false, columnNames.indexOf(columnMeta.getName().asInternal()), hidden)
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unsupported partition key type: " + columnMeta.getType().asCql(false, false)));
            columnHandles.add(columnHandle);
        }

        // add clustering columns
        for (ColumnMetadata columnMeta : tableMeta.getClusteringColumns().keySet()) {
            primaryKeySet.add(columnMeta.getName());
            boolean hidden = hiddenColumns.contains(columnMeta.getName().asInternal());
            Optional<CassandraColumnHandle> columnHandle = buildColumnHandle(tableMeta, columnMeta, false, true, columnNames.indexOf(columnMeta.getName().asInternal()), hidden);
            columnHandle.ifPresent(columnHandles::add);
        }

        // add other columns
        for (ColumnMetadata columnMeta : columns) {
            if (!primaryKeySet.contains(columnMeta.getName())) {
                boolean hidden = hiddenColumns.contains(columnMeta.getName().asInternal());
                Optional<CassandraColumnHandle> columnHandle = buildColumnHandle(tableMeta, columnMeta, false, false, columnNames.indexOf(columnMeta.getName().asInternal()), hidden);
                columnHandle.ifPresent(columnHandles::add);
            }
        }

        List<CassandraColumnHandle> sortedColumnHandles = columnHandles.build().stream()
                .sorted(comparing(CassandraColumnHandle::getOrdinalPosition))
                .collect(toList());

        CassandraNamedRelationHandle tableHandle = new CassandraNamedRelationHandle(tableMeta.getKeyspace().asInternal(), tableMeta.getName().asInternal());
        return new CassandraTable(tableHandle, sortedColumnHandles);
    }

    private KeyspaceMetadata getKeyspaceByCaseInsensitiveName(String caseInsensitiveSchemaName)
            throws SchemaNotFoundException
    {
        Collection<KeyspaceMetadata> keyspaces = executeWithSession(session -> session.getMetadata().getKeyspaces()).values();
        KeyspaceMetadata result = null;
        // Ensure that the error message is deterministic
        List<KeyspaceMetadata> sortedKeyspaces = keyspaces.stream()
                .sorted(Comparator.comparing(keyspaceMetadata -> keyspaceMetadata.getName().asInternal()))
                .collect(toImmutableList());
        for (KeyspaceMetadata keyspace : sortedKeyspaces) {
            if (keyspace.getName().asInternal().equalsIgnoreCase(caseInsensitiveSchemaName)) {
                if (result != null) {
                    throw new TrinoException(
                            NOT_SUPPORTED,
                            format("More than one keyspace has been found for the case insensitive schema name: %s -> (%s, %s)",
                                    caseInsensitiveSchemaName, result.getName(), keyspace.getName()));
                }
                result = keyspace;
            }
        }
        if (result == null) {
            throw new SchemaNotFoundException(caseInsensitiveSchemaName);
        }
        return result;
    }

    private static RelationMetadata getTableMetadata(KeyspaceMetadata keyspace, String caseInsensitiveTableName)
    {
        List<RelationMetadata> tables = Stream.concat(
                keyspace.getTables().values().stream(),
                keyspace.getViews().values().stream())
                .filter(table -> table.getName().asInternal().equalsIgnoreCase(caseInsensitiveTableName))
                .collect(toImmutableList());
        if (tables.size() == 0) {
            throw new TableNotFoundException(new SchemaTableName(keyspace.getName().asInternal(), caseInsensitiveTableName));
        }
        if (tables.size() == 1) {
            return tables.get(0);
        }
        String tableNames = tables.stream()
                .map(metadata -> metadata.getName().asInternal())
                .sorted()
                .collect(joining(", "));
        throw new TrinoException(
                NOT_SUPPORTED,
                format("More than one table has been found for the case insensitive table name: %s -> (%s)",
                        caseInsensitiveTableName, tableNames));
    }

    public boolean isMaterializedView(SchemaTableName schemaTableName)
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseInsensitiveName(schemaTableName.getSchemaName());
        return keyspace.getView(validTableName(schemaTableName.getTableName())).isPresent();
    }

    private static void checkColumnNames(Collection<ColumnMetadata> columns)
    {
        Map<String, ColumnMetadata> lowercaseNameToColumnMap = new HashMap<>();
        for (ColumnMetadata column : columns) {
            String lowercaseName = column.getName().asInternal().toLowerCase(ENGLISH);
            if (lowercaseNameToColumnMap.containsKey(lowercaseName)) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        format("More than one column has been found for the case insensitive column name: %s -> (%s, %s)",
                                lowercaseName, lowercaseNameToColumnMap.get(lowercaseName).getName(), column.getName()));
            }
            lowercaseNameToColumnMap.put(lowercaseName, column);
        }
    }

    private Optional<CassandraColumnHandle> buildColumnHandle(RelationMetadata tableMetadata, ColumnMetadata columnMeta, boolean partitionKey, boolean clusteringKey, int ordinalPosition, boolean hidden)
    {
        Optional<CassandraType> cassandraType = cassandraTypeManager.toCassandraType(columnMeta.getType());
        if (cassandraType.isEmpty()) {
            log.debug("Unsupported column type: %s", columnMeta.getType().asCql(false, false));
            return Optional.empty();
        }

        List<DataType> typeArgs = getTypeArguments(columnMeta.getType());
        for (DataType typeArgument : typeArgs) {
            if (!cassandraTypeManager.isFullySupported(typeArgument)) {
                log.debug("%s column has unsupported type: %s", columnMeta.getName(), typeArgument);
                return Optional.empty();
            }
        }
        boolean indexed = false;
        SchemaTableName schemaTableName = new SchemaTableName(tableMetadata.getKeyspace().asInternal(), tableMetadata.getName().asInternal());
        if (!isMaterializedView(schemaTableName)) {
            TableMetadata table = (TableMetadata) tableMetadata;
            for (IndexMetadata idx : table.getIndexes().values()) {
                if (idx.getTarget().equals(columnMeta.getName().asInternal())) {
                    indexed = true;
                    break;
                }
            }
        }
        return Optional.of(new CassandraColumnHandle(columnMeta.getName().asInternal(), ordinalPosition, cassandraType.get(), partitionKey, clusteringKey, indexed, hidden));
    }

    /**
     * Get the list of partitions matching the given filters on partition keys.
     *
     * @param table the table to get partitions from
     * @param filterPrefixes the list of possible values for each partition key.
     * Order of values should match {@link CassandraTable#getPartitionKeyColumns()}
     * @return list of {@link CassandraPartition}
     */
    public List<CassandraPartition> getPartitions(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        if (filterPrefixes.size() != partitionKeyColumns.size()) {
            return ImmutableList.of(CassandraPartition.UNPARTITIONED);
        }

        Iterable<Row> rows;
        if (getCassandraVersion().compareTo(PARTITION_FETCH_WITH_IN_PREDICATE_VERSION) > 0) {
            log.debug("Using IN predicate to fetch partitions.");
            rows = queryPartitionKeysWithInClauses(table, filterPrefixes);
        }
        else {
            log.debug("Using combination of partition values to fetch partitions.");
            rows = queryPartitionKeysLegacyWithMultipleQueries(table, filterPrefixes);
        }

        ByteBuffer buffer = ByteBuffer.allocate(1000);
        HashMap<ColumnHandle, NullableValue> map = new HashMap<>();
        Set<String> uniquePartitionIds = new HashSet<>();
        StringBuilder stringBuilder = new StringBuilder();

        boolean isComposite = partitionKeyColumns.size() > 1;

        ImmutableList.Builder<CassandraPartition> partitions = ImmutableList.builder();
        for (Row row : rows) {
            buffer.clear();
            map.clear();
            stringBuilder.setLength(0);
            for (int i = 0; i < partitionKeyColumns.size(); i++) {
                ByteBuffer component = row.getBytesUnsafe(i).duplicate();
                if (isComposite) {
                    // build composite key
                    short len = (short) component.limit();
                    buffer.putShort(len);
                    buffer.put(component);
                    buffer.put((byte) 0);
                }
                else {
                    buffer.put(component);
                }
                CassandraColumnHandle columnHandle = partitionKeyColumns.get(i);
                NullableValue keyPart = cassandraTypeManager.getColumnValue(columnHandle.getCassandraType(), row, i);
                map.put(columnHandle, keyPart);
                if (i > 0) {
                    stringBuilder.append(" AND ");
                }
                stringBuilder.append(CassandraCqlUtils.validColumnName(columnHandle.getName()));
                stringBuilder.append(" = ");
                stringBuilder.append(cassandraTypeManager.getColumnValueForCql(columnHandle.getCassandraType(), row, i));
            }
            buffer.flip();
            byte[] key = new byte[buffer.limit()];
            buffer.get(key);
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.fromFixedValues(map);
            String partitionId = stringBuilder.toString();
            if (uniquePartitionIds.add(partitionId)) {
                partitions.add(new CassandraPartition(key, partitionId, tupleDomain, false));
            }
        }
        return partitions.build();
    }

    public ResultSet execute(String cql)
    {
        log.debug("Execute cql: %s", cql);
        return executeWithSession(session -> session.execute(cql));
    }

    public PreparedStatement prepare(SimpleStatement statement)
    {
        log.debug("Execute SimpleStatement: %s", statement);
        return executeWithSession(session -> session.prepare(statement));
    }

    public ResultSet execute(Statement statement)
    {
        return executeWithSession(session -> session.execute(statement));
    }

    private Iterable<Row> queryPartitionKeysWithInClauses(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        CassandraNamedRelationHandle tableHandle = table.getTableHandle();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        Select partitionKeys = selectDistinctFrom(tableHandle, partitionKeyColumns)
                .where(getInRelations(partitionKeyColumns, filterPrefixes));

        log.debug("Execute cql for partition keys with IN clauses: %s", partitionKeys);
        return execute(partitionKeys.build()).all();
    }

    private Iterable<Row> queryPartitionKeysLegacyWithMultipleQueries(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        CassandraNamedRelationHandle tableHandle = table.getTableHandle();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        Set<List<Object>> filterCombinations = Sets.cartesianProduct(filterPrefixes);

        ImmutableList.Builder<Row> rowList = ImmutableList.builder();
        for (List<Object> combination : filterCombinations) {
            Select partitionKeys = selectDistinctFrom(tableHandle, partitionKeyColumns)
                    .where(getEqualityRelations(partitionKeyColumns, combination));

            log.debug("Execute cql for partition keys with multiple queries: %s", partitionKeys);
            List<Row> resultRows = execute(partitionKeys.build()).all();
            if (!resultRows.isEmpty()) {
                rowList.addAll(resultRows);
            }
        }

        return rowList.build();
    }

    private List<Relation> getInRelations(List<CassandraColumnHandle> partitionKeyColumns, List<Set<Object>> filterPrefixes)
    {
        return IntStream
                .range(0, Math.min(partitionKeyColumns.size(), filterPrefixes.size()))
                .mapToObj(i -> getInRelation(partitionKeyColumns.get(i), filterPrefixes.get(i)))
                .collect(toImmutableList());
    }

    private Relation getInRelation(CassandraColumnHandle column, Set<Object> filterPrefixes)
    {
        List<Term> values = filterPrefixes
                .stream()
                .map(value -> cassandraTypeManager.getJavaValue(column.getCassandraType().getKind(), value))
                .map(QueryBuilder::literal)
                .collect(toList());

        return Relation.column(CassandraCqlUtils.validColumnName(column.getName())).in(values);
    }

    private List<Relation> getEqualityRelations(List<CassandraColumnHandle> partitionKeyColumns, List<Object> filterPrefix)
    {
        return IntStream
                .range(0, Math.min(partitionKeyColumns.size(), filterPrefix.size()))
                .mapToObj(i -> {
                    CassandraColumnHandle column = partitionKeyColumns.get(i);
                    Object value = cassandraTypeManager.getJavaValue(column.getCassandraType().getKind(), filterPrefix.get(i));
                    return Relation.column(CassandraCqlUtils.validColumnName(column.getName())).isEqualTo(literal(value));
                })
                .collect(toImmutableList());
    }

    public List<SizeEstimate> getSizeEstimates(String keyspaceName, String tableName)
    {
        checkSizeEstimatesTableExist();
        SimpleStatement statement = selectFrom(SYSTEM, SIZE_ESTIMATES)
                .column("partitions_count")
                .where(Relation.column("keyspace_name").isEqualTo(literal(keyspaceName)),
                        Relation.column("table_name").isEqualTo(literal(tableName)))
                .build();

        ResultSet result = executeWithSession(session -> session.execute(statement));
        ImmutableList.Builder<SizeEstimate> estimates = ImmutableList.builder();
        for (Row row : result.all()) {
            SizeEstimate estimate = new SizeEstimate(row.getLong("partitions_count"));
            estimates.add(estimate);
        }

        return estimates.build();
    }

    private void checkSizeEstimatesTableExist()
    {
        Optional<KeyspaceMetadata> keyspaceMetadata = executeWithSession(session -> session.getMetadata().getKeyspace(SYSTEM));
        checkState(keyspaceMetadata.isPresent(), "system keyspace metadata must not be null");
        Optional<TableMetadata> sizeEstimatesTableMetadata = keyspaceMetadata.flatMap(metadata -> metadata.getTable(SIZE_ESTIMATES));
        if (sizeEstimatesTableMetadata.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Cassandra versions prior to 2.1.5 are not supported");
        }
    }

    private <T> T executeWithSession(SessionCallable<T> sessionCallable)
    {
        ReconnectionPolicy reconnectionPolicy = session.get().getContext().getReconnectionPolicy();
        ReconnectionPolicy.ReconnectionSchedule schedule = reconnectionPolicy.newControlConnectionSchedule(false);
        long deadline = System.currentTimeMillis() + noHostAvailableRetryTimeout.toMillis();
        while (true) {
            try {
                return sessionCallable.executeWithSession(session.get());
            }
            catch (AllNodesFailedException e) {
                long timeLeft = deadline - System.currentTimeMillis();
                if (timeLeft <= 0) {
                    throw e;
                }
                long delay = Math.min(schedule.nextDelay().toMillis(), timeLeft);
                log.warn(e.getMessage());
                log.warn("Reconnecting in %dms", delay);
                try {
                    Thread.sleep(delay);
                }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted", interrupted);
                }
            }
        }
    }

    private List<DataType> getTypeArguments(DataType dataType)
    {
        if (dataType instanceof UserDefinedType userDefinedType) {
            return ImmutableList.copyOf(userDefinedType.getFieldTypes());
        }

        if (dataType instanceof MapType mapType) {
            return ImmutableList.of(mapType.getKeyType(), mapType.getValueType());
        }

        if (dataType instanceof ListType listType) {
            return ImmutableList.of(listType.getElementType());
        }

        if (dataType instanceof TupleType tupleType) {
            return ImmutableList.copyOf(tupleType.getComponentTypes());
        }

        if (dataType instanceof SetType setType) {
            return ImmutableList.of(setType.getElementType());
        }

        return ImmutableList.of();
    }

    @Override
    public void close()
    {
        session.get().close();
    }

    private interface SessionCallable<T>
    {
        T executeWithSession(CqlSession session);
    }
}
