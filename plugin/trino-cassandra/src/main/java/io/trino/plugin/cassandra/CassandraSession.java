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

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy.ReconnectionSchedule;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.transform;
import static io.trino.plugin.cassandra.CassandraErrorCode.CASSANDRA_VERSION_ERROR;
import static io.trino.plugin.cassandra.CassandraMetadata.PRESTO_COMMENT_METADATA;
import static io.trino.plugin.cassandra.CassandraType.isFullySupported;
import static io.trino.plugin.cassandra.CassandraType.toCassandraType;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.selectDistinctFrom;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.validSchemaName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class CassandraSession
{
    private static final Logger log = Logger.get(CassandraSession.class);

    private static final String SYSTEM = "system";
    private static final String SIZE_ESTIMATES = "size_estimates";
    private static final VersionNumber PARTITION_FETCH_WITH_IN_PREDICATE_VERSION = VersionNumber.parse("2.2");

    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;
    private final Cluster cluster;
    private final Supplier<Session> session;
    private final Duration noHostAvailableRetryTimeout;

    public CassandraSession(JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec, Cluster cluster, Duration noHostAvailableRetryTimeout)
    {
        this.extraColumnMetadataCodec = requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");
        this.cluster = requireNonNull(cluster, "cluster is null");
        this.noHostAvailableRetryTimeout = requireNonNull(noHostAvailableRetryTimeout, "noHostAvailableRetryTimeout is null");
        this.session = memoize(cluster::connect);
    }

    public VersionNumber getCassandraVersion()
    {
        ResultSet result = executeWithSession(session -> session.execute("select release_version from system.local"));
        Row versionRow = result.one();
        if (versionRow == null) {
            throw new TrinoException(CASSANDRA_VERSION_ERROR, "The cluster version is not available. " +
                    "Please make sure that the Cassandra cluster is up and running, " +
                    "and that the contact points are specified correctly.");
        }
        return VersionNumber.parse(versionRow.getString("release_version"));
    }

    public ProtocolVersion getProtocolVersion()
    {
        return executeWithSession(session -> session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion());
    }

    public String getPartitioner()
    {
        return executeWithSession(session -> session.getCluster().getMetadata().getPartitioner());
    }

    public Set<TokenRange> getTokenRanges()
    {
        return executeWithSession(session -> session.getCluster().getMetadata().getTokenRanges());
    }

    public Set<Host> getReplicas(String caseSensitiveSchemaName, TokenRange tokenRange)
    {
        requireNonNull(caseSensitiveSchemaName, "caseSensitiveSchemaName is null");
        requireNonNull(tokenRange, "tokenRange is null");
        return executeWithSession(session ->
                session.getCluster().getMetadata().getReplicas(validSchemaName(caseSensitiveSchemaName), tokenRange));
    }

    public Set<Host> getReplicas(String caseSensitiveSchemaName, ByteBuffer partitionKey)
    {
        requireNonNull(caseSensitiveSchemaName, "caseSensitiveSchemaName is null");
        requireNonNull(partitionKey, "partitionKey is null");
        return executeWithSession(session ->
                session.getCluster().getMetadata().getReplicas(validSchemaName(caseSensitiveSchemaName), partitionKey));
    }

    public String getCaseSensitiveSchemaName(String caseInsensitiveSchemaName)
    {
        return getKeyspaceByCaseInsensitiveName(caseInsensitiveSchemaName).getName();
    }

    public List<String> getCaseSensitiveSchemaNames()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        List<KeyspaceMetadata> keyspaces = executeWithSession(session -> session.getCluster().getMetadata().getKeyspaces());
        for (KeyspaceMetadata meta : keyspaces) {
            builder.add(meta.getName());
        }
        return builder.build();
    }

    public List<String> getCaseSensitiveTableNames(String caseInsensitiveSchemaName)
            throws SchemaNotFoundException
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseInsensitiveName(caseInsensitiveSchemaName);
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (TableMetadata table : keyspace.getTables()) {
            builder.add(table.getName());
        }
        for (MaterializedViewMetadata materializedView : keyspace.getMaterializedViews()) {
            builder.add(materializedView.getName());
        }
        return builder.build();
    }

    public CassandraTable getTable(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseInsensitiveName(schemaTableName.getSchemaName());
        AbstractTableMetadata tableMeta = getTableMetadata(keyspace, schemaTableName.getTableName());

        List<String> columnNames = new ArrayList<>();
        List<ColumnMetadata> columns = tableMeta.getColumns();
        checkColumnNames(columns);
        for (ColumnMetadata columnMetadata : columns) {
            columnNames.add(columnMetadata.getName());
        }

        // check if there is a comment to establish column ordering
        String comment = tableMeta.getOptions().getComment();
        Set<String> hiddenColumns = ImmutableSet.of();
        if (comment != null && comment.startsWith(PRESTO_COMMENT_METADATA)) {
            String columnOrderingString = comment.substring(PRESTO_COMMENT_METADATA.length());

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
        Set<String> primaryKeySet = new HashSet<>();
        for (ColumnMetadata columnMeta : tableMeta.getPartitionKey()) {
            primaryKeySet.add(columnMeta.getName());
            boolean hidden = hiddenColumns.contains(columnMeta.getName());
            CassandraColumnHandle columnHandle = buildColumnHandle(tableMeta, columnMeta, true, false, columnNames.indexOf(columnMeta.getName()), hidden)
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unsupported partition key type: " + columnMeta.getType().getName()));
            columnHandles.add(columnHandle);
        }

        // add clustering columns
        for (ColumnMetadata columnMeta : tableMeta.getClusteringColumns()) {
            primaryKeySet.add(columnMeta.getName());
            boolean hidden = hiddenColumns.contains(columnMeta.getName());
            Optional<CassandraColumnHandle> columnHandle = buildColumnHandle(tableMeta, columnMeta, false, true, columnNames.indexOf(columnMeta.getName()), hidden);
            columnHandle.ifPresent(columnHandles::add);
        }

        // add other columns
        for (ColumnMetadata columnMeta : columns) {
            if (!primaryKeySet.contains(columnMeta.getName())) {
                boolean hidden = hiddenColumns.contains(columnMeta.getName());
                Optional<CassandraColumnHandle> columnHandle = buildColumnHandle(tableMeta, columnMeta, false, false, columnNames.indexOf(columnMeta.getName()), hidden);
                columnHandle.ifPresent(columnHandles::add);
            }
        }

        List<CassandraColumnHandle> sortedColumnHandles = columnHandles.build().stream()
                .sorted(comparing(CassandraColumnHandle::getOrdinalPosition))
                .collect(toList());

        CassandraTableHandle tableHandle = new CassandraTableHandle(tableMeta.getKeyspace().getName(), tableMeta.getName());
        return new CassandraTable(tableHandle, sortedColumnHandles);
    }

    private KeyspaceMetadata getKeyspaceByCaseInsensitiveName(String caseInsensitiveSchemaName)
            throws SchemaNotFoundException
    {
        List<KeyspaceMetadata> keyspaces = executeWithSession(session -> session.getCluster().getMetadata().getKeyspaces());
        KeyspaceMetadata result = null;
        // Ensure that the error message is deterministic
        List<KeyspaceMetadata> sortedKeyspaces = Ordering.from(comparing(KeyspaceMetadata::getName)).immutableSortedCopy(keyspaces);
        for (KeyspaceMetadata keyspace : sortedKeyspaces) {
            if (keyspace.getName().equalsIgnoreCase(caseInsensitiveSchemaName)) {
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

    private static AbstractTableMetadata getTableMetadata(KeyspaceMetadata keyspace, String caseInsensitiveTableName)
    {
        List<AbstractTableMetadata> tables = Stream.concat(
                keyspace.getTables().stream(),
                keyspace.getMaterializedViews().stream())
                .filter(table -> table.getName().equalsIgnoreCase(caseInsensitiveTableName))
                .collect(toImmutableList());
        if (tables.size() == 0) {
            throw new TableNotFoundException(new SchemaTableName(keyspace.getName(), caseInsensitiveTableName));
        }
        if (tables.size() == 1) {
            return tables.get(0);
        }
        String tableNames = tables.stream()
                .map(AbstractTableMetadata::getName)
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
        return keyspace.getMaterializedView(schemaTableName.getTableName()) != null;
    }

    private static void checkColumnNames(List<ColumnMetadata> columns)
    {
        Map<String, ColumnMetadata> lowercaseNameToColumnMap = new HashMap<>();
        for (ColumnMetadata column : columns) {
            String lowercaseName = column.getName().toLowerCase(ENGLISH);
            if (lowercaseNameToColumnMap.containsKey(lowercaseName)) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        format("More than one column has been found for the case insensitive column name: %s -> (%s, %s)",
                                lowercaseName, lowercaseNameToColumnMap.get(lowercaseName).getName(), column.getName()));
            }
            lowercaseNameToColumnMap.put(lowercaseName, column);
        }
    }

    private Optional<CassandraColumnHandle> buildColumnHandle(AbstractTableMetadata tableMetadata, ColumnMetadata columnMeta, boolean partitionKey, boolean clusteringKey, int ordinalPosition, boolean hidden)
    {
        Optional<CassandraType> cassandraType = toCassandraType(columnMeta.getType());
        if (cassandraType.isEmpty()) {
            log.debug("Unsupported column type: %s", columnMeta.getType().getName());
            return Optional.empty();
        }

        List<DataType> typeArgs = columnMeta.getType().getTypeArguments();
        for (DataType typeArgument : typeArgs) {
            if (!isFullySupported(typeArgument)) {
                log.debug("%s column has unsupported type: %s", columnMeta.getName(), typeArgument);
                return Optional.empty();
            }
        }
        boolean indexed = false;
        SchemaTableName schemaTableName = new SchemaTableName(tableMetadata.getKeyspace().getName(), tableMetadata.getName());
        if (!isMaterializedView(schemaTableName)) {
            TableMetadata table = (TableMetadata) tableMetadata;
            for (IndexMetadata idx : table.getIndexes()) {
                if (idx.getTarget().equals(columnMeta.getName())) {
                    indexed = true;
                    break;
                }
            }
        }
        return Optional.of(new CassandraColumnHandle(columnMeta.getName(), ordinalPosition, cassandraType.get(), partitionKey, clusteringKey, indexed, hidden));
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

        if (rows == null) {
            // just split the whole partition range
            return ImmutableList.of(CassandraPartition.UNPARTITIONED);
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
                ByteBuffer component = row.getBytesUnsafe(i);
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
                NullableValue keyPart = columnHandle.getCassandraType().getColumnValue(row, i);
                map.put(columnHandle, keyPart);
                if (i > 0) {
                    stringBuilder.append(" AND ");
                }
                stringBuilder.append(CassandraCqlUtils.validColumnName(columnHandle.getName()));
                stringBuilder.append(" = ");
                stringBuilder.append(columnHandle.getCassandraType().getColumnValueForCql(row, i));
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

    public PreparedStatement prepare(RegularStatement statement)
    {
        log.debug("Execute RegularStatement: %s", statement);
        return executeWithSession(session -> session.prepare(statement));
    }

    public ResultSet execute(Statement statement)
    {
        return executeWithSession(session -> session.execute(statement));
    }

    private Iterable<Row> queryPartitionKeysWithInClauses(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        CassandraTableHandle tableHandle = table.getTableHandle();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        Select partitionKeys = selectDistinctFrom(tableHandle, partitionKeyColumns);
        addWhereInClauses(partitionKeys.where(), partitionKeyColumns, filterPrefixes);

        log.debug("Execute cql for partition keys with IN clauses: %s", partitionKeys);
        return execute(partitionKeys).all();
    }

    private Iterable<Row> queryPartitionKeysLegacyWithMultipleQueries(CassandraTable table, List<Set<Object>> filterPrefixes)
    {
        CassandraTableHandle tableHandle = table.getTableHandle();
        List<CassandraColumnHandle> partitionKeyColumns = table.getPartitionKeyColumns();

        Set<List<Object>> filterCombinations = Sets.cartesianProduct(filterPrefixes);

        ImmutableList.Builder<Row> rowList = ImmutableList.builder();
        for (List<Object> combination : filterCombinations) {
            Select partitionKeys = selectDistinctFrom(tableHandle, partitionKeyColumns);
            addWhereClause(partitionKeys.where(), partitionKeyColumns, combination);

            log.debug("Execute cql for partition keys with multiple queries: %s", partitionKeys);
            List<Row> resultRows = execute(partitionKeys).all();
            if (resultRows != null && !resultRows.isEmpty()) {
                rowList.addAll(resultRows);
            }
        }

        return rowList.build();
    }

    private static void addWhereInClauses(Select.Where where, List<CassandraColumnHandle> partitionKeyColumns, List<Set<Object>> filterPrefixes)
    {
        for (int i = 0; i < filterPrefixes.size(); i++) {
            CassandraColumnHandle column = partitionKeyColumns.get(i);
            List<Object> values = filterPrefixes.get(i)
                    .stream()
                    .map(value -> column.getCassandraType().getJavaValue(value))
                    .collect(toList());
            Clause clause = QueryBuilder.in(CassandraCqlUtils.validColumnName(column.getName()), values);
            where.and(clause);
        }
    }

    private static void addWhereClause(Select.Where where, List<CassandraColumnHandle> partitionKeyColumns, List<Object> filterPrefix)
    {
        for (int i = 0; i < filterPrefix.size(); i++) {
            CassandraColumnHandle column = partitionKeyColumns.get(i);
            Object value = column.getCassandraType().getJavaValue(filterPrefix.get(i));
            Clause clause = QueryBuilder.eq(CassandraCqlUtils.validColumnName(column.getName()), value);
            where.and(clause);
        }
    }

    public List<SizeEstimate> getSizeEstimates(String keyspaceName, String tableName)
    {
        checkSizeEstimatesTableExist();
        Statement statement = select("partitions_count")
                .from(SYSTEM, SIZE_ESTIMATES)
                .where(eq("keyspace_name", keyspaceName))
                .and(eq("table_name", tableName));

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
        KeyspaceMetadata keyspaceMetadata = executeWithSession(session -> session.getCluster().getMetadata().getKeyspace(SYSTEM));
        checkState(keyspaceMetadata != null, "system keyspace metadata must not be null");
        TableMetadata table = keyspaceMetadata.getTable(SIZE_ESTIMATES);
        if (table == null) {
            throw new TrinoException(NOT_SUPPORTED, "Cassandra versions prior to 2.1.5 are not supported");
        }
    }

    private <T> T executeWithSession(SessionCallable<T> sessionCallable)
    {
        ReconnectionPolicy reconnectionPolicy = cluster.getConfiguration().getPolicies().getReconnectionPolicy();
        ReconnectionSchedule schedule = reconnectionPolicy.newSchedule();
        long deadline = System.currentTimeMillis() + noHostAvailableRetryTimeout.toMillis();
        while (true) {
            try {
                return sessionCallable.executeWithSession(session.get());
            }
            catch (NoHostAvailableException e) {
                long timeLeft = deadline - System.currentTimeMillis();
                if (timeLeft <= 0) {
                    throw e;
                }
                else {
                    long delay = Math.min(schedule.nextDelayMs(), timeLeft);
                    log.warn(e.getCustomMessage(10, true, true));
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
    }

    private interface SessionCallable<T>
    {
        T executeWithSession(Session session);
    }
}
