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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.kudu.properties.ColumnDesign;
import io.trino.plugin.kudu.properties.HashPartitionDefinition;
import io.trino.plugin.kudu.properties.KuduColumnProperties;
import io.trino.plugin.kudu.properties.KuduTableProperties;
import io.trino.plugin.kudu.properties.PartitionDesign;
import io.trino.plugin.kudu.properties.RangePartition;
import io.trino.plugin.kudu.properties.RangePartitionDefinition;
import io.trino.plugin.kudu.schema.SchemaEmulation;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.DiscreteValues;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import jakarta.annotation.PreDestroy;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.LocatedTablet.Replica;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.PartitionSchema.HashBucketSchema;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.HostAddress.fromParts;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;
import static org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import static org.apache.kudu.ColumnSchema.CompressionAlgorithm;
import static org.apache.kudu.ColumnSchema.Encoding;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER_EQUAL;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS_EQUAL;

public class KuduClientSession
{
    private static final Logger log = Logger.get(KuduClientSession.class);
    public static final String DEFAULT_SCHEMA = "default";
    private final KuduClientWrapper client;
    private final SchemaEmulation schemaEmulation;
    private final boolean allowLocalScheduling;

    public KuduClientSession(KuduClientWrapper client, SchemaEmulation schemaEmulation, boolean allowLocalScheduling)
    {
        this.client = client;
        this.schemaEmulation = schemaEmulation;
        this.allowLocalScheduling = allowLocalScheduling;
    }

    public List<String> listSchemaNames()
    {
        return schemaEmulation.listSchemaNames(client);
    }

    private List<String> internalListTables(String prefix)
    {
        try {
            if (prefix.isEmpty()) {
                return client.getTablesList().getTablesList();
            }
            return client.getTablesList(prefix).getTablesList();
        }
        catch (KuduException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public List<SchemaTableName> listTables(Optional<String> optSchemaName)
    {
        if (optSchemaName.isPresent()) {
            return listTablesSingleSchema(optSchemaName.get());
        }

        List<SchemaTableName> all = new ArrayList<>();
        for (String schemaName : listSchemaNames()) {
            List<SchemaTableName> single = listTablesSingleSchema(schemaName);
            all.addAll(single);
        }
        return all;
    }

    private List<SchemaTableName> listTablesSingleSchema(String schemaName)
    {
        String prefix = schemaEmulation.getPrefixForTablesOfSchema(schemaName);

        List<String> tables = internalListTables(prefix);
        if (schemaName.equals(DEFAULT_SCHEMA)) {
            tables = schemaEmulation.filterTablesForDefaultSchema(tables);
        }
        return tables.stream()
                .map(schemaEmulation::fromRawName)
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    public Schema getTableSchema(KuduTableHandle tableHandle)
    {
        KuduTable table = tableHandle.getTable(this);
        return table.getSchema();
    }

    public Map<String, Object> getTableProperties(KuduTableHandle tableHandle)
    {
        KuduTable table = tableHandle.getTable(this);
        return KuduTableProperties.toMap(table);
    }

    public List<KuduSplit> buildKuduSplits(KuduTableHandle tableHandle, DynamicFilter dynamicFilter)
    {
        KuduTable table = tableHandle.getTable(this);
        int primaryKeyColumnCount = table.getSchema().getPrimaryKeyColumnCount();
        KuduScanToken.KuduScanTokenBuilder builder = client.newScanTokenBuilder(table);
        // TODO: remove when kudu client bug is fixed: https://gerrit.cloudera.org/#/c/18166/
        builder.includeTabletMetadata(false);

        TupleDomain<ColumnHandle> constraint = tableHandle.getConstraint()
                .intersect(dynamicFilter.getCurrentPredicate().simplify(100));
        if (constraint.isNone()) {
            return ImmutableList.of();
        }
        addConstraintPredicates(table, builder, constraint);
        Optional<List<ColumnHandle>> desiredColumns = tableHandle.getDesiredColumns();

        List<Integer> columnIndexes;
        if (tableHandle.isRequiresRowId()) {
            if (desiredColumns.isPresent()) {
                columnIndexes = IntStream
                        .range(0, primaryKeyColumnCount)
                        .boxed().collect(toList());
                for (ColumnHandle column : desiredColumns.get()) {
                    KuduColumnHandle k = (KuduColumnHandle) column;
                    int index = k.ordinalPosition();
                    if (index >= primaryKeyColumnCount) {
                        columnIndexes.add(index);
                    }
                }
                columnIndexes = ImmutableList.copyOf(columnIndexes);
            }
            else {
                columnIndexes = IntStream
                        .range(0, table.getSchema().getColumnCount())
                        .boxed().collect(toImmutableList());
            }
        }
        else {
            if (desiredColumns.isPresent()) {
                columnIndexes = desiredColumns.get().stream()
                        .map(handle -> ((KuduColumnHandle) handle).ordinalPosition())
                        .collect(toImmutableList());
            }
            else {
                ImmutableList.Builder<Integer> columnIndexesBuilder = ImmutableList.builder();
                Schema schema = table.getSchema();
                for (int ordinal = 0; ordinal < schema.getColumnCount(); ordinal++) {
                    ColumnSchema column = schema.getColumnByIndex(ordinal);
                    // Skip hidden "row_uuid" column
                    if (!column.isKey() || !column.getName().equals(KuduColumnHandle.ROW_ID)) {
                        columnIndexesBuilder.add(ordinal);
                    }
                }
                columnIndexes = columnIndexesBuilder.build();
            }
        }

        builder.setProjectedColumnIndexes(columnIndexes);
        tableHandle.getLimit().ifPresent(builder::limit);

        List<KuduScanToken> tokens = builder.build();
        ImmutableList.Builder<KuduSplit> tokenBuilder = ImmutableList.builder();
        List<HashBucketSchema> hashBucketSchemas = table.getPartitionSchema().getHashBucketSchemas();
        for (KuduScanToken token : tokens) {
            List<Integer> hashBuckets = token.getTablet().getPartition().getHashBuckets();
            int bucket = KuduBucketFunction.getBucket(hashBuckets, hashBucketSchemas);
            tokenBuilder.add(toKuduSplit(tableHandle, token, primaryKeyColumnCount, bucket));
        }
        return tokenBuilder.build();
    }

    public KuduScanner createScanner(KuduSplit kuduSplit)
    {
        try {
            return client.deserializeIntoScanner(kuduSplit.getSerializedScanToken());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public KuduTable openTable(SchemaTableName schemaTableName)
    {
        String rawName = schemaEmulation.toRawName(schemaTableName);
        try {
            return client.openTable(rawName);
        }
        catch (KuduException e) {
            log.debug(e, "Error on doOpenTable");
            if (!listSchemaNames().contains(schemaTableName.getSchemaName())) {
                throw new SchemaNotFoundException(schemaTableName.getSchemaName(), e);
            }
            throw new TableNotFoundException(schemaTableName, e);
        }
    }

    public KuduSession newSession()
    {
        return client.newSession();
    }

    public void createSchema(String schemaName)
    {
        schemaEmulation.createSchema(client, schemaName);
    }

    public void dropSchema(String schemaName, boolean cascade)
    {
        schemaEmulation.dropSchema(client, schemaName, cascade);
    }

    public void dropTable(SchemaTableName schemaTableName)
    {
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            client.deleteTable(rawName);
        }
        catch (KuduException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void renameTable(SchemaTableName schemaTableName, SchemaTableName newSchemaTableName)
    {
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            String newRawName = schemaEmulation.toRawName(newSchemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            alterOptions.renameTable(newRawName);
            client.alterTable(rawName, alterOptions);
        }
        catch (KuduException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public KuduTable createTable(ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        try {
            String rawName = schemaEmulation.toRawName(tableMetadata.getTable());
            if (ignoreExisting) {
                if (client.tableExists(rawName)) {
                    return null;
                }
            }

            if (!schemaEmulation.existsSchema(client, tableMetadata.getTable().getSchemaName())) {
                throw new SchemaNotFoundException(tableMetadata.getTable().getSchemaName());
            }

            List<ColumnMetadata> columns = tableMetadata.getColumns();
            Map<String, Object> properties = tableMetadata.getProperties();

            Schema schema = buildSchema(columns);
            CreateTableOptions options = buildCreateTableOptions(schema, properties);
            tableMetadata.getComment().ifPresent(options::setComment);
            return client.createTable(rawName, schema, options);
        }
        catch (KuduException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void addColumn(SchemaTableName schemaTableName, ColumnMetadata column)
    {
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            Type type = TypeHelper.toKuduClientType(column.getType());
            ColumnSchemaBuilder builder = new ColumnSchemaBuilder(column.getName(), type)
                    .nullable(true)
                    .defaultValue(null)
                    .comment(nullToEmpty(column.getComment())); // Kudu doesn't allow null comment
            setTypeAttributes(column, builder);
            alterOptions.addColumn(builder.build());
            client.alterTable(rawName, alterOptions);
        }
        catch (KuduException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void dropColumn(SchemaTableName schemaTableName, String name)
    {
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            alterOptions.dropColumn(name);
            client.alterTable(rawName, alterOptions);
        }
        catch (KuduException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void renameColumn(SchemaTableName schemaTableName, String oldName, String newName)
    {
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            alterOptions.renameColumn(oldName, newName);
            client.alterTable(rawName, alterOptions);
        }
        catch (KuduException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void addRangePartition(SchemaTableName schemaTableName, RangePartition rangePartition)
    {
        changeRangePartition(schemaTableName, rangePartition, RangePartitionChange.ADD);
    }

    public void dropRangePartition(SchemaTableName schemaTableName, RangePartition rangePartition)
    {
        changeRangePartition(schemaTableName, rangePartition, RangePartitionChange.DROP);
    }

    private void changeRangePartition(SchemaTableName schemaTableName, RangePartition rangePartition,
            RangePartitionChange change)
    {
        try {
            String rawName = schemaEmulation.toRawName(schemaTableName);
            KuduTable table = client.openTable(rawName);
            Schema schema = table.getSchema();
            PartitionDesign design = KuduTableProperties.getPartitionDesign(table);
            RangePartitionDefinition definition = design.getRange();
            if (definition == null) {
                throw new TrinoException(QUERY_REJECTED, "Table " + schemaTableName + " has no range partition");
            }
            PartialRow lowerBound = KuduTableProperties.toRangeBoundToPartialRow(schema, definition, rangePartition.lower());
            PartialRow upperBound = KuduTableProperties.toRangeBoundToPartialRow(schema, definition, rangePartition.upper());
            AlterTableOptions alterOptions = new AlterTableOptions();
            switch (change) {
                case ADD:
                    alterOptions.addRangePartition(lowerBound, upperBound);
                    break;
                case DROP:
                    alterOptions.dropRangePartition(lowerBound, upperBound);
                    break;
            }
            client.alterTable(rawName, alterOptions);
        }
        catch (KuduException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private Schema buildSchema(List<ColumnMetadata> columns)
    {
        List<ColumnSchema> kuduColumns = columns.stream()
                .map(this::toColumnSchema)
                .collect(toImmutableList());
        return new Schema(kuduColumns);
    }

    private ColumnSchema toColumnSchema(ColumnMetadata columnMetadata)
    {
        String name = columnMetadata.getName();
        ColumnDesign design = KuduColumnProperties.getColumnDesign(columnMetadata.getProperties());
        Type ktype = TypeHelper.toKuduClientType(columnMetadata.getType());
        ColumnSchemaBuilder builder = new ColumnSchemaBuilder(name, ktype);
        builder.key(design.isPrimaryKey()).nullable(design.isNullable());
        setEncoding(name, builder, design);
        setCompression(name, builder, design);
        setTypeAttributes(columnMetadata, builder);
        return builder.build();
    }

    private void setTypeAttributes(ColumnMetadata columnMetadata, ColumnSchemaBuilder builder)
    {
        if (columnMetadata.getType() instanceof DecimalType type) {
            ColumnTypeAttributes attributes = new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
                    .precision(type.getPrecision())
                    .scale(type.getScale()).build();
            builder.typeAttributes(attributes);
        }
    }

    private void setCompression(String name, ColumnSchemaBuilder builder, ColumnDesign design)
    {
        if (design.getCompression() != null) {
            try {
                CompressionAlgorithm algorithm = KuduColumnProperties.lookupCompression(design.getCompression());
                builder.compressionAlgorithm(algorithm);
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unknown compression algorithm " + design.getCompression() + " for column " + name);
            }
        }
    }

    private void setEncoding(String name, ColumnSchemaBuilder builder, ColumnDesign design)
    {
        if (design.getEncoding() != null) {
            try {
                Encoding encoding = KuduColumnProperties.lookupEncoding(design.getEncoding());
                builder.encoding(encoding);
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unknown encoding " + design.getEncoding() + " for column " + name);
            }
        }
    }

    private CreateTableOptions buildCreateTableOptions(Schema schema, Map<String, Object> properties)
    {
        CreateTableOptions options = new CreateTableOptions();

        RangePartitionDefinition rangePartitionDefinition = null;
        PartitionDesign partitionDesign = KuduTableProperties.getPartitionDesign(properties);
        if (partitionDesign.getHash() != null) {
            for (HashPartitionDefinition partition : partitionDesign.getHash()) {
                options.addHashPartitions(partition.columns(), partition.buckets());
            }
        }
        if (partitionDesign.getRange() != null) {
            rangePartitionDefinition = partitionDesign.getRange();
            options.setRangePartitionColumns(rangePartitionDefinition.columns());
        }

        List<RangePartition> rangePartitions = KuduTableProperties.getRangePartitions(properties);
        if (rangePartitionDefinition != null && !rangePartitions.isEmpty()) {
            for (RangePartition rangePartition : rangePartitions) {
                PartialRow lower = KuduTableProperties.toRangeBoundToPartialRow(schema, rangePartitionDefinition, rangePartition.lower());
                PartialRow upper = KuduTableProperties.toRangeBoundToPartialRow(schema, rangePartitionDefinition, rangePartition.upper());
                options.addRangePartition(lower, upper);
            }
        }

        Optional<Integer> numReplicas = KuduTableProperties.getNumReplicas(properties);
        numReplicas.ifPresent(options::setNumReplicas);

        return options;
    }

    /**
     * translates TupleDomain to KuduPredicates.
     */
    private void addConstraintPredicates(KuduTable table, KuduScanToken.KuduScanTokenBuilder builder, TupleDomain<ColumnHandle> constraintSummary)
    {
        verify(!constraintSummary.isNone(), "constraintSummary is none");

        if (constraintSummary.isAll()) {
            return;
        }

        Schema schema = table.getSchema();
        constraintSummary.getDomains().orElseThrow().forEach((columnHandle, domain) -> {
            int position = ((KuduColumnHandle) columnHandle).ordinalPosition();
            ColumnSchema columnSchema = schema.getColumnByIndex(position);
            verify(!domain.isNone(), "Domain is none");
            if (domain.isAll()) {
                // no restriction
            }
            else if (domain.isOnlyNull()) {
                builder.addPredicate(KuduPredicate.newIsNullPredicate(columnSchema));
            }
            else if (!domain.getValues().isNone() && domain.isNullAllowed()) {
                // no restriction
            }
            else if (domain.getValues().isAll() && !domain.isNullAllowed()) {
                builder.addPredicate(KuduPredicate.newIsNotNullPredicate(columnSchema));
            }
            else if (domain.isSingleValue()) {
                KuduPredicate predicate = createEqualsPredicate(columnSchema, domain.getSingleValue());
                builder.addPredicate(predicate);
            }
            else {
                ValueSet valueSet = domain.getValues();
                if (valueSet instanceof EquatableValueSet) {
                    DiscreteValues discreteValues = valueSet.getDiscreteValues();
                    KuduPredicate predicate = createInListPredicate(columnSchema, discreteValues);
                    builder.addPredicate(predicate);
                }
                else if (valueSet instanceof SortedRangeSet sortedRangeSet) {
                    Ranges ranges = sortedRangeSet.getRanges();
                    List<Range> rangeList = ranges.getOrderedRanges();
                    if (rangeList.stream().allMatch(Range::isSingleValue)) {
                        io.trino.spi.type.Type type = TypeHelper.fromKuduColumn(columnSchema);
                        List<Object> javaValues = rangeList.stream()
                                .map(range -> TypeHelper.getJavaValue(type, range.getSingleValue()))
                                .collect(toImmutableList());
                        KuduPredicate predicate = KuduPredicate.newInListPredicate(columnSchema, javaValues);
                        builder.addPredicate(predicate);
                    }
                    else {
                        Range span = ranges.getSpan();
                        if (!span.isLowUnbounded()) {
                            KuduPredicate.ComparisonOp op = span.isLowInclusive() ? GREATER_EQUAL : GREATER;
                            KuduPredicate predicate = createComparisonPredicate(columnSchema, op, span.getLowBoundedValue());
                            builder.addPredicate(predicate);
                        }
                        if (!span.isHighUnbounded()) {
                            KuduPredicate.ComparisonOp op = span.isHighInclusive() ? LESS_EQUAL : LESS;
                            KuduPredicate predicate = createComparisonPredicate(columnSchema, op, span.getHighBoundedValue());
                            builder.addPredicate(predicate);
                        }
                    }
                }
                else {
                    throw new IllegalStateException("Unexpected domain: " + domain);
                }
            }
        });
    }

    private KuduPredicate createInListPredicate(ColumnSchema columnSchema, DiscreteValues discreteValues)
    {
        io.trino.spi.type.Type type = TypeHelper.fromKuduColumn(columnSchema);
        List<Object> javaValues = discreteValues.getValues().stream().map(value -> TypeHelper.getJavaValue(type, value)).collect(toImmutableList());
        return KuduPredicate.newInListPredicate(columnSchema, javaValues);
    }

    private KuduPredicate createEqualsPredicate(ColumnSchema columnSchema, Object value)
    {
        return createComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, value);
    }

    private KuduPredicate createComparisonPredicate(ColumnSchema columnSchema, KuduPredicate.ComparisonOp op, Object value)
    {
        io.trino.spi.type.Type type = TypeHelper.fromKuduColumn(columnSchema);
        Object javaValue = TypeHelper.getJavaValue(type, value);
        if (javaValue instanceof Long longValue) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, longValue);
        }
        if (javaValue instanceof BigDecimal bigDecimal) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, bigDecimal);
        }
        if (javaValue instanceof Integer integerValue) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, integerValue);
        }
        if (javaValue instanceof Short shortValue) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, shortValue);
        }
        if (javaValue instanceof Byte byteValue) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, byteValue);
        }
        if (javaValue instanceof String stringValue) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, stringValue);
        }
        if (javaValue instanceof Double doubleValue) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, doubleValue);
        }
        if (javaValue instanceof Float floatValue) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, floatValue);
        }
        if (javaValue instanceof Boolean booleanValue) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, booleanValue);
        }
        if (javaValue instanceof byte[] byteArrayValue) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, byteArrayValue);
        }
        if (javaValue instanceof ByteBuffer byteBuffer) {
            Slice slice = Slices.wrappedHeapBuffer(byteBuffer);
            return KuduPredicate.newComparisonPredicate(columnSchema, op, slice.getBytes(0, slice.length()));
        }
        if (javaValue == null) {
            throw new IllegalStateException("Unexpected null java value for column " + columnSchema.getName());
        }
        throw new IllegalStateException("Unexpected java value for column "
                + columnSchema.getName() + ": " + javaValue + "(" + javaValue.getClass() + ")");
    }

    private KuduSplit toKuduSplit(KuduTableHandle tableHandle, KuduScanToken token, int primaryKeyColumnCount, int bucketNumber)
    {
        try {
            byte[] serializedScanToken = token.serialize();
            List<HostAddress> addresses = ImmutableList.of();
            if (allowLocalScheduling) {
                List<Replica> replicas = token.getTablet().getReplicas();
                // KuduScanTokenBuilder uses ReplicaSelection.LEADER_ONLY by default, see org.apache.kudu.client.AbstractKuduScannerBuilder,
                // because use ReplicaSelection.CLOSEST_REPLICA may cause slow queries when tablet followers' data lag behind tablet leaders',
                // in this condition followers will wait until its data is synchronized with leaders' before returning
                addresses = replicas.stream()
                        .filter(replica -> replica.getRole().toLowerCase(ENGLISH).equals("leader"))
                        .map(replica -> fromParts(replica.getRpcHost(), replica.getRpcPort()))
                        .collect(toImmutableList());
            }

            return new KuduSplit(tableHandle.getSchemaTableName(), primaryKeyColumnCount, serializedScanToken, bucketNumber, addresses);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @PreDestroy
    public void close()
            throws KuduException
    {
        this.client.close();
    }
}
