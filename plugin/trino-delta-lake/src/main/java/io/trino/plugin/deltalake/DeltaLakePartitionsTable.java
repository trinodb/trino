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

package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractPartitionColumns;
import static java.util.Objects.requireNonNull;

public class DeltaLakePartitionsTable
        implements SystemTable
{
    private final SchemaTableName tableName;
    private final String tableLocation;
    private final TransactionLogAccess transactionLogAccess;
    private final TypeManager typeManager;
    private final ConnectorTableMetadata tableMetadata;
    private final List<DeltaLakeColumnHandle> partitionColumns;
    private static final DateTimeFormatter DELTA_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
            .toFormatter();

    public DeltaLakePartitionsTable(
            ConnectorSession session,
            SchemaTableName tableName,
            String tableLocation,
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.partitionColumns = getPartitionColumns(new SchemaTableName(tableName.getSchemaName(), DeltaLakeTableName.tableNameFrom(tableName.getTableName())), this.tableLocation, session);

        List<ColumnMetadata> columns = partitionColumns.stream()
                .map(column -> new ColumnMetadata(column.getBaseColumnName(), column.getType()))
                .collect(toImmutableList());

        this.tableMetadata = new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                          TupleDomain<Integer> constraint)
    {
        TableSnapshot tableSnapshot;
        MetadataEntry metadataEntry;
        try {
            // Verify the transaction log is readable
            SchemaTableName baseTableName = new SchemaTableName(tableName.getSchemaName(),
                    DeltaLakeTableName.tableNameFrom(tableName.getTableName()));
            tableSnapshot = transactionLogAccess.loadSnapshot(session, baseTableName, tableLocation);
            metadataEntry = transactionLogAccess.getMetadataEntry(tableSnapshot, session);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    "Unable to load table metadata from location: " + tableLocation, e);
        }
        if (partitionColumns.isEmpty()) {
            return new EmptyPageSource();
        }
        return new FixedPageSource(buildPages(session, tableSnapshot, metadataEntry, constraint));
    }

    private List<Page> buildPages(ConnectorSession session, TableSnapshot tableSnapshot, MetadataEntry metadataEntry,
                                  TupleDomain<Integer> constraint)
    {
        PageListBuilder pageListBuilder = PageListBuilder.forTable(tableMetadata);

        Stream<AddFileEntry> activeFiles = transactionLogAccess.loadActiveFiles(tableSnapshot, metadataEntry,
                transactionLogAccess.getProtocolEntry(session, tableSnapshot), TupleDomain.all(), alwaysTrue(), session);

        for (Map<String, Optional<String>> partitionValue : getDedupedPartitionValues(activeFiles)) {
            if (partitionValue.isEmpty()) {
                pageListBuilder.beginRow();
                pageListBuilder.appendNull();
                pageListBuilder.endRow();
                continue;
            }

            pageListBuilder.beginRow();
            for (DeltaLakeColumnHandle column : partitionColumns) {
                String actualPartitionValue = partitionValue.get(column.getBaseColumnName()).orElse(null);

                if (actualPartitionValue == null) {
                    pageListBuilder.appendNull();
                }
                else {
                    Type columnType = column.getType();
                    switch (columnType) {
                        case BooleanType booleanType ->
                                pageListBuilder.appendBoolean(Boolean.parseBoolean(actualPartitionValue));
                        case TimestampWithTimeZoneType timestampType ->
                                pageListBuilder.appendTimestampTzMillis(DELTA_TIMESTAMP_FORMATTER.parse(actualPartitionValue, LocalDateTime::from).toInstant(ZoneOffset.UTC).toEpochMilli(), TimeZoneKey.UTC_KEY);
                        case IntegerType integerType ->
                                pageListBuilder.appendInteger(Long.parseLong(actualPartitionValue));
                        case TinyintType tinyintType ->
                                pageListBuilder.appendTinyint(Long.parseLong(actualPartitionValue));
                        case SmallintType smallintType ->
                                pageListBuilder.appendSmallint(Long.parseLong(actualPartitionValue));
                        case BigintType bigintType ->
                                pageListBuilder.appendBigint(Long.parseLong(actualPartitionValue));
                        case DecimalType decimalType ->
                                pageListBuilder.appendDecimal(decimalType.getPrecision(), decimalType.getScale(), Int128.valueOf(new BigDecimal(actualPartitionValue).unscaledValue()));
                        case DoubleType doubleType ->
                                pageListBuilder.appendDouble(Double.parseDouble(actualPartitionValue));
                        case DateType dateType ->
                                pageListBuilder.appendDate(LocalDate.parse(actualPartitionValue).toEpochDay());
                        case RealType realType ->
                                pageListBuilder.appendReal(Float.parseFloat(actualPartitionValue));
                        case VarcharType varcharType ->
                                pageListBuilder.appendVarchar(actualPartitionValue);
                        case null, default -> pageListBuilder.appendNull();
                    }
                }
            }
            pageListBuilder.endRow();
        }
        return pageListBuilder.build();
    }

    private ImmutableList<Map<String, Optional<String>>> getDedupedPartitionValues(Stream<AddFileEntry> addFileEntries)
    {
        return addFileEntries
                .map(AddFileEntry::getCanonicalPartitionValues)
                .distinct()
                .collect(toImmutableList());
    }

    private List<DeltaLakeColumnHandle> getPartitionColumns(SchemaTableName tableName, String tableLocation, ConnectorSession session)
    {
        try {
            // Verify the transaction log is readable
            SchemaTableName baseTableName = new SchemaTableName(tableName.getSchemaName(), DeltaLakeTableName.tableNameFrom(tableName.getTableName()));
            TableSnapshot tableSnapshot = transactionLogAccess.loadSnapshot(session, baseTableName, tableLocation);
            MetadataEntry metadata = transactionLogAccess.getMetadataEntry(tableSnapshot, session);
            return extractPartitionColumns(metadata, transactionLogAccess.getProtocolEntry(session, tableSnapshot), typeManager);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Unable to load table metadata from location: " + tableLocation, e);
        }
    }
}
