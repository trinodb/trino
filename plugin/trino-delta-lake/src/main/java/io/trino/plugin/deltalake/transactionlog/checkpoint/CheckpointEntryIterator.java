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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.LongMath;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionEntry;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeParquetFileStatistics;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnHandle.ColumnType;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import jakarta.annotation.Nullable;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.isDeletionVectorEnabled;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogAccess.columnsWithStats;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.START_OF_MODERN_ERA_EPOCH_DAY;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.COMMIT;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.METADATA;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.REMOVE;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.TRANSACTION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.floorDiv;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class CheckpointEntryIterator
        extends AbstractIterator<DeltaLakeTransactionLogEntry>
{
    public enum EntryType
    {
        TRANSACTION("txn"),
        ADD("add"),
        REMOVE("remove"),
        METADATA("metadata"),
        PROTOCOL("protocol"),
        COMMIT("commitinfo");

        private final String columnName;

        EntryType(String columnName)
        {
            this.columnName = columnName;
        }

        public String getColumnName()
        {
            return columnName;
        }
    }

    private static final Logger log = Logger.get(CheckpointEntryIterator.class);

    private final String checkpointPath;
    private final ConnectorSession session;
    private final ConnectorPageSource pageSource;
    private final MapType stringMap;
    private final ArrayType stringList;
    private final Queue<DeltaLakeTransactionLogEntry> nextEntries;
    private final List<CheckPointFieldExtractor> extractors;
    private final boolean checkpointRowStatisticsWritingEnabled;
    private MetadataEntry metadataEntry;
    private ProtocolEntry protocolEntry;
    private List<DeltaLakeColumnMetadata> schema;
    private List<DeltaLakeColumnMetadata> columnsWithMinMaxStats;
    private Page page;
    private long pageIndex;
    private int pagePosition;

    public CheckpointEntryIterator(
            TrinoInputFile checkpoint,
            ConnectorSession session,
            long fileSize,
            CheckpointSchemaManager checkpointSchemaManager,
            TypeManager typeManager,
            Set<EntryType> fields,
            Optional<MetadataEntry> metadataEntry,
            Optional<ProtocolEntry> protocolEntry,
            FileFormatDataSourceStats stats,
            ParquetReaderOptions parquetReaderOptions,
            boolean checkpointRowStatisticsWritingEnabled,
            int domainCompactionThreshold)
    {
        this.checkpointPath = checkpoint.location().toString();
        this.session = requireNonNull(session, "session is null");
        this.stringList = (ArrayType) typeManager.getType(TypeSignature.arrayType(VARCHAR.getTypeSignature()));
        this.stringMap = (MapType) typeManager.getType(TypeSignature.mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
        this.checkpointRowStatisticsWritingEnabled = checkpointRowStatisticsWritingEnabled;
        checkArgument(fields.size() > 0, "fields is empty");
        Map<EntryType, CheckPointFieldExtractor> extractors = ImmutableMap.<EntryType, CheckPointFieldExtractor>builder()
                .put(TRANSACTION, this::buildTxnEntry)
                .put(ADD, this::buildAddEntry)
                .put(REMOVE, this::buildRemoveEntry)
                .put(METADATA, this::buildMetadataEntry)
                .put(PROTOCOL, this::buildProtocolEntry)
                .put(COMMIT, this::buildCommitInfoEntry)
                .buildOrThrow();
        // ADD requires knowing the metadata in order to figure out the Parquet schema
        if (fields.contains(ADD)) {
            checkArgument(metadataEntry.isPresent(), "Metadata entry must be provided when reading ADD entries from Checkpoint files");
            this.metadataEntry = metadataEntry.get();
            checkArgument(protocolEntry.isPresent(), "Protocol entry must be provided when reading ADD entries from Checkpoint files");
            this.protocolEntry = protocolEntry.get();
            this.schema = extractSchema(this.metadataEntry, this.protocolEntry, typeManager);
            this.columnsWithMinMaxStats = columnsWithStats(schema, this.metadataEntry.getOriginalPartitionColumns());
        }

        List<HiveColumnHandle> columns = fields.stream()
                .map(field -> buildColumnHandle(field, checkpointSchemaManager, this.metadataEntry, this.protocolEntry).toHiveColumnHandle())
                .collect(toImmutableList());

        TupleDomain<HiveColumnHandle> tupleDomain = columns.size() > 1 ?
                TupleDomain.all() :
                buildTupleDomainColumnHandle(getOnlyElement(fields), getOnlyElement(columns));

        ReaderPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                checkpoint,
                0,
                fileSize,
                columns,
                tupleDomain,
                true,
                DateTimeZone.UTC,
                stats,
                parquetReaderOptions,
                Optional.empty(),
                domainCompactionThreshold,
                OptionalLong.empty());

        verify(pageSource.getReaderColumns().isEmpty(), "All columns expected to be base columns");

        this.pageSource = pageSource.get();
        this.nextEntries = new ArrayDeque<>();
        this.extractors = fields.stream()
                .map(field -> requireNonNull(extractors.get(field), "No extractor found for field " + field))
                .collect(toImmutableList());
    }

    private DeltaLakeColumnHandle buildColumnHandle(EntryType entryType, CheckpointSchemaManager schemaManager, MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        Type type = switch (entryType) {
            case TRANSACTION -> schemaManager.getTxnEntryType();
            case ADD -> schemaManager.getAddEntryType(metadataEntry, protocolEntry, true, true);
            case REMOVE -> schemaManager.getRemoveEntryType();
            case METADATA -> schemaManager.getMetadataEntryType();
            case PROTOCOL -> schemaManager.getProtocolEntryType(true, true);
            case COMMIT -> schemaManager.getCommitInfoEntryType();
        };
        return new DeltaLakeColumnHandle(entryType.getColumnName(), type, OptionalInt.empty(), entryType.getColumnName(), type, REGULAR, Optional.empty());
    }

    /**
     * Constructs a TupleDomain which filters on a specific required primitive sub-column of the EntryType being
     * not null for effectively pushing down the predicate to the Parquet reader.
     * <p>
     * The particular field we select for each action is a required fields per the Delta Log specification, please see
     * https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Actions This is also enforced when we read entries.
     */
    private TupleDomain<HiveColumnHandle> buildTupleDomainColumnHandle(EntryType entryType, HiveColumnHandle column)
    {
        String field;
        Type type;
        switch (entryType) {
            case COMMIT, TRANSACTION -> {
                field = "version";
                type = BIGINT;
            }
            case ADD, REMOVE -> {
                field = "path";
                type = VARCHAR;
            }
            case METADATA -> {
                field = "id";
                type = VARCHAR;
            }
            case PROTOCOL -> {
                field = "minReaderVersion";
                type = BIGINT;
            }
            default -> throw new IllegalArgumentException("Unsupported Delta Lake checkpoint entry type: " + entryType);
        }
        HiveColumnHandle handle = new HiveColumnHandle(
                column.getBaseColumnName(),
                column.getBaseHiveColumnIndex(),
                column.getBaseHiveType(),
                column.getBaseType(),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0), // hiveColumnIndex; we provide fake value because we always find columns by name
                        ImmutableList.of(field),
                        HiveType.toHiveType(type),
                        type)),
                ColumnType.REGULAR,
                column.getComment());
        return TupleDomain.withColumnDomains(ImmutableMap.of(handle, Domain.notNull(handle.getType())));
    }

    private DeltaLakeTransactionLogEntry buildCommitInfoEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building commitInfo entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        int commitInfoFields = 12;
        int jobFields = 5;
        int notebookFields = 1;
        SqlRow commitInfoRow = block.getObject(pagePosition, SqlRow.class);
        int commitInfoRawIndex = commitInfoRow.getRawIndex();
        log.debug("Block %s has %s fields", block, commitInfoRow.getFieldCount());
        if (commitInfoRow.getFieldCount() != commitInfoFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", block, commitInfoFields, commitInfoRow.getFieldCount()));
        }
        SqlRow jobRow = commitInfoRow.getRawFieldBlock(9).getObject(commitInfoRawIndex, SqlRow.class);
        if (jobRow.getFieldCount() != jobFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", jobRow, jobFields, jobRow.getFieldCount()));
        }
        SqlRow notebookRow = commitInfoRow.getRawFieldBlock(7).getObject(commitInfoRawIndex, SqlRow.class);
        if (notebookRow.getFieldCount() != notebookFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", notebookRow, notebookFields, notebookRow.getFieldCount()));
        }
        CommitInfoEntry result = new CommitInfoEntry(
                getLong(commitInfoRow.getRawFieldBlock(0), commitInfoRawIndex),
                getLong(commitInfoRow.getRawFieldBlock(1), commitInfoRawIndex),
                getString(commitInfoRow.getRawFieldBlock(2), commitInfoRawIndex),
                getString(commitInfoRow.getRawFieldBlock(3), commitInfoRawIndex),
                getString(commitInfoRow.getRawFieldBlock(4), commitInfoRawIndex),
                getMap(commitInfoRow.getRawFieldBlock(5), commitInfoRawIndex),
                new CommitInfoEntry.Job(
                        getString(jobRow.getRawFieldBlock(0), jobRow.getRawIndex()),
                        getString(jobRow.getRawFieldBlock(1), jobRow.getRawIndex()),
                        getString(jobRow.getRawFieldBlock(2), jobRow.getRawIndex()),
                        getString(jobRow.getRawFieldBlock(3), jobRow.getRawIndex()),
                        getString(jobRow.getRawFieldBlock(4), jobRow.getRawIndex())),
                new CommitInfoEntry.Notebook(
                        getString(notebookRow.getRawFieldBlock(0), notebookRow.getRawIndex())),
                getString(commitInfoRow.getRawFieldBlock(8), commitInfoRawIndex),
                getLong(commitInfoRow.getRawFieldBlock(9), commitInfoRawIndex),
                getString(commitInfoRow.getRawFieldBlock(10), commitInfoRawIndex),
                Optional.of(getByte(commitInfoRow.getRawFieldBlock(11), commitInfoRawIndex) != 0));
        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.commitInfoEntry(result);
    }

    private DeltaLakeTransactionLogEntry buildProtocolEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building protocol entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        int minProtocolFields = 2;
        int maxProtocolFields = 4;
        SqlRow protocolEntryRow = block.getObject(pagePosition, SqlRow.class);
        int fieldCount = protocolEntryRow.getFieldCount();
        log.debug("Block %s has %s fields", block, fieldCount);
        if (fieldCount < minProtocolFields || fieldCount > maxProtocolFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have between %d and %d children, but found %s", block, minProtocolFields, maxProtocolFields, fieldCount));
        }
        int rawIndex = protocolEntryRow.getRawIndex();
        Block readerFeaturesField = protocolEntryRow.getRawFieldBlock(2);
        // The last entry should be writer feature when protocol entry size is 3 https://github.com/delta-io/delta/blob/master/PROTOCOL.md#disabled-features
        Block writerFeaturesField = fieldCount != 4 ? readerFeaturesField : protocolEntryRow.getRawFieldBlock(3);
        ProtocolEntry result = new ProtocolEntry(
                getInt(protocolEntryRow.getRawFieldBlock(0), rawIndex),
                getInt(protocolEntryRow.getRawFieldBlock(1), rawIndex),
                readerFeaturesField.isNull(rawIndex) ? Optional.empty() : Optional.of(getList(readerFeaturesField, rawIndex).stream().collect(toImmutableSet())),
                writerFeaturesField.isNull(rawIndex) ? Optional.empty() : Optional.of(getList(writerFeaturesField, rawIndex).stream().collect(toImmutableSet())));
        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.protocolEntry(result);
    }

    private DeltaLakeTransactionLogEntry buildMetadataEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building metadata entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        int metadataFields = 8;
        int formatFields = 2;
        SqlRow metadataEntryRow = block.getObject(pagePosition, SqlRow.class);
        int rawIndex = metadataEntryRow.getRawIndex();
        log.debug("Block %s has %s fields", block, metadataEntryRow.getFieldCount());
        if (metadataEntryRow.getFieldCount() != metadataFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", block, metadataFields, metadataEntryRow.getFieldCount()));
        }
        SqlRow formatRow = metadataEntryRow.getRawFieldBlock(3).getObject(rawIndex, SqlRow.class);
        if (formatRow.getFieldCount() != formatFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", formatRow, formatFields, formatRow.getFieldCount()));
        }
        MetadataEntry result = new MetadataEntry(
                getString(metadataEntryRow.getRawFieldBlock(0), rawIndex),
                getString(metadataEntryRow.getRawFieldBlock(1), rawIndex),
                getString(metadataEntryRow.getRawFieldBlock(2), rawIndex),
                new MetadataEntry.Format(
                        getString(formatRow.getRawFieldBlock(0), formatRow.getRawIndex()),
                        getMap(formatRow.getRawFieldBlock(1), formatRow.getRawIndex())),
                getString(metadataEntryRow.getRawFieldBlock(4), rawIndex),
                getList(metadataEntryRow.getRawFieldBlock(5), rawIndex),
                getMap(metadataEntryRow.getRawFieldBlock(6), rawIndex),
                getLong(metadataEntryRow.getRawFieldBlock(7), rawIndex));
        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.metadataEntry(result);
    }

    private DeltaLakeTransactionLogEntry buildRemoveEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building remove entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        int removeFields = 3;
        SqlRow removeEntryRow = block.getObject(pagePosition, SqlRow.class);
        log.debug("Block %s has %s fields", block, removeEntryRow.getFieldCount());
        if (removeEntryRow.getFieldCount() != removeFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", block, removeFields, removeEntryRow.getFieldCount()));
        }
        int rawIndex = removeEntryRow.getRawIndex();
        RemoveFileEntry result = new RemoveFileEntry(
                getString(removeEntryRow.getRawFieldBlock(0), rawIndex),
                getLong(removeEntryRow.getRawFieldBlock(1), rawIndex),
                getByte(removeEntryRow.getRawFieldBlock(2), rawIndex) != 0);
        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.removeFileEntry(result);
    }

    private DeltaLakeTransactionLogEntry buildAddEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building add entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        boolean deletionVectorsEnabled = isDeletionVectorEnabled(metadataEntry, protocolEntry);
        SqlRow addEntryRow = block.getObject(pagePosition, SqlRow.class);
        log.debug("Block %s has %s fields", block, addEntryRow.getFieldCount());
        int rawIndex = addEntryRow.getRawIndex();

        String path = getString(addEntryRow.getRawFieldBlock(0), rawIndex);
        Map<String, String> partitionValues = getMap(addEntryRow.getRawFieldBlock(1), rawIndex);
        long size = getLong(addEntryRow.getRawFieldBlock(2), rawIndex);
        long modificationTime = getLong(addEntryRow.getRawFieldBlock(3), rawIndex);
        boolean dataChange = getByte(addEntryRow.getRawFieldBlock(4), rawIndex) != 0;
        Optional<DeletionVectorEntry> deletionVector = Optional.empty();
        int position = 5;
        if (deletionVectorsEnabled) {
            if (!addEntryRow.getRawFieldBlock(5).isNull(rawIndex)) {
                deletionVector = Optional.of(parseDeletionVectorFromParquet(addEntryRow.getRawFieldBlock(5).getObject(rawIndex, Block.class)));
            }
            position = 6;
        }
        Map<String, String> tags = getMap(addEntryRow.getRawFieldBlock(position + 2), rawIndex);

        AddFileEntry result;
        if (!addEntryRow.getRawFieldBlock(position + 1).isNull(rawIndex)) {
            result = new AddFileEntry(
                    path,
                    partitionValues,
                    size,
                    modificationTime,
                    dataChange,
                    Optional.empty(),
                    Optional.of(parseStatisticsFromParquet(addEntryRow.getRawFieldBlock(position + 1).getObject(rawIndex, SqlRow.class))),
                    tags,
                    deletionVector);
        }
        else if (!addEntryRow.getRawFieldBlock(position).isNull(rawIndex)) {
            result = new AddFileEntry(
                    path,
                    partitionValues,
                    size,
                    modificationTime,
                    dataChange,
                    Optional.of(getString(addEntryRow.getRawFieldBlock(position), rawIndex)),
                    Optional.empty(),
                    tags,
                    deletionVector);
        }
        else {
            result = new AddFileEntry(
                    path,
                    partitionValues,
                    size,
                    modificationTime,
                    dataChange,
                    Optional.empty(),
                    Optional.empty(),
                    tags,
                    deletionVector);
        }

        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.addFileEntry(result);
    }

    private DeletionVectorEntry parseDeletionVectorFromParquet(Block block)
    {
        checkArgument(block.getPositionCount() == 5, "Deletion vector entry must have 5 fields");

        String storageType = getString(block, 0);
        String pathOrInlineDv = getString(block, 1);
        OptionalInt offset = block.isNull(2) ? OptionalInt.empty() : OptionalInt.of(getInt(block, 2));
        int sizeInBytes = getInt(block, 3);
        long cardinality = getLong(block, 4);
        return new DeletionVectorEntry(storageType, pathOrInlineDv, offset, sizeInBytes, cardinality);
    }

    private DeltaLakeParquetFileStatistics parseStatisticsFromParquet(SqlRow statsRow)
    {
        int rawIndex = statsRow.getRawIndex();
        long numRecords = getLong(statsRow.getRawFieldBlock(0), rawIndex);

        Optional<Map<String, Object>> minValues = Optional.empty();
        Optional<Map<String, Object>> maxValues = Optional.empty();
        Optional<Map<String, Object>> nullCount;
        if (!columnsWithMinMaxStats.isEmpty()) {
            minValues = Optional.of(readMinMax(statsRow.getRawFieldBlock(1), rawIndex, columnsWithMinMaxStats));
            maxValues = Optional.of(readMinMax(statsRow.getRawFieldBlock(2), rawIndex, columnsWithMinMaxStats));
            nullCount = Optional.of(readNullCount(statsRow.getRawFieldBlock(3), rawIndex, schema));
        }
        else {
            nullCount = Optional.of(readNullCount(statsRow.getRawFieldBlock(1), rawIndex, schema));
        }

        return new DeltaLakeParquetFileStatistics(
                Optional.of(numRecords),
                minValues,
                maxValues,
                nullCount);
    }

    private Map<String, Object> readMinMax(Block block, int blockPosition, List<DeltaLakeColumnMetadata> eligibleColumns)
    {
        if (block.isNull(blockPosition)) {
            // Statistics were not collected
            return ImmutableMap.of();
        }

        SqlRow row = block.getObject(blockPosition, SqlRow.class);
        ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();

        int rawIndex = row.getRawIndex();
        for (int i = 0; i < eligibleColumns.size(); i++) {
            DeltaLakeColumnMetadata metadata = eligibleColumns.get(i);
            String name = metadata.getPhysicalName();
            Type type = metadata.getPhysicalColumnType();

            Block fieldBlock = row.getRawFieldBlock(i);
            if (fieldBlock.isNull(rawIndex)) {
                continue;
            }
            if (type instanceof RowType rowType) {
                if (checkpointRowStatisticsWritingEnabled) {
                    // RowType column statistics are not used for query planning, but need to be copied when writing out new Checkpoint files.
                    values.put(name, rowType.getObject(fieldBlock, rawIndex));
                }
                continue;
            }
            if (type instanceof TimestampWithTimeZoneType) {
                long epochMillis = LongMath.divide((long) readNativeValue(TIMESTAMP_MILLIS, fieldBlock, rawIndex), MICROSECONDS_PER_MILLISECOND, UNNECESSARY);
                if (floorDiv(epochMillis, MILLISECONDS_PER_DAY) >= START_OF_MODERN_ERA_EPOCH_DAY) {
                    values.put(name, packDateTimeWithZone(epochMillis, UTC_KEY));
                }
                continue;
            }
            values.put(name, readNativeValue(type, fieldBlock, rawIndex));
        }
        return values.buildOrThrow();
    }

    private Map<String, Object> readNullCount(Block block, int blockPosition, List<DeltaLakeColumnMetadata> columns)
    {
        if (block.isNull(blockPosition)) {
            // Statistics were not collected
            return ImmutableMap.of();
        }

        SqlRow row = block.getObject(blockPosition, SqlRow.class);
        int rawIndex = row.getRawIndex();
        ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
        for (int i = 0; i < columns.size(); i++) {
            DeltaLakeColumnMetadata metadata = columns.get(i);

            Block fieldBlock = row.getRawFieldBlock(i);
            if (fieldBlock.isNull(rawIndex)) {
                continue;
            }
            if (metadata.getType() instanceof RowType) {
                if (checkpointRowStatisticsWritingEnabled) {
                    // RowType column statistics are not used for query planning, but need to be copied when writing out new Checkpoint files.
                    values.put(metadata.getPhysicalName(), fieldBlock.getObject(rawIndex, SqlRow.class));
                }
                continue;
            }

            values.put(metadata.getPhysicalName(), getLong(fieldBlock, rawIndex));
        }
        return values.buildOrThrow();
    }

    private DeltaLakeTransactionLogEntry buildTxnEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building txn entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        int txnFields = 3;
        SqlRow txnEntryRow = block.getObject(pagePosition, SqlRow.class);
        log.debug("Block %s has %s fields", block, txnEntryRow.getFieldCount());
        if (txnEntryRow.getFieldCount() != txnFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", block, txnFields, txnEntryRow.getFieldCount()));
        }
        int rawIndex = txnEntryRow.getRawIndex();
        TransactionEntry result = new TransactionEntry(
                getString(txnEntryRow.getRawFieldBlock(0), rawIndex),
                getLong(txnEntryRow.getRawFieldBlock(1), rawIndex),
                getLong(txnEntryRow.getRawFieldBlock(2), rawIndex));
        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.transactionEntry(result);
    }

    @Nullable
    private String getString(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return block.getSlice(position, 0, block.getSliceLength(position)).toString(UTF_8);
    }

    private long getLong(Block block, int position)
    {
        checkArgument(!block.isNull(position));
        return block.getLong(position, 0);
    }

    private int getInt(Block block, int position)
    {
        checkArgument(!block.isNull(position));
        return block.getInt(position, 0);
    }

    private byte getByte(Block block, int position)
    {
        checkArgument(!block.isNull(position));
        return block.getByte(position, 0);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> getMap(Block block, int position)
    {
        return (Map<String, String>) stringMap.getObjectValue(session, block, position);
    }

    @SuppressWarnings("unchecked")
    private List<String> getList(Block block, int position)
    {
        return (List<String>) stringList.getObjectValue(session, block, position);
    }

    @Override
    protected DeltaLakeTransactionLogEntry computeNext()
    {
        if (nextEntries.isEmpty()) {
            fillNextEntries();
        }
        if (!nextEntries.isEmpty()) {
            return nextEntries.remove();
        }
        try {
            pageSource.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return endOfData();
    }

    private boolean tryAdvancePage()
    {
        if (pageSource.isFinished()) {
            try {
                pageSource.close();
            }
            catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
            return false;
        }
        page = pageSource.getNextPage();
        if (page == null) {
            return false;
        }
        if (page.getChannelCount() != extractors.size()) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected page %d (%s) in %s to contain %d channels, but found %d",
                            pageIndex, page, checkpointPath, extractors.size(), page.getChannelCount()));
        }
        pagePosition = 0;
        pageIndex++;
        return true;
    }

    private void fillNextEntries()
    {
        while (nextEntries.isEmpty()) {
            // grab next page if needed
            while (page == null || pagePosition == page.getPositionCount()) {
                if (!tryAdvancePage()) {
                    return;
                }
            }

            // process page
            for (int i = 0; i < extractors.size(); ++i) {
                DeltaLakeTransactionLogEntry entry = extractors.get(i).getEntry(session, page.getBlock(i).getLoadedBlock(), pagePosition);
                if (entry != null) {
                    nextEntries.add(entry);
                }
            }
            pagePosition++;
        }
    }

    @VisibleForTesting
    OptionalLong getCompletedPositions()
    {
        return pageSource.getCompletedPositions();
    }

    @FunctionalInterface
    public interface CheckPointFieldExtractor
    {
        @Nullable
        DeltaLakeTransactionLogEntry getEntry(ConnectorSession session, Block block, int pagePosition);
    }
}
