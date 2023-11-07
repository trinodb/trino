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
import io.trino.parquet.Column;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.DeltaHiveTypeTranslator;
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
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.block.ValueBlock;
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

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeSplitManager.partitionMatchesPredicate;
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
    private final ParquetPageSource pageSource;
    private final Map<String, Type> parquetFields;
    private final MapType stringMap;
    private final ArrayType stringList;
    private final Queue<DeltaLakeTransactionLogEntry> nextEntries;
    private final List<CheckPointFieldExtractor> extractors;
    private final boolean checkpointRowStatisticsWritingEnabled;
    private final TupleDomain<DeltaLakeColumnHandle> partitionConstraint;
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
            int domainCompactionThreshold,
            TupleDomain<DeltaLakeColumnHandle> partitionConstraint,
            Optional<Predicate<String>> addStatsMinMaxColumnFilter)
    {
        this.checkpointPath = checkpoint.location().toString();
        this.session = requireNonNull(session, "session is null");
        this.stringList = (ArrayType) typeManager.getType(TypeSignature.arrayType(VARCHAR.getTypeSignature()));
        this.stringMap = (MapType) typeManager.getType(TypeSignature.mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
        this.checkpointRowStatisticsWritingEnabled = checkpointRowStatisticsWritingEnabled;
        this.partitionConstraint = requireNonNull(partitionConstraint, "partitionConstraint is null");
        requireNonNull(addStatsMinMaxColumnFilter, "addStatsMinMaxColumnFilter is null");
        checkArgument(!fields.isEmpty(), "fields is empty");
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
            checkArgument(addStatsMinMaxColumnFilter.isPresent(), "addStatsMinMaxColumnFilter must be provided when reading ADD entries from Checkpoint files");
            this.schema = extractSchema(this.metadataEntry, this.protocolEntry, typeManager);
            this.columnsWithMinMaxStats = columnsWithStats(schema, this.metadataEntry.getOriginalPartitionColumns());
            Predicate<String> columnStatsFilterFunction = addStatsMinMaxColumnFilter.orElseThrow();
            this.columnsWithMinMaxStats = columnsWithMinMaxStats.stream()
                    .filter(column -> columnStatsFilterFunction.test(column.getName()))
                    .collect(toImmutableList());
        }

        ImmutableList.Builder<HiveColumnHandle> columnsBuilder = ImmutableList.builderWithExpectedSize(fields.size());
        ImmutableList.Builder<TupleDomain<HiveColumnHandle>> disjunctDomainsBuilder = ImmutableList.builderWithExpectedSize(fields.size());
        for (EntryType field : fields) {
            HiveColumnHandle column = buildColumnHandle(field, checkpointSchemaManager, this.metadataEntry, this.protocolEntry, addStatsMinMaxColumnFilter).toHiveColumnHandle();
            columnsBuilder.add(column);
            disjunctDomainsBuilder.add(buildTupleDomainColumnHandle(field, column));
        }

        ReaderPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                checkpoint,
                0,
                fileSize,
                columnsBuilder.build(),
                disjunctDomainsBuilder.build(), // OR-ed condition
                true,
                DateTimeZone.UTC,
                stats,
                parquetReaderOptions,
                Optional.empty(),
                domainCompactionThreshold,
                OptionalLong.empty());

        verify(pageSource.getReaderColumns().isEmpty(), "All columns expected to be base columns");

        this.pageSource = (ParquetPageSource) pageSource.get();
        this.nextEntries = new ArrayDeque<>();
        // The size between parquetFields and extractors may not match when the requested field doesn't exist in Parquet file
        this.parquetFields = this.pageSource.getColumnFields().stream()
                .collect(toImmutableMap(Column::name, e -> e.field().getType()));
        this.extractors = fields.stream()
                .map(field -> requireNonNull(extractors.get(field), "No extractor found for field " + field))
                .collect(toImmutableList());
    }

    private DeltaLakeColumnHandle buildColumnHandle(
            EntryType entryType,
            CheckpointSchemaManager schemaManager,
            MetadataEntry metadataEntry,
            ProtocolEntry protocolEntry,
            Optional<Predicate<String>> addStatsMinMaxColumnFilter)
    {
        Type type = switch (entryType) {
            case TRANSACTION -> schemaManager.getTxnEntryType();
            case ADD -> schemaManager.getAddEntryType(metadataEntry, protocolEntry, addStatsMinMaxColumnFilter.orElseThrow(), true, true);
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

        ImmutableMap.Builder<HiveColumnHandle, Domain> domains = ImmutableMap.<HiveColumnHandle, Domain>builder()
                .put(handle, Domain.notNull(handle.getType()));
        if (entryType == ADD) {
            partitionConstraint.getDomains().orElseThrow().forEach((key, value) -> domains.put(toPartitionValuesParsedField(column, key), value));
        }

        return TupleDomain.withColumnDomains(domains.buildOrThrow());
    }

    private static HiveColumnHandle toPartitionValuesParsedField(HiveColumnHandle addColumn, DeltaLakeColumnHandle partitionColumn)
    {
        return new HiveColumnHandle(
                addColumn.getBaseColumnName(),
                addColumn.getBaseHiveColumnIndex(),
                addColumn.getBaseHiveType(),
                addColumn.getBaseType(),
                Optional.of(new HiveColumnProjectionInfo(
                        ImmutableList.of(0, 0), // hiveColumnIndex; we provide fake value because we always find columns by name
                        ImmutableList.of("partitionvalues_parsed", partitionColumn.getColumnName()),
                        DeltaHiveTypeTranslator.toHiveType(partitionColumn.getType()),
                        partitionColumn.getType())),
                HiveColumnHandle.ColumnType.REGULAR,
                addColumn.getComment());
    }

    private DeltaLakeTransactionLogEntry buildCommitInfoEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building commitInfo entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        RowType type = (RowType) parquetFields.get("commitinfo");
        int commitInfoFields = 12;
        int jobFields = 5;
        int notebookFields = 1;
        SqlRow commitInfoRow = block.getObject(pagePosition, SqlRow.class);
        CheckpointFieldReader commitInfo = new CheckpointFieldReader(session, commitInfoRow, type);
        log.debug("Block %s has %s fields", block, commitInfoRow.getFieldCount());
        if (commitInfoRow.getFieldCount() != commitInfoFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", block, commitInfoFields, commitInfoRow.getFieldCount()));
        }
        SqlRow jobRow = commitInfo.getRow("job");
        if (jobRow.getFieldCount() != jobFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", jobRow, jobFields, jobRow.getFieldCount()));
        }
        RowType.Field jobField = type.getFields().stream().filter(field -> field.getName().orElseThrow().equals("job")).collect(onlyElement());
        CheckpointFieldReader job = new CheckpointFieldReader(session, jobRow, (RowType) jobField.getType());

        SqlRow notebookRow = commitInfo.getRow("notebook");
        if (notebookRow.getFieldCount() != notebookFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", notebookRow, notebookFields, notebookRow.getFieldCount()));
        }
        RowType.Field notebookField = type.getFields().stream().filter(field -> field.getName().orElseThrow().equals("notebook")).collect(onlyElement());
        CheckpointFieldReader notebook = new CheckpointFieldReader(session, notebookRow, (RowType) notebookField.getType());

        CommitInfoEntry result = new CommitInfoEntry(
                commitInfo.getLong("version"),
                commitInfo.getLong("timestamp"),
                commitInfo.getString("userId"),
                commitInfo.getString("userName"),
                commitInfo.getString("operation"),
                commitInfo.getMap(stringMap, "operationParameters"),
                new CommitInfoEntry.Job(
                        job.getString("jobId"),
                        job.getString("jobName"),
                        job.getString("runId"),
                        job.getString("jobOwnerId"),
                        job.getString("triggerType")),
                new CommitInfoEntry.Notebook(
                        notebook.getString("notebookId")),
                commitInfo.getString("clusterId"),
                commitInfo.getInt("readVersion"),
                commitInfo.getString("isolationLevel"),
                Optional.of(commitInfo.getBoolean("isBlindAppend")));
        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.commitInfoEntry(result);
    }

    private DeltaLakeTransactionLogEntry buildProtocolEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building protocol entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        RowType type = (RowType) parquetFields.get("protocol");
        int minProtocolFields = 2;
        int maxProtocolFields = 4;
        SqlRow protocolEntryRow = block.getObject(pagePosition, SqlRow.class);
        int fieldCount = protocolEntryRow.getFieldCount();
        log.debug("Block %s has %s fields", block, fieldCount);
        if (fieldCount < minProtocolFields || fieldCount > maxProtocolFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have between %d and %d children, but found %s", block, minProtocolFields, maxProtocolFields, fieldCount));
        }

        CheckpointFieldReader protocol = new CheckpointFieldReader(session, protocolEntryRow, type);
        ProtocolEntry result = new ProtocolEntry(
                protocol.getInt("minReaderVersion"),
                protocol.getInt("minWriterVersion"),
                protocol.getOptionalSet(stringList, "readerFeatures"),
                protocol.getOptionalSet(stringList, "writerFeatures"));
        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.protocolEntry(result);
    }

    private DeltaLakeTransactionLogEntry buildMetadataEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building metadata entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        RowType type = (RowType) parquetFields.get("metadata");
        int metadataFields = 8;
        int formatFields = 2;
        SqlRow metadataEntryRow = block.getObject(pagePosition, SqlRow.class);
        CheckpointFieldReader metadata = new CheckpointFieldReader(session, metadataEntryRow, type);
        log.debug("Block %s has %s fields", block, metadataEntryRow.getFieldCount());
        if (metadataEntryRow.getFieldCount() != metadataFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", block, metadataFields, metadataEntryRow.getFieldCount()));
        }
        SqlRow formatRow = metadata.getRow("format");
        if (formatRow.getFieldCount() != formatFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", formatRow, formatFields, formatRow.getFieldCount()));
        }

        RowType.Field formatField = type.getFields().stream().filter(field -> field.getName().orElseThrow().equals("format")).collect(onlyElement());
        CheckpointFieldReader format = new CheckpointFieldReader(session, formatRow, (RowType) formatField.getType());
        MetadataEntry result = new MetadataEntry(
                metadata.getString("id"),
                metadata.getString("name"),
                metadata.getString("description"),
                new MetadataEntry.Format(
                        format.getString("provider"),
                        format.getMap(stringMap, "options")),
                metadata.getString("schemaString"),
                metadata.getList(stringList, "partitionColumns"),
                metadata.getMap(stringMap, "configuration"),
                metadata.getLong("createdTime"));
        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.metadataEntry(result);
    }

    private DeltaLakeTransactionLogEntry buildRemoveEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building remove entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        RowType type = (RowType) parquetFields.get("remove");
        int removeFields = 3;
        SqlRow removeEntryRow = block.getObject(pagePosition, SqlRow.class);
        log.debug("Block %s has %s fields", block, removeEntryRow.getFieldCount());
        if (removeEntryRow.getFieldCount() != removeFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", block, removeFields, removeEntryRow.getFieldCount()));
        }
        CheckpointFieldReader remove = new CheckpointFieldReader(session, removeEntryRow, type);
        RemoveFileEntry result = new RemoveFileEntry(
                remove.getString("path"),
                remove.getLong("deletionTimestamp"),
                remove.getBoolean("dataChange"));
        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.removeFileEntry(result);
    }

    private DeltaLakeTransactionLogEntry buildAddEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building add entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        RowType type = (RowType) parquetFields.get("add");
        boolean deletionVectorsEnabled = isDeletionVectorEnabled(metadataEntry, protocolEntry);
        SqlRow addEntryRow = block.getObject(pagePosition, SqlRow.class);
        log.debug("Block %s has %s fields", block, addEntryRow.getFieldCount());
        CheckpointFieldReader add = new CheckpointFieldReader(session, addEntryRow, type);

        String path = add.getString("path");
        Map<String, String> partitionValues = add.getMap(stringMap, "partitionValues");
        long size = add.getLong("size");
        long modificationTime = add.getLong("modificationTime");
        boolean dataChange = add.getBoolean("dataChange");

        Optional<DeletionVectorEntry> deletionVector = Optional.empty();
        if (deletionVectorsEnabled) {
            deletionVector = Optional.ofNullable(add.getRow("deletionVector"))
                    .map(row -> {
                        RowType.Field deletionVectorField = type.getFields().stream().filter(field -> field.getName().orElseThrow().equals("deletionVector")).collect(onlyElement());
                        return parseDeletionVectorFromParquet(session, row, (RowType) deletionVectorField.getType());
                    });
        }

        Optional<DeltaLakeParquetFileStatistics> parsedStats = Optional.ofNullable(add.getRow("stats_parsed")).map(row -> {
            RowType.Field parsedStatsField = type.getFields().stream().filter(field -> field.getName().orElseThrow().equals("stats_parsed")).collect(onlyElement());
            return parseStatisticsFromParquet(session, row, (RowType) parsedStatsField.getType());
        });
        Optional<String> stats = Optional.empty();
        if (parsedStats.isEmpty()) {
            stats = Optional.ofNullable(add.getString("stats"));
        }

        Map<String, String> tags = add.getMap(stringMap, "tags");
        AddFileEntry result = new AddFileEntry(
                path,
                partitionValues,
                size,
                modificationTime,
                dataChange,
                stats,
                parsedStats,
                tags,
                deletionVector);

        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.addFileEntry(result);
    }

    private DeletionVectorEntry parseDeletionVectorFromParquet(ConnectorSession session, SqlRow row, RowType type)
    {
        checkArgument(row.getFieldCount() == 5, "Deletion vector entry must have 5 fields");

        CheckpointFieldReader deletionVector = new CheckpointFieldReader(session, row, type);
        String storageType = deletionVector.getString("storageType");
        String pathOrInlineDv = deletionVector.getString("pathOrInlineDv");
        OptionalInt offset = deletionVector.getOptionalInt("offset");
        int sizeInBytes = deletionVector.getInt("sizeInBytes");
        long cardinality = deletionVector.getLong("cardinality");
        return new DeletionVectorEntry(storageType, pathOrInlineDv, offset, sizeInBytes, cardinality);
    }

    private DeltaLakeParquetFileStatistics parseStatisticsFromParquet(ConnectorSession session, SqlRow statsRow, RowType type)
    {
        CheckpointFieldReader stats = new CheckpointFieldReader(session, statsRow, type);
        long numRecords = stats.getLong("numRecords");

        Optional<Map<String, Object>> minValues = Optional.empty();
        Optional<Map<String, Object>> maxValues = Optional.empty();
        Optional<Map<String, Object>> nullCount;
        if (!columnsWithMinMaxStats.isEmpty()) {
            minValues = Optional.of(parseMinMax(stats.getRow("minValues"), columnsWithMinMaxStats));
            maxValues = Optional.of(parseMinMax(stats.getRow("maxValues"), columnsWithMinMaxStats));
        }
        nullCount = Optional.of(parseNullCount(stats.getRow("nullCount"), schema));

        return new DeltaLakeParquetFileStatistics(
                Optional.of(numRecords),
                minValues,
                maxValues,
                nullCount);
    }

    private ImmutableMap<String, Object> parseMinMax(@Nullable SqlRow row, List<DeltaLakeColumnMetadata> eligibleColumns)
    {
        if (row == null) {
            // Statistics were not collected
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();

        for (int i = 0; i < eligibleColumns.size(); i++) {
            DeltaLakeColumnMetadata metadata = eligibleColumns.get(i);
            String name = metadata.getPhysicalName();
            Type type = metadata.getPhysicalColumnType();

            ValueBlock fieldBlock = row.getUnderlyingFieldBlock(i);
            int fieldIndex = row.getUnderlyingFieldPosition(i);
            if (fieldBlock.isNull(fieldIndex)) {
                continue;
            }
            if (type instanceof RowType rowType) {
                if (checkpointRowStatisticsWritingEnabled) {
                    // RowType column statistics are not used for query planning, but need to be copied when writing out new Checkpoint files.
                    values.put(name, rowType.getObject(fieldBlock, fieldIndex));
                }
                continue;
            }
            if (type instanceof TimestampWithTimeZoneType) {
                long epochMillis = LongMath.divide((long) readNativeValue(TIMESTAMP_MILLIS, fieldBlock, fieldIndex), MICROSECONDS_PER_MILLISECOND, UNNECESSARY);
                if (floorDiv(epochMillis, MILLISECONDS_PER_DAY) >= START_OF_MODERN_ERA_EPOCH_DAY) {
                    values.put(name, packDateTimeWithZone(epochMillis, UTC_KEY));
                }
                continue;
            }
            values.put(name, readNativeValue(type, fieldBlock, fieldIndex));
        }
        return values.buildOrThrow();
    }

    private Map<String, Object> parseNullCount(SqlRow row, List<DeltaLakeColumnMetadata> columns)
    {
        if (row == null) {
            // Statistics were not collected
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
        for (int i = 0; i < columns.size(); i++) {
            DeltaLakeColumnMetadata metadata = columns.get(i);

            ValueBlock fieldBlock = row.getUnderlyingFieldBlock(i);
            int fieldIndex = row.getUnderlyingFieldPosition(i);
            if (fieldBlock.isNull(fieldIndex)) {
                continue;
            }
            if (metadata.getType() instanceof RowType) {
                if (checkpointRowStatisticsWritingEnabled) {
                    // RowType column statistics are not used for query planning, but need to be copied when writing out new Checkpoint files.
                    values.put(metadata.getPhysicalName(), fieldBlock.getObject(fieldIndex, SqlRow.class));
                }
                continue;
            }

            values.put(metadata.getPhysicalName(), getLongField(row, i));
        }
        return values.buildOrThrow();
    }

    private DeltaLakeTransactionLogEntry buildTxnEntry(ConnectorSession session, Block block, int pagePosition)
    {
        log.debug("Building txn entry from %s pagePosition %d", block, pagePosition);
        if (block.isNull(pagePosition)) {
            return null;
        }
        RowType type = (RowType) parquetFields.get("txn");
        int txnFields = 3;
        SqlRow txnEntryRow = block.getObject(pagePosition, SqlRow.class);
        log.debug("Block %s has %s fields", block, txnEntryRow.getFieldCount());
        if (txnEntryRow.getFieldCount() != txnFields) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    format("Expected block %s to have %d children, but found %s", block, txnFields, txnEntryRow.getFieldCount()));
        }
        CheckpointFieldReader txn = new CheckpointFieldReader(session, txnEntryRow, type);
        TransactionEntry result = new TransactionEntry(
                txn.getString("appId"),
                txn.getLong("version"),
                txn.getLong("lastUpdated"));
        log.debug("Result: %s", result);
        return DeltaLakeTransactionLogEntry.transactionEntry(result);
    }

    private static long getLongField(SqlRow row, int field)
    {
        LongArrayBlock valueBlock = (LongArrayBlock) row.getUnderlyingFieldBlock(field);
        return valueBlock.getLong(row.getUnderlyingFieldPosition(field));
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
        pageSource.close();
        return endOfData();
    }

    private boolean tryAdvancePage()
    {
        if (pageSource.isFinished()) {
            pageSource.close();
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
                    if (entry.getAdd() != null) {
                        if (partitionConstraint.isAll() ||
                                partitionMatchesPredicate(entry.getAdd().getCanonicalPartitionValues(), partitionConstraint.getDomains().orElseThrow())) {
                            nextEntries.add(entry);
                        }
                    }
                    else {
                        nextEntries.add(entry);
                    }
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

    @VisibleForTesting
    long getCompletedBytes()
    {
        return pageSource.getCompletedBytes();
    }

    @FunctionalInterface
    public interface CheckPointFieldExtractor
    {
        @Nullable
        DeltaLakeTransactionLogEntry getEntry(ConnectorSession session, Block block, int pagePosition);
    }
}
