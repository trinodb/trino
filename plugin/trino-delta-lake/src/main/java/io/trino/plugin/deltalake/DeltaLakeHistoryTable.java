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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
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
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DeltaLakeHistoryTable
        implements SystemTable
{
    private final SchemaTableName tableName;
    private final String tableLocation;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TransactionLogAccess transactionLogAccess;
    private final ConnectorTableMetadata tableMetadata;

    public DeltaLakeHistoryTable(
            SchemaTableName tableName,
            String tableLocation,
            TrinoFileSystemFactory fileSystemFactory,
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");

        this.tableMetadata = new ConnectorTableMetadata(
                requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("version", BIGINT))
                        .add(new ColumnMetadata("timestamp", TIMESTAMP_TZ_MILLIS))
                        .add(new ColumnMetadata("user_id", VARCHAR))
                        .add(new ColumnMetadata("user_name", VARCHAR))
                        .add(new ColumnMetadata("operation", VARCHAR))
                        .add(new ColumnMetadata("operation_parameters", typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()))))
                        .add(new ColumnMetadata("cluster_id", VARCHAR))
                        .add(new ColumnMetadata("read_version", BIGINT))
                        .add(new ColumnMetadata("isolation_level", VARCHAR))
                        .add(new ColumnMetadata("is_blind_append", BOOLEAN))
                        //TODO add support for operationMetrics, userMetadata, engineInfo
                        .build());
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
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        long snapshotVersion;
        try {
            // Verify the transaction log is readable
            SchemaTableName baseTableName = new SchemaTableName(tableName.getSchemaName(), DeltaLakeTableName.tableNameFrom(tableName.getTableName()));
            TableSnapshot tableSnapshot = transactionLogAccess.loadSnapshot(session, baseTableName, tableLocation, Optional.empty());
            snapshotVersion = tableSnapshot.getVersion();
            transactionLogAccess.getMetadataEntry(session, tableSnapshot);
        }
        catch (IOException e) {
            throw new TrinoException(DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA, "Unable to load table metadata from location: " + tableLocation, e);
        }

        int versionColumnIndex = IntStream.range(0, tableMetadata.getColumns().size())
                .filter(i -> tableMetadata.getColumns().get(i).getName().equals("version"))
                .boxed()
                .collect(onlyElement());

        Optional<Long> startVersionExclusive = Optional.empty();
        Optional<Long> endVersionInclusive = Optional.empty();

        if (constraint.getDomains().isPresent()) {
            Map<Integer, Domain> domains = constraint.getDomains().get();
            if (domains.containsKey(versionColumnIndex)) {
                Domain versionDomain = domains.get(versionColumnIndex); // The zero value here relies on the column ordering defined in the constructor
                Range range = versionDomain.getValues().getRanges().getSpan();
                if (range.isSingleValue()) {
                    long value = (long) range.getSingleValue();
                    startVersionExclusive = Optional.of(value - 1);
                    endVersionInclusive = Optional.of(value);
                }
                else {
                    Optional<Long> lowValue = range.getLowValue().map(Long.class::cast);
                    if (lowValue.isPresent()) {
                        startVersionExclusive = Optional.of(lowValue.get() - (range.isLowInclusive() ? 1 : 0));
                    }

                    Optional<Long> highValue = range.getHighValue().map(Long.class::cast);
                    if (highValue.isPresent()) {
                        endVersionInclusive = Optional.of(highValue.get() - (range.isHighInclusive() ? 0 : 1));
                    }
                }
            }
        }

        if (startVersionExclusive.isPresent() && endVersionInclusive.isPresent() && startVersionExclusive.get() >= endVersionInclusive.get()) {
            return new EmptyPageSource();
        }

        if (endVersionInclusive.isEmpty()) {
            endVersionInclusive = Optional.of(snapshotVersion);
        }

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        try {
            List<CommitInfoEntry> commitInfoEntries = loadNewTailBackward(fileSystem, tableLocation, startVersionExclusive, endVersionInclusive.get()).stream()
                    .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                    .filter(Objects::nonNull)
                    .collect(toImmutableList())
                    .reverse();
            return new FixedPageSource(buildPages(session, commitInfoEntries));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Error getting commit info entries from " + tableLocation, e);
        }
    }

    // Load a section of the Transaction Log JSON entries. Optionally from a given end version (inclusive) through an start version (exclusive)
    private static List<DeltaLakeTransactionLogEntry> loadNewTailBackward(
            TrinoFileSystem fileSystem,
            String tableLocation,
            Optional<Long> startVersion,
            long endVersion)
            throws IOException
    {
        ImmutableList.Builder<DeltaLakeTransactionLogEntry> entriesBuilder = ImmutableList.builder();
        String transactionLogDir = getTransactionLogDir(tableLocation);

        long version = endVersion;
        long entryNumber = version;
        boolean endOfHead = false;

        while (!endOfHead) {
            Optional<List<DeltaLakeTransactionLogEntry>> results = getEntriesFromJson(entryNumber, transactionLogDir, fileSystem);
            if (results.isPresent()) {
                entriesBuilder.addAll(results.get());
                version = entryNumber;
                entryNumber--;
            }
            else {
                // When there is a gap in the transaction log version, indicate the end of the current head
                endOfHead = true;
            }
            if ((startVersion.isPresent() && version == startVersion.get() + 1) || entryNumber < 0) {
                endOfHead = true;
            }
        }
        return entriesBuilder.build();
    }

    private List<Page> buildPages(ConnectorSession session, List<CommitInfoEntry> commitInfoEntries)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();

        commitInfoEntries.forEach(commitInfoEntry -> {
            pagesBuilder.beginRow();

            pagesBuilder.appendBigint(commitInfoEntry.version());
            pagesBuilder.appendTimestampTzMillis(commitInfoEntry.timestamp(), timeZoneKey);
            write(commitInfoEntry.userId(), pagesBuilder);
            write(commitInfoEntry.userName(), pagesBuilder);
            write(commitInfoEntry.operation(), pagesBuilder);
            if (commitInfoEntry.operationParameters() == null) {
                pagesBuilder.appendNull();
            }
            else {
                pagesBuilder.appendVarcharVarcharMap(commitInfoEntry.operationParameters());
            }
            write(commitInfoEntry.clusterId(), pagesBuilder);
            pagesBuilder.appendBigint(commitInfoEntry.readVersion());
            write(commitInfoEntry.isolationLevel(), pagesBuilder);
            commitInfoEntry.isBlindAppend().ifPresentOrElse(pagesBuilder::appendBoolean, pagesBuilder::appendNull);

            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }

    private static void write(String value, PageListBuilder pagesBuilder)
    {
        if (value == null) {
            pagesBuilder.appendNull();
        }
        else {
            pagesBuilder.appendVarchar(value);
        }
    }
}
