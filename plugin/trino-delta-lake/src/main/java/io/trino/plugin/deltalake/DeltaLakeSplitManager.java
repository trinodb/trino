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
import io.airlift.units.DataSize;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.net.URLDecoder;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.createStatisticsPredicate;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getMaxInitialSplitSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getMaxSplitSize;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializePartitionValue;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class DeltaLakeSplitManager
        implements ConnectorSplitManager
{
    private final TypeManager typeManager;
    private final BiFunction<ConnectorSession, HiveTransactionHandle, DeltaLakeMetastore> metastoreProvider;
    private final ExecutorService executor;
    private final int maxInitialSplits;
    private final int maxSplitsPerSecond;
    private final int maxOutstandingSplits;

    @Inject
    public DeltaLakeSplitManager(
            TypeManager typeManager,
            BiFunction<ConnectorSession, HiveTransactionHandle, DeltaLakeMetastore> metastoreProvider,
            ExecutorService executor,
            DeltaLakeConfig config)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
        this.executor = requireNonNull(executor, "executor is null");
        requireNonNull(config, "config is null");
        this.maxInitialSplits = config.getMaxInitialSplits();
        this.maxSplitsPerSecond = config.getMaxSplitsPerSecond();
        this.maxOutstandingSplits = config.getMaxOutstandingSplits();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle handle,
            ConnectorSplitManager.SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        DeltaLakeTableHandle deltaLakeTableHandle = (DeltaLakeTableHandle) handle;
        if (deltaLakeTableHandle.getEnforcedPartitionConstraint().isNone() || deltaLakeTableHandle.getNonPartitionConstraint().isNone()) {
            if (deltaLakeTableHandle.isRecordScannedFiles()) {
                return new FixedSplitSource(ImmutableList.of(), ImmutableList.of());
            }
            return new FixedSplitSource(ImmutableList.of());
        }

        DeltaLakeSplitSource splitSource = new DeltaLakeSplitSource(
                deltaLakeTableHandle.getSchemaTableName(),
                getSplits(transaction, deltaLakeTableHandle, session, deltaLakeTableHandle.getMaxScannedFileSize(), dynamicFilter.getColumnsCovered(), constraint),
                executor,
                maxSplitsPerSecond,
                maxOutstandingSplits,
                dynamicFilter,
                getDynamicFilteringWaitTimeout(session),
                deltaLakeTableHandle.isRecordScannedFiles());

        return new ClassLoaderSafeConnectorSplitSource(splitSource, Thread.currentThread().getContextClassLoader());
    }

    private Stream<DeltaLakeSplit> getSplits(
            ConnectorTransactionHandle transaction,
            DeltaLakeTableHandle tableHandle,
            ConnectorSession session,
            Optional<DataSize> maxScannedFileSize,
            Set<ColumnHandle> columnsCoveredByDynamicFilter,
            Constraint constraint)
    {
        DeltaLakeMetastore metastore = getMetastore(session, transaction);
        String tableLocation = metastore.getTableLocation(tableHandle.getSchemaTableName(), session);
        List<AddFileEntry> validDataFiles = metastore.getValidDataFiles(tableHandle.getSchemaTableName(), session);
        TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint = tableHandle.getEnforcedPartitionConstraint();
        TupleDomain<DeltaLakeColumnHandle> nonPartitionConstraint = tableHandle.getNonPartitionConstraint();

        // Delta Lake handles updates and deletes by copying entire data files, minus updates/deletes. Because of this we can only have one Split/UpdatablePageSource
        // per file.
        boolean splittable = tableHandle.getWriteType().isEmpty();
        AtomicInteger remainingInitialSplits = new AtomicInteger(maxInitialSplits);
        Optional<Instant> filesModifiedAfter = tableHandle.getAnalyzeHandle().flatMap(AnalyzeHandle::getFilesModifiedAfter);
        Optional<Long> maxScannedFileSizeInBytes = maxScannedFileSize.map(DataSize::toBytes);

        Set<String> predicatedColumnNames = Stream.concat(
                nonPartitionConstraint.getDomains().orElseThrow().keySet().stream(),
                columnsCoveredByDynamicFilter.stream()
                        .map(DeltaLakeColumnHandle.class::cast))
                .map(column -> column.getName().toLowerCase(ENGLISH)) // TODO is DeltaLakeColumnHandle.name normalized?
                .collect(toImmutableSet());
        List<ColumnMetadata> schema = extractSchema(tableHandle.getMetadataEntry(), typeManager);
        List<ColumnMetadata> predicatedColumns = schema.stream()
                .filter(column -> predicatedColumnNames.contains(column.getName())) // ColumnMetadata.name is lowercase
                .collect(toImmutableList());

        return validDataFiles.stream()
                .flatMap(addAction -> {
                    if (tableHandle.getAnalyzeHandle().isPresent() && !tableHandle.getAnalyzeHandle().get().isInitialAnalyze() && !addAction.isDataChange()) {
                        // skip files which do not introduce data change on non-initial ANALYZE
                        return Stream.empty();
                    }

                    if (filesModifiedAfter.isPresent() && addAction.getModificationTime() <= filesModifiedAfter.get().toEpochMilli()) {
                        return Stream.empty();
                    }

                    if (maxScannedFileSizeInBytes.isPresent() && addAction.getSize() > maxScannedFileSizeInBytes.get()) {
                        return Stream.empty();
                    }

                    Map<DeltaLakeColumnHandle, Domain> enforcedDomains = enforcedPartitionConstraint.getDomains().orElseThrow();
                    if (!partitionMatchesPredicate(addAction.getCanonicalPartitionValues(), enforcedDomains)) {
                        return Stream.empty();
                    }

                    TupleDomain<DeltaLakeColumnHandle> statisticsPredicate = createStatisticsPredicate(
                            addAction,
                            predicatedColumns,
                            tableHandle.getMetadataEntry().getCanonicalPartitionColumns());
                    if (!nonPartitionConstraint.overlaps(statisticsPredicate)) {
                        return Stream.empty();
                    }

                    if (constraint.predicate().isPresent()) {
                        Map<String, Optional<String>> partitionValues = addAction.getCanonicalPartitionValues();
                        Map<ColumnHandle, NullableValue> deserializedValues = constraint.getPredicateColumns().orElseThrow().stream()
                                .filter(column -> column instanceof DeltaLakeColumnHandle)
                                .filter(column -> partitionValues.containsKey(((DeltaLakeColumnHandle) column).getName()))
                                .collect(toImmutableMap(identity(), column -> {
                                    DeltaLakeColumnHandle deltaLakeColumn = (DeltaLakeColumnHandle) column;
                                    return NullableValue.of(
                                            deltaLakeColumn.getType(),
                                            deserializePartitionValue(deltaLakeColumn, addAction.getCanonicalPartitionValues().get(deltaLakeColumn.getName())));
                                }));
                        if (!constraint.predicate().get().test(deserializedValues)) {
                            return Stream.empty();
                        }
                    }

                    return splitsForFile(
                            session,
                            addAction,
                            tableLocation,
                            addAction.getCanonicalPartitionValues(),
                            statisticsPredicate,
                            splittable,
                            remainingInitialSplits)
                            .stream();
                });
    }

    public static boolean partitionMatchesPredicate(Map<String, Optional<String>> partitionKeys, Map<DeltaLakeColumnHandle, Domain> domains)
    {
        for (Map.Entry<DeltaLakeColumnHandle, Domain> enforcedDomainsEntry : domains.entrySet()) {
            DeltaLakeColumnHandle partitionColumn = enforcedDomainsEntry.getKey();
            Domain partitionDomain = enforcedDomainsEntry.getValue();
            if (!partitionDomain.includesNullableValue(deserializePartitionValue(partitionColumn, partitionKeys.get(partitionColumn.getName())))) {
                return false;
            }
        }
        return true;
    }

    private List<DeltaLakeSplit> splitsForFile(
            ConnectorSession session,
            AddFileEntry addFileEntry,
            String tableLocation,
            Map<String, Optional<String>> partitionKeys,
            TupleDomain<DeltaLakeColumnHandle> statisticsPredicate,
            boolean splittable,
            AtomicInteger remainingInitialSplits)
    {
        long fileSize = addFileEntry.getSize();
        String splitPath = buildSplitPath(tableLocation, addFileEntry);

        if (!splittable) {
            // remainingInitialSplits is not used when !splittable
            return ImmutableList.of(new DeltaLakeSplit(
                    splitPath,
                    0,
                    fileSize,
                    fileSize,
                    addFileEntry.getModificationTime(),
                    ImmutableList.of(),
                    statisticsPredicate,
                    partitionKeys));
        }

        ImmutableList.Builder<DeltaLakeSplit> splits = ImmutableList.builder();
        long currentOffset = 0;
        while (currentOffset < fileSize) {
            long splitSize;
            if (remainingInitialSplits.get() > 0 && remainingInitialSplits.getAndDecrement() > 0) {
                splitSize = getMaxInitialSplitSize(session).toBytes();
            }
            else {
                splitSize = getMaxSplitSize(session).toBytes();
            }

            splitSize = Math.min(splitSize, fileSize - currentOffset);

            splits.add(new DeltaLakeSplit(
                    splitPath,
                    currentOffset,
                    splitSize,
                    fileSize,
                    addFileEntry.getModificationTime(),
                    ImmutableList.of(),
                    statisticsPredicate,
                    partitionKeys));

            currentOffset += splitSize;
        }

        return splits.build();
    }

    private static String buildSplitPath(String tableLocation, AddFileEntry addAction)
    {
        // paths are relative to the table location and URL encoded
        return tableLocation + '/' + URLDecoder.decode(addAction.getPath(), UTF_8);
    }

    private DeltaLakeMetastore getMetastore(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metastoreProvider.apply(session, (HiveTransactionHandle) transactionHandle);
    }
}
