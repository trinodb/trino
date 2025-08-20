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
package io.trino.plugin.hudi.split;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.metastore.Column;
import io.trino.metastore.Partition;
import io.trino.metastore.StorageFormat;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfoLoader;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.query.index.HudiPartitionStatsIndexSupport;
import io.trino.plugin.hudi.query.index.IndexSupportFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveStylePartitionValueExtractor;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.NonPartitionedExtractor;
import org.apache.hudi.hive.SinglePartPartitionValueExtractor;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;
import org.apache.hudi.util.Lazy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiSessionProperties.getSplitGeneratorParallelism;
import static io.trino.plugin.hudi.HudiSessionProperties.getTargetSplitSize;
import static io.trino.plugin.hudi.HudiSessionProperties.isMetadataPartitionListingEnabled;
import static io.trino.plugin.hudi.partition.HiveHudiPartitionInfo.NON_PARTITION;
import static java.util.Objects.requireNonNull;

public class HudiBackgroundSplitLoader
        implements Runnable
{
    private static final Logger log = Logger.get(HudiBackgroundSplitLoader.class);
    public static final String DELIMITER_STR = "/";
    public static final String EQUALS_STR = "=";

    private final HudiTableHandle tableHandle;
    private final HudiDirectoryLister hudiDirectoryLister;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Executor executor;
    private final int splitGeneratorNumThreads;
    private final HudiSplitFactory hudiSplitFactory;
    private final Lazy<Map<String, Partition>> lazyPartitionMap;
    private final Consumer<Throwable> errorListener;
    private final boolean enableMetadataTable;
    private final Lazy<HoodieTableMetadata> lazyTableMetadata;
    private final Optional<HudiPartitionStatsIndexSupport> partitionIndexSupportOpt;
    private final boolean isMetadataPartitionListingEnabled;
    private final Lazy<HoodieTableMetaClient> lazyMetaClient;

    public HudiBackgroundSplitLoader(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            HudiDirectoryLister hudiDirectoryLister,
            AsyncQueue<ConnectorSplit> asyncQueue,
            ExecutorService executor,
            HudiSplitWeightProvider hudiSplitWeightProvider,
            Lazy<Map<String, Partition>> lazyPartitionMap,
            boolean enableMetadataTable,
            Lazy<HoodieTableMetadata> lazyTableMetadata,
            CachingHostAddressProvider cachingHostAddressProvider,
            Consumer<Throwable> errorListener)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.hudiDirectoryLister = requireNonNull(hudiDirectoryLister, "hudiDirectoryLister is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.splitGeneratorNumThreads = getSplitGeneratorParallelism(session);
        this.hudiSplitFactory = new HudiSplitFactory(tableHandle, hudiSplitWeightProvider, getTargetSplitSize(session), cachingHostAddressProvider);
        this.lazyPartitionMap = requireNonNull(lazyPartitionMap, "partitions is null");
        this.enableMetadataTable = enableMetadataTable;
        this.executor = requireNonNull(executor, "executor is null");
        this.errorListener = requireNonNull(errorListener, "errorListener is null");
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        this.lazyTableMetadata = lazyTableMetadata;
        this.lazyMetaClient = Lazy.lazily(tableHandle::getMetaClient);
        this.partitionIndexSupportOpt = enableMetadataTable ?
                IndexSupportFactory.createPartitionStatsIndexSupport(schemaTableName, lazyMetaClient, lazyTableMetadata, tableHandle.getRegularPredicates(), session) : Optional.empty();
        this.isMetadataPartitionListingEnabled = isMetadataPartitionListingEnabled(session);
    }

    @Override
    public void run()
    {
        // Wrap entire logic so that ANY error will be thrown out and not cause program to get stuc
        try {
            if (enableMetadataTable) {
                generateSplits(true);
                return;
            }

            // Fallback to partition pruning generator
            generateSplits(false);
        }
        catch (Exception e) {
            errorListener.accept(e);
        }
    }

    private void generateSplits(boolean useIndex)
    {
        // Attempt to apply partition pruning using partition stats index
        Deque<HiveHudiPartitionInfo> partitionQueue = getPartitionInfos(useIndex);
        if (partitionQueue.isEmpty()) {
            asyncQueue.finish();
            return;
        }

        List<HudiPartitionInfoLoader> splitGenerators = new ArrayList<>();
        List<ListenableFuture<Void>> futures = new ArrayList<>();

        int splitGeneratorParallelism = Math.clamp(splitGeneratorNumThreads, 1, partitionQueue.size());
        Executor splitGeneratorExecutor = new BoundedExecutor(executor, splitGeneratorParallelism);

        for (int i = 0; i < splitGeneratorParallelism; i++) {
            HudiPartitionInfoLoader generator = new HudiPartitionInfoLoader(hudiDirectoryLister, tableHandle.getLatestCommitTime(), hudiSplitFactory,
                    asyncQueue, partitionQueue, useIndex);
            splitGenerators.add(generator);
            ListenableFuture<Void> future = Futures.submit(generator, splitGeneratorExecutor);
            addExceptionCallback(future, errorListener);
            futures.add(future);
        }

        // Signal all generators to stop once partition queue is drained
        splitGenerators.forEach(HudiPartitionInfoLoader::stopRunning);

        log.info("Wait for partition pruning split generation to finish on table %s.%s", tableHandle.getSchemaName(), tableHandle.getTableName());
        try {
            Futures.whenAllComplete(futures)
                    .run(asyncQueue::finish, directExecutor())
                    .get();
            log.info("Partition pruning split generation finished on table %s.%s", tableHandle.getSchemaName(), tableHandle.getTableName());
        }
        catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Error generating Hudi split", e);
        }
    }

    private Deque<HiveHudiPartitionInfo> getPartitionInfos(boolean useIndex)
    {
        Map<String, Partition> metadataPartitions;
        if (enableMetadataTable && isMetadataPartitionListingEnabled) {
            try {
                PartitionValueExtractor partitionValueExtractor = getPartitionValueExtractor(lazyMetaClient.get().getTableConfig());
                List<HiveColumnHandle> hivePartitionColumns = tableHandle.getPartitionColumns();

                List<Column> partitionColumns = hivePartitionColumns.stream()
                        .map(column -> new Column(
                                column.getName(),
                                column.getHiveType(),
                                column.getComment(),
                                Collections.emptyMap()))
                        .toList();
                log.info("Listing partitions for %s.%s via metadata table using partition value extractor %s",
                        tableHandle.getSchemaName(), tableHandle.getTableName(), partitionValueExtractor.getClass().getSimpleName());
                metadataPartitions = lazyTableMetadata.get()
                        .getAllPartitionPaths().stream()
                        .map(partitionPath -> buildPartition(partitionPath, partitionColumns, tableHandle, partitionValueExtractor))
                        .collect(Collectors.toMap(
                                this::getHivePartitionName,
                                Function.identity()));

                if (metadataPartitions.isEmpty()) {
                    log.warn("No partitions found via metadata table for %s.%s, switching to metastore-based listing.",
                            tableHandle.getSchemaName(), tableHandle.getTableName());
                    metadataPartitions = lazyPartitionMap.get();
                }
            }
            catch (Exception e) {
                log.error(e, "Failed to get partitions from metadata table %s.%s, falling back to metastore based partition listing",
                        tableHandle.getSchemaName(), tableHandle.getTableName());
                metadataPartitions = lazyPartitionMap.get();
            }
        }
        else {
            metadataPartitions = lazyPartitionMap.get();
        }

        List<String> allPartitions = new ArrayList<>(metadataPartitions.keySet());

        List<String> effectivePartitions = Optional.ofNullable(useIndex && partitionIndexSupportOpt.isPresent()
                ? partitionIndexSupportOpt.get().prunePartitions(allPartitions).orElse(null)
                : null).orElse(allPartitions);

        Map<String, Partition> finalMetadataPartitions = metadataPartitions;
        List<HiveHudiPartitionInfo> hiveHudiPartitionInfos = effectivePartitions.stream()
                .map(partitionName -> buildHiveHudiPartitionInfo(tableHandle, partitionName, finalMetadataPartitions.get(partitionName)))
                .filter(hudiPartitionInfo -> hudiPartitionInfo.doesMatchPredicates() || hudiPartitionInfo.getHivePartitionName().equals(NON_PARTITION))
                .toList();

        return new ConcurrentLinkedDeque<>(hiveHudiPartitionInfos);
    }

    private HiveHudiPartitionInfo buildHiveHudiPartitionInfo(HudiTableHandle tableHandle, String partitionName, Partition partition)
    {
        return new HiveHudiPartitionInfo(
                tableHandle.getSchemaTableName(),
                Location.of(tableHandle.getBasePath()),
                partitionName,
                partition,
                tableHandle.getPartitionColumns(),
                tableHandle.getPartitionPredicates());
    }

    private Partition buildPartition(String partitionPath, List<Column> partitionColumns, HudiTableHandle tableHandle, PartitionValueExtractor partitionValueExtractor)
    {
        if (partitionPath == null || partitionPath.isEmpty()) {
            return Partition.builder()
                    .setDatabaseName(tableHandle.getSchemaName())
                    .setTableName(tableHandle.getTableName())
                    .withStorage(storageBuilder ->
                            storageBuilder.setLocation(tableHandle.getBasePath())
                                    .setStorageFormat(StorageFormat.NULL_STORAGE_FORMAT))
                    .setColumns(ImmutableList.of())
                    .setValues(ImmutableList.of())
                    .build();
        }

        List<String> values = partitionValueExtractor.extractPartitionValuesInPath(partitionPath).stream().map(this::unescapePathName).toList();
        if (partitionColumns.size() != values.size()) {
            throw new HoodieException("Cannot extract partition values from partition path: " + partitionPath);
        }

        return Partition.builder()
                .setDatabaseName(tableHandle.getSchemaName())
                .setTableName(tableHandle.getTableName())
                .withStorage(storageBuilder ->
                        storageBuilder.setLocation(getFullPath(tableHandle.getBasePath(), partitionPath))
                                // Storage format is unused by the Hudi connector
                                .setStorageFormat(StorageFormat.NULL_STORAGE_FORMAT))
                .setColumns(partitionColumns)
                .setValues(values)
                .build();
    }

    private String getFullPath(String basePath, String relativePartitionPath)
    {
        return basePath.endsWith(DELIMITER_STR)
                ? basePath + relativePartitionPath
                : basePath + DELIMITER_STR + relativePartitionPath;
    }

    String getHivePartitionName(Partition partition)
    {
        List<Column> columns = partition.getColumns();
        List<String> values = partition.getValues();

        return IntStream.range(0, columns.size())
                .mapToObj(i -> columns.get(i).getName() + EQUALS_STR + values.get(i))
                .collect(Collectors.joining(DELIMITER_STR));
    }

    private PartitionValueExtractor getPartitionValueExtractor(HoodieTableConfig tableConfig)
    {
        Option<String[]> partitionFieldsOpt = tableConfig.getPartitionFields();

        if (partitionFieldsOpt.isEmpty()) {
            return new NonPartitionedExtractor();
        }

        String[] partitionFields = partitionFieldsOpt.get();
        if (partitionFields.length == 1) {
            return Boolean.parseBoolean(tableConfig.getHiveStylePartitioningEnable()) ?
                    new HiveStylePartitionValueExtractor() : new SinglePartPartitionValueExtractor();
        }

        return new MultiPartKeysValueExtractor();
    }

    private String unescapePathName(String path)
    {
        // fast path, no escaped characters and therefore no copying necessary
        int escapedAtIndex = path.indexOf('%');
        if (escapedAtIndex < 0 || escapedAtIndex + 2 >= path.length()) {
            return path;
        }

        // slow path, unescape into a new string copy
        StringBuilder sb = new StringBuilder();
        int fromIndex = 0;
        while (escapedAtIndex >= 0 && escapedAtIndex + 2 < path.length()) {
            // preceding sequence without escaped characters
            if (escapedAtIndex > fromIndex) {
                sb.append(path, fromIndex, escapedAtIndex);
            }
            // try to parse the to digits after the percent sign as hex
            try {
                int code = HexFormat.fromHexDigits(path, escapedAtIndex + 1, escapedAtIndex + 3);
                sb.append((char) code);
                // advance past the percent sign and both hex digits
                fromIndex = escapedAtIndex + 3;
            }
            catch (NumberFormatException e) {
                // invalid escape sequence, only advance past the percent sign
                sb.append('%');
                fromIndex = escapedAtIndex + 1;
            }
            // find next escaped character
            escapedAtIndex = path.indexOf('%', fromIndex);
        }
        // trailing sequence without escaped characters
        if (fromIndex < path.length()) {
            sb.append(path, fromIndex, path.length());
        }
        return sb.toString();
    }
}
