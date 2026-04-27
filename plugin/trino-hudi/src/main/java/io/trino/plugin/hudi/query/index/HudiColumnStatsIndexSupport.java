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
package io.trino.plugin.hudi.query.index;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.hudi.util.TupleDomainUtils;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.metadata.ColumnStatsIndexPrefixRawKey;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.HudiSessionProperties.getColumnStatsWaitTimeout;
import static io.trino.plugin.hudi.HudiSessionProperties.isScopeColumnStatsToPrunedPartitions;

public class HudiColumnStatsIndexSupport
        extends HudiBaseIndexSupport
{
    private static final Logger log = Logger.get(HudiColumnStatsIndexSupport.class);
    // file name -> column name -> domain with column stats
    // volatile because the future may be created lazily from a different thread when scoped
    // lookups are enabled (see setPrunedPartitionPaths).
    private volatile CompletableFuture<Optional<Map<String, Map<String, Domain>>>> domainsWithStatsFuture;
    protected final TupleDomain<String> regularColumnPredicates;
    private final List<String> regularColumns;
    private final Map<String, Type> columnTypes;
    private final Lazy<HoodieTableMetadata> lazyTableMetadata;
    private final Duration columnStatsWaitTimeout;
    private final boolean scopeColumnStatsToPrunedPartitions;
    private volatile long futureStartTimeMs;

    public HudiColumnStatsIndexSupport(ConnectorSession session, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient, Lazy<HoodieTableMetadata> lazyTableMetadata, TupleDomain<String> regularColumnPredicates)
    {
        this(log, session, schemaTableName, lazyMetaClient, lazyTableMetadata, regularColumnPredicates);
    }

    public HudiColumnStatsIndexSupport(Logger log, ConnectorSession session, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient, Lazy<HoodieTableMetadata> lazyTableMetadata, TupleDomain<String> regularColumnPredicates)
    {
        super(log, schemaTableName, lazyMetaClient);
        this.columnStatsWaitTimeout = getColumnStatsWaitTimeout(session);
        this.scopeColumnStatsToPrunedPartitions = isScopeColumnStatsToPrunedPartitions(session);
        this.regularColumnPredicates = regularColumnPredicates;
        this.regularColumns = regularColumnPredicates.getDomains()
                .map(domains -> new ArrayList<>(domains.keySet()))
                .orElseGet(ArrayList::new);
        this.columnTypes = regularColumnPredicates.getDomains()
                .map(domains -> domains.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getType())))
                .orElseGet(Map::of);
        this.lazyTableMetadata = lazyTableMetadata;

        if (regularColumnPredicates.isAll() || regularColumnPredicates.getDomains().isEmpty()) {
            this.domainsWithStatsFuture = CompletableFuture.completedFuture(Optional.empty());
            this.futureStartTimeMs = System.currentTimeMillis();
        }
        else if (scopeColumnStatsToPrunedPartitions) {
            // Defer the lookup until pruned partition paths are provided via
            // setPrunedPartitionPaths(). shouldSkipFileSlice() falls back to no-skip if that
            // never happens.
            this.domainsWithStatsFuture = null;
            this.futureStartTimeMs = 0L;
        }
        else {
            // Existing behaviour: fire a column-only prefix lookup eagerly and let it run
            // concurrently with file system view loading.
            this.domainsWithStatsFuture = startLookup(buildColumnOnlyPrefixKeys());
            this.futureStartTimeMs = System.currentTimeMillis();
        }
    }

    @Override
    public void setPrunedPartitionPaths(List<String> relativePartitionPaths)
    {
        if (!scopeColumnStatsToPrunedPartitions || domainsWithStatsFuture != null) {
            return;
        }
        if (regularColumnPredicates.isAll() || regularColumnPredicates.getDomains().isEmpty()) {
            return;
        }
        List<ColumnStatsIndexPrefixRawKey> rawKeys = buildColumnPartitionPrefixKeys(relativePartitionPaths);
        this.futureStartTimeMs = System.currentTimeMillis();
        this.domainsWithStatsFuture = startLookup(rawKeys);
    }

    private List<ColumnStatsIndexPrefixRawKey> buildColumnOnlyPrefixKeys()
    {
        return regularColumns.stream()
                .map(ColumnStatsIndexPrefixRawKey::new)
                .collect(Collectors.toList());
    }

    private List<ColumnStatsIndexPrefixRawKey> buildColumnPartitionPrefixKeys(List<String> relativePartitionPaths)
    {
        ImmutableList.Builder<ColumnStatsIndexPrefixRawKey> builder = ImmutableList.builder();
        for (String column : regularColumns) {
            for (String partition : relativePartitionPaths) {
                builder.add(new ColumnStatsIndexPrefixRawKey(column, partition));
            }
        }
        return builder.build();
    }

    private CompletableFuture<Optional<Map<String, Map<String, Domain>>>> startLookup(List<ColumnStatsIndexPrefixRawKey> rawKeys)
    {
        if (rawKeys.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return CompletableFuture.supplyAsync(() -> {
            HoodieTimer timer = HoodieTimer.start();
            if (!lazyMetaClient.get().getTableConfig().getMetadataPartitions()
                    .contains(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)) {
                return Optional.empty();
            }

            Map<String, Map<String, Domain>> domainsWithStats =
                    lazyTableMetadata.get().getRecordsByKeyPrefixes(
                                    HoodieListData.lazy(rawKeys),
                                    HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, true)
                            .collectAsList()
                            .stream()
                            .filter(f -> f.getData().getColumnStatMetadata().isPresent())
                            .map(f -> f.getData().getColumnStatMetadata().get())
                            .collect(Collectors.groupingBy(
                                    HoodieMetadataColumnStats::getFileName,
                                    Collectors.toMap(
                                            HoodieMetadataColumnStats::getColumnName,
                                            // Pre-compute the Domain object for each HoodieMetadataColumnStats
                                            stats -> getDomainFromColumnStats(stats.getColumnName(), columnTypes.get(stats.getColumnName()), stats))));

            log.debug("Column stats lookup took %s ms over %d prefix keys and identified %d relevant file IDs.",
                    timer.endTimer(), rawKeys.size(), domainsWithStats.size());

            return Optional.of(domainsWithStats);
        });
    }

    @Override
    public boolean shouldSkipFileSlice(FileSlice slice)
    {
        CompletableFuture<Optional<Map<String, Map<String, Domain>>>> future = domainsWithStatsFuture;
        if (future == null) {
            // Scoped lookup was enabled but pruned partition paths were never published.
            // Fall back to no-skip rather than blocking forever or throwing.
            return false;
        }
        try {
            if (future.isDone()) {
                Optional<Map<String, Map<String, Domain>>> domainsWithStatsOpt = future.get();
                return domainsWithStatsOpt
                        .map(domainsWithStats -> shouldSkipFileSlice(slice, domainsWithStats, regularColumnPredicates, regularColumns))
                        .orElse(false);
            }

            long elapsedMs = System.currentTimeMillis() - futureStartTimeMs;
            if (elapsedMs > columnStatsWaitTimeout.toMillis()) {
                // Took too long; skip decision
                return false;
            }

            // If still within the timeout window, wait up to the remaining time
            long remainingMs = Math.max(0, columnStatsWaitTimeout.toMillis() - elapsedMs);
            Optional<Map<String, Map<String, Domain>>> statsOpt =
                    future.get(remainingMs, TimeUnit.MILLISECONDS);

            return statsOpt
                    .map(stats -> shouldSkipFileSlice(slice, stats, regularColumnPredicates, regularColumns))
                    .orElse(false);
        }
        catch (TimeoutException | InterruptedException | ExecutionException e) {
            return false;
        }
    }

    @Override
    public boolean canApply(TupleDomain<String> tupleDomain)
    {
        boolean isIndexSupported = isIndexSupportAvailable();
        // indexDefinition is only available after table version EIGHT
        // For tables that have versions < EIGHT, column stats index is available as long as partition in metadata is available
        if (!isIndexSupported || lazyMetaClient.get().getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
            log.debug("Column Stats Index partition is not enabled in metadata.");
            return isIndexSupported;
        }

        Map<String, HoodieIndexDefinition> indexDefinitions = getAllIndexDefinitions();
        HoodieIndexDefinition colStatsDefinition = indexDefinitions.get(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
        if (colStatsDefinition == null || colStatsDefinition.getSourceFields() == null || colStatsDefinition.getSourceFields().isEmpty()) {
            log.warn("Column stats index definition is missing or has no source fields defined");
            return false;
        }

        // Optimization applied: Only consider applicable if predicates reference indexed columns
        List<String> sourceFields = colStatsDefinition.getSourceFields();
        boolean applicable = TupleDomainUtils.areSomeFieldsReferenced(tupleDomain, sourceFields);

        if (applicable) {
            log.debug("Column Stats Index is available and applicable (predicates reference indexed columns).");
        }
        else {
            log.debug("Column Stats Index is available, but predicates do not reference any indexed columns.");
        }
        return applicable;
    }

    public boolean isIndexSupportAvailable()
    {
        return lazyMetaClient.get().getTableConfig().getMetadataPartitions()
                .contains(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
    }

    private static boolean shouldSkipFileSlice(
            FileSlice fileSlice,
            Map<String, Map<String, Domain>> domainsWithStats,
            TupleDomain<String> regularColumnPredicates,
            List<String> regularColumns)
    {
        List<String> filesToLookUp = new ArrayList<>();
        fileSlice.getBaseFile()
                .map(BaseFile::getFileName)
                .ifPresent(filesToLookUp::add);

        if (fileSlice.hasLogFiles()) {
            fileSlice.getLogFiles().forEach(logFile -> filesToLookUp.add(logFile.getFileName()));
        }

        // if any log or base file in the file slice matches the predicate, all files in the file slice needs to be read
        return filesToLookUp.stream().allMatch(fileName -> {
            // If no stats exist for this specific file, we cannot prune it.
            if (!domainsWithStats.containsKey(fileName)) {
                return false;
            }
            Map<String, Domain> fileDomainsWithStats = domainsWithStats.get(fileName);
            return !TupleDomainUtils.evaluateStatisticPredicate(regularColumnPredicates, fileDomainsWithStats, regularColumns);
        });
    }

    static Domain getDomainFromColumnStats(String colName, Type type, HoodieMetadataColumnStats statistics)
    {
        if (statistics == null) {
            return Domain.all(type);
        }
        boolean hasNullValue = statistics.getNullCount() != 0L;
        boolean hasNonNullValue = statistics.getValueCount() - statistics.getNullCount() > 0;
        if (!hasNonNullValue || statistics.getMaxValue() == null || statistics.getMinValue() == null) {
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
        if (!(statistics.getMinValue() instanceof GenericRecord) ||
                !(statistics.getMaxValue() instanceof GenericRecord)) {
            return Domain.all(type);
        }
        return TupleDomainUtils.getDomainFromColumnStats(colName, type, ((GenericRecord) statistics.getMinValue()).get(0),
                ((GenericRecord) statistics.getMaxValue()).get(0), hasNullValue);
    }
}
