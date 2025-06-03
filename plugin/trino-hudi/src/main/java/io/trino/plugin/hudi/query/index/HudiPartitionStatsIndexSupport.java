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

import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.util.TupleDomainUtils;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.util.TupleDomainUtils.hasSimpleNullCheck;

public class HudiPartitionStatsIndexSupport
        extends HudiColumnStatsIndexSupport
{
    private static final Logger log = Logger.get(HudiColumnStatsIndexSupport.class);

    public HudiPartitionStatsIndexSupport(HoodieTableMetaClient metaClient, TupleDomain<HiveColumnHandle> regularColumnPredicates)
    {
        super(log, metaClient, regularColumnPredicates);
    }

    @Override
    public Map<String, List<FileSlice>> lookupCandidateFilesInMetadataTable(HoodieTableMetadata metadataTable, Map<String, List<FileSlice>> inputFileSlices, TupleDomain<String> regularColumnPredicates)
    {
        throw new UnsupportedOperationException("This method is not supported by " + getClass().getSimpleName());
    }

    public Optional<List<String>> prunePartitions(
            List<String> allPartitions,
            HoodieTableMetadata metadataTable,
            TupleDomain<String> regularColumnPredicates)
    {
        // Filter out predicates containing simple null checks (`IS NULL` or `IS NOT NULL`)
        TupleDomain<String> filteredRegularPredicates = regularColumnPredicates.filter((_, domain) -> !hasSimpleNullCheck(domain));

        // Sanity check, if no regular domains, return immediately
        if (filteredRegularPredicates.getDomains().isEmpty()) {
            return Optional.empty();
        }

        List<String> regularColumns = new ArrayList<>(filteredRegularPredicates.getDomains().get().keySet());

        // Get columns to filter on
        List<String> encodedTargetColumnNames = regularColumns
                .stream()
                .map(col -> new ColumnIndexID(col).asBase64EncodedString()).toList();

        // Map of partition stats keyed by partition name
        Map<String, List<HoodieMetadataColumnStats>> statsByPartitionName = metadataTable.getRecordsByKeyPrefixes(
                        encodedTargetColumnNames,
                        HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS, true)
                .collectAsList()
                .stream()
                .filter(f -> f.getData().getColumnStatMetadata().isPresent())
                .map(f -> f.getData().getColumnStatMetadata().get())
                .collect(Collectors.groupingBy(HoodieMetadataColumnStats::getFileName));

        // For each partition, determine if it should be kept based on stats availability and predicate evaluation
        List<String> prunedPartitions = allPartitions.stream()
                .filter(partition -> {
                    // Check if stats exist for this partition
                    List<HoodieMetadataColumnStats> partitionStats = statsByPartitionName.get(partition);
                    if (partitionStats == null) {
                        // Partition has no stats in the index, keep it
                        return true;
                    }
                    else {
                        // Partition has stats, evaluate the predicate against them
                        // Keep the partition only if the predicate evaluates to true
                        // Important: If some columns in encodedTargetColumnNames is not available in partition stats, partition will not be pruned iff all available predicate
                        // evaluates to true. Since we cannot determine if the predicate will evaluate to true or not on the missing stat, adopt conservative measure to true,
                        // i.e. to not prune
                        return evaluateStatisticPredicate(filteredRegularPredicates, partitionStats, regularColumns);
                    }
                })
                .collect(Collectors.toList());

        return Optional.of(prunedPartitions);
    }

    @Override
    public boolean isIndexSupportAvailable()
    {
        return metaClient.getTableConfig().getMetadataPartitions()
                .contains(HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS);
    }

    @Override
    public boolean canApply(TupleDomain<String> tupleDomain)
    {
        // Important: has the same implementation as col stats superclass, only difference is that log messages are different
        if (!isIndexSupportAvailable()) {
            log.debug("Partition Stats Index partition is not enabled in metadata table.");
            return false;
        }

        Map<String, HoodieIndexDefinition> indexDefinitions = getAllIndexDefinitions();
        HoodieIndexDefinition partitionsStatsIndex = indexDefinitions.get(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
        if (partitionsStatsIndex == null || partitionsStatsIndex.getSourceFields() == null || partitionsStatsIndex.getSourceFields().isEmpty()) {
            log.warn("Partition stats index definition is missing or has no source fields defined");
            return false;
        }

        // Optimization applied: Only consider applicable if predicates reference indexed columns
        List<String> sourceFields = partitionsStatsIndex.getSourceFields();
        boolean applicable = TupleDomainUtils.areSomeFieldsReferenced(tupleDomain, sourceFields);

        if (applicable) {
            log.debug("Partition Stats Index is available and applicable (predicates reference indexed columns).");
        }
        else {
            log.debug("Partition Stats Index is available, but predicates do not reference any indexed columns.");
        }
        return applicable;
    }
}
