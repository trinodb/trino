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
import io.trino.plugin.hudi.util.TupleDomainUtils;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.query.index.HudiRecordLevelIndexSupport.constructRecordKeys;
import static io.trino.plugin.hudi.query.index.HudiRecordLevelIndexSupport.extractPredicatesForColumns;

public class HudiSecondaryIndexSupport
        extends HudiBaseIndexSupport
{
    private static final Logger log = Logger.get(HudiSecondaryIndexSupport.class);

    public HudiSecondaryIndexSupport(HoodieTableMetaClient metaClient)
    {
        super(log, metaClient);
    }

    @Override
    public Map<String, List<FileSlice>> lookupCandidateFilesInMetadataTable(
            HoodieTableMetadata metadataTable,
            Map<String, List<FileSlice>> inputFileSlices,
            TupleDomain<String> regularColumnPredicates)
    {
        if (regularColumnPredicates.isAll() || metaClient.getIndexMetadata().isEmpty()) {
            return inputFileSlices;
        }

        Map<String, HoodieIndexDefinition> indexDefinitionMap = metaClient.getIndexMetadata().get()
                .getIndexDefinitions();

        if (indexDefinitionMap.isEmpty()) {
            return inputFileSlices;
        }

        List<String> queryReferencedColumns = regularColumnPredicates.getDomains().get().keySet().stream().toList();

        // Build a map of indexName to sourceFields (column names) that secondary index is built on
        // Each List<String> should only contain one element as secondary indices only support one column
        // Only filter for sourceFields that match the predicates
        Map<String, List<String>> indexDefinitions = indexDefinitionMap.entrySet()
                .stream()
                .filter(e -> e.getKey().contains(HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX)
                        && queryReferencedColumns.contains(e.getValue().getSourceFields().getFirst()))
                .collect(Collectors.toMap(e -> e.getValue().getIndexName(),
                        e -> e.getValue().getSourceFields()));

        if (indexDefinitions.isEmpty()) {
            return inputFileSlices;
        }

        // Only use the first key in the indexDefinition
        Iterator<Map.Entry<String, List<String>>> indexDefinitionsIter = indexDefinitions.entrySet().iterator();
        if (!indexDefinitionsIter.hasNext()) {
            return inputFileSlices;
        }

        Map.Entry<String, List<String>> entry = indexDefinitionsIter.next();
        TupleDomain<String> filteredDomains = extractPredicatesForColumns(regularColumnPredicates, entry.getValue());
        List<String> secondaryKeys = constructRecordKeys(filteredDomains, entry.getValue().stream().toList());
        Map<String, List<HoodieRecordGlobalLocation>> recordKeyLocationsMap =
                metadataTable.readSecondaryIndex(secondaryKeys, entry.getKey());

        // Prune files
        List<String> fileIds = recordKeyLocationsMap.values().stream()
                .flatMap(List::stream).map(HoodieRecordGlobalLocation::getFileId).toList();
        Map<String, List<FileSlice>> candidateFileSlices = inputFileSlices.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e
                        .getValue()
                        .stream()
                        .filter(fileSlice -> fileIds.contains(fileSlice.getFileId()))
                        .collect(Collectors.toList())));

        this.printDebugMessage(candidateFileSlices, inputFileSlices);

        return candidateFileSlices;
    }

    /**
     * Determines whether secondary index (SI) should be used based on the given tuple domain and index definitions.
     * <p>
     * This method first filters out the secondary index definitions from the provided map of index definitions.
     * It then checks if there are any secondary indices defined. If no secondary indices are found, it returns {@code false}.
     * <p>
     * For each secondary index definition, the method verifies two conditions:
     * <ol>
     * <li>All fields referenced in the tuple domain must be part of the source fields of the secondary index.</li>
     * <li>The predicates on these fields must be either of type IN or EQUAL.</li>
     * </ol>
     * <p>
     * If at least one secondary index definition meets these conditions, the method returns {@code true}; otherwise,
     * it returns {@code false}.
     *
     * @param tupleDomain the domain representing the constraints on the columns
     * @param metaClient hoodie table metaclient
     * HoodieIndexDefinition object
     * @return {@code true} if at least one secondary index can be used based on the given tuple domain; otherwise,
     * {@code false}
     */
    public static boolean shouldUseIndex(TupleDomain<String> tupleDomain, HoodieTableMetaClient metaClient)
    {
        // Filter out definitions that are secondary indices
        Map<String, HoodieIndexDefinition> secondaryIndexDefinitions = IndexSupportFactory.getIndexDefinitions(metaClient)
                .entrySet().stream()
                .filter(e -> e.getKey().contains(HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX))
                .collect(Collectors.toMap(e -> e.getValue().getIndexName(),
                        Map.Entry::getValue));

        if (secondaryIndexDefinitions.isEmpty()) {
            return false;
        }

        // Predicates referencing columns with secondary index needs to be IN or EQUAL only
        boolean atLeastOnePredicateValid = false;
        for (Map.Entry<String, HoodieIndexDefinition> secondaryIndexDefinition : secondaryIndexDefinitions.entrySet()) {
            List<String> sourceFields = secondaryIndexDefinition.getValue().getSourceFields();
            boolean isSecondaryIndexDefUsable = (TupleDomainUtils.areAllFieldsReferenced(tupleDomain, sourceFields)
                    && TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields));
            atLeastOnePredicateValid |= isSecondaryIndexDefUsable;
        }

        return atLeastOnePredicateValid;
    }
}
