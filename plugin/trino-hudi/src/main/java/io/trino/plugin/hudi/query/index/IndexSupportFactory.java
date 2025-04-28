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

import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.Map;
import java.util.Optional;

/**
 * Factory Pattern to facilitate index initialization
 */
public class IndexSupportFactory
{
    // Utility classes should not have a public or default constructor
    private IndexSupportFactory() {}

    public static Map<String, HoodieIndexDefinition> getIndexDefinitions(HoodieTableMetaClient metaClient)
    {
        if (metaClient.getIndexMetadata().isEmpty()) {
            return Map.of();
        }

        Map<String, HoodieIndexDefinition> indexDefinitions = metaClient.getIndexMetadata().get().getIndexDefinitions();
        return indexDefinitions;
    }

    /**
     * Chooses the index implementation to use base on the available indexes in the hudi table and predicate
     * of the query.
     * Priority:
     * <ol>
     *     <li>HudiRecordLevelIndexSupport</li>
     *     <li>HudiBucketIndexSupport (TODO: voon)</li>
     *     <li>HudiSecondaryIndexSupport</li>
     *     <li>HudiExpressionIndexSupport (TODO: voon)</li>
     *     <li>HudiBloomFiltersIndexSupport (TODO: voon)</li>
     *     <li>HudiColumnStatsIndexSupport</li>
     * </ol>
     */
    public static Optional<HudiIndexSupport> createIndexDefinition(
            HoodieTableMetaClient metaClient, TupleDomain<String> tupleDomain)
    {
        if (HudiRecordLevelIndexSupport.shouldUseIndex(tupleDomain, metaClient)) {
            return Optional.of(new HudiRecordLevelIndexSupport(metaClient));
        }
        else if (HudiSecondaryIndexSupport.shouldUseIndex(tupleDomain, metaClient)) {
            return Optional.of(new HudiSecondaryIndexSupport(metaClient));
        }
        else if (HudiColumnStatsIndexSupport.shouldUseIndex(metaClient, tupleDomain)) {
            return Optional.of(new HudiColumnStatsIndexSupport(metaClient));
        }
        else {
            return Optional.empty();
        }
    }
}
