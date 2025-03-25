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
     *
     * @param metaClient
     * @param tupleDomain
     * @return
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
