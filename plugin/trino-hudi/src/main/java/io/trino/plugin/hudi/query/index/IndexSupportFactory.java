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
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.plugin.hudi.HudiSessionProperties.isColumnStatsIndexEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isNoOpIndexEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isPartitionStatsIndexEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isRecordLevelIndexEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isResolveColumnNameCasingEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isSecondaryIndexEnabled;
import static io.trino.plugin.hudi.HudiUtil.getFieldFromSchema;
import static java.util.Objects.requireNonNull;

/**
 * Factory to create the appropriate HudiIndexSupport strategy based on:
 * 1. Query predicates
 * 2. Available table indexes
 * 3. Configuration flags
 */
public class IndexSupportFactory
{
    private static final Logger log = Logger.get(IndexSupportFactory.class);

    private IndexSupportFactory() {}

    /**
     * Creates the most suitable HudiIndexSupport strategy, considering configuration.
     * Uses Supplier-based lazy instantiation combined with config checks.
     *
     * @param hudiTableHandle The hudi table handle
     * @param lazyMetaClient The Hudi table metadata client that is lazily instantiated.
     * @param tupleDomain The query predicates.
     * @param session Session containing session properties, which is required to control index behaviours for testing/debugging
     * @return An Optional containing the chosen HudiIndexSupport strategy, or empty if none are applicable or enabled.
     */
    public static Optional<HudiIndexSupport> createIndexSupport(
            HudiTableHandle hudiTableHandle,
            Lazy<HoodieTableMetaClient> lazyMetaClient,
            Lazy<HoodieTableMetadata> lazyTableMetadata,
            TupleDomain<HiveColumnHandle> tupleDomain,
            ConnectorSession session)
    {
        TupleDomain<String> transformedTupleDomain = transformTupleDomain(session, hudiTableHandle, tupleDomain);
        SchemaTableName schemaTableName = hudiTableHandle.getSchemaTableName();
        // Define strategies as Suppliers paired with their config (isEnabled) flag
        // IMPORTANT: Order of strategy here determines which index implementation is preferred first
        List<StrategyProvider> strategyProviders = List.of(
                new StrategyProvider(
                        () -> isRecordLevelIndexEnabled(session),
                        () -> new HudiRecordLevelIndexSupport(session, schemaTableName, lazyMetaClient, lazyTableMetadata, transformedTupleDomain)),
                new StrategyProvider(
                        () -> isSecondaryIndexEnabled(session),
                        () -> new HudiSecondaryIndexSupport(session, schemaTableName, lazyMetaClient, lazyTableMetadata, transformedTupleDomain)),
                new StrategyProvider(
                        () -> isColumnStatsIndexEnabled(session),
                        () -> new HudiColumnStatsIndexSupport(session, schemaTableName, lazyMetaClient, lazyTableMetadata, transformedTupleDomain)),
                new StrategyProvider(
                        () -> isNoOpIndexEnabled(session),
                        () -> new HudiNoOpIndexSupport(schemaTableName, lazyMetaClient)));

        for (StrategyProvider provider : strategyProviders) {
            // Check if the strategy is enabled via config before instantiating
            if (provider.isEnabled()) {
                HudiIndexSupport strategy = provider.getStrategy();
                String strategyName = strategy.getClass().getSimpleName(); // Get name for logging

                // Check if the instantiated strategy is applicable
                if (strategy.canApply(transformedTupleDomain)) {
                    log.debug(String.format("Selected %s strategy (Enabled & Applicable).", strategyName));
                    return Optional.of(strategy);
                }
                else {
                    log.debug(String.format("%s is enabled but not applicable for this query.", strategyName));
                    // Strategy object becomes eligible for GC here, acceptable penalty as the object is lightweight
                }
            }
            else {
                log.debug(String.format("Strategy associated with supplier %s is disabled by configuration.", provider.supplier.get().getClass().getSimpleName()));
            }
        }

        log.debug("No suitable and enabled index support strategy found to be applicable.");
        return Optional.empty();
    }

    public static Optional<HudiPartitionStatsIndexSupport> createPartitionStatsIndexSupport(
            HudiTableHandle hudiTableHandle,
            Lazy<HoodieTableMetaClient> lazyMetaClient,
            Lazy<HoodieTableMetadata> lazyTableMetadata,
            TupleDomain<HiveColumnHandle> tupleDomain,
            ConnectorSession session)
    {
        TupleDomain<String> transformedTupleDomain = transformTupleDomain(session, hudiTableHandle, tupleDomain);

        StrategyProvider partitionStatsStrategy = new StrategyProvider(
                () -> isPartitionStatsIndexEnabled(session), () -> new HudiPartitionStatsIndexSupport(session, hudiTableHandle.getSchemaTableName(), lazyMetaClient, lazyTableMetadata, transformedTupleDomain));

        if (partitionStatsStrategy.isEnabled() && partitionStatsStrategy.getStrategy().canApply(transformedTupleDomain)) {
            return Optional.of((HudiPartitionStatsIndexSupport) partitionStatsStrategy.getStrategy());
        }
        return Optional.empty();
    }

    private static TupleDomain<String> transformTupleDomain(ConnectorSession session, HudiTableHandle hudiTableHandle, TupleDomain<HiveColumnHandle> tupleDomain)
    {
        if (isResolveColumnNameCasingEnabled(session)) {
            // if column case reconciliation is enabled, transform the tuple domain keys to match the column names from the Hudi table.
            return tupleDomain.transformKeys(hiveColumnHandle ->
                    getFieldFromSchema(hiveColumnHandle.getName(), hudiTableHandle.getTableSchema()).name());
        }
        return tupleDomain.transformKeys(HiveColumnHandle::getName);
    }

    /**
     * Helper class to pair the configuration check with the strategy supplier to allow for lazy initialization.
     */
    private static class StrategyProvider
    {
        private final Supplier<Boolean> isEnabled;
        private final Supplier<HudiIndexSupport> supplier;

        StrategyProvider(Supplier<Boolean> isEnabled, Supplier<HudiIndexSupport> supplier)
        {
            this.isEnabled = requireNonNull(isEnabled);
            this.supplier = requireNonNull(supplier);
        }

        boolean isEnabled()
        {
            return isEnabled.get();
        }

        HudiIndexSupport getStrategy()
        {
            return supplier.get();
        }
    }
}
