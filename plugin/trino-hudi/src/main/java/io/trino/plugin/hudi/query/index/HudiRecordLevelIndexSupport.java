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
import io.airlift.units.Duration;
import io.trino.plugin.hudi.util.TupleDomainUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiSessionProperties.getRecordIndexWaitTimeout;
import static io.trino.plugin.hudi.HudiUtil.collectAsMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HudiRecordLevelIndexSupport
        extends HudiBaseIndexSupport
{
    private static final Logger log = Logger.get(HudiRecordLevelIndexSupport.class);

    private final CompletableFuture<Optional<Set<String>>> relevantFileIdsFuture;
    private final Duration recordIndexWaitTimeout;
    private final long futureStartTimeMs;

    public HudiRecordLevelIndexSupport(ConnectorSession session, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient, Lazy<HoodieTableMetadata> lazyTableMetadata, TupleDomain<String> regularColumnPredicates)
    {
        super(log, schemaTableName, lazyMetaClient);
        this.recordIndexWaitTimeout = getRecordIndexWaitTimeout(session);
        if (regularColumnPredicates.isAll()) {
            log.debug("Predicates cover all data, skipping record level index lookup.");
            this.relevantFileIdsFuture = CompletableFuture.completedFuture(Optional.empty());
        }
        else {
            this.relevantFileIdsFuture = CompletableFuture.supplyAsync(() -> {
                HoodieTimer timer = HoodieTimer.start();
                Option<String[]> recordKeyFieldsOpt = lazyMetaClient.get().getTableConfig().getRecordKeyFields();
                if (recordKeyFieldsOpt.isEmpty() || recordKeyFieldsOpt.get().length == 0) {
                    // Should not happen since canApply checks for this, include for safety
                    throw new TrinoException(HUDI_BAD_DATA, "Record key fields must be defined to use Record Level Index.");
                }
                List<String> recordKeyFields = Arrays.asList(recordKeyFieldsOpt.get());

                // Only extract the predicates relevant to the record key fields
                TupleDomain<String> filteredDomains = TupleDomainUtils.extractPredicatesForColumns(regularColumnPredicates, recordKeyFields);

                // Construct the actual record keys based on the filtered predicates using Hudi's encoding scheme
                List<String> recordKeys = TupleDomainUtils.constructRecordKeys(filteredDomains, recordKeyFields);

                if (recordKeys.isEmpty()) {
                    // If key construction fails (e.g., incompatible predicates not caught by canApply, or placeholder issue)
                    log.warn("Took %s ms, but could not construct record keys from predicates. Skipping record index pruning for table %s",
                            timer.endTimer(), schemaTableName);
                    return Optional.empty();
                }
                log.debug(String.format("Constructed %d record keys for index lookup.", recordKeys.size()));

                // Perform index lookup in metadataTable
                // Map is keyed by the Hudi record key string (as encoded by the table's key generator),
                // and the value is the global location (partition path + file ID) of that record.
                Map<String, HoodieRecordGlobalLocation> recordIndex = collectAsMap(lazyTableMetadata.get().readRecordIndexLocationsWithKeys(HoodieListData.eager(recordKeys)));
                if (recordIndex.isEmpty()) {
                    log.debug("Record level index lookup took %s ms but returned no locations for the given keys %s for table %s",
                            timer.endTimer(), recordKeys, schemaTableName);
                    // Return all original fileSlices
                    return Optional.empty();
                }

                // Collect fileIds for pruning
                Set<String> relevantFiles = recordIndex.values().stream()
                        .map(HoodieRecordGlobalLocation::getFileId)
                        .collect(Collectors.toSet());
                log.debug("Record level index lookup took %s ms and identified %d relevant file IDs.", timer.endTimer(), relevantFiles.size());

                return Optional.of(relevantFiles);
            });
        }
        this.futureStartTimeMs = System.currentTimeMillis();
    }

    @Override
    public boolean shouldSkipFileSlice(FileSlice slice)
    {
        try {
            if (relevantFileIdsFuture.isDone()) {
                Optional<Set<String>> relevantFileIds = relevantFileIdsFuture.get();
                return relevantFileIds.map(fileIds -> !fileIds.contains(slice.getFileId())).orElse(false);
            }

            long elapsedMs = System.currentTimeMillis() - futureStartTimeMs;
            if (elapsedMs > recordIndexWaitTimeout.toMillis()) {
                // Took too long; skip decision
                return false;
            }

            long remainingMs = Math.max(0, recordIndexWaitTimeout.toMillis() - elapsedMs);
            Optional<Set<String>> relevantFileIds = relevantFileIdsFuture.get(remainingMs, MILLISECONDS);
            return relevantFileIds.map(fileIds -> !fileIds.contains(slice.getFileId())).orElse(false);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        catch (TimeoutException | ExecutionException e) {
            return false;
        }
    }

    /**
     * Checks if the Record Level Index is available and the query predicates
     * reference all record key fields with compatible (IN/EQUAL) constraints.
     */
    @Override
    public boolean canApply(TupleDomain<String> tupleDomain)
    {
        if (!isIndexSupportAvailable()) {
            log.debug("Record Level Index partition is not enabled in metadata.");
            return false;
        }

        Option<String[]> recordKeyFieldsOpt = lazyMetaClient.get().getTableConfig().getRecordKeyFields();
        if (recordKeyFieldsOpt.isEmpty() || recordKeyFieldsOpt.get().length == 0) {
            log.debug("Record key fields are not defined in table config.");
            return false;
        }
        List<String> recordKeyFields = Arrays.asList(recordKeyFieldsOpt.get());

        // Ensure that predicates reference all record key fields and use IN/EQUAL
        boolean applicable = TupleDomainUtils.areAllFieldsReferenced(tupleDomain, recordKeyFields)
                && TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, recordKeyFields);

        if (!applicable) {
            log.debug("Predicates do not reference all record key fields or use non-compatible (non IN/EQUAL) constraints.");
        }
        else {
            log.debug("Record Level Index is available and applicable based on predicates.");
        }
        return applicable;
    }

    private boolean isIndexSupportAvailable()
    {
        return lazyMetaClient.get().getTableConfig().getMetadataPartitions()
                .contains(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX);
    }
}
