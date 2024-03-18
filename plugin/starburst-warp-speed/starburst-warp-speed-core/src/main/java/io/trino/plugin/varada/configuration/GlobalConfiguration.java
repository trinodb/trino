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
package io.trino.plugin.varada.configuration;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import io.airlift.log.Logger;
import io.varada.tools.certification.SwaggerExposingLevel;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.LOCAL_DATA_STORAGE_PREFIX;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;

@SuppressWarnings("unused")
public class GlobalConfiguration
{
    public static final String CONFIG_IS_SINGLE = "config.is-single";
    public static final String CLUSTER_UP_TIME = "cluster_up_time";
    public static final String ENABLE_DEFAULT_WARMING = "enable-default-warming";
    public static final String DEFAULT_WARMING_INDEX = "default-warming-index";
    public static final String LOCAL_STORE_PATH = "local-store.path";
    public static final String MAX_COLLECT_COLUMNS_SKIP_DEFAULT_WARMING = "max-collect-columns-skip-default-warming";
    public static final String CERT_LOCAL_LOCATION = "config.cert-local-location";
    public static final String DEVICE_NQN_IDENTIFIER = "config.device-identifier";
    public static final String AZURE_CONNECTION_STRING = "config.azure.connection-string";
    public static final String STATS_COLLECTION_ENABLED = "config.stats-collection-enabled";
    public static final String FAILURE_GENERATOR_ENABLED = "config.failure-generator-enabled";
    public static final String DATA_ONLY_WARMING = "data-only-warming";
    public static final int MAX_NUMBER_OF_MAPPED_MATCH_COLLECT_ELEMENTS = 1 << Byte.SIZE; //256
    private static final Logger logger = Logger.get(GlobalConfiguration.class);

    private final Optional<String> authorization = Optional.empty();  // by default, no authorization
    private int stripeSize = 32;
    private String cardinalityBuckets = "1000,1000000"; // allows applying most selective predicate first when using predicate push-down
    private boolean isSingle;
    private Set<String> unsupportedFunctions = Collections.emptySet();
    private long clusterUpTime;
    private boolean enableDefaultWarming = true;
    private boolean createIndexInDefaultWarming;
    private int consistentSplitBucketsPerWorker = 2048;
    private int exportDelayInSeconds;
    private String localStorePath = "/opt/data/";
    private int maxCollectColumnsSkipDefaultWarming = 128;
    private int predicateSimplifyThreshold = 1_000_000;
    private long reservationUsageForSingleTxInBytes = 1024L * 1024 * 128;
    private int maxWarmRetries = 2;
    private int warmRetryBackoffFactorInMillis = 1000;
    private int maxWarmupIterationsPerQuery = 2000;
    private int warmDataVarcharMaxLength = 2048;
    private String certLocalLocalLocation;
    private String deviceIdentifier;
    private String azureConnectionString;
    private boolean allowVaradaStatsCollection;
    private boolean failureGeneratorEnabled;
    private SwaggerExposingLevel swaggerExposingLevel = SwaggerExposingLevel.DEBUG;

    private boolean enableImportExport;
    private boolean enableExportAppendOnCloud = true;
    private boolean enableMatchCollect = true;
    private boolean enableMappedMatchCollect = true;
    private boolean enableOrPushdown = true;
    private boolean enableRangeFilter = true;
    private int shapingLoggerThreshold = 1000;
    private Duration shapingLoggerDuration = Duration.ofSeconds(60);
    private int shapingLoggerNumberOfSamples = 3;
    private boolean dataOnlyWarming;
    private boolean enableInverseWithNulls;

    public int getStripeSize()
    {
        return stripeSize;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.stripesize")
    @Config(WARP_SPEED_PREFIX + "config.stripesize")
    public void setStripeSize(int stripeSize)
    {
        this.stripeSize = stripeSize;
    }

    public boolean getIsSingle()
    {
        return isSingle;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + CONFIG_IS_SINGLE)
    @Config(WARP_SPEED_PREFIX + CONFIG_IS_SINGLE)
    public void setIsSingle(boolean isSingle)
    {
        this.isSingle = isSingle;
    }

    public String getCardinalityBuckets()
    {
        return cardinalityBuckets;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.cardinality-buckets")
    @Config(WARP_SPEED_PREFIX + "config.cardinality-buckets")
    public void setCardinalityBuckets(String cardinalityBuckets)
    {
        this.cardinalityBuckets = cardinalityBuckets;
    }

    public boolean getEnableImportExport()
    {
        return enableImportExport;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.import-export")
    @Config(WARP_SPEED_PREFIX + "enable.import-export")
    public void setEnableImportExport(boolean enableImportExport)
    {
        this.enableImportExport = enableImportExport;
    }

    public boolean getEnableExportAppendOnCloud()
    {
        return enableExportAppendOnCloud;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.export-append-on-cloud")
    @Config(WARP_SPEED_PREFIX + "enable.export-append-on-cloud")
    public void setEnableExportAppendOnCloud(boolean enableExportAppendOnCloud)
    {
        this.enableExportAppendOnCloud = enableExportAppendOnCloud;
    }

    public boolean getEnableMatchCollect()
    {
        return enableMatchCollect;
    }

    public boolean getEnableMappedMatchCollect()
    {
        return enableMappedMatchCollect;
    }

    public boolean getEnableInverseWithNulls()
    {
        return enableInverseWithNulls;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.match-collect")
    @Config(WARP_SPEED_PREFIX + "enable.match-collect")
    public void setEnableMatchCollect(boolean enableMatchCollect)
    {
        this.enableMatchCollect = enableMatchCollect;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.mapped_match-collect")
    @Config(WARP_SPEED_PREFIX + "enable.mapped-match-collect")
    public void setEnableMappedMatchCollect(boolean enableMappedMatchCollect)
    {
        this.enableMappedMatchCollect = enableMappedMatchCollect;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.inverse-with-nulls")
    @Config(WARP_SPEED_PREFIX + "enable.inverse-with-nulls")
    public void setEnableInverseWithNulls(boolean enableInverseWithNulls)
    {
        this.enableInverseWithNulls = enableInverseWithNulls;
    }

    public boolean getEnableOrPushdown()
    {
        return enableOrPushdown;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.or-pushdown")
    @Config(WARP_SPEED_PREFIX + "enable.or-pushdown")
    public void setEnableOrPushdown(boolean enableOrPushdown)
    {
        this.enableOrPushdown = enableOrPushdown;
    }

    public boolean isEnableDefaultWarming()
    {
        return enableDefaultWarming;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + ENABLE_DEFAULT_WARMING)
    @Config(WARP_SPEED_PREFIX + ENABLE_DEFAULT_WARMING)
    public void setEnableDefaultWarming(boolean enableDefaultWarming)
    {
        this.enableDefaultWarming = enableDefaultWarming;
    }

    public boolean isCreateIndexInDefaultWarming()
    {
        return createIndexInDefaultWarming;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + DEFAULT_WARMING_INDEX)
    @Config(WARP_SPEED_PREFIX + DEFAULT_WARMING_INDEX)
    public void setCreateIndexInDefaultWarming(boolean createIndexInDefaultWarming)
    {
        this.createIndexInDefaultWarming = createIndexInDefaultWarming;
    }

    @Min(1000)
    @Max(7000)
    public int getWarmDataVarcharMaxLength()
    {
        return warmDataVarcharMaxLength;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.warm-data-varchar-max-length")
    @Config(WARP_SPEED_PREFIX + "config.warm-data-varchar-max-length")
    public void setWarmDataVarcharMaxLength(int warmDataVarcharMaxLength)
    {
        this.warmDataVarcharMaxLength = warmDataVarcharMaxLength;
    }

    public long getClusterUpTime()
    {
        return clusterUpTime;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + CLUSTER_UP_TIME)
    @Config(WARP_SPEED_PREFIX + CLUSTER_UP_TIME)
    public void setClusterUpTime(long clusterUpTime)
    {
        this.clusterUpTime = clusterUpTime;
    }

    public int getConsistentSplitBucketsPerWorker()
    {
        return consistentSplitBucketsPerWorker;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.consistent-split-buckets-per-worker")
    @Config(WARP_SPEED_PREFIX + "config.consistent-split-buckets-per-worker")
    public void setConsistentSplitBucketsPerWorker(int consistentSplitBucketsPerWorker)
    {
        this.consistentSplitBucketsPerWorker = consistentSplitBucketsPerWorker;
    }

    @Min(0)
    @Max(3600)
    public int getExportDelayInSeconds()
    {
        return exportDelayInSeconds;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.export.delay-in-seconds")
    @Config(WARP_SPEED_PREFIX + "config.export.delay-in-seconds")
    public void setExportDelayInSeconds(int exportDelayInSeconds)
    {
        this.exportDelayInSeconds = exportDelayInSeconds;
    }

    public Set<String> getUnsupportedFunctions()
    {
        return unsupportedFunctions;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "debug.unsupported-functions")
    @Config(WARP_SPEED_PREFIX + "debug.unsupported-functions")
    public void setUnsupportedFunctions(String unsupportedFunctionsAsString)
    {
        try {
            unsupportedFunctions = Arrays.stream(unsupportedFunctionsAsString.trim().split(",")).map(String::trim).collect(Collectors.toSet());
        }
        catch (Exception e) {
            logger.error("failed to set warp-speed.debug.unsupported-functions list");
        }
    }

    public String getLocalStorePath()
    {
        return localStorePath;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + LOCAL_STORE_PATH)
    @Config(WARP_SPEED_PREFIX + LOCAL_STORE_PATH)
    public void setLocalStorePath(String localStorePath)
    {
        this.localStorePath = localStorePath;
    }

    public int getMaxCollectColumnsSkipDefaultWarming()
    {
        return maxCollectColumnsSkipDefaultWarming;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + MAX_COLLECT_COLUMNS_SKIP_DEFAULT_WARMING)
    @Config(WARP_SPEED_PREFIX + MAX_COLLECT_COLUMNS_SKIP_DEFAULT_WARMING)
    public void setMaxCollectColumnsSkipDefaultWarming(int maxCollectColumnsSkipDefaultWarming)
    {
        this.maxCollectColumnsSkipDefaultWarming = maxCollectColumnsSkipDefaultWarming;
    }

    public long getReservationUsageForSingleTxInBytes()
    {
        return reservationUsageForSingleTxInBytes;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.reservation-usage-for-single-tx-in-bytes")
    @Config(WARP_SPEED_PREFIX + "config.reservation-usage-for-single-tx-in-bytes")
    public void setReservationUsageForSingleTxInBytes(long reservationUsageForSingleTxInBytes)
    {
        this.reservationUsageForSingleTxInBytes = reservationUsageForSingleTxInBytes;
    }

    @Min(32)
    @Max(1_000_000)
    public int getPredicateSimplifyThreshold()
    {
        return predicateSimplifyThreshold;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.predicate-simplify-threshold")
    @Config(WARP_SPEED_PREFIX + "config.predicate-simplify-threshold")
    public void setPredicateSimplifyThreshold(int predicateSimplifyThreshold)
    {
        this.predicateSimplifyThreshold = predicateSimplifyThreshold;
    }

    public int getMaxWarmRetries()
    {
        return maxWarmRetries;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.max-warm-retries")
    @Config(WARP_SPEED_PREFIX + "config.max-warm-retries")
    public void setMaxWarmRetries(int maxWarmRetries)
    {
        this.maxWarmRetries = maxWarmRetries;
    }

    public int getWarmRetryBackoffFactorInMillis()
    {
        return warmRetryBackoffFactorInMillis;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.warm-retry-backoff-factor-in-millis")
    @Config(WARP_SPEED_PREFIX + "config.warm-retry-backoff-factor-in-millis")
    public void setWarmRetryBackoffFactorInMillis(int warmRetryBackoffFactorInMillis)
    {
        this.warmRetryBackoffFactorInMillis = warmRetryBackoffFactorInMillis;
    }

    public int getMaxWarmupIterationsPerQuery()
    {
        return maxWarmupIterationsPerQuery;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.max-warmup-iterations-per-query")
    @Config(WARP_SPEED_PREFIX + "config.max-warmup-iterations-per-query")
    public void setMaxWarmupIterationsPerQuery(int maxWarmupIterationsPerQuery)
    {
        this.maxWarmupIterationsPerQuery = maxWarmupIterationsPerQuery;
    }

    public String getCertLocalLocalLocation()
    {
        return certLocalLocalLocation;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + CERT_LOCAL_LOCATION)
    @Config(WARP_SPEED_PREFIX + CERT_LOCAL_LOCATION)
    public void setCertLocalLocalLocation(String certLocalLocalLocation)
    {
        this.certLocalLocalLocation = certLocalLocalLocation;
    }

    public String getAzureConnectionString()
    {
        return azureConnectionString;
    }

    @ConfigSecuritySensitive
    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + AZURE_CONNECTION_STRING)
    @Config(WARP_SPEED_PREFIX + AZURE_CONNECTION_STRING)
    public void setAzureConnectionString(String azureConnectionString)
    {
        this.azureConnectionString = azureConnectionString;
    }

    public boolean isAllowVaradaStatsCollection()
    {
        return allowVaradaStatsCollection;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + STATS_COLLECTION_ENABLED)
    @Config(WARP_SPEED_PREFIX + STATS_COLLECTION_ENABLED)
    public void setAllowVaradaStatsCollection(boolean allowVaradaStatsCollection)
    {
        this.allowVaradaStatsCollection = allowVaradaStatsCollection;
    }

    public boolean isFailureGeneratorEnabled()
    {
        return failureGeneratorEnabled;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + FAILURE_GENERATOR_ENABLED)
    @Config(WARP_SPEED_PREFIX + FAILURE_GENERATOR_ENABLED)
    public void setFailureGeneratorEnabled(boolean failureGeneratorEnabled)
    {
        this.failureGeneratorEnabled = failureGeneratorEnabled;
    }

    public SwaggerExposingLevel getSwaggerExposingLevel()
    {
        return swaggerExposingLevel;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.swagger-exposing-level")
    @Config(WARP_SPEED_PREFIX + "config.swagger-exposing-level")
    public void setSwaggerExposingLevel(SwaggerExposingLevel swaggerExposingLevel)
    {
        this.swaggerExposingLevel = swaggerExposingLevel;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.range-filter")
    @Config(WARP_SPEED_PREFIX + "enable.range-filter")
    public void setEnableRangeFilter(boolean enableRangeFilter)
    {
        this.enableRangeFilter = enableRangeFilter;
    }

    public boolean getEnableRangeFilter()
    {
        return enableRangeFilter;
    }

    public int getShapingLoggerThreshold()
    {
        return shapingLoggerThreshold;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "shaping-logger.threshold")
    @Config(WARP_SPEED_PREFIX + "shaping-logger.threshold")
    public void setShapingLoggerThreshold(int threshold)
    {
        this.shapingLoggerThreshold = threshold;
    }

    public Duration getShapingLoggerDuration()
    {
        return shapingLoggerDuration;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "shaping-logger.duration")
    @Config(WARP_SPEED_PREFIX + "shaping-logger.duration")
    public void setShapingLoggerDuration(io.airlift.units.Duration duration)
    {
        this.shapingLoggerDuration = duration.toJavaTime();
    }

    public int getShapingLoggerNumberOfSamples()
    {
        return shapingLoggerNumberOfSamples;
    }

    @Config(WARP_SPEED_PREFIX + "shaping-logger-num-samples")
    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "shaping-logger-num-samples")
    public void setShapingLoggerNumberOfSamples(int shapingLoggerNumberOfSamples)
    {
        this.shapingLoggerNumberOfSamples = shapingLoggerNumberOfSamples;
    }

    public boolean isDataOnlyWarming()
    {
        return dataOnlyWarming;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + DATA_ONLY_WARMING)
    @Config(WARP_SPEED_PREFIX + DATA_ONLY_WARMING)
    public void setDataOnlyWarming(boolean dataOnlyWarming)
    {
        this.dataOnlyWarming = dataOnlyWarming;
    }

    @Override
    public String toString()
    {
        return "GlobalConfiguration{" +
                "authorization=" + authorization +
                ", stripeSize=" + stripeSize +
                ", cardinalityBuckets='" + cardinalityBuckets + '\'' +
                ", isSingle=" + isSingle +
                ", unsupportedFunctions=" + unsupportedFunctions +
                ", clusterUpTime=" + clusterUpTime +
                ", enableDefaultWarming=" + enableDefaultWarming +
                ", createIndexInDefaultWarming=" + createIndexInDefaultWarming +
                ", consistentSplitBucketsPerWorker=" + consistentSplitBucketsPerWorker +
                ", exportDelayInSeconds=" + exportDelayInSeconds +
                ", localStorePath='" + localStorePath + '\'' +
                ", maxCollectColumnsSkipDefaultWarming=" + maxCollectColumnsSkipDefaultWarming +
                ", predicateSimplifyThreshold=" + predicateSimplifyThreshold +
                ", reservationUsageForSingleTxInBytes=" + reservationUsageForSingleTxInBytes +
                ", maxWarmRetries=" + maxWarmRetries +
                ", warmRetryBackoffFactorInMillis=" + warmRetryBackoffFactorInMillis +
                ", maxWarmupIterationsPerQuery=" + maxWarmupIterationsPerQuery +
                ", enableImportExport=" + enableImportExport +
                ", enableExportAppendOnCloud=" + enableExportAppendOnCloud +
                ", enableMatchCollect=" + enableMatchCollect +
                ", certLocalLocalLocation='" + certLocalLocalLocation + '\'' +
                ", deviceIdentifier='" + deviceIdentifier + '\'' +
                ", azureConnectionString='" + azureConnectionString + '\'' +
                ", allowVaradaStatsCollection=" + allowVaradaStatsCollection +
                ", failureGeneratorEnabled=" + failureGeneratorEnabled +
                ", swaggerExposingLevel=" + swaggerExposingLevel +
                ", enableRangeFilter=" + enableRangeFilter +
                ", shapingLoggerThreshold=" + shapingLoggerThreshold +
                ", shapingLoggerDuration=" + shapingLoggerDuration +
                ", shapingLoggerNumberOfSamplings=" + shapingLoggerNumberOfSamples +
                ", dataOnlyWarming=" + dataOnlyWarming +
                '}';
    }
}
