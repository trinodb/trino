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
import io.airlift.configuration.LegacyConfig;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.plugin.warp.gen.constants.CompressionUsers;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.LOCAL_DATA_STORAGE_PREFIX;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;

public class NativeConfiguration
{
    public static final int READERS_WARMERS_RATIO = 32;
    public static final int MIN_WARMING_THREADS = 2;
    public static final int DEFAULT_GENERAL_RESERVED_MEMORY_IN_GB = 8;
    public static final int DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES = 4 * 1024 * 1024;
    public static final String EXCEPTIONAL_LIST_COMPRESSION = "enable.compression.exceptional-list";
    private static final Logger logger = Logger.get(NativeConfiguration.class);

    private int predicateBundleSizeInMegaBytes = 110;
    private DataSize generalReservedMemory = DataSize.of(0, DataSize.Unit.GIGABYTE);
    private DataSize bundleSize = DataSize.of(64, DataSize.Unit.MEGABYTE);
    private DataSize maxRecJufferSize = DataSize.of(16, DataSize.Unit.MEGABYTE);
    private int lz4HcPercent;
    private DataSize collectTxSize = DataSize.of(8, DataSize.Unit.MEGABYTE);
    private int storageCacheSizeInPages;
    private int skipIndexPercent = 80;
    private int taskMaxWorkerThreads = Runtime.getRuntime().availableProcessors() * 2;
    private int debugPanicHaltPolicy;
    private int clusterLevel = -1;
    private int maxPageSourcesWithoutWarmingLimit = 8;
    private int taskMinWarmingThreads;

    //////////////////////////// Enable flags and lists ///////////////////////////////
    // Exceptional Lists of record types is optional and not used for all features
    // If exists, the exceptional list contains exceptions to the general rule:
    //     In case the general rule is Enable, the exceptional list will contain Disabled record types
    //     In case the general rule is Disable, the exceptional list will contain Enabled record types
    // With this approach field engineers can very easily disable/enable fully or partially any supported feature
    // Exception lists in NativeConfiguration are held as integer bitmaps while every bit represents each potential
    // enum value (on - in the list, off - not in the list)
    private boolean enableSingleChunk = true;
    private boolean enableQueryResultType;
    private boolean enablePackedChunk = true;
    private boolean enableCompression = true;
    private int exceptionalListCompression;
    private Set<String> unsupportedNativeFunctions = Collections.emptySet();
    private Duration storageTemporaryExceptionDuration = Duration.of(5, ChronoUnit.MINUTES);
    private int storageTemporaryExceptionNumTries = 3;
    private Duration storageTemporaryExceptionExpiryDuration = Duration.of(1, ChronoUnit.HOURS);

    @Min(100)
    @Max(600)
    public int getPredicateBundleSizeInMegaBytes()
    {
        return predicateBundleSizeInMegaBytes;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.predicate-bundle-size-mb")
    @Config(WARP_SPEED_PREFIX + "config.predicate-bundle-size-mb")
    public void setPredicateBundleSizeInMegaBytes(int predicateBundleSizeInMegaBytes)
    {
        this.predicateBundleSizeInMegaBytes = predicateBundleSizeInMegaBytes;
    }

    public long getGeneralReservedMemory()
    {
        if (generalReservedMemory.toBytes() == 0) {
            generalReservedMemory = switch (getClusterLevel()) {
                case 0 -> DataSize.of(0, DataSize.Unit.GIGABYTE);
                case 1, 2 -> DataSize.of(DEFAULT_GENERAL_RESERVED_MEMORY_IN_GB >> 2, DataSize.Unit.GIGABYTE);
                case 3 -> DataSize.of(DEFAULT_GENERAL_RESERVED_MEMORY_IN_GB >> 1, DataSize.Unit.GIGABYTE);
                default -> DataSize.of(DEFAULT_GENERAL_RESERVED_MEMORY_IN_GB, DataSize.Unit.GIGABYTE);
            };
        }
        return generalReservedMemory.toBytes();
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.general-reserved-memory-in-gb")
    @Config(WARP_SPEED_PREFIX + "config.general-reserved-memory-in-gb")
    public void setGeneralReservedMemory(long generalReservedMemoryInGigaBytes)
    {
        this.generalReservedMemory = DataSize.of(generalReservedMemoryInGigaBytes, DataSize.Unit.GIGABYTE);
    }

    @Min(16 * 1024 * 1024)
    @Max(256 * 1024 * 1024)
    public int getBundleSize()
    {
        return (int) bundleSize.toBytes();
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.bundle-size-mb")
    @Config(WARP_SPEED_PREFIX + "config.bundle-size-mb")
    public void setBundleSize(int bundleSizeInMegaBytes)
    {
        this.bundleSize = DataSize.of(bundleSizeInMegaBytes, DataSize.Unit.MEGABYTE);
    }

    @Min(4 * 1024 * 1024)
    @Max(16 * 1024 * 1024)
    public int getMaxRecJufferSize()
    {
        return (int) maxRecJufferSize.toBytes();
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.max-rec-juffer-size-mb")
    @Config(WARP_SPEED_PREFIX + "config.max-rec-juffer-size-mb")
    public void setMaxRecJufferSize(int maxRecJufferSizeInMegaBytes)
    {
        this.maxRecJufferSize = DataSize.of(maxRecJufferSizeInMegaBytes, DataSize.Unit.MEGABYTE);
    }

    @Min(0)
    @Max(12)
    public int getCompressionLevel()
    {
        return lz4HcPercent;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.task.lz4-hc-percent")
    @Config(WARP_SPEED_PREFIX + "config.task.lz4-hc-percent")
    public void setCompressionLevel(int lz4HcPercent)
    {
        this.lz4HcPercent = lz4HcPercent;
    }

    @Min(1024 * 1024)
    @Max(32 * 1024 * 1024)
    public int getCollectTxSize()
    {
        return (int) collectTxSize.toBytes();
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.collect-tx-size-mb")
    @Config(WARP_SPEED_PREFIX + "config.collect-tx-size-mb")
    public void setCollectTxSize(int collectTxSizeInMegaBytes)
    {
        this.collectTxSize = DataSize.of(collectTxSizeInMegaBytes, DataSize.Unit.MEGABYTE);
    }

    @Min(32 * 1024)
    @Max(16 * 1024 * 1024)
    public int getStorageCacheSizeInPages()
    {
        if (storageCacheSizeInPages == 0) {
            storageCacheSizeInPages = switch (getClusterLevel()) {
                case 0 -> (DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES >> 6) - 1;
                case 1 -> DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES >> 3;
                case 2 -> DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES >> 2;
                case 3 -> DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES >> 1;
                default -> DEFAULT_STORAGE_CACHE_SIZE_IN_PAGES;
            };
        }
        return storageCacheSizeInPages;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.storage-cache-size-in-pages")
    @Config(WARP_SPEED_PREFIX + "config.storage-cache-size-in-pages")
    public void setStorageCacheSizeInPages(int storageCacheSizeInPages)
    {
        this.storageCacheSizeInPages = storageCacheSizeInPages;
    }

    /**
     * In case of RANGE predicate, it sometimes faster to get complete chunk without entering to index-chunk itself.
     * if our estimation of records to read is higher than the percent you give us, we skip the index.
     */
    public int getSkipIndexPercent()
    {
        return skipIndexPercent;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.skip-index-percent")
    @Config(WARP_SPEED_PREFIX + "config.skip-index-percent")
    public void setSkipIndexPercent(int skipIndexPercent)
    {
        this.skipIndexPercent = skipIndexPercent;
    }

    public boolean getEnableSingleChunk()
    {
        return enableSingleChunk;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.single-chunk")
    @Config(WARP_SPEED_PREFIX + "enable.single-chunk")
    public void setEnableSingleChunk(boolean enableSingleChunk)
    {
        this.enableSingleChunk = enableSingleChunk;
    }

    public boolean getEnableQueryResultType()
    {
        return enableQueryResultType;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.query-result-type")
    @Config(WARP_SPEED_PREFIX + "enable.query-result-type")
    public void setEnableQueryResultType(boolean enableQueryResultType)
    {
        this.enableQueryResultType = enableQueryResultType;
    }

    public boolean getEnablePackedChunk()
    {
        return enablePackedChunk;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.packed-chunk")
    @Config(WARP_SPEED_PREFIX + "enable.packed-chunk")
    public void setEnablePackedChunk(boolean enablePackedChunk)
    {
        this.enablePackedChunk = enablePackedChunk;
    }

    public boolean getEnableCompression()
    {
        return enableCompression;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.compression")
    @Config(WARP_SPEED_PREFIX + "enable.compression")
    public void setEnableCompression(boolean enableCompression)
    {
        this.enableCompression = enableCompression;
    }

    public int getTaskMaxWorkerThreads()
    {
        return taskMaxWorkerThreads;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + EXCEPTIONAL_LIST_COMPRESSION)
    @Config(WARP_SPEED_PREFIX + EXCEPTIONAL_LIST_COMPRESSION)
    public void setExceptionalListCompression(String exceptionalListCompression)
    {
        try {
            this.exceptionalListCompression = string2CompressionUsersList(exceptionalListCompression);
        }
        catch (Exception e) {
            logger.error("failed to set %s list", EXCEPTIONAL_LIST_COMPRESSION);
        }
    }

    public int getExceptionalListCompression()
    {
        return exceptionalListCompression;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.task.max-worker-threads")
    @Config(WARP_SPEED_PREFIX + "config.task.max-worker-threads")
    public void setTaskMaxWorkerThreads(int maxWorkerThreads)
    {
        this.taskMaxWorkerThreads = maxWorkerThreads;
    }

    public int getDebugPanicHaltPolicy()
    {
        return debugPanicHaltPolicy;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "debug.panic-halt-policy")
    @Config(WARP_SPEED_PREFIX + "debug.panic-halt-policy")
    public void setDebugPanicHaltPolicy(int debugPanicHaltPolicy)
    {
        this.debugPanicHaltPolicy = debugPanicHaltPolicy;
    }

    @Min(0)
    @Max(4)
    public int getClusterLevel()
    {
        if (clusterLevel == -1) {
            if (taskMaxWorkerThreads <= 16) { // 1x 2x
                clusterLevel = 0;
            }
            else if (taskMaxWorkerThreads <= 32) { // 4x
                clusterLevel = 1;
            }
            else if (taskMaxWorkerThreads <= 64) { // 8x
                clusterLevel = 2;
            }
            else if (taskMaxWorkerThreads <= 96) {
                clusterLevel = 3;
            }
            else { // 16x and up
                clusterLevel = 4;
            }
        }
        return clusterLevel;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.cluster-level")
    @Config(WARP_SPEED_PREFIX + "config.cluster-level")
    public void setClusterLevel(int clusterLevel)
    {
        this.clusterLevel = clusterLevel;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.max-page-sources-without-warming-limit")
    @Config(WARP_SPEED_PREFIX + "config.max-page-sources-without-warming-limit")
    public void setMaxPageSourcesWithoutWarmingLimit(int maxPageSourcesWithoutWarmingLimit)
    {
        this.maxPageSourcesWithoutWarmingLimit = maxPageSourcesWithoutWarmingLimit;
    }

    public int getMaxPageSourcesWithoutWarmingLimit()
    {
        return maxPageSourcesWithoutWarmingLimit;
    }

    @Min(0)
    public int getTaskMinWarmingThreads()
    {
        return taskMinWarmingThreads == 0 ? Math.max((taskMaxWorkerThreads / READERS_WARMERS_RATIO), MIN_WARMING_THREADS) : taskMinWarmingThreads;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.task.min-warming-threads")
    @Config(WARP_SPEED_PREFIX + "config.task.min-warming-threads")
    public void setTaskMinWarmingThreads(int minWarmingThreads)
    {
        this.taskMinWarmingThreads = minWarmingThreads;
    }

    public Set<String> getUnsupportedNativeFunctions()
    {
        return unsupportedNativeFunctions;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "debug.unsupported-native-functions")
    @Config(WARP_SPEED_PREFIX + "debug.unsupported-native-functions")
    public void setUnsupportedNativeFunctions(String unsupportedNativeFunctionsAsString)
    {
        try {
            unsupportedNativeFunctions = Arrays.stream(unsupportedNativeFunctionsAsString.trim().split(",")).map(String::trim).collect(Collectors.toSet());
        }
        catch (Exception e) {
            logger.error("failed to set warp-speed.debug.unsupported-native-functions list");
        }
    }

    public Duration getStorageTemporaryExceptionDuration()
    {
        return storageTemporaryExceptionDuration;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.storage-temp-except.duration")
    @Config(WARP_SPEED_PREFIX + "config.storage-temp-except.duration")
    public void setStorageTemporaryExceptionDuration(io.airlift.units.Duration duration)
    {
        this.storageTemporaryExceptionDuration = duration.toJavaTime();
    }

    public int getStorageTemporaryExceptionNumTries()
    {
        return storageTemporaryExceptionNumTries;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.storage-temp-except.num-tries")
    @Config(WARP_SPEED_PREFIX + "config.storage-temp-except.num-tries")
    public void setStorageTemporaryExceptionNumTries(int storageTemporaryExceptionNumTries)
    {
        this.storageTemporaryExceptionNumTries = storageTemporaryExceptionNumTries;
    }

    public Duration getStorageTemporaryExceptionExpiryDuration()
    {
        return storageTemporaryExceptionExpiryDuration;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.storage-temp-except.expiry.duration")
    @Config(WARP_SPEED_PREFIX + "config.storage-temp-except.expiry.duration")
    public void setStorageTemporaryExceptionExpiryDuration(io.airlift.units.Duration duration)
    {
        this.storageTemporaryExceptionExpiryDuration = duration.toJavaTime();
    }

    public boolean isDebug()
    {
        return debugPanicHaltPolicy > 0;
    }

    private int string2CompressionUsersList(String str)
    {
        List<CompressionUsers> compressionUsers = Arrays.stream(str.split(",")).map(CompressionUsers::valueOf).toList();
        int res = 0;

        for (CompressionUsers user : compressionUsers) {
            res |= (1 << user.ordinal());
        }
        return res;
    }
}
