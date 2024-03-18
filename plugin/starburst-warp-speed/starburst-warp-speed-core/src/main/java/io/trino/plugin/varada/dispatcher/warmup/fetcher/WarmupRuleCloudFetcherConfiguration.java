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
package io.trino.plugin.varada.dispatcher.warmup.fetcher;

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;

import java.time.Duration;

import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.LOCAL_DATA_STORAGE_PREFIX;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;

public class WarmupRuleCloudFetcherConfiguration
        extends CloudVendorConfiguration
{
    public static final String PREFIX = "warp-speed.objectstore";
    public static final String STORE_TYPE = "objectstore.store.type";
    public static final String STORE_PATH = "objectstore.store.path";
    public static final String REGION = "objectstore.store.region";
    public static final String WARMUP_FETCH_DURATION = "objectstore.warmup.fetch.duration";

    private Duration fetchDuration = Duration.ofHours(1);
    private Duration fetchDelayDuration = Duration.ofMinutes(1);
    private int downloadRetries = 3;
    private Duration downloadDuration = Duration.ofSeconds(10);

    public WarmupRuleCloudFetcherConfiguration() {}

    public Duration getFetchDuration()
    {
        return fetchDuration;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + WARMUP_FETCH_DURATION)
    @Config(WARP_SPEED_PREFIX + WARMUP_FETCH_DURATION)
    public void setFetchDuration(io.airlift.units.Duration fetchDuration)
    {
        this.fetchDuration = fetchDuration.toJavaTime();
    }

    public Duration getFetchDelayDuration()
    {
        return fetchDelayDuration;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "objectstore.warmup.fetch.delay.duration")
    @Config(WARP_SPEED_PREFIX + "objectstore.warmup.fetch.delay.duration")
    public void setFetchDelayDuration(io.airlift.units.Duration fetchDelayDuration)
    {
        this.fetchDelayDuration = fetchDelayDuration.toJavaTime();
    }

    public int getDownloadRetries()
    {
        return downloadRetries;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "objectstore.warmup.cloud.retries")
    @Config(WARP_SPEED_PREFIX + "objectstore.warmup.cloud.retries")
    public void setDownloadRetries(int downloadRetries)
    {
        this.downloadRetries = downloadRetries;
    }

    public Duration getDownloadDuration()
    {
        return downloadDuration;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "objectstore.warmup.cloud.duration")
    @Config(WARP_SPEED_PREFIX + "objectstore.warmup.cloud.duration")
    public void setDownloadDuration(io.airlift.units.Duration downloadDuration)
    {
        this.downloadDuration = downloadDuration.toJavaTime();
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + STORE_TYPE)
    @Config(WARP_SPEED_PREFIX + STORE_TYPE)
    @Override
    public void setStoreType(String storeType)
    {
        super.setStoreType(storeType);
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + STORE_PATH)
    @Config(WARP_SPEED_PREFIX + STORE_PATH)
    @Override
    public void setStorePath(String storePath)
    {
        super.setStorePath(storePath);
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + REGION)
    @Config(WARP_SPEED_PREFIX + REGION)
    @Override
    public void setRegion(String region)
    {
        super.setRegion(region);
    }
}
