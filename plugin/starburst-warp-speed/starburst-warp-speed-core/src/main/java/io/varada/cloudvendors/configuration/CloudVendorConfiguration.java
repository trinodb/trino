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
package io.varada.cloudvendors.configuration;

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.LOCAL_DATA_STORAGE_PREFIX;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;

public class CloudVendorConfiguration
{
    public static final String STORE_TYPE = "config.store.type";
    public static final String STORE_PATH = "store.path";
    public static final String REGION = "config.region";

    private StoreType storeType;
    private String storePath;
    private String region;
    private long requestRetryDelay = 500L;
    private long requestRetryMaxDelay = 8000L;
    private int requestRetryRetries = 5;

    public StoreType getStoreType()
    {
        if (storeType == null && storePath != null) {
            storeType = StoreType.byPathPrefix(getStorePath());
        }
        return storeType;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + STORE_TYPE)
    @Config(WARP_SPEED_PREFIX + STORE_TYPE)
    public void setStoreType(String storeType)
    {
        this.storeType = StoreType.ofConfigName(null, storeType);
    }

    public String getStorePath()
    {
        return storePath;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + STORE_PATH)
    @Config(WARP_SPEED_PREFIX + STORE_PATH)
    public void setStorePath(String storePath)
    {
        this.storePath = storePath;
    }

    public String getRegion()
    {
        return region;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + REGION)
    @Config(WARP_SPEED_PREFIX + REGION)
    public void setRegion(String region)
    {
        this.region = region;
    }

    public long getRequestRetryDelay()
    {
        return requestRetryDelay;
    }

    public void setRequestRetryDelay(long requestRetryDelay)
    {
        this.requestRetryDelay = requestRetryDelay;
    }

    public long getRequestRetryMaxDelay()
    {
        return requestRetryMaxDelay;
    }

    public void setRequestRetryMaxDelay(long requestRetryMaxDelay)
    {
        this.requestRetryMaxDelay = requestRetryMaxDelay;
    }

    public int getRequestRetryRetries()
    {
        return requestRetryRetries;
    }

    public void setRequestRetryRetries(int requestRetryRetries)
    {
        this.requestRetryRetries = requestRetryRetries;
    }
}
