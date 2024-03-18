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
package io.trino.plugin.warp.extension.configuration;

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.LOCAL_DATA_STORAGE_PREFIX;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;

public class CallHomeConfiguration
{
    private boolean enable = true;

    private int intervalInSeconds = 3600;
    private int maxWaitTimeInSeconds = 120;

    public int getIntervalInSeconds()
    {
        return intervalInSeconds;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "call-home.interval.seconds")
    @Config(WARP_SPEED_PREFIX + "call-home.interval.seconds")
    public void setIntervalInSeconds(int intervalInSeconds)
    {
        this.intervalInSeconds = intervalInSeconds;
    }

    public boolean isEnable()
    {
        return enable;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "call-home.enable")
    @Config(WARP_SPEED_PREFIX + "call-home.enable")
    public void setEnable(boolean enable)
    {
        this.enable = enable;
    }

    public int getMaxWaitTimeInSeconds()
    {
        return maxWaitTimeInSeconds;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "call-home.maxWaitTimeInSeconds")
    @Config(WARP_SPEED_PREFIX + "call-home.maxWaitTimeInSeconds")
    public void setMaxWaitTimeInSeconds(int maxWaitTimeInSeconds)
    {
        this.maxWaitTimeInSeconds = maxWaitTimeInSeconds;
    }

    @Override
    public String toString()
    {
        return "CallHomeConfiguration{" +
                "enable=" + enable +
                ", intervalInSeconds=" + intervalInSeconds +
                ", maxWaitTimeInSeconds=" + maxWaitTimeInSeconds +
                '}';
    }
}
