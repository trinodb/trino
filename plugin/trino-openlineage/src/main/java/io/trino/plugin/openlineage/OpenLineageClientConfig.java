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
package io.trino.plugin.openlineage;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class OpenLineageClientConfig
{
    private OpenLineageSink sink = OpenLineageSink.API;
    private String url;
    private Optional<String> apiKey = Optional.empty();
    private int retryCount = 3;
    private Duration retryDelay = Duration.valueOf("1s");
    private double backoffBase = 2.0;
    private Duration maxDelay = Duration.valueOf("1m");

    @NotNull
    public String getUrl()
    {
        return url;
    }

    @ConfigDescription("URL of receiving server. Explicitly set the scheme https:// to use symmetric encryption")
    @Config("openlineage-event-listener.connect-url")
    public OpenLineageClientConfig setUrl(String url)
    {
        this.url = url;
        return this;
    }

    @ConfigDescription("Number of retries on server error")
    @Config("openlineage-event-listener.connect-retry-count")
    public OpenLineageClientConfig setRetryCount(int retryCount)
    {
        this.retryCount = retryCount;
        return this;
    }

    @Min(value = 0, message = "Retry count must be a positive value. Use 0 or leave empty for no retries.")
    public int getRetryCount()
    {
        return retryCount;
    }

    @ConfigDescription("Delay in seconds between retries")
    @Config("openlineage-event-listener.connect-retry-delay")
    public OpenLineageClientConfig setRetryDelay(Duration retryDelay)
    {
        this.retryDelay = retryDelay;
        return this;
    }

    public Duration getRetryDelay()
    {
        return this.retryDelay;
    }

    @ConfigDescription("Base used for exponential backoff when retying on server error. Formula is attemptDelay = retryDelay * backoffBase ^ attemptCount. " +
            "Attempt counting starts from 0. Leave empty or set to 1 to disable.")
    @Config("openlineage-event-listener.connect-backoff-base")
    public OpenLineageClientConfig setBackoffBase(double backoffBase)
    {
        this.backoffBase = backoffBase;
        return this;
    }

    @Min(value = 1, message = "Exponential base must be a positive, non-zero integer.")
    public double getBackoffBase()
    {
        return this.backoffBase;
    }

    @ConfigDescription("Maximum delay between retries. This should be used with exponential backoff.")
    @Config("openlineage-event-listener.connect-max-delay")
    public OpenLineageClientConfig setMaxDelay(Duration maxDelay)
    {
        this.maxDelay = maxDelay;
        return this;
    }

    public Duration getMaxDelay()
    {
        return this.maxDelay;
    }

    @ConfigDescription("API Key to authenticate within OpenLineage API.")
    @Config("openlineage-event-listener.connect-api-key")
    public OpenLineageClientConfig setApiKey(String apiKey)
    {
        this.apiKey = Optional.ofNullable(apiKey);
        return this;
    }

    public Optional<String> getApiKey()
    {
        return apiKey;
    }

    @ConfigDescription("Type of sink to emit lineage information to.")
    @Config("openlineage-event-listener.connect-sink")
    public OpenLineageClientConfig setSink(OpenLineageSink sink)
    {
        this.sink = sink;
        return this;
    }

    public OpenLineageSink getSink()
    {
        return sink;
    }
}
