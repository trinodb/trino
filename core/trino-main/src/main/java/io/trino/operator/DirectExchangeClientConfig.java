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
package io.trino.operator;

import io.airlift.configuration.Config;
import io.airlift.configuration.DefunctConfig;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import io.trino.execution.ThreadCountParser;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

@DefunctConfig("exchange.min-error-duration")
public class DirectExchangeClientConfig
{
    private DataSize maxBufferSize = DataSize.of(32, Unit.MEGABYTE);
    private int concurrentRequestMultiplier = 3;
    private Duration maxErrorDuration = new Duration(1, TimeUnit.MINUTES);
    private DataSize maxResponseSize = new HttpClientConfig().getMaxContentLength();
    private int clientThreads = 25;
    private int pageBufferClientMaxCallbackThreads = 25;
    private boolean acknowledgePages = true;
    private DataSize deduplicationBufferSize = DataSize.of(32, Unit.MEGABYTE);

    @NotNull
    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    @Config("exchange.max-buffer-size")
    public DirectExchangeClientConfig setMaxBufferSize(DataSize maxBufferSize)
    {
        this.maxBufferSize = maxBufferSize;
        return this;
    }

    @Min(1)
    public int getConcurrentRequestMultiplier()
    {
        return concurrentRequestMultiplier;
    }

    @Config("exchange.concurrent-request-multiplier")
    public DirectExchangeClientConfig setConcurrentRequestMultiplier(int concurrentRequestMultiplier)
    {
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getMaxErrorDuration()
    {
        return maxErrorDuration;
    }

    @Config("exchange.max-error-duration")
    public DirectExchangeClientConfig setMaxErrorDuration(Duration maxErrorDuration)
    {
        this.maxErrorDuration = maxErrorDuration;
        return this;
    }

    @NotNull
    @MinDataSize("1MB")
    public DataSize getMaxResponseSize()
    {
        return maxResponseSize;
    }

    @Config("exchange.max-response-size")
    public DirectExchangeClientConfig setMaxResponseSize(DataSize maxResponseSize)
    {
        this.maxResponseSize = maxResponseSize;
        return this;
    }

    @Min(1)
    public int getClientThreads()
    {
        return clientThreads;
    }

    @Config("exchange.client-threads")
    public DirectExchangeClientConfig setClientThreads(String clientThreads)
    {
        this.clientThreads = ThreadCountParser.DEFAULT.parse(clientThreads);
        return this;
    }

    @Min(1)
    public int getPageBufferClientMaxCallbackThreads()
    {
        return pageBufferClientMaxCallbackThreads;
    }

    @Config("exchange.page-buffer-client.max-callback-threads")
    public DirectExchangeClientConfig setPageBufferClientMaxCallbackThreads(String pageBufferClientMaxCallbackThreads)
    {
        this.pageBufferClientMaxCallbackThreads = ThreadCountParser.DEFAULT.parse(pageBufferClientMaxCallbackThreads);
        return this;
    }

    public boolean isAcknowledgePages()
    {
        return acknowledgePages;
    }

    @Config("exchange.acknowledge-pages")
    public DirectExchangeClientConfig setAcknowledgePages(boolean acknowledgePages)
    {
        this.acknowledgePages = acknowledgePages;
        return this;
    }

    @NotNull
    public DataSize getDeduplicationBufferSize()
    {
        return deduplicationBufferSize;
    }

    @Config("exchange.deduplication-buffer-size")
    public DirectExchangeClientConfig setDeduplicationBufferSize(DataSize deduplicationBufferSize)
    {
        this.deduplicationBufferSize = deduplicationBufferSize;
        return this;
    }
}
