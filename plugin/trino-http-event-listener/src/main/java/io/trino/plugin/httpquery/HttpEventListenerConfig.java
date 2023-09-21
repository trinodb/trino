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
package io.trino.plugin.httpquery;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HttpEventListenerConfig
{
    private int retryCount;
    private Duration retryDelay = Duration.valueOf("1s");
    private double backoffBase = 2.0;
    private Duration maxDelay = Duration.valueOf("1m");
    private final EnumSet<HttpEventListenerEventType> loggedEvents = EnumSet.noneOf(HttpEventListenerEventType.class);
    private String ingestUri;
    private Map<String, String> httpHeaders = ImmutableMap.of();

    @ConfigDescription("Will log io.trino.spi.eventlistener.QueryCreatedEvent")
    @Config("http-event-listener.log-created")
    public HttpEventListenerConfig setLogCreated(boolean logCreated)
    {
        if (logCreated) {
            loggedEvents.add(HttpEventListenerEventType.QUERY_CREATED);
        }
        return this;
    }

    public boolean getLogCreated()
    {
        return loggedEvents.contains(HttpEventListenerEventType.QUERY_CREATED);
    }

    @ConfigDescription("Will log io.trino.spi.eventlistener.QueryCompletedEvent")
    @Config("http-event-listener.log-completed")
    public HttpEventListenerConfig setLogCompleted(boolean logCompleted)
    {
        if (logCompleted) {
            loggedEvents.add(HttpEventListenerEventType.QUERY_COMPLETED);
        }
        return this;
    }

    public boolean getLogCompleted()
    {
        return loggedEvents.contains(HttpEventListenerEventType.QUERY_COMPLETED);
    }

    @ConfigDescription("Will log io.trino.spi.eventlistener.SplitCompletedEvent")
    @Config("http-event-listener.log-split")
    public HttpEventListenerConfig setLogSplit(boolean logSplit)
    {
        if (logSplit) {
            loggedEvents.add(HttpEventListenerEventType.QUERY_SPLIT);
        }
        return this;
    }

    public boolean getLogSplit()
    {
        return loggedEvents.contains(HttpEventListenerEventType.QUERY_SPLIT);
    }

    @NotNull
    public String getIngestUri()
    {
        return ingestUri;
    }

    @ConfigDescription("URL of receiving server. Explicitly set the scheme https:// to use symmetric encryption")
    @Config("http-event-listener.connect-ingest-uri")
    public HttpEventListenerConfig setIngestUri(String ingestUri)
    {
        this.ingestUri = ingestUri;
        return this;
    }

    public Map<String, String> getHttpHeaders()
    {
        return httpHeaders;
    }

    @ConfigDescription("List of custom custom HTTP headers provided as: \"Header-Name-1: header value 1, Header-Value-2: header value 2, ...\" ")
    @Config("http-event-listener.connect-http-headers")
    public HttpEventListenerConfig setHttpHeaders(List<String> httpHeaders)
    {
        try {
            this.httpHeaders = httpHeaders
                    .stream()
                    .collect(Collectors.toUnmodifiableMap(kvs -> kvs.split(":", 2)[0], kvs -> kvs.split(":", 2)[1]));
        }
        catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format("Cannot parse http headers from property http-event-listener.connect-http-headers; value provided was %s, " +
                    "expected format is \"Header-Name-1: header value 1, Header-Value-2: header value 2, ...\"", String.join(", ", httpHeaders)), e);
        }
        return this;
    }

    @ConfigDescription("Number of retries on server error")
    @Config("http-event-listener.connect-retry-count")
    public HttpEventListenerConfig setRetryCount(int retryCount)
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
    @Config("http-event-listener.connect-retry-delay")
    public HttpEventListenerConfig setRetryDelay(Duration retryDelay)
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
    @Config("http-event-listener.connect-backoff-base")
    public HttpEventListenerConfig setBackoffBase(double backoffBase)
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
    @Config("http-event-listener.connect-max-delay")
    public HttpEventListenerConfig setMaxDelay(Duration maxDelay)
    {
        this.maxDelay = maxDelay;
        return this;
    }

    public Duration getMaxDelay()
    {
        return this.maxDelay;
    }
}
