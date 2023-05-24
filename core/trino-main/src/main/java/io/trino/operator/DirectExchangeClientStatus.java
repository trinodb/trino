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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.Mergeable;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DirectExchangeClientStatus
        implements Mergeable<DirectExchangeClientStatus>, OperatorInfo
{
    private final long bufferedBytes;
    private final long maxBufferedBytes;
    private final long averageBytesPerRequest;
    private final long successfulRequestsCount;
    private final int bufferedPages;
    private final int spilledPages;
    private final long spilledBytes;
    private final boolean noMoreLocations;
    private final List<PageBufferClientStatus> pageBufferClientStatuses;
    private final TDigestHistogram requestDuration;

    @JsonCreator
    public DirectExchangeClientStatus(
            @JsonProperty("bufferedBytes") long bufferedBytes,
            @JsonProperty("maxBufferedBytes") long maxBufferedBytes,
            @JsonProperty("averageBytesPerRequest") long averageBytesPerRequest,
            @JsonProperty("successfulRequestsCount") long successFullRequestsCount,
            @JsonProperty("bufferedPages") int bufferedPages,
            @JsonProperty("spilledPages") int spilledPages,
            @JsonProperty("spilledBytes") long spilledBytes,
            @JsonProperty("noMoreLocations") boolean noMoreLocations,
            @JsonProperty("pageBufferClientStatuses") List<PageBufferClientStatus> pageBufferClientStatuses,
            @JsonProperty("requestDuration") TDigestHistogram requestDuration)
    {
        this.bufferedBytes = bufferedBytes;
        this.maxBufferedBytes = maxBufferedBytes;
        this.averageBytesPerRequest = averageBytesPerRequest;
        this.successfulRequestsCount = successFullRequestsCount;
        this.bufferedPages = bufferedPages;
        this.spilledPages = spilledPages;
        this.spilledBytes = spilledBytes;
        this.noMoreLocations = noMoreLocations;
        this.pageBufferClientStatuses = ImmutableList.copyOf(requireNonNull(pageBufferClientStatuses, "pageBufferClientStatuses is null"));
        this.requestDuration = requireNonNull(requestDuration, "requestsDuration is null");
    }

    @JsonProperty
    public long getBufferedBytes()
    {
        return bufferedBytes;
    }

    @JsonProperty
    public long getMaxBufferedBytes()
    {
        return maxBufferedBytes;
    }

    @JsonProperty
    public long getAverageBytesPerRequest()
    {
        return averageBytesPerRequest;
    }

    @JsonProperty
    public long getSuccessfulRequestsCount()
    {
        return successfulRequestsCount;
    }

    @JsonProperty
    public int getBufferedPages()
    {
        return bufferedPages;
    }

    @JsonProperty
    public int getSpilledPages()
    {
        return spilledPages;
    }

    @JsonProperty
    public long getSpilledBytes()
    {
        return spilledBytes;
    }

    @JsonProperty
    public boolean isNoMoreLocations()
    {
        return noMoreLocations;
    }

    @JsonProperty
    public List<PageBufferClientStatus> getPageBufferClientStatuses()
    {
        return pageBufferClientStatuses;
    }

    @JsonProperty
    public TDigestHistogram getRequestDuration()
    {
        return requestDuration;
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bufferBytes", bufferedBytes)
                .add("maxBufferedBytes", maxBufferedBytes)
                .add("averageBytesPerRequest", averageBytesPerRequest)
                .add("successfulRequestsCount", successfulRequestsCount)
                .add("bufferedPages", bufferedPages)
                .add("spilledPages", spilledPages)
                .add("spilledBytes", spilledBytes)
                .add("noMoreLocations", noMoreLocations)
                .add("pageBufferClientStatuses", pageBufferClientStatuses)
                .add("requestDuration", requestDuration)
                .toString();
    }

    @Override
    public DirectExchangeClientStatus mergeWith(DirectExchangeClientStatus other)
    {
        return new DirectExchangeClientStatus(
                (bufferedBytes + other.bufferedBytes) / 2, // this is correct as long as all clients have the same buffer size (capacity)
                Math.max(maxBufferedBytes, other.maxBufferedBytes),
                mergeAvgs(averageBytesPerRequest, successfulRequestsCount, other.averageBytesPerRequest, other.successfulRequestsCount),
                successfulRequestsCount + other.successfulRequestsCount,
                bufferedPages + other.bufferedPages,
                spilledPages + other.spilledPages,
                spilledBytes + other.spilledBytes,
                noMoreLocations && other.noMoreLocations, // if at least one has some locations, mergee has some too
                ImmutableList.of(), // pageBufferClientStatuses may be long, so we don't want to combine the lists
                requestDuration.mergeWith(other.requestDuration)); // this is correct as long as all clients have the same shape of histogram
    }

    @Override
    public DirectExchangeClientStatus mergeWith(List<DirectExchangeClientStatus> others)
    {
        if (others.isEmpty()) {
            return this;
        }

        long bufferedBytes = this.bufferedBytes;
        long maxBufferedBytes = this.maxBufferedBytes;
        long averageBytesPerRequest = this.averageBytesPerRequest;
        long successfulRequestsCount = this.successfulRequestsCount;
        int bufferedPages = this.bufferedPages;
        int spilledPages = this.spilledPages;
        long spilledBytes = this.spilledBytes;
        boolean noMoreLocations = this.noMoreLocations;
        ImmutableList.Builder<TDigestHistogram> requestDurations = ImmutableList.builderWithExpectedSize(others.size());
        for (DirectExchangeClientStatus other : others) {
            bufferedBytes = (bufferedBytes + other.bufferedBytes) / 2; // this is correct as long as all clients have the same buffer size (capacity)
            maxBufferedBytes = Math.max(maxBufferedBytes, other.maxBufferedBytes);
            averageBytesPerRequest = mergeAvgs(averageBytesPerRequest, successfulRequestsCount, other.averageBytesPerRequest, other.successfulRequestsCount);
            successfulRequestsCount = successfulRequestsCount + other.successfulRequestsCount;
            bufferedPages = bufferedPages + other.bufferedPages;
            spilledPages = spilledPages + other.spilledPages;
            spilledBytes = spilledBytes + other.spilledBytes;
            noMoreLocations = noMoreLocations && other.noMoreLocations; // if at least one has some locations, mergee has some too
            requestDurations.add(other.requestDuration);
        }

        return new DirectExchangeClientStatus(
                bufferedBytes,
                maxBufferedBytes,
                averageBytesPerRequest,
                successfulRequestsCount,
                bufferedPages,
                spilledPages,
                spilledBytes,
                noMoreLocations,
                ImmutableList.of(), // pageBufferClientStatuses may be long, so we don't want to combine the lists
                TDigestHistogram.merge(requestDurations.build()).orElseThrow()); // this is correct as long as all clients have the same shape of histogram
    }

    private static long mergeAvgs(long value1, long count1, long value2, long count2)
    {
        if (count1 == 0) {
            return value2;
        }
        if (count2 == 0) {
            return value1;
        }
        // AVG_n+m = AVG_n * n / (n + m) + AVG_m * m / (n + m)
        return (value1 * count1 / (count1 + count2)) + (value2 * count2 / (count1 + count2));
    }
}
