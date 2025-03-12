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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

@DefunctConfig({
        "pinot.thread-pool-size",
        "pinot.idle-timeout",
        "pinot.max-backlog-per-server",
        "pinot.max-connections-per-server",
        "pinot.min-connections-per-server",
        "pinot.request-timeout",
        "pinot.estimated-size-in-bytes-for-non-numeric-column",
        "pinot.grpc.enabled",
})
public class PinotConfig
{
    private List<URI> controllerUrls = ImmutableList.of();

    private Optional<HostAndPort> brokerUrl = Optional.empty();

    private Duration connectionTimeout = new Duration(1, TimeUnit.MINUTES);

    private Duration metadataCacheExpiry = new Duration(2, TimeUnit.MINUTES);

    private boolean preferBrokerQueries;
    private boolean forbidSegmentQueries;
    private int segmentsPerSplit = 1;
    private int fetchRetryCount = 2;
    private int nonAggregateLimitForBrokerQueries = 25_000;
    private int maxRowsForBrokerQueries = 50_000;
    private boolean aggregationPushdownEnabled = true;
    private boolean countDistinctPushdownEnabled = true;
    private boolean proxyEnabled;
    private DataSize targetSegmentPageSize = DataSize.of(1, MEGABYTE);

    @NotEmpty(message = "pinot.controller-urls cannot be empty")
    public List<URI> getControllerUrls()
    {
        return controllerUrls;
    }

    @Config("pinot.controller-urls")
    public PinotConfig setControllerUrls(List<String> controllerUrl)
    {
        this.controllerUrls = controllerUrl.stream()
                .map(PinotConfig::stringToUri)
                .collect(toImmutableList());
        return this;
    }

    public Optional<HostAndPort> getBrokerUrl()
    {
        return brokerUrl;
    }

    @Config("pinot.broker-url")
    @ConfigDescription("Provide global broker host and port. Setting this property will disable broker discovery mechanism.")
    public PinotConfig setBrokerUrl(HostAndPort brokerUrl)
    {
        this.brokerUrl = Optional.ofNullable(brokerUrl);
        return this;
    }

    @MinDuration("15s")
    @NotNull
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("pinot.connection-timeout")
    public PinotConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @MinDuration("0s")
    @NotNull
    public Duration getMetadataCacheExpiry()
    {
        return metadataCacheExpiry;
    }

    @Config("pinot.metadata-expiry")
    public PinotConfig setMetadataCacheExpiry(Duration metadataCacheExpiry)
    {
        this.metadataCacheExpiry = metadataCacheExpiry;
        return this;
    }

    public boolean isPreferBrokerQueries()
    {
        return preferBrokerQueries;
    }

    @Config("pinot.prefer-broker-queries")
    public PinotConfig setPreferBrokerQueries(boolean preferBrokerQueries)
    {
        this.preferBrokerQueries = preferBrokerQueries;
        return this;
    }

    public boolean isForbidSegmentQueries()
    {
        return forbidSegmentQueries;
    }

    @Config("pinot.forbid-segment-queries")
    public PinotConfig setForbidSegmentQueries(boolean forbidSegmentQueries)
    {
        this.forbidSegmentQueries = forbidSegmentQueries;
        return this;
    }

    public int getSegmentsPerSplit()
    {
        return this.segmentsPerSplit;
    }

    @Config("pinot.segments-per-split")
    public PinotConfig setSegmentsPerSplit(int segmentsPerSplit)
    {
        checkArgument(segmentsPerSplit > 0, "Segments per split must be greater than zero");
        this.segmentsPerSplit = segmentsPerSplit;
        return this;
    }

    public int getFetchRetryCount()
    {
        return fetchRetryCount;
    }

    @Config("pinot.fetch-retry-count")
    public PinotConfig setFetchRetryCount(int fetchRetryCount)
    {
        this.fetchRetryCount = fetchRetryCount;
        return this;
    }

    public int getNonAggregateLimitForBrokerQueries()
    {
        return nonAggregateLimitForBrokerQueries;
    }

    @Config("pinot.non-aggregate-limit-for-broker-queries")
    public PinotConfig setNonAggregateLimitForBrokerQueries(int nonAggregateLimitForBrokerQueries)
    {
        this.nonAggregateLimitForBrokerQueries = nonAggregateLimitForBrokerQueries;
        return this;
    }

    private static URI stringToUri(String server)
    {
        if (server.startsWith("http://") || server.startsWith("https://")) {
            return URI.create(server);
        }
        return URI.create("http://" + server);
    }

    public int getMaxRowsForBrokerQueries()
    {
        return maxRowsForBrokerQueries;
    }

    @Config("pinot.max-rows-for-broker-queries")
    public PinotConfig setMaxRowsForBrokerQueries(int maxRowsForBrokerQueries)
    {
        this.maxRowsForBrokerQueries = maxRowsForBrokerQueries;
        return this;
    }

    public boolean isAggregationPushdownEnabled()
    {
        return aggregationPushdownEnabled;
    }

    @Config("pinot.aggregation-pushdown.enabled")
    public PinotConfig setAggregationPushdownEnabled(boolean aggregationPushdownEnabled)
    {
        this.aggregationPushdownEnabled = aggregationPushdownEnabled;
        return this;
    }

    public boolean isCountDistinctPushdownEnabled()
    {
        return countDistinctPushdownEnabled;
    }

    @Config("pinot.count-distinct-pushdown.enabled")
    @ConfigDescription("Controls whether distinct count is pushed down to Pinot. Distinct count pushdown can cause Pinot to do a full scan. Aggregation pushdown must also be enabled in addition to this parameter otherwise no pushdowns will be enabled.")
    public PinotConfig setCountDistinctPushdownEnabled(boolean countDistinctPushdownEnabled)
    {
        this.countDistinctPushdownEnabled = countDistinctPushdownEnabled;
        return this;
    }

    public boolean isTlsEnabled()
    {
        return "https".equalsIgnoreCase(getControllerUrls().get(0).getScheme());
    }

    public boolean getProxyEnabled()
    {
        return proxyEnabled;
    }

    @Config("pinot.proxy.enabled")
    public PinotConfig setProxyEnabled(boolean proxyEnabled)
    {
        this.proxyEnabled = proxyEnabled;
        return this;
    }

    public DataSize getTargetSegmentPageSize()
    {
        return this.targetSegmentPageSize;
    }

    @Config("pinot.target-segment-page-size")
    public PinotConfig setTargetSegmentPageSize(DataSize targetSegmentPageSize)
    {
        this.targetSegmentPageSize = targetSegmentPageSize;
        return this;
    }

    @AssertTrue(message = "Invalid configuration: pinot.aggregation-pushdown.enabled must be enabled if pinot.count-distinct-pushdown.enabled")
    public boolean isValidConfiguration()
    {
        return !countDistinctPushdownEnabled || aggregationPushdownEnabled;
    }

    @AssertTrue(message = "All controller URLs must have the same scheme")
    public boolean allUrlSchemesEqual()
    {
        return controllerUrls.stream()
                .map(URI::getScheme)
                .distinct()
                .count() == 1;
    }
}
