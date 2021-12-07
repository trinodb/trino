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
package io.trino.plugin.atop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.time.Instant.ofEpochSecond;
import static java.util.Objects.requireNonNull;

public class AtopSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AtopSplit.class).instanceSize();

    private final HostAddress host;
    private final long epochSeconds;
    private final String timeZoneId;

    @JsonCreator
    public AtopSplit(
            @JsonProperty("host") HostAddress host,
            @JsonProperty("epochSeconds") long epochSeconds,
            @JsonProperty("timeZoneId") String timeZoneId)
    {
        this.host = requireNonNull(host, "host is null");
        this.epochSeconds = epochSeconds;
        this.timeZoneId = requireNonNull(timeZoneId, "timeZoneId  is null");
    }

    @JsonProperty
    public HostAddress getHost()
    {
        return host;
    }

    @JsonProperty
    public long getEpochSeconds()
    {
        return epochSeconds;
    }

    @JsonProperty
    public String getTimeZoneId()
    {
        return timeZoneId;
    }

    public ZonedDateTime getDate()
    {
        return ZonedDateTime.ofInstant(ofEpochSecond(epochSeconds), ZoneId.of(timeZoneId));
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        // discard the port number
        return ImmutableList.of(HostAddress.fromString(host.getHostText()));
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + host.getRetainedSizeInBytes()
                + estimatedSizeOf(timeZoneId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("host", host)
                .add("epochSeconds", epochSeconds)
                .add("timeZoneId", timeZoneId)
                .toString();
    }
}
