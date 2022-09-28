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
package io.trino.plugin.accumulo.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.accumulo.core.data.Range;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class AccumuloSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AccumuloSplit.class).instanceSize();

    private final Optional<String> hostPort;
    private final List<HostAddress> addresses;
    private final List<SerializedRange> ranges;

    @JsonCreator
    public AccumuloSplit(
            @JsonProperty("ranges") List<SerializedRange> ranges,
            @JsonProperty("hostPort") Optional<String> hostPort)
    {
        this.hostPort = requireNonNull(hostPort, "hostPort is null");
        this.ranges = ImmutableList.copyOf(requireNonNull(ranges, "ranges is null"));

        // Parse the host address into a list of addresses, this would be an Accumulo Tablet server or some localhost thing
        if (hostPort.isPresent()) {
            addresses = ImmutableList.of(HostAddress.fromString(hostPort.get()));
        }
        else {
            addresses = ImmutableList.of();
        }
    }

    @JsonProperty
    public Optional<String> getHostPort()
    {
        return hostPort;
    }

    @JsonProperty("ranges")
    public List<SerializedRange> getSerializedRanges()
    {
        return ranges;
    }

    @JsonIgnore
    public List<Range> getRanges()
    {
        return ranges.stream().map(SerializedRange::deserialize).collect(Collectors.toList());
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
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
                + sizeOf(hostPort, SizeOf::estimatedSizeOf)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes)
                + estimatedSizeOf(ranges, SerializedRange::getRetainedSizeInBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("addresses", addresses)
                .add("numRanges", ranges.size())
                .add("hostPort", hostPort)
                .toString();
    }
}
