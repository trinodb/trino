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
package io.prestosql.plugin.accumulo.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;
import org.apache.accumulo.core.data.Range;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AccumuloSplit
        implements ConnectorSplit
{
    private final Optional<String> hostPort;
    private final List<HostAddress> addresses;
    private final List<AccumuloColumnConstraint> constraints;
    private final List<WrappedRange> ranges;

    @JsonCreator
    public AccumuloSplit(
            @JsonProperty("ranges") List<WrappedRange> ranges,
            @JsonProperty("constraints") List<AccumuloColumnConstraint> constraints,
            @JsonProperty("hostPort") Optional<String> hostPort)
    {
        this.constraints = ImmutableList.copyOf(requireNonNull(constraints, "constraints is null"));
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
    public List<WrappedRange> getWrappedRanges()
    {
        return ranges;
    }

    @JsonIgnore
    public List<Range> getRanges()
    {
        return ranges.stream().map(WrappedRange::getRange).collect(Collectors.toList());
    }

    @JsonProperty
    public List<AccumuloColumnConstraint> getConstraints()
    {
        return constraints;
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
    public String toString()
    {
        return toStringHelper(this)
                .add("addresses", addresses)
                .add("numRanges", ranges.size())
                .add("constraints", constraints)
                .add("hostPort", hostPort)
                .toString();
    }
}
