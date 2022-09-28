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
package io.trino.plugin.tpcds;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public class TpcdsSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TpcdsSplit.class).instanceSize();

    private final int totalParts;
    private final int partNumber;
    private final List<HostAddress> addresses;
    private final boolean noSexism;

    @JsonCreator
    public TpcdsSplit(
            @JsonProperty("partNumber") int partNumber,
            @JsonProperty("totalParts") int totalParts,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("noSexism") boolean noSexism)
    {
        checkState(partNumber >= 0, "partNumber must be >= 0");
        checkState(totalParts >= 1, "totalParts must be >= 1");
        checkState(totalParts > partNumber, "totalParts must be > partNumber");
        requireNonNull(addresses, "addresses is null");

        this.partNumber = partNumber;
        this.totalParts = totalParts;
        this.addresses = ImmutableList.copyOf(addresses);
        this.noSexism = noSexism;
    }

    @JsonProperty
    public int getTotalParts()
    {
        return totalParts;
    }

    @JsonProperty
    public int getPartNumber()
    {
        return partNumber;
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
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public boolean isNoSexism()
    {
        return noSexism;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TpcdsSplit other = (TpcdsSplit) obj;
        return Objects.equals(this.totalParts, other.totalParts) &&
                Objects.equals(this.partNumber, other.partNumber) &&
                Objects.equals(this.noSexism, other.noSexism);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(totalParts, partNumber, noSexism);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partNumber", partNumber)
                .add("totalParts", totalParts)
                .add("noSexism", noSexism)
                .toString();
    }
}
