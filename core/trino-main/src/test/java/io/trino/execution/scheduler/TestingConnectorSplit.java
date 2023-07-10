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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

class TestingConnectorSplit
        implements ConnectorSplit
{
    private static final long INSTANCE_SIZE = instanceSize(TestingConnectorSplit.class);

    private final int id;
    private final OptionalInt bucket;
    private final Optional<List<HostAddress>> addresses;
    private final SplitWeight weight;

    public TestingConnectorSplit(int id, OptionalInt bucket, Optional<List<HostAddress>> addresses)
    {
        this(id, bucket, addresses, SplitWeight.standard().getRawValue());
    }

    public TestingConnectorSplit(int id, OptionalInt bucket, Optional<List<HostAddress>> addresses, long weight)
    {
        this.id = id;
        this.bucket = requireNonNull(bucket, "bucket is null");
        this.addresses = addresses.map(ImmutableList::copyOf);
        this.weight = SplitWeight.fromRawValue(weight);
    }

    public int getId()
    {
        return id;
    }

    public OptionalInt getBucket()
    {
        return bucket;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return addresses.isEmpty();
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses.orElse(ImmutableList.of());
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        return weight;
    }

    @Override
    public Object getInfo()
    {
        return null;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(bucket)
                + sizeOf(addresses, value -> estimatedSizeOf(value, HostAddress::getRetainedSizeInBytes));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestingConnectorSplit that = (TestingConnectorSplit) o;
        return id == that.id && weight == that.weight && Objects.equals(bucket, that.bucket) && Objects.equals(addresses, that.addresses);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, bucket, addresses, weight);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("bucket", bucket)
                .add("addresses", addresses)
                .add("weight", weight)
                .toString();
    }

    public static int getSplitId(Split split)
    {
        return ((TestingConnectorSplit) split.getConnectorSplit()).getId();
    }
}
