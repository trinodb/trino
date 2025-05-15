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
package io.trino.testing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;

public class TestingSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(TestingSplit.class);

    private static final HostAddress localHost = HostAddress.fromString("127.0.0.1");

    private final boolean remotelyAccessible;
    private final List<HostAddress> addresses;

    public static TestingSplit createLocalSplit()
    {
        return new TestingSplit(false, ImmutableList.of(localHost));
    }

    public static TestingSplit createEmptySplit()
    {
        return new TestingSplit(false, ImmutableList.of());
    }

    public static TestingSplit createRemoteSplit()
    {
        return new TestingSplit(true, ImmutableList.of());
    }

    @JsonCreator
    public TestingSplit(@JsonProperty("remotelyAccessible") boolean remotelyAccessible, @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.addresses = addresses;
        this.remotelyAccessible = remotelyAccessible;
    }

    @JsonProperty
    @Override
    public boolean isRemotelyAccessible()
    {
        return remotelyAccessible;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("remotelyAccessible", remotelyAccessible)
                .add("addresses", addresses)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }
}
