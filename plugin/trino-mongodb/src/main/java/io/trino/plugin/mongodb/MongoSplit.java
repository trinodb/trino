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
package io.trino.plugin.mongodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class MongoSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(MongoSplit.class);

    private final List<HostAddress> addresses;

    @JsonCreator
    public MongoSplit(@JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    @JsonProperty
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
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }
}
