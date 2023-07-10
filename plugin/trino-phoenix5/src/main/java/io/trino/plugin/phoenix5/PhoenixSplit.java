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
package io.trino.plugin.phoenix5;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.spi.HostAddress;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class PhoenixSplit
        extends JdbcSplit
{
    private static final int INSTANCE_SIZE = instanceSize(PhoenixSplit.class);

    private final List<HostAddress> addresses;
    private final SerializedPhoenixInputSplit serializedPhoenixInputSplit;

    @JsonCreator
    public PhoenixSplit(
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("serializedPhoenixInputSplit") SerializedPhoenixInputSplit serializedPhoenixInputSplit)
    {
        super(Optional.empty());
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.serializedPhoenixInputSplit = requireNonNull(serializedPhoenixInputSplit, "serializedPhoenixInputSplit is null");
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public SerializedPhoenixInputSplit getSerializedPhoenixInputSplit()
    {
        return serializedPhoenixInputSplit;
    }

    @JsonIgnore
    public PhoenixInputSplit getPhoenixInputSplit()
    {
        return serializedPhoenixInputSplit.deserialize();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(getAdditionalPredicate(), SizeOf::estimatedSizeOf)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes)
                + serializedPhoenixInputSplit.getRetainedSizeInBytes();
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.of("addresses", addresses);
    }
}
