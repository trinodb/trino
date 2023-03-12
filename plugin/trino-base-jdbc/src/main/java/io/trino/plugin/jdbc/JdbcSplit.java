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
package io.trino.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class JdbcSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(JdbcSplit.class);

    private final Optional<String> additionalPredicate;
    private final TupleDomain<JdbcColumnHandle> dynamicFilter;

    public JdbcSplit(Optional<String> additionalPredicate)
    {
        this(additionalPredicate, TupleDomain.all());
    }

    @JsonCreator
    public JdbcSplit(
            @JsonProperty("additionalPredicate") Optional<String> additionalPredicate,
            @JsonProperty("dynamicFilter") TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        this.additionalPredicate = requireNonNull(additionalPredicate, "additionalPredicate is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
    }

    public JdbcSplit withDynamicFilter(TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        return new JdbcSplit(additionalPredicate, dynamicFilter);
    }

    @JsonProperty
    public Optional<String> getAdditionalPredicate()
    {
        return additionalPredicate;
    }

    @JsonProperty
    public TupleDomain<JdbcColumnHandle> getDynamicFilter()
    {
        return dynamicFilter;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
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
                + sizeOf(additionalPredicate, SizeOf::estimatedSizeOf)
                + dynamicFilter.getRetainedSizeInBytes(JdbcColumnHandle::getRetainedSizeInBytes);
    }
}
