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
package io.prestosql.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class JdbcSplit
        implements ConnectorSplit
{
    private final Optional<String> additionalPredicate;
    private final TupleDomain<ColumnHandle> additionalConstraint;

    public JdbcSplit()
    {
        this(Optional.empty(), TupleDomain.all());
    }

    @JsonCreator
    public JdbcSplit(
            @JsonProperty("additionalPredicate") Optional<String> additionalPredicate,
            @JsonProperty("additionalConstraint") TupleDomain<ColumnHandle> additionalConstraint)
    {
        this.additionalPredicate = requireNonNull(additionalPredicate, "additionalPredicate is null");
        this.additionalConstraint = requireNonNull(additionalConstraint, "additionalConstraint is null");
    }

    @JsonProperty
    public Optional<String> getAdditionalPredicate()
    {
        return additionalPredicate;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getAdditionalConstraint()
    {
        return additionalConstraint;
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
}
