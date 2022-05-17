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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CombinedIcebergSplit
        implements ConnectorSplit
{
    List<IcebergSplit> entries;

    @JsonCreator
    public CombinedIcebergSplit(@JsonProperty("entries") List<IcebergSplit> entries)
    {
        this.entries = ImmutableList.copyOf(requireNonNull(entries, "entries is null"));
    }

    @JsonProperty
    public List<IcebergSplit> getEntries()
    {
        return entries;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return entries.stream()
                .map(IcebergSplit::getAddresses)
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    @Override
    public Object getInfo()
    {
        return entries.stream()
                .map(IcebergSplit::getInfo)
                .collect(toImmutableList());
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return entries.stream()
                .mapToLong(IcebergSplit::getRetainedSizeInBytes)
                .sum();
    }

    @Override
    public String toString()
    {
        ToStringHelper toStringHelper = toStringHelper(this);
        entries.forEach(toStringHelper::addValue);
        return toStringHelper.toString();
    }
}
