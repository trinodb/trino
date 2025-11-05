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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorTablePartitioning;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record IcebergTablePartitioning(
        boolean active,
        IcebergPartitioningHandle partitioningHandle,
        List<IcebergColumnHandle> partitioningColumns,
        List<Integer> partitionStructFields)
{
    public IcebergTablePartitioning
    {
        requireNonNull(partitioningHandle, "partitioningHandle is null");
        partitioningColumns = ImmutableList.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
        partitionStructFields = ImmutableList.copyOf(requireNonNull(partitionStructFields, "partitionStructFields is null"));
        checkArgument(partitioningHandle.partitionFunctions().size() == partitionStructFields.size(), "partitioningColumns and partitionStructFields must have the same size");
    }

    public IcebergTablePartitioning activate()
    {
        return new IcebergTablePartitioning(true, partitioningHandle, partitioningColumns, partitionStructFields);
    }

    public Optional<ConnectorTablePartitioning> toConnectorTablePartitioning()
    {
        return active ? Optional.of(new ConnectorTablePartitioning(partitioningHandle, ImmutableList.copyOf(partitioningColumns))) : Optional.empty();
    }
}
