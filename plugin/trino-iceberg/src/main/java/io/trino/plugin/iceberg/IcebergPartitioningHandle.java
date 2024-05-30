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
import io.trino.spi.connector.ConnectorPartitioningHandle;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public record IcebergPartitioningHandle(List<String> partitioning, List<IcebergColumnHandle> partitioningColumns)
        implements ConnectorPartitioningHandle
{
    public IcebergPartitioningHandle
    {
        partitioning = ImmutableList.copyOf(requireNonNull(partitioning, "partitioning is null"));
        partitioningColumns = ImmutableList.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitioning", partitioning)
                .toString();
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
        IcebergPartitioningHandle that = (IcebergPartitioningHandle) o;
        return Objects.equals(partitioning, that.partitioning) && Objects.equals(partitioningColumns, that.partitioningColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioning, partitioningColumns);
    }
}
