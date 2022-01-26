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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorPartitioningHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DeltaLakePartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final List<DeltaLakeColumnHandle> partitioningColumns;

    @JsonCreator
    public DeltaLakePartitioningHandle(@JsonProperty("partitioningColumns") List<DeltaLakeColumnHandle> partitioningColumns)
    {
        this.partitioningColumns = ImmutableList.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
    }

    @JsonProperty
    public List<DeltaLakeColumnHandle> getPartitioningColumns()
    {
        return partitioningColumns;
    }
}
