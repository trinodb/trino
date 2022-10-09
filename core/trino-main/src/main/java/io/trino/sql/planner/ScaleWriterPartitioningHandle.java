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
package io.trino.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorPartitioningHandle;

import static java.util.Objects.requireNonNull;

public class ScaleWriterPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final ConnectorPartitioningHandle partitioningHandle;

    public static PartitioningHandle wrapScalingPartitioningHandle(PartitioningHandle partitioning)
    {
        return new PartitioningHandle(
                partitioning.getCatalogHandle(),
                partitioning.getTransactionHandle(),
                new ScaleWriterPartitioningHandle(partitioning.getConnectorHandle()));
    }

    @JsonCreator
    public ScaleWriterPartitioningHandle(@JsonProperty("partitioning") ConnectorPartitioningHandle partitioningHandle)
    {
        this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
    }

    @Override
    public boolean isSingleNode()
    {
        return partitioningHandle.isSingleNode();
    }

    @Override
    public boolean isCoordinatorOnly()
    {
        return partitioningHandle.isCoordinatorOnly();
    }

    @JsonProperty
    public ConnectorPartitioningHandle getPartitioningHandle()
    {
        return partitioningHandle;
    }
}
