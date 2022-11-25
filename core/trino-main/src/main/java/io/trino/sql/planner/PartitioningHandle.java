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
import io.trino.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static java.util.Objects.requireNonNull;

public class PartitioningHandle
{
    private final Optional<CatalogHandle> catalogHandle;
    private final Optional<ConnectorTransactionHandle> transactionHandle;
    private final ConnectorPartitioningHandle connectorHandle;
    /**
     * False if rows for a given partition can be distributed between multiple streams.
     */
    private final boolean deterministic;
    private final boolean scaleWriters;

    public static boolean isScaledWriterHashDistribution(PartitioningHandle partitioning)
    {
        return partitioning.isScaleWriters()
                && (partitioning.equals(SCALED_WRITER_HASH_DISTRIBUTION) || partitioning.getCatalogHandle().isPresent());
    }

    public static PartitioningHandle createPartitioning(
            Optional<CatalogHandle> catalogHandle,
            Optional<ConnectorTransactionHandle> transactionHandle,
            ConnectorPartitioningHandle connectorHandle)
    {
        return new PartitioningHandle(catalogHandle, transactionHandle, connectorHandle, true, false);
    }

    public static PartitioningHandle createNonDeterministicPartitioning(
            Optional<CatalogHandle> catalogHandle,
            Optional<ConnectorTransactionHandle> transactionHandle,
            ConnectorPartitioningHandle connectorHandle)
    {
        return new PartitioningHandle(catalogHandle, transactionHandle, connectorHandle, false, false);
    }

    public static PartitioningHandle createScaledWriterPartitioning(
            Optional<CatalogHandle> catalogHandle,
            Optional<ConnectorTransactionHandle> transactionHandle,
            ConnectorPartitioningHandle connectorHandle)
    {
        return new PartitioningHandle(catalogHandle, transactionHandle, connectorHandle, false, true);
    }

    /*
     * This constructor is for JSON deserialization only. Do not use.
     * It's marked as @Deprecated to help avoid usage, and not because we plan to remove it.
     */
    @Deprecated
    @JsonCreator
    public PartitioningHandle(
            @JsonProperty("catalogHandle") Optional<CatalogHandle> catalogHandle,
            @JsonProperty("transactionHandle") Optional<ConnectorTransactionHandle> transactionHandle,
            @JsonProperty("connectorHandle") ConnectorPartitioningHandle connectorHandle,
            @JsonProperty("deterministic") boolean deterministic,
            @JsonProperty("scaleWriters") boolean scaleWriters)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        checkArgument(catalogHandle.isEmpty() || transactionHandle.isPresent(), "transactionHandle is required when catalogHandle is present");
        this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
        checkArgument(!(deterministic && scaleWriters), "Scale writers partitioning handle cannot be deterministic");
        this.deterministic = deterministic;
        this.scaleWriters = scaleWriters;
    }

    @JsonProperty
    public Optional<CatalogHandle> getCatalogHandle()
    {
        return catalogHandle;
    }

    @JsonProperty
    public Optional<ConnectorTransactionHandle> getTransactionHandle()
    {
        return transactionHandle;
    }

    @JsonProperty
    public ConnectorPartitioningHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    @JsonProperty
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @JsonProperty
    public boolean isScaleWriters()
    {
        return scaleWriters;
    }

    public boolean isSingleNode()
    {
        return connectorHandle.isSingleNode();
    }

    public boolean isCoordinatorOnly()
    {
        return connectorHandle.isCoordinatorOnly();
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
        PartitioningHandle that = (PartitioningHandle) o;

        return Objects.equals(catalogHandle, that.catalogHandle) &&
                Objects.equals(transactionHandle, that.transactionHandle) &&
                Objects.equals(connectorHandle, that.connectorHandle) &&
                deterministic == that.deterministic &&
                scaleWriters == that.scaleWriters;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogHandle, transactionHandle, connectorHandle, deterministic, scaleWriters);
    }

    @Override
    public String toString()
    {
        String result = connectorHandle.toString();
        if (scaleWriters) {
            result = result + " (scale writers)";
        }
        if (catalogHandle.isPresent()) {
            result = catalogHandle.get() + ":" + result;
        }
        return result;
    }
}
