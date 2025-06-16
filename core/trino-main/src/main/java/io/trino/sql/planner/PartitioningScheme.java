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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitioningScheme
{
    private final Partitioning partitioning;
    private final List<Symbol> outputLayout;
    private final boolean replicateNullsAndAny;
    private final Optional<int[]> bucketToPartition;
    private final Optional<Integer> partitionCount;

    public PartitioningScheme(Partitioning partitioning, List<Symbol> outputLayout)
    {
        this(
                partitioning,
                outputLayout,
                false,
                Optional.empty(),
                Optional.empty());
    }

    @JsonCreator
    public PartitioningScheme(
            @JsonProperty("partitioning") Partitioning partitioning,
            @JsonProperty("outputLayout") List<Symbol> outputLayout,
            @JsonProperty("replicateNullsAndAny") boolean replicateNullsAndAny,
            @JsonProperty("bucketToPartition") Optional<int[]> bucketToPartition,
            @JsonProperty("partitionCount") Optional<Integer> partitionCount)
    {
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.outputLayout = ImmutableList.copyOf(requireNonNull(outputLayout, "outputLayout is null"));

        Set<Symbol> columns = partitioning.getColumns();
        checkArgument(ImmutableSet.copyOf(outputLayout).containsAll(columns),
                "Output layout (%s) don't include all partition columns (%s)", outputLayout, columns);

        checkArgument(!replicateNullsAndAny || columns.size() <= 1, "Must have at most one partitioning column when nullPartition is REPLICATE.");
        this.replicateNullsAndAny = replicateNullsAndAny;
        this.bucketToPartition = requireNonNull(bucketToPartition, "bucketToPartition is null");
        this.partitionCount = requireNonNull(partitionCount, "partitionCount is null");
        checkArgument(
                partitionCount.isEmpty() || partitioning.getHandle().getConnectorHandle() instanceof SystemPartitioningHandle,
                "Connector partitioning handle should be of type system partitioning when partitionCount is present");
    }

    @JsonProperty
    public Partitioning getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public List<Symbol> getOutputLayout()
    {
        return outputLayout;
    }

    @JsonProperty
    public boolean isReplicateNullsAndAny()
    {
        return replicateNullsAndAny;
    }

    @JsonProperty
    public Optional<int[]> getBucketToPartition()
    {
        return bucketToPartition;
    }

    @JsonProperty
    public Optional<Integer> getPartitionCount()
    {
        return partitionCount;
    }

    public PartitioningScheme withBucketToPartition(Optional<int[]> bucketToPartition)
    {
        return new PartitioningScheme(partitioning, outputLayout, replicateNullsAndAny, bucketToPartition, partitionCount);
    }

    public PartitioningScheme withPartitioningHandle(PartitioningHandle partitioningHandle)
    {
        Partitioning newPartitioning = partitioning.withAlternativePartitioningHandle(partitioningHandle);
        return new PartitioningScheme(newPartitioning, outputLayout, replicateNullsAndAny, bucketToPartition, partitionCount);
    }

    public PartitioningScheme withPartitionCount(Optional<Integer> partitionCount)
    {
        return new PartitioningScheme(partitioning, outputLayout, replicateNullsAndAny, bucketToPartition, partitionCount);
    }

    public PartitioningScheme translateOutputLayout(List<Symbol> newOutputLayout)
    {
        requireNonNull(newOutputLayout, "newOutputLayout is null");

        checkArgument(newOutputLayout.size() == outputLayout.size());

        Partitioning newPartitioning = partitioning.translate(symbol -> newOutputLayout.get(outputLayout.indexOf(symbol)));

        return new PartitioningScheme(newPartitioning, newOutputLayout, replicateNullsAndAny, bucketToPartition, partitionCount);
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
        PartitioningScheme that = (PartitioningScheme) o;
        return Objects.equals(partitioning, that.partitioning) &&
                Objects.equals(outputLayout, that.outputLayout) &&
                replicateNullsAndAny == that.replicateNullsAndAny &&
                Objects.equals(bucketToPartition, that.bucketToPartition) &&
                Objects.equals(partitionCount, that.partitionCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioning, outputLayout, replicateNullsAndAny, bucketToPartition, partitionCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitioning", partitioning)
                .add("outputLayout", outputLayout)
                .add("replicateNullsAndAny", replicateNullsAndAny)
                .add("bucketToPartition", bucketToPartition)
                .add("partitionCount", partitionCount)
                .toString();
    }
}
