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
import com.fasterxml.jackson.annotation.JsonGetter;
import com.google.common.base.VerifyException;
import io.trino.execution.scheduler.FaultTolerantPartitioningScheme;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.type.Type;
import io.trino.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.RoundRobinBucketFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getLast;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_OPERATION_NUMBER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class MergePartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final Optional<PartitioningScheme> insertPartitioning;
    private final Optional<PartitioningScheme> updatePartitioning;

    @JsonCreator
    public MergePartitioningHandle(Optional<PartitioningScheme> insertPartitioning, Optional<PartitioningScheme> updatePartitioning)
    {
        this.insertPartitioning = requireNonNull(insertPartitioning, "insertPartitioning is null");
        this.updatePartitioning = requireNonNull(updatePartitioning, "updatePartitioning is null");
        checkArgument(insertPartitioning.isPresent() || updatePartitioning.isPresent(), "insert or update partitioning must be present");
    }

    @JsonGetter
    public Optional<PartitioningScheme> getInsertPartitioning()
    {
        return insertPartitioning;
    }

    @JsonGetter
    public Optional<PartitioningScheme> getUpdatePartitioning()
    {
        return updatePartitioning;
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
        MergePartitioningHandle that = (MergePartitioningHandle) o;
        return insertPartitioning.equals(that.insertPartitioning) &&
                updatePartitioning.equals(that.updatePartitioning);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(insertPartitioning, updatePartitioning);
    }

    @Override
    public String toString()
    {
        List<String> parts = new ArrayList<>();
        insertPartitioning.ifPresent(scheme -> parts.add("insert = " + scheme.getPartitioning().getHandle()));
        updatePartitioning.ifPresent(scheme -> parts.add("update = " + scheme.getPartitioning().getHandle()));
        return "MERGE " + parts;
    }

    public NodePartitionMap getNodePartitioningMap(Function<PartitioningHandle, NodePartitionMap> getMap)
    {
        Optional<NodePartitionMap> optionalInsertMap = insertPartitioning.map(scheme -> scheme.getPartitioning().getHandle()).map(getMap);
        Optional<NodePartitionMap> optionalUpdateMap = updatePartitioning.map(scheme -> scheme.getPartitioning().getHandle()).map(getMap);

        if (optionalInsertMap.isPresent() && optionalUpdateMap.isPresent()) {
            NodePartitionMap insertMap = optionalInsertMap.get();
            NodePartitionMap updateMap = optionalUpdateMap.get();
            if (!insertMap.getPartitionToNode().equals(updateMap.getPartitionToNode()) ||
                    !Arrays.equals(insertMap.getBucketToPartition(), updateMap.getBucketToPartition())) {
                throw new TrinoException(NOT_SUPPORTED, "Insert and update layout have mismatched BucketNodeMap");
            }
        }

        return optionalInsertMap.orElseGet(optionalUpdateMap::orElseThrow);
    }

    public FaultTolerantPartitioningScheme getFaultTolerantPartitioningScheme(Function<PartitioningHandle, FaultTolerantPartitioningScheme> getScheme)
    {
        Optional<FaultTolerantPartitioningScheme> optionalInsertScheme = insertPartitioning.map(scheme -> scheme.getPartitioning().getHandle()).map(getScheme);
        Optional<FaultTolerantPartitioningScheme> optionalUpdateScheme = updatePartitioning.map(scheme -> scheme.getPartitioning().getHandle()).map(getScheme);

        if (optionalInsertScheme.isPresent() && optionalUpdateScheme.isPresent()) {
            FaultTolerantPartitioningScheme insertScheme = optionalInsertScheme.get();
            FaultTolerantPartitioningScheme updateScheme = optionalUpdateScheme.get();
            if (insertScheme.getPartitionCount() != updateScheme.getPartitionCount()
                    || !Arrays.equals(insertScheme.getBucketToPartitionMap().orElse(null), updateScheme.getBucketToPartitionMap().orElse(null))
                    || !Objects.equals(insertScheme.getPartitionToNodeMap(), updateScheme.getPartitionToNodeMap())) {
                throw new TrinoException(NOT_SUPPORTED, "Insert and update layout have mismatched BucketNodeMap");
            }
        }

        return optionalInsertScheme.orElseGet(optionalUpdateScheme::orElseThrow);
    }

    public PartitionFunction getPartitionFunction(PartitionFunctionLookup partitionFunctionLookup, List<Type> types, int[] bucketToPartition)
    {
        // channels: merge row, insert arguments, update row ID
        List<Type> insertTypes = types.subList(1, types.size() - (updatePartitioning.isPresent() ? 1 : 0));

        Optional<PartitionFunction> insertFunction = insertPartitioning.map(scheme ->
                partitionFunctionLookup.get(scheme, insertTypes));

        Optional<PartitionFunction> updateFunction = updatePartitioning.map(scheme ->
                partitionFunctionLookup.get(scheme, List.of(getLast(types))));

        return getPartitionFunction(insertFunction, updateFunction, insertTypes.size(), bucketToPartition);
    }

    private static PartitionFunction getPartitionFunction(Optional<PartitionFunction> insertFunction, Optional<PartitionFunction> updateFunction, int insertArguments, int[] bucketToPartition)
    {
        if (insertFunction.isPresent() && updateFunction.isPresent()) {
            return new MergePartitionFunction(
                    insertFunction.get(),
                    updateFunction.get(),
                    IntStream.range(1, insertArguments + 1).toArray(),
                    new int[] {insertArguments + 1});
        }

        PartitionFunction roundRobinFunction = new BucketPartitionFunction(new RoundRobinBucketFunction(bucketToPartition.length), bucketToPartition);

        if (insertFunction.isPresent()) {
            return new MergePartitionFunction(
                    insertFunction.get(),
                    roundRobinFunction,
                    IntStream.range(1, insertArguments + 1).toArray(),
                    new int[] {});
        }

        if (updateFunction.isPresent()) {
            return new MergePartitionFunction(
                    roundRobinFunction,
                    updateFunction.get(),
                    new int[] {},
                    new int[] {insertArguments + 1});
        }

        throw new AssertionError();
    }

    public interface PartitionFunctionLookup
    {
        PartitionFunction get(PartitioningScheme scheme, List<Type> partitionChannelTypes);
    }

    private static final class MergePartitionFunction
            implements PartitionFunction
    {
        private final PartitionFunction insertFunction;
        private final PartitionFunction updateFunction;
        private final int[] insertColumns;
        private final int[] updateColumns;

        public MergePartitionFunction(PartitionFunction insertFunction, PartitionFunction updateFunction, int[] insertColumns, int[] updateColumns)
        {
            this.insertFunction = requireNonNull(insertFunction, "insertFunction is null");
            this.updateFunction = requireNonNull(updateFunction, "updateFunction is null");
            this.insertColumns = requireNonNull(insertColumns, "insertColumns is null");
            this.updateColumns = requireNonNull(updateColumns, "updateColumns is null");
            checkArgument(insertFunction.getPartitionCount() == updateFunction.getPartitionCount(), "partition counts must match");
        }

        @Override
        public int getPartitionCount()
        {
            return insertFunction.getPartitionCount();
        }

        @Override
        public int getPartition(Page page, int position)
        {
            Block operationBlock = page.getBlock(0);
            int operation = toIntExact(TINYINT.getLong(operationBlock, position));
            return switch (operation) {
                case INSERT_OPERATION_NUMBER, UPDATE_INSERT_OPERATION_NUMBER -> insertFunction.getPartition(page.getColumns(insertColumns), position);
                case UPDATE_OPERATION_NUMBER, DELETE_OPERATION_NUMBER, UPDATE_DELETE_OPERATION_NUMBER -> updateFunction.getPartition(page.getColumns(updateColumns), position);
                default -> throw new VerifyException("Invalid merge operation number: " + operation);
            };
        }
    }
}
