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
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.type.Type;
import io.trino.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.RoundRobinBucketFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getLast;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
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

    public PartitionFunction getPartitionFunction(BucketFunctionLookup bucketFunctionLookup, List<Type> types, int[] bucketToPartition)
    {
        int bucketCount = bucketToPartition.length;

        // channels: merge row, insert arguments, update row ID
        List<Type> insertTypes = types.subList(1, types.size() - (updatePartitioning.isPresent() ? 1 : 0));

        Optional<BucketFunction> insertFunction = insertPartitioning.map(scheme ->
                bucketFunctionLookup.get(scheme.getPartitioning().getHandle(), insertTypes, bucketCount));

        Optional<BucketFunction> updateFunction = updatePartitioning.map(scheme ->
                bucketFunctionLookup.get(scheme.getPartitioning().getHandle(), List.of(getLast(types)), bucketCount));

        BucketFunction function = getBucketFunction(insertFunction, updateFunction, insertTypes.size(), bucketCount);
        return new BucketPartitionFunction(function, bucketToPartition);
    }

    private static BucketFunction getBucketFunction(Optional<BucketFunction> insertFunction, Optional<BucketFunction> updateFunction, int insertArguments, int bucketCount)
    {
        if (insertFunction.isPresent() && updateFunction.isPresent()) {
            return new MergeBucketFunction(
                    insertFunction.get(),
                    updateFunction.get(),
                    IntStream.range(1, insertArguments + 1).toArray(),
                    new int[] {insertArguments + 1});
        }

        if (insertFunction.isPresent()) {
            return new MergeBucketFunction(
                    insertFunction.get(),
                    new RoundRobinBucketFunction(bucketCount),
                    IntStream.range(1, insertArguments + 1).toArray(),
                    new int[] {});
        }

        if (updateFunction.isPresent()) {
            return new MergeBucketFunction(
                    new RoundRobinBucketFunction(bucketCount),
                    updateFunction.get(),
                    new int[] {},
                    new int[] {insertArguments + 1});
        }

        throw new AssertionError();
    }

    public interface BucketFunctionLookup
    {
        BucketFunction get(PartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount);
    }

    private static final class MergeBucketFunction
            implements BucketFunction
    {
        private final BucketFunction insertFunction;
        private final BucketFunction updateFunction;
        private final int[] insertColumns;
        private final int[] updateColumns;

        public MergeBucketFunction(BucketFunction insertFunction, BucketFunction updateFunction, int[] insertColumns, int[] updateColumns)
        {
            this.insertFunction = requireNonNull(insertFunction, "insertFunction is null");
            this.updateFunction = requireNonNull(updateFunction, "updateFunction is null");
            this.insertColumns = requireNonNull(insertColumns, "insertColumns is null");
            this.updateColumns = requireNonNull(updateColumns, "updateColumns is null");
        }

        @Override
        public int getBucket(Page page, int position)
        {
            Block operationBlock = page.getBlock(0);
            int operation = toIntExact(TINYINT.getLong(operationBlock, position));
            switch (operation) {
                case INSERT_OPERATION_NUMBER:
                    return insertFunction.getBucket(page.getColumns(insertColumns), position);
                case UPDATE_OPERATION_NUMBER:
                case DELETE_OPERATION_NUMBER:
                    return updateFunction.getBucket(page.getColumns(updateColumns), position);
                default:
                    throw new VerifyException("Invalid merge operation number: " + operation);
            }
        }
    }
}
