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
package io.prestosql.plugin.blackhole;

import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.BucketFunction;
import io.prestosql.spi.connector.ConnectorBucketNodeMap;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.function.ToIntFunction;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.InvocationConvention.simpleConvention;
import static java.util.Objects.requireNonNull;

public class BlackHoleNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;
    private final TypeOperators typeOperators;

    public BlackHoleNodePartitioningProvider(NodeManager nodeManager, TypeOperators typeOperators)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeOperators = typeOperators;
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        // create one bucket per node
        return createBucketNodeMap(nodeManager.getRequiredWorkerNodes().size());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> {
            throw new PrestoException(NOT_SUPPORTED, "Black hole connector does not supported distributed reads");
        };
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes, int bucketCount)
    {
        List<MethodHandle> hashCodeInvokers = partitionChannelTypes.stream()
                .map(type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)))
                .collect(toImmutableList());

        return (page, position) -> {
            long hash = 13;
            for (int i = 0; i < partitionChannelTypes.size(); i++) {
                try {
                    hash = 31 * hash + (long) hashCodeInvokers.get(i).invokeExact(page.getBlock(i), 0);
                }
                catch (Throwable throwable) {
                    throwIfUnchecked(throwable);
                    throw new RuntimeException(throwable);
                }
            }

            // clear the sign bit
            hash &= 0x7fff_ffff_ffff_ffffL;

            return (int) (hash % bucketCount);
        };
    }
}
