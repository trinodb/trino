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
package io.trino.plugin.pinot;

import com.google.inject.Inject;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PinotNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;
    private DateTimeFormatter dateFormat;

    @Inject
    public PinotNodePartitioningProvider(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        PinotPartitioningHandle handle = (PinotPartitioningHandle) partitioningHandle;
        int bucketCount = Math.max(handle.getSegmentCount().getAsInt(), nodeManager.getRequiredWorkerNodes().size());

        return Optional.of(createBucketNodeMap(bucketCount));
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        PinotPartitioningHandle pinotPartitioningHandle = (PinotPartitioningHandle) partitioningHandle;
        if (pinotPartitioningHandle.getNodes().isPresent()) {
            return value -> ((PinotSplit) value).getBucket();
        }
        return value -> 0;
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        Type type = getOnlyElement(partitionChannelTypes);
        // Note: Pinot time column must be of long or timestamp type.
        // Using floating point types leads to unpredictable results.
        // Currently Pinot timestamp type maps to Trino timestamp(3).
        checkState(type instanceof BigintType || (type instanceof TimestampType && ((TimestampType) type).getPrecision() == 3), "Unexpected type");
        PinotPartitioningHandle pinotPartitioningHandle = (PinotPartitioningHandle) partitioningHandle;
        checkState(pinotPartitioningHandle.getDateTimeField().isPresent(), "DateTimeField is not present");
        LongUnaryOperator transformFunction = pinotPartitioningHandle.getDateTimeField().get().getToMillisTransform();
        return (page, position) -> {
            long timeValue = page.getBlock(0).getLong(position, 0);
            long epochMillis = transformFunction.applyAsLong(timeValue);
            LocalDate localDate = Instant.ofEpochMilli(epochMillis).atZone(ZoneId.of("UTC")).toLocalDate();
            String format = dateFormat.format(localDate);
            return toIntExact((Objects.hash(format) & 0xFFFF_FFFFL) % bucketCount);
        };
    }
}
