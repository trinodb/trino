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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.Iterators.limit;
import static io.prestosql.plugin.iceberg.DomainConverter.convertTupleDomainTypes;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_PARTITION_VALUE;
import static io.prestosql.plugin.iceberg.IcebergUtil.getIdentityPartitions;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.iceberg.types.Type.TypeID.BINARY;
import static org.apache.iceberg.types.Type.TypeID.FIXED;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    private final CloseableIterable<CombinedScanTask> combinedScanIterable;
    private final TupleDomain<IcebergColumnHandle> predicate;
    private final Iterator<FileScanTask> fileScanIterator;

    public IcebergSplitSource(
            CloseableIterable<CombinedScanTask> combinedScanIterable,
            TupleDomain<IcebergColumnHandle> predicate)
    {
        this.combinedScanIterable = requireNonNull(combinedScanIterable, "combinedScanIterable is null");
        this.predicate = requireNonNull(predicate, "predicate is null");

        this.fileScanIterator = Streams.stream(combinedScanIterable)
                .map(CombinedScanTask::files)
                .flatMap(Collection::stream)
                .iterator();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        // TODO: move this to a background thread
        List<ConnectorSplit> splits = new ArrayList<>();
        TupleDomain<IcebergColumnHandle> predicate = convertTupleDomainTypes(this.predicate);
        Iterator<FileScanTask> iterator = limit(fileScanIterator, maxSize);
        while (iterator.hasNext()) {
            FileScanTask task = iterator.next();
            splits.add(toIcebergSplit(predicate, task));
        }
        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    @Override
    public boolean isFinished()
    {
        return !fileScanIterator.hasNext();
    }

    @Override
    public void close()
    {
        try {
            combinedScanIterable.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ConnectorSplit toIcebergSplit(TupleDomain<IcebergColumnHandle> predicate, FileScanTask task)
    {
        // TODO: We should leverage residual expression and convert that to TupleDomain.
        //       The predicate here is used by readers for predicate push down at reader level,
        //       so when we do not use residual expression, we are just wasting CPU cycles
        //       on reader side evaluating a condition that we know will always be true.

        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                ImmutableList.of(),
                predicate,
                getPartitionKeys(task));
    }

    private static Map<Integer, String> getPartitionKeys(FileScanTask scanTask)
    {
        StructLike partition = scanTask.file().partition();
        PartitionSpec spec = scanTask.spec();
        Map<PartitionField, Integer> fieldToIndex = getIdentityPartitions(spec);
        ImmutableMap.Builder<Integer, String> partitionKeys = ImmutableMap.builder();

        fieldToIndex.forEach((field, index) -> {
            int id = field.sourceId();
            Type type = spec.schema().findType(id);
            Class<?> javaClass = type.typeId().javaClass();
            Object value = partition.get(index, javaClass);

            if (value == null) {
                throw new PrestoException(ICEBERG_INVALID_PARTITION_VALUE, format(
                        "File %s has no partition data for partitioning column %s",
                        scanTask.file().path().toString(),
                        field.name()));
            }

            String partitionValue;
            if (type.typeId() == FIXED || type.typeId() == BINARY) {
                // this is safe because Iceberg PartitionData directly wraps the byte array
                partitionValue = new String(((ByteBuffer) value).array(), UTF_8);
            }
            else {
                partitionValue = value.toString();
            }
            partitionKeys.put(id, partitionValue);
        });

        return partitionKeys.build();
    }
}
