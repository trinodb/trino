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
import com.google.common.collect.Streams;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePartitionKey;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterators.limit;
import static io.prestosql.plugin.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static io.prestosql.plugin.iceberg.DomainConverter.convertTupleDomainTypes;
import static io.prestosql.plugin.iceberg.IcebergUtil.getIdentityPartitions;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").withZone(UTC);

    private final CloseableIterable<CombinedScanTask> combinedScanIterable;
    private final TupleDomain<HiveColumnHandle> predicate;
    private final Map<String, Integer> nameToId;
    private final Iterator<FileScanTask> fileScanIterator;

    public IcebergSplitSource(
            CloseableIterable<CombinedScanTask> combinedScanIterable,
            TupleDomain<HiveColumnHandle> predicate,
            Schema schema)
    {
        this.combinedScanIterable = requireNonNull(combinedScanIterable, "combinedScanIterable is null");
        this.predicate = requireNonNull(predicate, "predicate is null");

        this.nameToId = requireNonNull(schema, "schema is null").columns().stream()
                .collect(toImmutableMap(NestedField::name, NestedField::fieldId));

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
        TupleDomain<HiveColumnHandle> predicate = convertTupleDomainTypes(this.predicate);
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

    private ConnectorSplit toIcebergSplit(TupleDomain<HiveColumnHandle> predicate, FileScanTask task)
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
                nameToId,
                predicate,
                getPartitionKeys(task));
    }

    private static List<HivePartitionKey> getPartitionKeys(FileScanTask scanTask)
    {
        StructLike partition = scanTask.file().partition();
        PartitionSpec spec = scanTask.spec();
        Map<PartitionField, Integer> fieldToIndex = getIdentityPartitions(spec);
        List<HivePartitionKey> partitionKeys = new ArrayList<>();

        fieldToIndex.forEach((field, index) -> {
            String name = field.name();
            Type sourceType = spec.schema().findType(field.sourceId());
            Type partitionType = field.transform().getResultType(sourceType);
            Class<?> javaClass = partitionType.typeId().javaClass();
            Object value = partition.get(index, javaClass);
            String partitionValue = HIVE_DEFAULT_DYNAMIC_PARTITION;
            if (value != null) {
                switch (partitionType.typeId()) {
                    case DATE:
                        partitionValue = DATE_FORMATTER.format(LocalDate.ofEpochDay((int) value));
                        break;
                    case TIMESTAMP:
                        partitionValue = TIMESTAMP_FORMATTER.format(toLocalDateTime((long) value));
                        break;
                    case FIXED:
                    case BINARY:
                        partitionValue = new String(((ByteBuffer) value).array(), UTF_8);
                        break;
                    default:
                        partitionValue = value.toString();
                }
            }
            partitionKeys.add(new HivePartitionKey(name, partitionValue));
        });
        return partitionKeys;
    }

    private static LocalDateTime toLocalDateTime(long epochMicro)
    {
        long epochSecond = MICROSECONDS.toSeconds(epochMicro);
        long microOfSecond = epochMicro - SECONDS.toMicros(epochSecond);
        int nanoOfSecond = toIntExact(MICROSECONDS.toNanos(microOfSecond));
        return LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, UTC);
    }
}
