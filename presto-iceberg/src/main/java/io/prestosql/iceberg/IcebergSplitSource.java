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
package io.prestosql.iceberg;

import com.google.common.collect.Lists;
import com.netflix.iceberg.CombinedScanTask;
import com.netflix.iceberg.FileScanTask;
import com.netflix.iceberg.PartitionField;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types.NestedField;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePartitionKey;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.prestosql.iceberg.IcebergUtil.getIdentityPartitions;
import static io.prestosql.plugin.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static java.nio.charset.StandardCharsets.UTF_8;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    public static final String LOCALHOST = "localhost";

    private final String database;
    private final String tableName;
    private final Iterator<CombinedScanTask> scanTaskIterator;
    private final TupleDomain<HiveColumnHandle> predicates;
    private final ConnectorSession session;
    private final Schema tableSchema;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeTranslator typeTranslator;
    private final TypeManager typeRegistry;
    private final HdfsEnvironment.HdfsContext hdfsContext;
    private boolean closed;
    private Map<String, HiveColumnHandle> columnNameToHiveColumnHandleMap;
    private final Long snapshotId;
    private final Long snapshotTimestamp;

    public IcebergSplitSource(String database,
            String tableName,
            Iterator<CombinedScanTask> fileScanTaskIterator,
            TupleDomain<HiveColumnHandle> predicates,
            ConnectorSession session,
            Schema schema,
            HdfsEnvironment hdfsEnvironment,
            TypeTranslator typeTranslator,
            TypeManager typeRegistry,
            Map<String, HiveColumnHandle> columnNameToHiveColumnHandleMap,
            Long snapshotId,
            Long snapshotTimestamp)
    {
        this.database = database;
        this.tableName = tableName;
        this.scanTaskIterator = fileScanTaskIterator;
        this.predicates = predicates;
        this.session = session;
        this.tableSchema = schema;
        this.hdfsEnvironment = hdfsEnvironment;
        this.snapshotId = snapshotId;
        this.snapshotTimestamp = snapshotTimestamp;
        this.hdfsContext = new HdfsEnvironment.HdfsContext(session, database, tableName);
        this.typeTranslator = typeTranslator;
        this.typeRegistry = typeRegistry;
        this.columnNameToHiveColumnHandleMap = columnNameToHiveColumnHandleMap;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        List<ConnectorSplit> splits = new ArrayList<>();
        final TupleDomain<HiveColumnHandle> predicates = DomainConverter.handleTypeDifference(this.predicates);
        while (scanTaskIterator.hasNext() && maxSize != 0) {
            CombinedScanTask combinedScanTask = scanTaskIterator.next();
            for (FileScanTask scanTask : combinedScanTask.files()) {
                List<HivePartitionKey> partitionKeys = getPartitionKeys(scanTask);
                List<HostAddress> addresses = getHostAddresses(scanTask.file().path().toString(), scanTask.start(), scanTask.length());
                splits.add(new IcebergSplit(this.database,
                        this.tableName,
                        scanTask.file().path().toString(),
                        scanTask.start(),
                        scanTask.length(),
                        addresses,
                        this.tableSchema.columns().stream().collect(Collectors.toMap(NestedField::name, NestedField::fieldId)),
                        // TODO: We should leverage residual expression and convert that to TupleDomain. The predicate here is used by
                        // readers for predicate push down at reader level so when we do not use residual expression we are just
                        // wasting CPU cycles on reader side evaluating condition that we know will always be true.
                        predicates,
                        partitionKeys,
                        HiveSessionProperties.isForceLocalScheduling(this.session),
                        snapshotId,
                        snapshotTimestamp));

                maxSize--;
            }
        }
        if (!scanTaskIterator.hasNext()) {
            this.closed = true;
        }
        return CompletableFuture.completedFuture(new ConnectorSplitBatch(splits, !scanTaskIterator.hasNext()));
    }

    private List<HivePartitionKey> getPartitionKeys(FileScanTask scanTask)
    {
        final StructLike partition = scanTask.file().partition();
        final PartitionSpec spec = scanTask.spec();
        final Map<PartitionField, Integer> fieldToIndex = getIdentityPartitions(spec);
        List<HivePartitionKey> partitionKeys = new ArrayList<>();

        for (Map.Entry<PartitionField, Integer> entry : fieldToIndex.entrySet()) {
            PartitionField field = entry.getKey();
            Integer index = entry.getValue();
            String name = field.name();
            Type sourceType = spec.schema().findType(field.sourceId());
            Type partitionType = field.transform().getResultType(sourceType);
            Class<?> javaClass = partitionType.typeId().javaClass();
            Object value = partition.get(index, javaClass);
            String partitionValue = HIVE_DEFAULT_DYNAMIC_PARTITION;
            if (value != null) {
                switch (partitionType.typeId()) {
                    case DATE:
                        Instant instant = Instant.EPOCH.plus((Integer) value, ChronoUnit.DAYS);
                        final ZonedDateTime date = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
                        final DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                        partitionValue = df.format(date);
                        break;
                    case TIMESTAMP:
                        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
                        final long millis = TimeUnit.MICROSECONDS.toMillis((Long) value);
                        partitionValue = dateTimeFormatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC")));
                        break;
                    case FIXED:
                    case BINARY:
                        byte[] bytes = ((ByteBuffer) value).array();
                        partitionValue = new String(bytes, 0, bytes.length, UTF_8);
                        break;
                    default:
                        partitionValue = value.toString();
                }
            }
            partitionKeys.add(new HivePartitionKey(name, partitionValue));
        }
        return partitionKeys;
    }

    private List<HostAddress> getHostAddresses(String path, long start, long length)
    {
        /* The code is commented because hdfsEnvironment.getFileSystem(hdfsContext, hadoopPath) returns an instance of PrestoFileSystemCache$FileSystemWrapper
        which does not delegate the call getFileBlockLocations(string, long, long) to the underlying PrestoS3FileSystem. This results in it invoking the default
        implementation which ends up calling getFileStatus() method. The getFileStatus() call is delegated to underlying PrestoS3FileSystem which results in a
        s3 call slowing down the planning process significantly.
        try {
            final Path hadoopPath = new Path(path);
            final BlockLocation[] blocks = hdfsEnvironment.getFileSystem(hdfsContext, hadoopPath).getFileBlockLocations(hadoopPath, start, length);
            return Arrays.stream(blocks[0].getHosts()).map(h -> HostAddress.fromString(h)).collect(Collectors.toList());
        }
        catch (IOException e) {
            // ignore the exception as it only means localization will not happen.
            return Lists.newArrayList(HostAddress.fromString(LOCALHOST + ":-1"));
        }*/
        return Lists.newArrayList(HostAddress.fromString(LOCALHOST + ":65535"));
    }

    @Override
    public void close()
    {
        this.closed = true;
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }
}
