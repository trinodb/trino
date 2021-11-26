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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergFileWriter;
import io.trino.plugin.iceberg.IcebergFileWriterFactory;
import io.trino.plugin.iceberg.MetricsWrapper;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.hive.util.ConfigurationUtils.toJobConf;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.iceberg.io.DeleteSchemaUtil.pathPosSchema;

public class IcebergPositionDeletePageSink
        implements ConnectorPageSink
{
    private static final int MAX_PAGE_POSITIONS = 4096;
    private static final Schema POSITION_DELETE_SCHEMA = pathPosSchema();

    private final Optional<PartitionData> partition;
    private final String outputPath;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final IcebergFileWriter writer;

    private long writtenBytes;
    private long systemMemoryUsage;
    private long validationCpuNanos;
    private boolean writtenData;

    public IcebergPositionDeletePageSink(
            PartitionSpec partitionSpec,
            Optional<PartitionData> partition,
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            IcebergFileFormat fileFormat)
    {
        JobConf jobConf = toJobConf(hdfsEnvironment.getConfiguration(hdfsContext, new Path(locationProvider.newDataLocation("position-delete-file"))));
        String fileName = fileFormat.toIceberg().addExtension("delete/position/" + randomUUID());
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.partition = requireNonNull(partition, "partition is null");
        this.outputPath = partition.map(p -> locationProvider.newDataLocation(partitionSpec, p, fileName)).orElse(locationProvider.newDataLocation(fileName));
        this.writer = fileWriterFactory.createFileWriter(
                new Path(outputPath),
                POSITION_DELETE_SCHEMA,
                jobConf,
                session,
                hdfsContext,
                fileFormat,
                MetricsConfig.fromProperties(ImmutableMap.of()), // TODO: Need a real one?
                FileContent.POSITION_DELETES);
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public long getMemoryUsage()
    {
        return systemMemoryUsage;
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        doAppend(page);
        writtenData = true;
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Collection<Slice> commitTasks = new ArrayList<>();
        if (writtenData) {
            writer.commit();
            CommitTaskData task = new CommitTaskData(
                    outputPath,
                    writer.getWrittenBytes(),
                    new MetricsWrapper(writer.getMetrics()),
                    partition.map(PartitionData::toJson),
                    FileContent.POSITION_DELETES);
            Long recordCount = task.getMetrics().recordCount();
            if (recordCount != null && recordCount > 0) {
                commitTasks.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));
            }
            writtenBytes = writer.getWrittenBytes();
            validationCpuNanos = writer.getValidationCpuNanos();
        }
        else {
            // clean up the empty delete file
            writer.rollback();
        }
        return completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        writer.rollback();
    }

    private void doAppend(Page page)
    {
        while (page.getPositionCount() > MAX_PAGE_POSITIONS) {
            Page chunk = page.getRegion(0, MAX_PAGE_POSITIONS);
            page = page.getRegion(MAX_PAGE_POSITIONS, page.getPositionCount() - MAX_PAGE_POSITIONS);
            writePage(chunk);
        }

        writePage(page);
    }

    private void writePage(Page page)
    {
        long currentWritten = writer.getWrittenBytes();
        long currentMemory = writer.getMemoryUsage();

        writer.appendRows(page);

        writtenBytes += (writer.getWrittenBytes() - currentWritten);
        systemMemoryUsage += (writer.getMemoryUsage() - currentMemory);
    }
}
