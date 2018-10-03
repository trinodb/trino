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

import com.google.common.primitives.Ints;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.Metrics;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.hadoop.HadoopOutputFile;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.parquet.Parquet;
import com.netflix.iceberg.parquet.TypeToMessageType;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.iceberg.parquet.writer.PrestoWriteSupport;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.DateTimeEncoding;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static io.prestosql.iceberg.MetricsParser.toJson;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.DATE;
import static io.prestosql.spi.type.StandardTypes.DECIMAL;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.REAL;
import static io.prestosql.spi.type.StandardTypes.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP;
import static io.prestosql.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.StandardTypes.TINYINT;
import static io.prestosql.spi.type.StandardTypes.VARBINARY;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class IcebergPageSink
        implements ConnectorPageSink
{
    private Schema outputSchema;
    private PartitionSpec partitionSpec;
    private String outputDir;
    private Configuration configuration;
    private List<HiveColumnHandle> inputColumns;
    private List<HiveColumnHandle> partitionColumns;
    private Map<String, PartitionWriteContext> partitionToWriterContext;
    private Map<String, String> partitionToFile;
    private Map<String, PartitionData> partitionToPartitionData;
    private JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final TypeManager typeManager;
    private final FileFormat fileFormat;
    private final List<Type> partitionTypes;
    private final List<Boolean> isNullable;
    private static final TypeToMessageType typeToMessageType = new TypeToMessageType();

    public IcebergPageSink(Schema outputSchema,
            PartitionSpec partitionSpec,
            String outputDir,
            Configuration configuration,
            List<HiveColumnHandle> inputColumns,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session, FileFormat fileFormat)
    {
        this.outputSchema = outputSchema;
        this.partitionSpec = partitionSpec;
        this.outputDir = outputDir;
        this.configuration = configuration;
        // TODO we only mark identity columns as partition columns as of now but we need to extract the schema on the coordinator side and provide partition
        // transforms in ConnectorOutputTableHandle
        this.partitionColumns = inputColumns.stream().filter(col -> col.isPartitionKey()).collect(toList());
        this.jsonCodec = jsonCodec;
        this.session = session;
        this.typeManager = typeManager;
        this.fileFormat = fileFormat;
        this.partitionToWriterContext = new ConcurrentHashMap<>();
        this.partitionToFile = new ConcurrentHashMap<>();
        this.partitionToPartitionData = new ConcurrentHashMap<>();
        this.partitionTypes = partitionColumns.stream().map(col -> typeManager.getType(col.getTypeSignature())).collect(toList());
        this.inputColumns = inputColumns;
        this.isNullable = outputSchema.columns().stream().map(column -> column.isOptional()).collect(toList());
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        int numRows = page.getPositionCount();

        for (int rowNum = 0; rowNum < numRows; rowNum++) {
            PartitionData partitionData = getPartitionData(session, page, rowNum);
            String partitionPath = partitionSpec.partitionToPath(partitionData);

            if (!partitionToWriterContext.containsKey(partitionPath)) {
                partitionToWriterContext.put(partitionPath, new PartitionWriteContext(new ArrayList<>(), addWriter(partitionPath)));
                partitionToPartitionData.put(partitionPath, partitionData);
            }
            partitionToWriterContext.get(partitionPath).getRowNum().add(rowNum);
        }

        for (Map.Entry<String, PartitionWriteContext> partitionToWriter : partitionToWriterContext.entrySet()) {
            List<Integer> rowNums = partitionToWriter.getValue().getRowNum();
            FileAppender<Page> writer = partitionToWriter.getValue().getWriter();
            Page partition = page.getPositions(Ints.toArray(rowNums), 0, rowNums.size());
            writer.add(partition);
        }

        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Collection<Slice> commitTasks = new ArrayList<>();
        for (String partition : partitionToFile.keySet()) {
            String file = partitionToFile.get(partition);
            FileAppender<Page> fileAppender = partitionToWriterContext.get(partition).getWriter();
            try {
                fileAppender.close();
            }
            catch (IOException e) {
                abort();
                throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Failed to close" + file);
            }
            Metrics metrics = fileAppender.metrics();
            String partitionDataJson = partitionToPartitionData.containsKey(partition) ? partitionToPartitionData.get(partition).toJson() : null;
            commitTasks.add(Slices.wrappedBuffer(jsonCodec.toJsonBytes(new CommitTaskData(file, toJson(metrics), partition, partitionDataJson))));
        }

        return CompletableFuture.completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        partitionToFile.values().stream().forEach(s -> {
            try {
                new Path(s).getFileSystem(configuration).delete(new Path(s), false);
            }
            catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        });
    }

    private FileAppender<Page> addWriter(String partitionPath)
    {
        Path dataDir = new Path(outputDir);
        String pathUUId = randomUUID().toString(); // TODO add more context here instead of just random UUID, ip of the host, taskId
        Path outputPath = (partitionPath != null && !partitionPath.isEmpty()) ? new Path(new Path(dataDir, partitionPath), pathUUId) : new Path(dataDir, pathUUId);
        String outputFilePath = fileFormat.addExtension(outputPath.toString());
        OutputFile outputFile = HadoopOutputFile.fromPath(new Path(outputFilePath), configuration);
        switch (fileFormat) {
            case PARQUET:
                try {
                    FileAppender<Page> writer = Parquet.write(outputFile)
                            .schema(outputSchema)
                            .writeSupport(new PrestoWriteSupport(inputColumns, typeToMessageType.convert(outputSchema, "presto_schema"), outputSchema, typeManager, session, isNullable))
                            .build();
                    partitionToFile.put(partitionPath, outputFile.location());
                    return writer;
                }
                catch (IOException e) {
                    throw new RuntimeIOException("Could not create writer ", e);
                }
            case ORC:
            case AVRO:
            default:
                throw new UnsupportedOperationException("Only parquet is supported for iceberg as of now");
        }
    }

    private class PartitionWriteContext
    {
        private final List<Integer> rowNum;
        private final FileAppender<Page> writer;

        private PartitionWriteContext(List<Integer> rowNum, FileAppender<Page> writer)
        {
            this.rowNum = rowNum;
            this.writer = writer;
        }

        public List<Integer> getRowNum()
        {
            return rowNum;
        }

        public FileAppender<Page> getWriter()
        {
            return writer;
        }
    }

    private final PartitionData getPartitionData(ConnectorSession session, Page page, int rowNum)
    {
        // TODO only handles identity columns right now, handle all transforms
        Object[] values = new Object[partitionColumns.size()];
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle columnHandle = partitionColumns.get(i);
            Type type = partitionTypes.get(i);
            values[i] = getValue(session, page.getBlock(columnHandle.getHiveColumnIndex()), rowNum, type);
        }

        return new PartitionData(values);
    }

    public static final Object getValue(ConnectorSession session, Block block, int rownum, Type type)
    {
        if (block.isNull(rownum)) {
            return null;
        }
        switch (type.getTypeSignature().getBase()) {
            case BIGINT:
                return type.getLong(block, rownum);
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return toIntExact(type.getLong(block, rownum));
            case BOOLEAN:
                return type.getBoolean(block, rownum);
            case DATE:
                return ((SqlDate) type.getObjectValue(session, block, rownum)).getDays();
            case DECIMAL:
                return ((SqlDecimal) type.getObjectValue(session, block, rownum)).toBigDecimal();
            case REAL:
                return intBitsToFloat((int) type.getLong(block, rownum));
            case DOUBLE:
                return type.getDouble(block, rownum);
            case TIMESTAMP:
                return MILLISECONDS.toMicros(type.getLong(block, rownum));
            case TIMESTAMP_WITH_TIME_ZONE:
                return MILLISECONDS.toMicros(DateTimeEncoding.unpackMillisUtc(type.getLong(block, rownum)));
            case VARBINARY:
                return ((SqlVarbinary) type.getObjectValue(session, block, rownum)).getBytes();
            case VARCHAR:
                return type.getObjectValue(session, block, rownum);
            default:
                throw new UnsupportedOperationException(type + " is not supported as partition column");
        }
    }
}
