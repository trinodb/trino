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
package io.trino.plugin.hive.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingOutputStream;
import io.trino.hive.formats.avro.AvroCompressionKind;
import io.trino.hive.formats.avro.AvroFileWriter;
import io.trino.hive.formats.avro.AvroTypeException;
import io.trino.hive.formats.avro.AvroTypeManager;
import io.trino.hive.formats.avro.AvroTypeUtils;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.hive.FileWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static java.util.Objects.requireNonNull;

public class AvroHiveFileWriter
        implements FileWriter
{
    private static final int INSTANCE_SIZE = instanceSize(AvroHiveFileWriter.class);

    private final AvroFileWriter fileWriter;
    private final List<Block> typeCorrectNullBlocks;
    private final CountingOutputStream countingOutputStream;
    private final AggregatedMemoryContext outputStreamMemoryContext;

    private final Closeable rollbackAction;

    public AvroHiveFileWriter(
            OutputStream outputStream,
            AggregatedMemoryContext outputStreamMemoryContext,
            Schema fileSchema,
            AvroTypeManager typeManager,
            Closeable rollbackAction,
            List<String> inputColumnNames,
            List<Type> inputColumnTypes,
            AvroCompressionKind compressionKind,
            Map<String, String> metadata)
            throws IOException, AvroTypeException
    {
        countingOutputStream = new CountingOutputStream(requireNonNull(outputStream, "outputStream is null"));
        this.outputStreamMemoryContext = requireNonNull(outputStreamMemoryContext, "outputStreamMemoryContext is null");
        verify(requireNonNull(fileSchema, "fileSchema is null").getType() == Schema.Type.RECORD, "file schema must be record schema");
        verify(inputColumnNames.size() == inputColumnTypes.size(), "column names must be equal to column types");
        // file writer will reorder input columns to schema, we just need to impute nulls for schema fields without input columns
        ImmutableList.Builder<String> outputColumnNames = ImmutableList.<String>builder().addAll(inputColumnNames);
        ImmutableList.Builder<Type> outputColumnTypes = ImmutableList.<Type>builder().addAll(inputColumnTypes);
        Map<String, Schema> fields = fileSchema.getFields().stream().collect(Collectors.toMap(Schema.Field::name, Schema.Field::schema));
        for (String inputColumnName : inputColumnNames) {
            Schema fieldSchema = fields.remove(inputColumnName);
            if (fieldSchema == null) {
                throw new AvroTypeException("File schema doesn't have input field " + inputColumnName);
            }
        }
        ImmutableList.Builder<Block> blocks = ImmutableList.builder();
        for (Map.Entry<String, Schema> entry : fields.entrySet()) {
            outputColumnNames.add(entry.getKey());
            Type type = AvroTypeUtils.typeFromAvro(entry.getValue(), typeManager);
            outputColumnTypes.add(type);
            blocks.add(type.createBlockBuilder(null, 1).appendNull().build());
        }
        typeCorrectNullBlocks = blocks.build();
        fileWriter = new AvroFileWriter(countingOutputStream, fileSchema, typeManager, compressionKind, metadata, outputColumnNames.build(), outputColumnTypes.build());
        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");
    }

    @Override
    public long getWrittenBytes()
    {
        return countingOutputStream.getCount();
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE + fileWriter.getRetainedSize() + outputStreamMemoryContext.getBytes();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        try {
            Block[] blocks = new Block[dataPage.getChannelCount() + typeCorrectNullBlocks.size()];
            for (int i = 0; i < dataPage.getChannelCount(); i++) {
                blocks[i] = dataPage.getBlock(i);
            }
            for (int i = 0; i < typeCorrectNullBlocks.size(); i++) {
                blocks[i + dataPage.getChannelCount()] = RunLengthEncodedBlock.create(typeCorrectNullBlocks.get(i), dataPage.getPositionCount());
            }
            fileWriter.write(new Page(blocks));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_WRITER_DATA_ERROR, "Failed to write data page to Avro file", e);
        }
    }

    @Override
    public Closeable commit()
    {
        try {
            fileWriter.close();
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Failed to close AvroFileWriter", e);
        }
        return rollbackAction;
    }

    @Override
    public void rollback()
    {
        try (rollbackAction) {
            fileWriter.close();
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }
}
