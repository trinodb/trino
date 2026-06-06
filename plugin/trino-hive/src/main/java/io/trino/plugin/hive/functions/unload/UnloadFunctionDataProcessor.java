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
package io.trino.plugin.hive.functions.unload;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.metastore.HiveType;
import io.trino.metastore.HiveTypeName;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;
import jakarta.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveCompressionCodecs.selectCompressionCodec;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInput;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class UnloadFunctionDataProcessor
        implements TableFunctionDataProcessor
{
    private final ConnectorSession session;
    private final Set<HiveFileWriterFactory> fileWriterFactories;
    private final UnloadFunctionHandle handle;

    private FileWriter writer;
    private Location filePath;
    private long rowsWritten;
    private boolean finished;

    public UnloadFunctionDataProcessor(
            ConnectorSession session,
            Set<HiveFileWriterFactory> fileWriterFactories,
            UnloadFunctionHandle handle)
    {
        this.session = requireNonNull(session, "session is null");
        this.fileWriterFactories = requireNonNull(fileWriterFactories, "fileWriterFactories is null");
        this.handle = requireNonNull(handle, "handle is null");
    }

    @Override
    public TableFunctionProcessorState process(@Nullable List<Optional<Page>> input)
    {
        if (finished) {
            return FINISHED;
        }

        if (input == null) {
            // All input processed - finish writing and return result
            return finishWriting();
        }

        Optional<Page> page = input.get(0);
        if (page.isEmpty()) {
            // Input exhausted
            return finishWriting();
        }

        Page inputPage = page.get();
        if (inputPage.getPositionCount() == 0) {
            return usedInput();
        }

        if (writer == null) {
            createWriter();
        }

        writer.appendRows(inputPage);
        rowsWritten += inputPage.getPositionCount();

        return usedInput();
    }

    private void createWriter()
    {
        HiveStorageFormat storageFormat = handle.storageFormat();
        String fileName = UUID.randomUUID() + getFileExtension(storageFormat);
        filePath = Location.of(handle.location()).appendPath(fileName);

        Map<String, String> schema = buildSchemaMap();
        HiveCompressionCodec compressionCodec = selectCompressionCodec(session, storageFormat);

        for (HiveFileWriterFactory factory : fileWriterFactories) {
            Optional<FileWriter> optionalWriter = factory.createFileWriter(
                    filePath,
                    handle.columnNames(),
                    storageFormat.toStorageFormat(),
                    compressionCodec,
                    schema,
                    session,
                    OptionalInt.empty(),
                    AcidTransaction.NO_ACID_TRANSACTION,
                    false,
                    WriterKind.INSERT);
            if (optionalWriter.isPresent()) {
                this.writer = optionalWriter.get();
                return;
            }
        }

        throw new TrinoException(
                HIVE_UNSUPPORTED_FORMAT,
                "No writer available for format: " + storageFormat);
    }

    private Map<String, String> buildSchemaMap()
    {
        String columnNames = String.join(",", handle.columnNames());
        String columnTypes = handle.columnTypes().stream()
                .map(type -> toHiveType(type))
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":"));

        return ImmutableMap.of(
                LIST_COLUMNS, columnNames,
                LIST_COLUMN_TYPES, columnTypes);
    }

    private TableFunctionProcessorState finishWriting()
    {
        finished = true;

        if (writer == null) {
            // No data was written - return an empty result page
            return produced(createResultPage("", 0, 0));
        }

        long bytesWritten = writer.getWrittenBytes();
        Closeable rollback = writer.commit();

        try {
            rollback.close();
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Failed to close writer rollback", e);
        }

        return produced(createResultPage(filePath.toString(), rowsWritten, bytesWritten));
    }

    private static Page createResultPage(String path, long rowsWritten, long bytesWritten)
    {
        BlockBuilder pathBuilder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeSlice(pathBuilder, utf8Slice(path));

        BlockBuilder rowsBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(rowsBuilder, rowsWritten);

        BlockBuilder bytesBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(bytesBuilder, bytesWritten);

        return new Page(1, pathBuilder.build(), rowsBuilder.build(), bytesBuilder.build());
    }

    private static String getFileExtension(HiveStorageFormat format)
    {
        return switch (format) {
            case ORC -> ".orc";
            case PARQUET -> ".parquet";
            case AVRO -> ".avro";
            case CSV -> ".csv";
            case JSON, OPENX_JSON -> ".json";
            case TEXTFILE -> ".txt";
            case SEQUENCEFILE, SEQUENCEFILE_PROTOBUF, RCBINARY, RCTEXT, REGEX, ESRI -> "";
        };
    }
}
