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
package io.trino.plugin.hive.line;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineSerializer;
import io.trino.hive.formats.line.LineSerializerFactory;
import io.trino.hive.formats.line.LineWriter;
import io.trino.hive.formats.line.LineWriterFactory;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.fromProperties;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.trino.plugin.hive.HiveMetadata.SKIP_HEADER_COUNT_KEY;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.util.HiveUtil.getColumnNames;
import static io.trino.plugin.hive.util.HiveUtil.getColumnTypes;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public abstract class LineFileWriterFactory
        implements HiveFileWriterFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final Predicate<ConnectorSession> activation;
    private final LineSerializerFactory lineSerializerFactory;
    private final LineWriterFactory lineWriterFactory;
    private final boolean headerSupported;

    protected LineFileWriterFactory(
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            LineSerializerFactory lineSerializerFactory,
            LineWriterFactory lineWriterFactory,
            Predicate<ConnectorSession> activation,
            boolean headerSupported)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.activation = requireNonNull(activation, "activation is null");
        this.lineSerializerFactory = requireNonNull(lineSerializerFactory, "lineSerializerFactory is null");
        this.lineWriterFactory = requireNonNull(lineWriterFactory, "lineWriterFactory is null");
        this.headerSupported = headerSupported;
    }

    @Override
    public Optional<FileWriter> createFileWriter(
            Location location,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            HiveCompressionCodec compressionCodec,
            Properties schema,
            ConnectorSession session,
            OptionalInt bucketNumber,
            AcidTransaction transaction,
            boolean useAcidSchema,
            WriterKind writerKind)
    {
        if (!lineWriterFactory.getHiveOutputFormatClassName().equals(storageFormat.getOutputFormat()) ||
                !lineSerializerFactory.getHiveSerDeClassNames().contains(storageFormat.getSerde()) ||
                !activation.test(session)) {
            return Optional.empty();
        }

        // existing tables and partitions may have columns in a different order than the writer is providing, so build
        // an index to rearrange columns in the proper order
        List<String> fileColumnNames = getColumnNames(schema);
        List<Type> fileColumnTypes = getColumnTypes(schema).stream()
                .map(hiveType -> hiveType.getType(typeManager, getTimestampPrecision(session)))
                .toList();

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();

        List<Column> columns = IntStream.range(0, fileColumnNames.size())
                .mapToObj(ordinal -> new Column(fileColumnNames.get(ordinal), fileColumnTypes.get(ordinal), ordinal))
                .toList();

        LineSerializer lineSerializer = lineSerializerFactory.create(columns, fromProperties(schema));

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity());
            AggregatedMemoryContext outputStreamMemoryContext = newSimpleAggregatedMemoryContext();
            OutputStream outputStream = fileSystem.newOutputFile(location).create(outputStreamMemoryContext);

            LineWriter lineWriter = lineWriterFactory.createLineWriter(session, outputStream, compressionCodec.getHiveCompressionKind());

            Optional<Slice> header = getFileHeader(schema, columns);
            if (header.isPresent()) {
                lineWriter.write(header.get());
            }
            return Optional.of(new LineFileWriter(
                    lineWriter,
                    lineSerializer,
                    () -> fileSystem.deleteFile(location),
                    fileInputColumnIndexes));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(HIVE_WRITER_OPEN_ERROR, "Error creating file", e);
        }
    }

    private Optional<Slice> getFileHeader(Properties schema, List<Column> columns)
            throws IOException
    {
        String skipHeaderCount = schema.getProperty(SKIP_HEADER_COUNT_KEY, "0");
        if (skipHeaderCount.equals("0")) {
            return Optional.empty();
        }
        if (!skipHeaderCount.equals("1") && !headerSupported) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "%s=%s not supported".formatted(SKIP_HEADER_COUNT_KEY, skipHeaderCount));
        }

        // header line is rendered using the line serializer with all VARCHAR columns
        LineSerializer headerSerializer = lineSerializerFactory.create(
                columns.stream()
                        .map(column -> new Column(column.name(), VARCHAR, column.ordinal()))
                        .collect(toImmutableList()),
                fromProperties(schema));

        PageBuilder pageBuilder = new PageBuilder(headerSerializer.getTypes());
        pageBuilder.declarePosition();
        for (int channel = 0; channel < columns.size(); channel++) {
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(channel), Slices.utf8Slice(columns.get(channel).name()));
        }
        Page page = pageBuilder.build();

        DynamicSliceOutput output = new DynamicSliceOutput(1024);
        headerSerializer.write(page, 0, output);
        return Optional.of(output.slice());
    }
}
