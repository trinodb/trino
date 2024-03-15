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
package io.trino.plugin.hive.functions;

import com.google.common.base.Enums;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveCompressionOption;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.HivePageSink;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.HiveTypeName;
import io.trino.plugin.hive.HiveWriter;
import io.trino.plugin.hive.HiveWriterStats;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.plugin.hive.PartitionUpdate;
import io.trino.plugin.hive.WriterFactory;
import io.trino.plugin.hive.avro.AvroHiveFileUtils;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableArgument;
import io.trino.spi.function.table.TableArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.avro.SchemaParseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.Functions.checkFunctionArgument;
import static io.trino.plugin.hive.HiveCompressionCodecs.selectCompressionCodec;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveMetadata.CSV_SEPARATOR_KEY;
import static io.trino.plugin.hive.HiveMetadata.SKIP_HEADER_COUNT_KEY;
import static io.trino.plugin.hive.HiveMetadata.TEXT_FIELD_SEPARATOR_KEY;
import static io.trino.plugin.hive.HiveMetadata.verifyHiveColumnName;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.util.HiveWriteUtils.directoryExists;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.function.table.Descriptor.descriptor;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInput;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInputAndProduced;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * The target location must be an empty directory.
 * The function throws an exception if the directory doesn't exist or files already exits in the directory.
 * The supported format is the same as {@link io.trino.plugin.hive.HiveStorageFormat}.
 * In the case of a failure, this function does not attempt to delete orphaned data.
 */
public class Unload
        implements Provider<ConnectorTableFunction>
{
    private final LocationAccessControl locationAccessControl;
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public Unload(LocationAccessControl locationAccessControl, TrinoFileSystemFactory fileSystemFactory)
    {
        this.locationAccessControl = requireNonNull(locationAccessControl, "locationAccessControl is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new UnloadFunction(locationAccessControl, fileSystemFactory), getClass().getClassLoader());
    }

    public static class UnloadFunction
            extends AbstractConnectorTableFunction
    {
        private static final String TABLE_ARGUMENT_NAME = "INPUT";
        private static final String LOCATION_ARGUMENT_NAME = "LOCATION";
        private static final String FORMAT_ARGUMENT_NAME = "FORMAT";
        private static final String COMPRESSION_ARGUMENT_NAME = "COMPRESSION";
        private static final String SEPARATOR_ARGUMENT_NAME = "SEPARATOR";
        private static final String HEADER_ARGUMENT_NAME = "HEADER";

        private final LocationAccessControl locationAccessControl;
        private final TrinoFileSystemFactory fileSystemFactory;

        public UnloadFunction(LocationAccessControl locationAccessControl, TrinoFileSystemFactory fileSystemFactory)
        {
            super(
                    "system",
                    "unload",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name(TABLE_ARGUMENT_NAME)
                                    .pruneWhenEmpty()
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(LOCATION_ARGUMENT_NAME)
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(FORMAT_ARGUMENT_NAME)
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(COMPRESSION_ARGUMENT_NAME)
                                    .type(VARCHAR)
                                    .defaultValue(utf8Slice("NONE"))
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(SEPARATOR_ARGUMENT_NAME)
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(HEADER_ARGUMENT_NAME)
                                    .type(BOOLEAN)
                                    .defaultValue(null)
                                    .build()),
                    new ReturnTypeSpecification.DescribedTable(descriptor(ImmutableList.of("path", "count"), ImmutableList.of(VARCHAR, BIGINT))));
            this.locationAccessControl = requireNonNull(locationAccessControl, "locationAccessControl is null");
            this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl,
                RetryMode retryMode)
        {
            if (retryMode != NO_RETRIES) {
                throw new TrinoException(NOT_SUPPORTED, "The function does not support query retries");
            }

            TableArgument tableArgument = (TableArgument) arguments.get(TABLE_ARGUMENT_NAME);

            ScalarArgument locationArgument = (ScalarArgument) arguments.get(LOCATION_ARGUMENT_NAME);
            checkFunctionArgument(locationArgument.getValue() != null, "location cannot be null");
            Location location = Location.of(((Slice) locationArgument.getValue()).toStringUtf8());
            locationAccessControl.checkCanUseLocation(session.getIdentity(), location.toString());

            ScalarArgument formatArgument = (ScalarArgument) arguments.get(FORMAT_ARGUMENT_NAME);
            checkFunctionArgument(formatArgument.getValue() != null, "format cannot be null");
            String formatValue = ((Slice) formatArgument.getValue()).toStringUtf8();
            HiveStorageFormat format = Enums.getIfPresent(HiveStorageFormat.class, formatValue.toUpperCase(ENGLISH)).toJavaUtil()
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, formatValue + " format isn't supported"));
            if (format == HiveStorageFormat.REGEX) {
                throw new TrinoException(NOT_SUPPORTED, "REGEX format is read-only");
            }

            ScalarArgument compressionArgument = (ScalarArgument) arguments.get(COMPRESSION_ARGUMENT_NAME);
            checkFunctionArgument(compressionArgument.getValue() != null, "compression cannot be null"); // the default is NONE
            String compressionValue = ((Slice) compressionArgument.getValue()).toStringUtf8();
            HiveCompressionOption compressionOption = Enums.getIfPresent(HiveCompressionOption.class, compressionValue.toUpperCase(ENGLISH)).toJavaUtil()
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, compressionValue + " compression isn't supported"));
            HiveCompressionCodec compression = selectCompressionCodec(compressionOption, format);

            ScalarArgument separatorArgument = (ScalarArgument) arguments.get(SEPARATOR_ARGUMENT_NAME);
            Slice separator = (Slice) separatorArgument.getValue();

            ScalarArgument headerArgument = (ScalarArgument) arguments.get(HEADER_ARGUMENT_NAME);
            Boolean header = (Boolean) headerArgument.getValue();

            List<RowType.Field> inputSchema = tableArgument.getRowType().getFields();
            List<String> partitionColumns = tableArgument.getPartitionBy();
            List<Integer> requiredColumns = IntStream.range(0, inputSchema.size()).boxed().collect(toImmutableList());

            if (inputSchema.size() == partitionColumns.size()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "INPUT contains only partition columns");
            }

            ImmutableList.Builder<HiveColumnHandle> columnHandles = ImmutableList.builderWithExpectedSize(inputSchema.size());
            ImmutableList.Builder<String> dataColumnNames = ImmutableList.builderWithExpectedSize(inputSchema.size());
            ImmutableList.Builder<HiveType> dataColumnTypes = ImmutableList.builderWithExpectedSize(inputSchema.size());
            ImmutableList.Builder<Type> partitionColumnTypes = ImmutableList.builderWithExpectedSize(partitionColumns.size());
            for (RowType.Field field : inputSchema) {
                String columnName = field.getName().orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Column name must exist")).toLowerCase(ENGLISH);
                verifyHiveColumnName(columnName);
                if (format == HiveStorageFormat.CSV && !field.getType().equals(VARCHAR)) {
                    throw new TrinoException(NOT_SUPPORTED, "CSV only supports VARCHAR columns: '%s'".formatted(columnName));
                }
                if (format == HiveStorageFormat.JSON && field.getType().equals(VARBINARY)) {
                    // Disable VARBINARY type with JSON format as it has a correctness issue for some data, e.g. X'0001020304050607080DF9367AA7000000'
                    throw new TrinoException(NOT_SUPPORTED, "UNLOAD table function does not support VARBINARY columns for JSON format: '%s'".formatted(columnName));
                }
                if ((format == HiveStorageFormat.PARQUET || format == HiveStorageFormat.AVRO || format == HiveStorageFormat.RCBINARY) && field.getType() instanceof TimestampType) {
                    // TODO Fix correctness issue for timestamp type in Parquet, Avro, RCBinary formats
                    throw new TrinoException(NOT_SUPPORTED, "UNLOAD table function does not support timestamp columns for %s format: '%s'".formatted(columnName, format));
                }

                HiveColumnHandle.ColumnType columnType;
                if (partitionColumns.contains(columnName)) {
                    partitionColumnTypes.add(field.getType());
                    columnType = HiveColumnHandle.ColumnType.PARTITION_KEY;
                }
                else {
                    dataColumnNames.add(columnName);
                    dataColumnTypes.add(toHiveType(field.getType()));
                    columnType = HiveColumnHandle.ColumnType.REGULAR;
                }
                columnHandles.add(new HiveColumnHandle(
                        columnName,
                        0,
                        toHiveType(field.getType()),
                        field.getType(),
                        Optional.empty(),
                        columnType,
                        Optional.empty()));
            }

            ImmutableMap.Builder<String, String> schemaBuilder = ImmutableMap.<String, String>builder()
                    .put(LIST_COLUMNS, String.join(",", dataColumnNames.build()))
                    .put(LIST_COLUMN_TYPES, dataColumnTypes.build().stream()
                            .map(HiveType::getHiveTypeName)
                            .map(HiveTypeName::toString)
                            .collect(joining(":")));
            if (separator != null) {
                switch (format) {
                    case TEXTFILE -> schemaBuilder.put(TEXT_FIELD_SEPARATOR_KEY, separator.toStringUtf8());
                    case CSV -> schemaBuilder.put(CSV_SEPARATOR_KEY, separator.toStringUtf8());
                    default -> throw new TrinoException(INVALID_TABLE_PROPERTY, "Cannot specify separator for storage format: " + format);
                }
            }
            if (header != null) {
                switch (format) {
                    case TEXTFILE, CSV -> schemaBuilder.put(SKIP_HEADER_COUNT_KEY, header ? "1" : "0");
                    default -> throw new TrinoException(INVALID_TABLE_PROPERTY, "Cannot specify header for storage format: " + format);
                }
            }
            Map<String, String> schema = schemaBuilder.buildOrThrow();

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            if (format == HiveStorageFormat.AVRO) {
                try {
                    AvroHiveFileUtils.determineSchemaOrThrowException(fileSystem, schema);
                }
                catch (SchemaParseException e) {
                    throw new TrinoException(NOT_SUPPORTED, "AVRO format does not support the definition", e);
                }
                catch (IOException e) {
                    throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + location, e);
                }
            }

            if (!directoryExists(fileSystem, location).orElse(true)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Location does not exist: " + location);
            }
            try {
                if (fileSystem.listFiles(location).hasNext()) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Location must be empty: " + location);
                }
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + location, e);
            }

            UnloadFunctionHandle handle = new UnloadFunctionHandle(
                    columnHandles.build(),
                    dataColumnNames.build(),
                    partitionColumns,
                    partitionColumnTypes.build(),
                    schema,
                    location.toString(),
                    format,
                    compression);

            return TableFunctionAnalysis.builder()
                    .requiredColumns(TABLE_ARGUMENT_NAME, requiredColumns)
                    .handle(handle)
                    .build();
        }
    }

    public record UnloadFunctionHandle(
            List<HiveColumnHandle> columnHandles,
            List<String> dataColumnNames,
            List<String> partitionColumnNames,
            List<Type> partitionColumnTypes,
            Map<String, String> schema,
            String location,
            HiveStorageFormat format,
            HiveCompressionCodec compression)
            implements ConnectorTableFunctionHandle
    {
        public UnloadFunctionHandle
        {
            columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
            dataColumnNames = ImmutableList.copyOf(requireNonNull(dataColumnNames, "dataColumnNames is null"));
            partitionColumnNames = ImmutableList.copyOf(requireNonNull(partitionColumnNames, "partitionColumnNames is null"));
            partitionColumnTypes = ImmutableList.copyOf(requireNonNull(partitionColumnTypes, "partitionColumnTypes is null"));
            checkArgument(partitionColumnNames.size() == partitionColumnTypes.size(), "column names must be equal to column types");
            schema = ImmutableMap.copyOf(requireNonNull(schema, "schema is null"));
            requireNonNull(location, "location is null");
            requireNonNull(format, "format is null");
            requireNonNull(compression, "compression is null");
        }
    }

    public static TableFunctionProcessorProvider getUnloadFunctionProcessorProvider(
            Set<HiveFileWriterFactory> fileWriterFactories,
            HiveWriterStats hiveWriterStats,
            PageIndexerFactory pageIndexerFactory,
            ListeningExecutorService writeVerificationExecutor,
            JsonCodec<PartitionUpdate> partitionUpdateCodec)
    {
        return new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle)
            {
                UnloadFunctionHandle unloadFunctionHandle = (UnloadFunctionHandle) handle;
                Location location = Location.of(unloadFunctionHandle.location());

                ImmutableMap.Builder<String, Long> unloadResults = ImmutableMap.builder();
                Consumer<HiveWriter> onCommit = hiveWriter -> unloadResults.put(hiveWriter.getTargetPath(), hiveWriter.getRowCount());

                final AtomicBoolean finished = new AtomicBoolean();
                WriterFactory writerFactory = new UnloadWriterFactory(
                        session,
                        fileWriterFactories,
                        hiveWriterStats,
                        location,
                        unloadFunctionHandle.format,
                        unloadFunctionHandle.compression,
                        unloadFunctionHandle.dataColumnNames,
                        unloadFunctionHandle.schema,
                        unloadFunctionHandle.partitionColumnNames,
                        unloadFunctionHandle.partitionColumnTypes,
                        onCommit);
                HivePageSink sink = new HivePageSink(
                        false,
                        writerFactory,
                        unloadFunctionHandle.columnHandles,
                        false,
                        Optional.empty(),
                        pageIndexerFactory,
                        1,
                        writeVerificationExecutor,
                        partitionUpdateCodec,
                        session);

                return input -> {
                    if (finished.get()) {
                        return FINISHED;
                    }

                    if (input == null) {
                        sink.finish();
                        finished.set(true);

                        // Return result
                        PageBuilder resultBuilder = new PageBuilder(ImmutableList.of(VARCHAR, BIGINT));
                        for (Map.Entry<String, Long> result : unloadResults.buildOrThrow().entrySet()) {
                            resultBuilder.declarePosition();
                            VARCHAR.writeSlice(resultBuilder.getBlockBuilder(0), utf8Slice(result.getKey()));
                            BIGINT.writeLong(resultBuilder.getBlockBuilder(1), result.getValue());
                        }
                        Page result = resultBuilder.build();
                        resultBuilder.reset();
                        return usedInputAndProduced(result);
                    }

                    Page page = getOnlyElement(input).orElseThrow();
                    sink.appendPage(page);
                    return usedInput();
                };
            }
        };
    }
}
