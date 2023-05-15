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
package io.trino.plugin.hive.parquet;

import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.joda.time.DateTimeZone;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.Supplier;

import static io.trino.parquet.writer.ParquetSchemaConverter.HIVE_PARQUET_USE_INT96_TIMESTAMP_ENCODING;
import static io.trino.parquet.writer.ParquetSchemaConverter.HIVE_PARQUET_USE_LEGACY_DECIMAL_ENCODING;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveSessionProperties.isParquetOptimizedReaderEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isParquetOptimizedWriterValidate;
import static io.trino.plugin.hive.util.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveUtil.getColumnNames;
import static io.trino.plugin.hive.util.HiveUtil.getColumnTypes;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ParquetFileWriterFactory
        implements HiveFileWriterFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final NodeVersion nodeVersion;
    private final TypeManager typeManager;
    private final DateTimeZone parquetTimeZone;
    private final FileFormatDataSourceStats readStats;

    @Inject
    public ParquetFileWriterFactory(
            TrinoFileSystemFactory fileSystemFactory,
            NodeVersion nodeVersion,
            TypeManager typeManager,
            HiveConfig hiveConfig,
            FileFormatDataSourceStats readStats)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.parquetTimeZone = hiveConfig.getParquetDateTimeZone();
        this.readStats = requireNonNull(readStats, "readStats is null");
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
        if (!HiveSessionProperties.isParquetOptimizedWriterEnabled(session)) {
            return Optional.empty();
        }

        if (!MAPRED_PARQUET_OUTPUT_FORMAT_CLASS.equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxPageSize(HiveSessionProperties.getParquetWriterPageSize(session))
                .setMaxBlockSize(HiveSessionProperties.getParquetWriterBlockSize(session))
                .setBatchSize(HiveSessionProperties.getParquetBatchSize(session))
                .build();

        List<String> fileColumnNames = getColumnNames(schema);
        List<Type> fileColumnTypes = getColumnTypes(schema).stream()
                .map(hiveType -> hiveType.getType(typeManager, getTimestampPrecision(session)))
                .collect(toList());

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);

            Closeable rollbackAction = () -> fileSystem.deleteFile(location);

            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                    fileColumnTypes,
                    fileColumnNames,
                    HIVE_PARQUET_USE_LEGACY_DECIMAL_ENCODING,
                    HIVE_PARQUET_USE_INT96_TIMESTAMP_ENCODING);

            Optional<Supplier<ParquetDataSource>> validationInputFactory = Optional.empty();
            if (isParquetOptimizedWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> {
                    try {
                        TrinoInputFile inputFile = fileSystem.newInputFile(location);
                        return new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), readStats);
                    }
                    catch (IOException e) {
                        throw new TrinoException(HIVE_WRITE_VALIDATION_FAILED, e);
                    }
                });
            }

            return Optional.of(new ParquetFileWriter(
                    fileSystem.newOutputFile(location),
                    rollbackAction,
                    fileColumnTypes,
                    fileColumnNames,
                    schemaConverter.getMessageType(),
                    schemaConverter.getPrimitiveTypes(),
                    parquetWriterOptions,
                    fileInputColumnIndexes,
                    compressionCodec.getParquetCompressionCodec(),
                    nodeVersion.toString(),
                    isParquetOptimizedReaderEnabled(session),
                    Optional.of(parquetTimeZone),
                    validationInputFactory));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_WRITER_OPEN_ERROR, "Error creating Parquet file", e);
        }
    }

    @Managed
    @Flatten
    public FileFormatDataSourceStats getReadStats()
    {
        return readStats;
    }
}
