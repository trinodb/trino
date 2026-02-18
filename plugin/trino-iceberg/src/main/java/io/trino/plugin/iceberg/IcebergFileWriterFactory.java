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
package io.trino.plugin.iceberg;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.orc.OrcDataSink;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.OutputStreamOrcDataSink;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveCompressionOption;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.iceberg.fileio.EncryptedTrinoInputFile;
import io.trino.plugin.iceberg.fileio.EncryptedTrinoOutputFile;
import io.trino.plugin.iceberg.fileio.ForwardingInputFile;
import io.trino.plugin.iceberg.fileio.ForwardingOutputFile;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.weakref.jmx.Managed;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveCompressionCodecs.toCompressionCodec;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.TRINO_VERSION_NAME;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_OPEN_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITE_VALIDATION_FAILED;
import static io.trino.plugin.iceberg.IcebergFileFormat.AVRO;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcStringStatisticsLimit;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxDictionaryMemory;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxRowGroupRows;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxStripeRows;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxStripeSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcWriterMinStripeSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcWriterValidateMode;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getParquetWriterBatchSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getParquetWriterBlockSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getParquetWriterPageSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getParquetWriterPageValueCount;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isOrcWriterValidate;
import static io.trino.plugin.iceberg.IcebergTableProperties.ORC_BLOOM_FILTER_FPP_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.getHiveCompressionCodec;
import static io.trino.plugin.iceberg.IcebergUtil.getOrcBloomFilterColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getOrcBloomFilterFpp;
import static io.trino.plugin.iceberg.IcebergUtil.getParquetBloomFilterColumns;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.util.OrcTypeConverter.toOrcType;
import static io.trino.plugin.iceberg.util.PrimitiveTypeMapBuilder.makeTypeMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE;
import static org.apache.iceberg.io.DeleteSchemaUtil.pathPosSchema;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.convert;

public class IcebergFileWriterFactory
{
    private static final Schema POSITION_DELETE_SCHEMA = pathPosSchema();
    private static final MetricsConfig FULL_METRICS_CONFIG = MetricsConfig.fromProperties(ImmutableMap.of(DEFAULT_WRITE_METRICS_MODE, "full"));
    private static final Splitter COLUMN_NAMES_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final FileFormatDataSourceStats readStats;
    private final OrcWriterStats orcWriterStats = new OrcWriterStats();
    private final HiveCompressionOption hiveCompressionOption;
    private final OrcWriterOptions orcWriterOptions;

    @Inject
    public IcebergFileWriterFactory(
            TypeManager typeManager,
            NodeVersion nodeVersion,
            FileFormatDataSourceStats readStats,
            IcebergConfig icebergConfig,
            OrcWriterConfig orcWriterConfig)
    {
        checkArgument(!orcWriterConfig.isUseLegacyVersion(), "the ORC writer shouldn't be configured to use a legacy version");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.readStats = requireNonNull(readStats, "readStats is null");
        this.hiveCompressionOption = icebergConfig.getCompressionCodec();
        this.orcWriterOptions = orcWriterConfig.toOrcWriterOptions();
    }

    @Managed
    public OrcWriterStats getOrcWriterStats()
    {
        return orcWriterStats;
    }

    public IcebergFileWriter createDataFileWriter(
            TrinoFileSystem fileSystem,
            Location outputPath,
            Schema icebergSchema,
            ConnectorSession session,
            IcebergFileFormat fileFormat,
            MetricsConfig metricsConfig,
            Map<String, String> storageProperties,
            Optional<EncryptionManager> encryptionManager)
    {
        return switch (fileFormat) {
            // TODO use metricsConfig https://github.com/trinodb/trino/issues/9791
            case PARQUET -> createParquetWriter(MetricsConfig.getDefault(), fileSystem, outputPath, icebergSchema, session, storageProperties, encryptionManager);
            case ORC -> createOrcWriter(metricsConfig, fileSystem, outputPath, icebergSchema, session, storageProperties, getOrcStringStatisticsLimit(session), encryptionManager);
            case AVRO -> createAvroWriter(fileSystem, outputPath, icebergSchema, storageProperties, encryptionManager);
        };
    }

    public IcebergFileWriter createPositionDeleteWriter(
            TrinoFileSystem fileSystem,
            Location outputPath,
            ConnectorSession session,
            IcebergFileFormat fileFormat,
            Map<String, String> storageProperties,
            Optional<EncryptionManager> encryptionManager)
    {
        return switch (fileFormat) {
            case PARQUET -> createParquetWriter(FULL_METRICS_CONFIG, fileSystem, outputPath, POSITION_DELETE_SCHEMA, session, storageProperties, encryptionManager);
            case ORC -> createOrcWriter(FULL_METRICS_CONFIG, fileSystem, outputPath, POSITION_DELETE_SCHEMA, session, storageProperties, DataSize.ofBytes(Integer.MAX_VALUE), encryptionManager);
            case AVRO -> createAvroWriter(fileSystem, outputPath, POSITION_DELETE_SCHEMA, storageProperties, encryptionManager);
        };
    }

    private IcebergFileWriter createParquetWriter(
            MetricsConfig metricsConfig,
            TrinoFileSystem fileSystem,
            Location outputPath,
            Schema icebergSchema,
            ConnectorSession session,
            Map<String, String> storageProperties,
            Optional<EncryptionManager> encryptionManager)
    {
        List<String> fileColumnNames = icebergSchema.columns().stream()
                .map(Types.NestedField::name)
                .collect(toImmutableList());
        List<Type> fileColumnTypes = icebergSchema.columns().stream()
                .map(column -> toTrinoType(column.type(), typeManager))
                .collect(toImmutableList());

        try {
            EncryptedOutput encryptedOutput = createOutputFile(fileSystem, outputPath, encryptionManager);
            TrinoOutputFile outputFile = encryptedOutput.trinoOutputFile();

            Closeable rollbackAction = () -> fileSystem.deleteFile(outputPath);

            ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                    .setMaxPageSize(getParquetWriterPageSize(session))
                    .setMaxPageValueCount(getParquetWriterPageValueCount(session))
                    .setMaxBlockSize(getParquetWriterBlockSize(session))
                    .setBatchSize(getParquetWriterBatchSize(session))
                    .setBloomFilterColumns(getParquetBloomFilterColumns(storageProperties))
                    .build();

            HiveCompressionCodec compressionCodec = getHiveCompressionCodec(PARQUET, storageProperties)
                    .orElse(toCompressionCodec(hiveCompressionOption));

            IcebergFileWriter writer = new IcebergParquetFileWriter(
                    metricsConfig,
                    outputFile,
                    rollbackAction,
                    fileColumnTypes,
                    fileColumnNames,
                    convert(icebergSchema, "table"),
                    makeTypeMap(fileColumnTypes, fileColumnNames),
                    parquetWriterOptions,
                    IntStream.range(0, fileColumnNames.size()).toArray(),
                    compressionCodec.getParquetCompressionCodec()
                            .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Compression codec %s not supported for Parquet".formatted(compressionCodec))),
                    nodeVersion.toString());
            return withEncryptionKeyMetadata(writer, encryptedOutput.keyMetadata());
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating Parquet file", e);
        }
    }

    private IcebergFileWriter createOrcWriter(
            MetricsConfig metricsConfig,
            TrinoFileSystem fileSystem,
            Location outputPath,
            Schema icebergSchema,
            ConnectorSession session,
            Map<String, String> storageProperties,
            DataSize stringStatisticsLimit,
            Optional<EncryptionManager> encryptionManager)
    {
        try {
            EncryptedOutput encryptedOutput = createOutputFile(fileSystem, outputPath, encryptionManager);
            OrcDataSink orcDataSink = OutputStreamOrcDataSink.create(encryptedOutput.trinoOutputFile());

            Closeable rollbackAction = () -> fileSystem.deleteFile(outputPath);

            List<Types.NestedField> columnFields = icebergSchema.columns();
            List<String> fileColumnNames = columnFields.stream()
                    .map(Types.NestedField::name)
                    .collect(toImmutableList());
            List<Type> fileColumnTypes = columnFields.stream()
                    .map(Types.NestedField::type)
                    .map(type -> toTrinoType(type, typeManager))
                    .collect(toImmutableList());

            Optional<Supplier<OrcDataSource>> validationInputFactory = Optional.empty();
            if (isOrcWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> {
                    try {
                        TrinoInputFile inputFile = createValidationInputFile(fileSystem, outputPath, encryptedOutput.keyMetadata(), encryptionManager);
                        return new TrinoOrcDataSource(inputFile, new OrcReaderOptions(), readStats);
                    }
                    catch (IOException | UncheckedIOException e) {
                        throw new TrinoException(ICEBERG_WRITE_VALIDATION_FAILED, e);
                    }
                });
            }

            HiveCompressionCodec compressionCodec = getHiveCompressionCodec(ORC, storageProperties)
                    .orElse(toCompressionCodec(hiveCompressionOption));

            IcebergFileWriter writer = new IcebergOrcFileWriter(
                    metricsConfig,
                    icebergSchema,
                    orcDataSink,
                    rollbackAction,
                    fileColumnNames,
                    fileColumnTypes,
                    toOrcType(icebergSchema),
                    compressionCodec.getOrcCompressionKind(),
                    withBloomFilterOptions(orcWriterOptions, storageProperties)
                            .withStripeMinSize(getOrcWriterMinStripeSize(session))
                            .withStripeMaxSize(getOrcWriterMaxStripeSize(session))
                            .withStripeMaxRowCount(getOrcWriterMaxStripeRows(session))
                            .withRowGroupMaxRowCount(getOrcWriterMaxRowGroupRows(session))
                            .withDictionaryMaxMemory(getOrcWriterMaxDictionaryMemory(session))
                            .withMaxStringStatisticsLimit(stringStatisticsLimit),
                    IntStream.range(0, fileColumnNames.size()).toArray(),
                    ImmutableMap.<String, String>builder()
                            .put(TRINO_VERSION_NAME, nodeVersion.toString())
                            .put(TRINO_QUERY_ID_NAME, session.getQueryId())
                            .buildOrThrow(),
                    validationInputFactory,
                    getOrcWriterValidateMode(session),
                    orcWriterStats);
            return withEncryptionKeyMetadata(writer, encryptedOutput.keyMetadata());
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating ORC file", e);
        }
    }

    public static OrcWriterOptions withBloomFilterOptions(OrcWriterOptions orcWriterOptions, Map<String, String> storageProperties)
    {
        Optional<String> orcBloomFilterColumns = getOrcBloomFilterColumns(storageProperties);
        Optional<String> orcBloomFilterFpp = getOrcBloomFilterFpp(storageProperties);
        if (orcBloomFilterColumns.isPresent()) {
            try {
                double fpp = orcBloomFilterFpp.map(Double::parseDouble).orElseGet(orcWriterOptions::getBloomFilterFpp);
                return OrcWriterOptions.builderFrom(orcWriterOptions)
                        .setBloomFilterColumns(ImmutableSet.copyOf(COLUMN_NAMES_SPLITTER.splitToList(orcBloomFilterColumns.get())))
                        .setBloomFilterFpp(fpp)
                        .build();
            }
            catch (NumberFormatException e) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, format("Invalid value for %s property: %s", ORC_BLOOM_FILTER_FPP_PROPERTY, orcBloomFilterFpp.get()));
            }
        }
        return orcWriterOptions;
    }

    private IcebergFileWriter createAvroWriter(
            TrinoFileSystem fileSystem,
            Location outputPath,
            Schema icebergSchema,
            Map<String, String> storageProperties,
            Optional<EncryptionManager> encryptionManager)
    {
        Closeable rollbackAction = () -> fileSystem.deleteFile(outputPath);

        List<Type> columnTypes = icebergSchema.columns().stream()
                .map(column -> toTrinoType(column.type(), typeManager))
                .collect(toImmutableList());

        HiveCompressionCodec compressionCodec = getHiveCompressionCodec(AVRO, storageProperties)
                .orElse(toCompressionCodec(hiveCompressionOption));

        EncryptedOutput encryptedOutput = createOutputFile(fileSystem, outputPath, encryptionManager);

        IcebergFileWriter writer = new IcebergAvroFileWriter(
                encryptedOutput.icebergOutputFile(),
                rollbackAction,
                icebergSchema,
                columnTypes,
                compressionCodec);
        return withEncryptionKeyMetadata(writer, encryptedOutput.keyMetadata());
    }

    private static TrinoInputFile createValidationInputFile(
            TrinoFileSystem fileSystem,
            Location outputPath,
            Optional<byte[]> keyMetadata,
            Optional<EncryptionManager> encryptionManager)
    {
        TrinoInputFile inputFile = fileSystem.newInputFile(outputPath);
        if (keyMetadata.isEmpty() || encryptionManager.isEmpty()) {
            return inputFile;
        }
        InputFile encryptedInputFile = new ForwardingInputFile(inputFile);
        InputFile decryptedInputFile = encryptionManager.get().decrypt(EncryptedFiles.encryptedInput(encryptedInputFile, keyMetadata.get()));
        return new EncryptedTrinoInputFile(inputFile, decryptedInputFile);
    }

    private EncryptedOutput createOutputFile(TrinoFileSystem fileSystem, Location outputPath, Optional<EncryptionManager> encryptionManager)
    {
        OutputFile icebergOutputFile = new ForwardingOutputFile(fileSystem, outputPath);
        EncryptedOutputFile encryptedOutputFile = encryptionManager
                .map(manager -> manager.encrypt(icebergOutputFile))
                .orElseGet(() -> EncryptionUtil.plainAsEncryptedOutput(icebergOutputFile));
        OutputFile encryptingOutputFile = encryptedOutputFile.encryptingOutputFile();
        TrinoOutputFile trinoOutputFile = new EncryptedTrinoOutputFile(outputPath, encryptingOutputFile);
        Optional<byte[]> keyMetadata = Optional.ofNullable(encryptedOutputFile.keyMetadata().buffer())
                .map(ByteBuffers::toByteArray);
        return new EncryptedOutput(trinoOutputFile, encryptingOutputFile, keyMetadata);
    }

    private static IcebergFileWriter withEncryptionKeyMetadata(IcebergFileWriter writer, Optional<byte[]> keyMetadata)
    {
        if (keyMetadata.isEmpty()) {
            return writer;
        }
        return new EncryptionMetadataFileWriter(writer, keyMetadata);
    }

    private record EncryptedOutput(TrinoOutputFile trinoOutputFile, OutputFile icebergOutputFile, Optional<byte[]> keyMetadata) {}

    private static class EncryptionMetadataFileWriter
            implements IcebergFileWriter
    {
        private final IcebergFileWriter delegate;
        private final Optional<byte[]> keyMetadata;

        private EncryptionMetadataFileWriter(IcebergFileWriter delegate, Optional<byte[]> keyMetadata)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.keyMetadata = requireNonNull(keyMetadata, "keyMetadata is null");
        }

        @Override
        public FileMetrics getFileMetrics()
        {
            return delegate.getFileMetrics();
        }

        @Override
        public Optional<byte[]> getEncryptionKeyMetadata()
        {
            return keyMetadata;
        }

        @Override
        public long getWrittenBytes()
        {
            return delegate.getWrittenBytes();
        }

        @Override
        public long getMemoryUsage()
        {
            return delegate.getMemoryUsage();
        }

        @Override
        public void appendRows(io.trino.spi.Page dataPage)
        {
            delegate.appendRows(dataPage);
        }

        @Override
        public Closeable commit()
        {
            return delegate.commit();
        }

        @Override
        public void rollback()
        {
            delegate.rollback();
        }

        @Override
        public long getValidationCpuNanos()
        {
            return delegate.getValidationCpuNanos();
        }
    }
}
