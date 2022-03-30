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
import io.trino.orc.OrcDataSink;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.OutputStreamOrcDataSink;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.orc.HdfsOrcDataSource;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_OPEN_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITE_VALIDATION_FAILED;
import static io.trino.plugin.iceberg.IcebergMetadata.ORC_BLOOM_FILTER_COLUMNS_KEY;
import static io.trino.plugin.iceberg.IcebergMetadata.ORC_BLOOM_FILTER_FPP_KEY;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getCompressionCodec;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcStringStatisticsLimit;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxDictionaryMemory;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxStripeRows;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxStripeSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcWriterMinStripeSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getOrcWriterValidateMode;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getParquetWriterBatchSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getParquetWriterBlockSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getParquetWriterPageSize;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isOrcWriterValidate;
import static io.trino.plugin.iceberg.IcebergTableProperties.ORC_BLOOM_FILTER_FPP;
import static io.trino.plugin.iceberg.TypeConverter.toOrcType;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.util.PrimitiveTypeMapBuilder.makeTypeMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Double.parseDouble;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.convert;

public class IcebergFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final FileFormatDataSourceStats readStats;
    private final OrcWriterStats orcWriterStats = new OrcWriterStats();
    private final OrcWriterOptions orcWriterOptions;
    private static final Splitter COLUMN_NAMES_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    @Inject
    public IcebergFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            FileFormatDataSourceStats readStats,
            OrcWriterConfig orcWriterConfig)
    {
        checkArgument(!requireNonNull(orcWriterConfig, "orcWriterConfig is null").isUseLegacyVersion(), "the ORC writer shouldn't be configured to use a legacy version");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.readStats = requireNonNull(readStats, "readStats is null");
        this.orcWriterOptions = orcWriterConfig.toOrcWriterOptions();
    }

    @Managed
    public OrcWriterStats getOrcWriterStats()
    {
        return orcWriterStats;
    }

    public IcebergFileWriter createFileWriter(
            Path outputPath,
            Schema icebergSchema,
            JobConf jobConf,
            ConnectorSession session,
            HdfsContext hdfsContext,
            IcebergFileFormat fileFormat,
            MetricsConfig metricsConfig,
            Map<String, String> storageProperties)
    {
        switch (fileFormat) {
            case PARQUET:
                // TODO use metricsConfig
                return createParquetWriter(outputPath, icebergSchema, jobConf, session, hdfsContext);
            case ORC:
                return createOrcWriter(metricsConfig, outputPath, icebergSchema, jobConf, session, storageProperties);
            default:
                throw new TrinoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + fileFormat);
        }
    }

    private IcebergFileWriter createParquetWriter(
            Path outputPath,
            Schema icebergSchema,
            JobConf jobConf,
            ConnectorSession session,
            HdfsContext hdfsContext)
    {
        List<String> fileColumnNames = icebergSchema.columns().stream()
                .map(Types.NestedField::name)
                .collect(toImmutableList());
        List<Type> fileColumnTypes = icebergSchema.columns().stream()
                .map(column -> toTrinoType(column.type(), typeManager))
                .collect(toImmutableList());

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getIdentity(), outputPath, jobConf);

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(outputPath, false);
                return null;
            };

            ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                    .setMaxPageSize(getParquetWriterPageSize(session))
                    .setMaxBlockSize(getParquetWriterBlockSize(session))
                    .setBatchSize(getParquetWriterBatchSize(session))
                    .build();

            return new IcebergParquetFileWriter(
                    hdfsEnvironment.doAs(session.getIdentity(), () -> fileSystem.create(outputPath)),
                    rollbackAction,
                    fileColumnTypes,
                    convert(icebergSchema, "table"),
                    makeTypeMap(fileColumnTypes, fileColumnNames),
                    parquetWriterOptions,
                    IntStream.range(0, fileColumnNames.size()).toArray(),
                    getCompressionCodec(session).getParquetCompressionCodec(),
                    nodeVersion.toString(),
                    outputPath,
                    hdfsEnvironment,
                    hdfsContext);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating Parquet file", e);
        }
    }

    private IcebergFileWriter createOrcWriter(
            MetricsConfig metricsConfig,
            Path outputPath,
            Schema icebergSchema,
            JobConf jobConf,
            ConnectorSession session,
            Map<String, String> storageProperties)
    {
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getIdentity(), outputPath, jobConf);
            OrcDataSink orcDataSink = hdfsEnvironment.doAs(session.getIdentity(), () -> new OutputStreamOrcDataSink(fileSystem.create(outputPath)));
            Callable<Void> rollbackAction = () -> {
                hdfsEnvironment.doAs(session.getIdentity(), () -> fileSystem.delete(outputPath, false));
                return null;
            };

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
                        return new HdfsOrcDataSource(
                                new OrcDataSourceId(outputPath.toString()),
                                hdfsEnvironment.doAs(session.getIdentity(), () -> fileSystem.getFileStatus(outputPath).getLen()),
                                new OrcReaderOptions(),
                                hdfsEnvironment.doAs(session.getIdentity(), () -> fileSystem.open(outputPath)),
                                readStats);
                    }
                    catch (IOException e) {
                        throw new TrinoException(ICEBERG_WRITE_VALIDATION_FAILED, e);
                    }
                });
            }

            return new IcebergOrcFileWriter(
                    metricsConfig,
                    icebergSchema,
                    orcDataSink,
                    rollbackAction,
                    fileColumnNames,
                    fileColumnTypes,
                    toOrcType(icebergSchema),
                    getCompressionCodec(session).getOrcCompressionKind(),
                    getOrcWriterOptions(storageProperties, orcWriterOptions)
                            .withStripeMinSize(getOrcWriterMinStripeSize(session))
                            .withStripeMaxSize(getOrcWriterMaxStripeSize(session))
                            .withStripeMaxRowCount(getOrcWriterMaxStripeRows(session))
                            .withDictionaryMaxMemory(getOrcWriterMaxDictionaryMemory(session))
                            .withMaxStringStatisticsLimit(getOrcStringStatisticsLimit(session)),
                    IntStream.range(0, fileColumnNames.size()).toArray(),
                    ImmutableMap.<String, String>builder()
                            .put(PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .buildOrThrow(),
                    validationInputFactory,
                    getOrcWriterValidateMode(session),
                    orcWriterStats);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating ORC file", e);
        }
    }

    public static OrcWriterOptions getOrcWriterOptions(Map<String, String> storageProperties, OrcWriterOptions orcWriterOptions)
    {
        if (storageProperties.containsKey(ORC_BLOOM_FILTER_COLUMNS_KEY)) {
            if (!storageProperties.containsKey(ORC_BLOOM_FILTER_FPP_KEY)) {
                throw new TrinoException(HIVE_INVALID_METADATA, format("FPP for bloom filter is missing"));
            }
            try {
                double fpp = parseDouble(storageProperties.get(ORC_BLOOM_FILTER_FPP_KEY));
                return orcWriterOptions
                        .withBloomFilterColumns(ImmutableSet.copyOf(COLUMN_NAMES_SPLITTER.splitToList(storageProperties.get(ORC_BLOOM_FILTER_COLUMNS_KEY))))
                        .withBloomFilterFpp(fpp);
            }
            catch (NumberFormatException e) {
                throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, format("Invalid value for %s property: %s", ORC_BLOOM_FILTER_FPP, storageProperties.get(ORC_BLOOM_FILTER_FPP_KEY)));
            }
        }
        return orcWriterOptions;
    }
}
