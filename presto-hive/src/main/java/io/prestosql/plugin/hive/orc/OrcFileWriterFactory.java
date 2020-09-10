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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableMap;
import io.prestosql.orc.OrcDataSink;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.orc.OutputStreamOrcDataSink;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.FileWriter;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveFileWriterFactory;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcConf;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static io.prestosql.orc.metadata.OrcType.createRootOrcType;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMaxDictionaryMemory;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMaxStripeRows;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMaxStripeSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMinStripeSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterValidateMode;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcStringStatisticsLimit;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcOptimizedWriterValidate;
import static io.prestosql.plugin.hive.util.HiveUtil.getColumnNames;
import static io.prestosql.plugin.hive.util.HiveUtil.getColumnTypes;
import static io.prestosql.plugin.hive.util.HiveUtil.getOrcWriterOptions;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OrcFileWriterFactory
        implements HiveFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final FileFormatDataSourceStats readStats;
    private final OrcWriterStats stats = new OrcWriterStats();
    private final OrcWriterOptions orcWriterOptions;
    private final boolean writeLegacyVersion;

    @Inject
    public OrcFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            OrcWriterConfig orcWriterConfig,
            FileFormatDataSourceStats readStats,
            OrcWriterConfig config)
    {
        this(
                hdfsEnvironment,
                typeManager,
                nodeVersion,
                requireNonNull(orcWriterConfig, "orcWriterConfig is null").isUseLegacyVersion(),
                readStats,
                requireNonNull(config, "config is null").toOrcWriterOptions());
    }

    public OrcFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            boolean writeLegacyVersion,
            FileFormatDataSourceStats readStats,
            OrcWriterOptions orcWriterOptions)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.writeLegacyVersion = writeLegacyVersion;
        this.readStats = requireNonNull(readStats, "stats is null");
        this.orcWriterOptions = requireNonNull(orcWriterOptions, "orcWriterOptions is null");
    }

    @Managed
    @Flatten
    public OrcWriterStats getStats()
    {
        return stats;
    }

    @Override
    public Optional<FileWriter> createFileWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            JobConf configuration,
            ConnectorSession session)
    {
        if (!OrcOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        CompressionKind compression = getCompression(schema, configuration);

        // existing tables and partitions may have columns in a different order than the writer is providing, so build
        // an index to rearrange columns in the proper order
        List<String> fileColumnNames = getColumnNames(schema);
        List<Type> fileColumnTypes = getColumnTypes(schema).stream()
                .map(hiveType -> hiveType.getType(typeManager))
                .collect(toList());

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            OrcDataSink orcDataSink = createOrcDataSink(fileSystem, path);

            Optional<Supplier<OrcDataSource>> validationInputFactory = Optional.empty();
            if (isOrcOptimizedWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> {
                    try {
                        return new HdfsOrcDataSource(
                                new OrcDataSourceId(path.toString()),
                                fileSystem.getFileStatus(path).getLen(),
                                new OrcReaderOptions(),
                                fileSystem.open(path),
                                readStats);
                    }
                    catch (IOException e) {
                        throw new PrestoException(HIVE_WRITE_VALIDATION_FAILED, e);
                    }
                });
            }

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(path, false);
                return null;
            };

            return Optional.of(new OrcFileWriter(
                    orcDataSink,
                    rollbackAction,
                    fileColumnNames,
                    fileColumnTypes,
                    createRootOrcType(fileColumnNames, fileColumnTypes),
                    compression,
                    getOrcWriterOptions(schema, orcWriterOptions)
                            .withStripeMinSize(getOrcOptimizedWriterMinStripeSize(session))
                            .withStripeMaxSize(getOrcOptimizedWriterMaxStripeSize(session))
                            .withStripeMaxRowCount(getOrcOptimizedWriterMaxStripeRows(session))
                            .withDictionaryMaxMemory(getOrcOptimizedWriterMaxDictionaryMemory(session))
                            .withMaxStringStatisticsLimit(getOrcStringStatisticsLimit(session)),
                    writeLegacyVersion,
                    fileInputColumnIndexes,
                    ImmutableMap.<String, String>builder()
                            .put(PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .build(),
                    validationInputFactory,
                    getOrcOptimizedWriterValidateMode(session),
                    stats));
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, "Error creating ORC file", e);
        }
    }

    public static OrcDataSink createOrcDataSink(FileSystem fileSystem, Path path)
            throws IOException
    {
        return new OutputStreamOrcDataSink(fileSystem.create(path));
    }

    private static CompressionKind getCompression(Properties schema, JobConf configuration)
    {
        String compressionName = OrcConf.COMPRESS.getString(schema, configuration);
        if (compressionName == null) {
            return CompressionKind.ZLIB;
        }

        CompressionKind compression;
        try {
            compression = CompressionKind.valueOf(compressionName.toUpperCase(ENGLISH));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Unknown ORC compression type " + compressionName);
        }
        return compression;
    }
}
