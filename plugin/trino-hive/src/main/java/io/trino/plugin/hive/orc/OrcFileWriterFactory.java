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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableMap;
import io.trino.orc.OrcDataSink;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.OutputStreamOrcDataSink;
import io.trino.orc.metadata.CompressionKind;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
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
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static io.trino.orc.metadata.OrcType.createRootOrcType;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMaxDictionaryMemory;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMaxStripeRows;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMaxStripeSize;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMinStripeSize;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterValidateMode;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcStringStatisticsLimit;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveSessionProperties.isOrcOptimizedWriterValidate;
import static io.trino.plugin.hive.acid.AcidSchema.ACID_COLUMN_NAMES;
import static io.trino.plugin.hive.acid.AcidSchema.createAcidColumnPrestoTypes;
import static io.trino.plugin.hive.acid.AcidSchema.createRowType;
import static io.trino.plugin.hive.util.HiveUtil.getColumnNames;
import static io.trino.plugin.hive.util.HiveUtil.getColumnTypes;
import static io.trino.plugin.hive.util.HiveUtil.getOrcWriterOptions;
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

    @Inject
    public OrcFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            FileFormatDataSourceStats readStats,
            OrcWriterConfig config)
    {
        this(
                hdfsEnvironment,
                typeManager,
                nodeVersion,
                readStats,
                requireNonNull(config, "config is null").toOrcWriterOptions());
    }

    public OrcFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            FileFormatDataSourceStats readStats,
            OrcWriterOptions orcWriterOptions)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.readStats = requireNonNull(readStats, "readStats is null");
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
            ConnectorSession session,
            OptionalInt bucketNumber,
            AcidTransaction transaction,
            boolean useAcidSchema,
            WriterKind writerKind)
    {
        if (!OrcOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        CompressionKind compression = getCompression(schema, configuration);

        // existing tables and partitions may have columns in a different order than the writer is providing, so build
        // an index to rearrange columns in the proper order
        List<String> fileColumnNames = getColumnNames(schema);
        List<Type> fileColumnTypes = getColumnTypes(schema).stream()
                .map(hiveType -> hiveType.getType(typeManager, getTimestampPrecision(session)))
                .collect(toList());

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();
        if (transaction.isAcidDeleteOperation(writerKind)) {
            // For delete, set the "row" column to -1
            fileInputColumnIndexes[fileInputColumnIndexes.length - 1] = -1;
        }

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getIdentity(), path, configuration);
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
                        throw new TrinoException(HIVE_WRITE_VALIDATION_FAILED, e);
                    }
                });
            }

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(path, false);
                return null;
            };

            if (transaction.isInsert() && useAcidSchema) {
                // Only add the ACID columns if the request is for insert-type operations - - for delete operations,
                // the columns are added by the caller.  This is because the ACID columns for delete operations
                // depend on the rows being deleted, whereas the ACID columns for INSERT are completely determined
                // by bucket and writeId.
                Type rowType = createRowType(fileColumnNames, fileColumnTypes);
                fileColumnNames = ACID_COLUMN_NAMES;
                fileColumnTypes = createAcidColumnPrestoTypes(rowType);
            }

            return Optional.of(new OrcFileWriter(
                    orcDataSink,
                    writerKind,
                    transaction,
                    useAcidSchema,
                    bucketNumber,
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
            throw new TrinoException(HIVE_WRITER_OPEN_ERROR, "Error creating ORC file", e);
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
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Unknown ORC compression type " + compressionName);
        }
        return compression;
    }
}
