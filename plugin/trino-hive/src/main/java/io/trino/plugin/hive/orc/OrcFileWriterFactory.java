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
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.metastore.StorageFormat;
import io.trino.orc.OrcDataSink;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.OutputStreamOrcDataSink;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static io.trino.hive.formats.HiveClassNames.ORC_OUTPUT_FORMAT_CLASS;
import static io.trino.orc.metadata.OrcType.createRootOrcType;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.TRINO_VERSION_NAME;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMaxDictionaryMemory;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMaxStripeRows;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMaxStripeSize;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterMinStripeSize;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcOptimizedWriterValidateMode;
import static io.trino.plugin.hive.HiveSessionProperties.getOrcStringStatisticsLimit;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveSessionProperties.isOrcOptimizedWriterValidate;
import static io.trino.plugin.hive.acid.AcidSchema.ACID_COLUMN_NAMES;
import static io.trino.plugin.hive.acid.AcidSchema.createAcidColumnTrinoTypes;
import static io.trino.plugin.hive.acid.AcidSchema.createRowType;
import static io.trino.plugin.hive.util.HiveTypeUtil.getType;
import static io.trino.plugin.hive.util.HiveUtil.getColumnNames;
import static io.trino.plugin.hive.util.HiveUtil.getColumnTypes;
import static io.trino.plugin.hive.util.HiveUtil.getOrcWriterOptions;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OrcFileWriterFactory
        implements HiveFileWriterFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final FileFormatDataSourceStats readStats;
    private final OrcWriterStats stats = new OrcWriterStats();
    private final OrcWriterOptions orcWriterOptions;

    @Inject
    public OrcFileWriterFactory(
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            FileFormatDataSourceStats readStats,
            OrcWriterConfig config)
    {
        this(
                typeManager,
                nodeVersion,
                readStats,
                config.toOrcWriterOptions(),
                fileSystemFactory);
    }

    public OrcFileWriterFactory(
            TypeManager typeManager,
            NodeVersion nodeVersion,
            FileFormatDataSourceStats readStats,
            OrcWriterOptions orcWriterOptions,
            TrinoFileSystemFactory fileSystemFactory)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.readStats = requireNonNull(readStats, "readStats is null");
        this.orcWriterOptions = requireNonNull(orcWriterOptions, "orcWriterOptions is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Managed
    @Flatten
    public OrcWriterStats getStats()
    {
        return stats;
    }

    @Override
    public Optional<FileWriter> createFileWriter(
            Location location,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            HiveCompressionCodec compressionCodec,
            Map<String, String> schema,
            ConnectorSession session,
            OptionalInt bucketNumber,
            AcidTransaction transaction,
            boolean useAcidSchema,
            WriterKind writerKind)
    {
        if (!ORC_OUTPUT_FORMAT_CLASS.equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        // existing tables and partitions may have columns in a different order than the writer is providing, so build
        // an index to rearrange columns in the proper order
        List<String> fileColumnNames = getColumnNames(schema);
        List<Type> fileColumnTypes = getColumnTypes(schema).stream()
                .map(hiveType -> getType(hiveType, typeManager, getTimestampPrecision(session)))
                .collect(toList());

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            OrcDataSink orcDataSink = createOrcDataSink(fileSystem, location);

            Optional<Supplier<OrcDataSource>> validationInputFactory = Optional.empty();
            if (isOrcOptimizedWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> {
                    try {
                        TrinoInputFile inputFile = fileSystem.newInputFile(location);
                        return new HdfsOrcDataSource(
                                new OrcDataSourceId(location.toString()),
                                inputFile.length(),
                                new OrcReaderOptions(),
                                inputFile,
                                readStats);
                    }
                    catch (IOException e) {
                        throw new TrinoException(HIVE_WRITE_VALIDATION_FAILED, e);
                    }
                });
            }

            Closeable rollbackAction = () -> fileSystem.deleteFile(location);

            if (transaction.isInsert() && useAcidSchema) {
                // Only add the ACID columns if the request is for insert-type operations - - for delete operations,
                // the columns are added by the caller.  This is because the ACID columns for delete operations
                // depend on the rows being deleted, whereas the ACID columns for INSERT are completely determined
                // by bucket and writeId.
                Type rowType = createRowType(fileColumnNames, fileColumnTypes);
                fileColumnNames = ACID_COLUMN_NAMES;
                fileColumnTypes = createAcidColumnTrinoTypes(rowType);
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
                    compressionCodec.getOrcCompressionKind(),
                    getOrcWriterOptions(schema, orcWriterOptions)
                            .withStripeMinSize(getOrcOptimizedWriterMinStripeSize(session))
                            .withStripeMaxSize(getOrcOptimizedWriterMaxStripeSize(session))
                            .withStripeMaxRowCount(getOrcOptimizedWriterMaxStripeRows(session))
                            .withDictionaryMaxMemory(getOrcOptimizedWriterMaxDictionaryMemory(session))
                            .withMaxStringStatisticsLimit(getOrcStringStatisticsLimit(session)),
                    fileInputColumnIndexes,
                    ImmutableMap.<String, String>builder()
                            .put(TRINO_VERSION_NAME, nodeVersion.toString())
                            .put(TRINO_QUERY_ID_NAME, session.getQueryId())
                            .buildOrThrow(),
                    validationInputFactory,
                    getOrcOptimizedWriterValidateMode(session),
                    stats));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_WRITER_OPEN_ERROR, "Error creating ORC file", e);
        }
    }

    public static OrcDataSink createOrcDataSink(TrinoFileSystem fileSystem, Location location)
            throws IOException
    {
        return OutputStreamOrcDataSink.create(fileSystem.newOutputFile(location));
    }
}
