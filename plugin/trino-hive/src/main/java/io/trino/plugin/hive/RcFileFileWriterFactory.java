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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.encodings.ColumnEncodingFactory;
import io.trino.hive.formats.encodings.binary.BinaryColumnEncodingFactory;
import io.trino.hive.formats.encodings.text.TextColumnEncodingFactory;
import io.trino.hive.formats.encodings.text.TextEncodingOptions;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.Supplier;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveSessionProperties.isRcfileOptimizedWriterValidate;
import static io.trino.plugin.hive.util.HiveClassNames.COLUMNAR_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.LAZY_BINARY_COLUMNAR_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.RCFILE_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveUtil.getColumnNames;
import static io.trino.plugin.hive.util.HiveUtil.getColumnTypes;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RcFileFileWriterFactory
        implements HiveFileWriterFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;

    @Inject
    public RcFileFileWriterFactory(
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            HiveConfig hiveConfig)
    {
        this(fileSystemFactory, typeManager, nodeVersion, hiveConfig.getRcfileDateTimeZone());
    }

    public RcFileFileWriterFactory(
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            DateTimeZone timeZone)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
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
        if (!RCFILE_OUTPUT_FORMAT_CLASS.equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        ColumnEncodingFactory columnEncodingFactory;
        if (LAZY_BINARY_COLUMNAR_SERDE_CLASS.equals(storageFormat.getSerde())) {
            columnEncodingFactory = new BinaryColumnEncodingFactory(timeZone);
        }
        else if (COLUMNAR_SERDE_CLASS.equals(storageFormat.getSerde())) {
            columnEncodingFactory = new TextColumnEncodingFactory(TextEncodingOptions.fromSchema(Maps.fromProperties(schema)));
        }
        else {
            return Optional.empty();
        }

        // existing tables and partitions may have columns in a different order than the writer is providing, so build
        // an index to rearrange columns in the proper order
        List<String> fileColumnNames = getColumnNames(schema);
        List<Type> fileColumnTypes = getColumnTypes(schema).stream()
                .map(hiveType -> hiveType.getType(typeManager, getTimestampPrecision(session)))
                .collect(toList());

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            AggregatedMemoryContext outputStreamMemoryContext = newSimpleAggregatedMemoryContext();
            OutputStream outputStream = fileSystem.newOutputFile(location).create(outputStreamMemoryContext);

            Optional<Supplier<TrinoInputFile>> validationInputFactory = Optional.empty();
            if (isRcfileOptimizedWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> fileSystem.newInputFile(location));
            }

            Closeable rollbackAction = () -> fileSystem.deleteFile(location);

            return Optional.of(new RcFileFileWriter(
                    outputStream,
                    outputStreamMemoryContext,
                    rollbackAction,
                    columnEncodingFactory,
                    fileColumnTypes,
                    compressionCodec.getHiveCompressionKind(),
                    fileInputColumnIndexes,
                    ImmutableMap.<String, String>builder()
                            .put(PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .buildOrThrow(),
                    validationInputFactory));
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_WRITER_OPEN_ERROR, "Error creating RCFile file", e);
        }
    }
}
