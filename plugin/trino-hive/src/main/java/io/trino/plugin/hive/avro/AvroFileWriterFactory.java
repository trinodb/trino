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
package io.trino.plugin.hive.avro;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.hive.formats.avro.AvroCompressionKind;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.avro.Schema;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveSessionProperties.isAvroNativeWriterEnabled;
import static io.trino.plugin.hive.util.HiveClassNames.AVRO_CONTAINER_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveUtil.getColumnNames;
import static io.trino.plugin.hive.util.HiveUtil.getColumnTypes;
import static java.util.Objects.requireNonNull;

public class AvroFileWriterFactory
        implements HiveFileWriterFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;

    @Inject
    public AvroFileWriterFactory(
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            NodeVersion nodeVersion)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion");
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
        if (!isAvroNativeWriterEnabled(session)) {
            return Optional.empty();
        }
        if (!AVRO_CONTAINER_OUTPUT_FORMAT_CLASS.equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        AvroCompressionKind compressionKind = compressionCodec.getAvroCompressionKind().orElse(AvroCompressionKind.NULL);
        if (!compressionKind.isSupportedLocally()) {
            throw new VerifyException("Avro Compression codec %s is not supported in the environment".formatted(compressionKind));
        }

        HiveTimestampPrecision hiveTimestampPrecision = getTimestampPrecision(session);
        // existing tables and partitions may have columns in a different order than the writer is providing, so build
        // an index to rearrange columns in the proper order
        List<String> fileColumnNames = getColumnNames(schema);
        List<Type> fileColumnTypes = getColumnTypes(schema).stream()
                .map(hiveType -> hiveType.getType(typeManager, hiveTimestampPrecision))
                .collect(toImmutableList());

        List<Type> inputColumnTypes = inputColumnNames.stream().map(inputColumnName -> {
            int index = fileColumnNames.indexOf(inputColumnName);
            checkArgument(index >= 0, "Input column name [%s] not preset in file columns names %s", inputColumnName, fileColumnNames);
            return fileColumnTypes.get(index);
        }).collect(toImmutableList());

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity());
            Schema fileSchema = AvroHiveFileUtils.determineSchemaOrThrowException(fileSystem, schema);
            TrinoOutputFile outputFile = fileSystem.newOutputFile(location);
            AggregatedMemoryContext outputStreamMemoryContext = newSimpleAggregatedMemoryContext();

            Closeable rollbackAction = () -> fileSystem.deleteFile(location);

            return Optional.of(new AvroHiveFileWriter(
                    outputFile.create(outputStreamMemoryContext),
                    outputStreamMemoryContext,
                    fileSchema,
                    new HiveAvroTypeManager(hiveTimestampPrecision),
                    rollbackAction,
                    inputColumnNames,
                    inputColumnTypes,
                    compressionKind,
                    ImmutableMap.<String, String>builder()
                            .put(PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .buildOrThrow()));
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_WRITER_OPEN_ERROR, "Error creating Avro Container file", e);
        }
    }
}
