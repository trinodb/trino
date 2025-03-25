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
package io.trino.plugin.hive.ion;

import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.hive.formats.line.Column;
import io.trino.metastore.StorageFormat;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.WriterKind;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static io.trino.hive.formats.HiveClassNames.ION_OUTPUT_FORMAT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.util.HiveTypeUtil.getType;
import static io.trino.plugin.hive.util.HiveUtil.getColumnNames;
import static io.trino.plugin.hive.util.HiveUtil.getColumnTypes;
import static java.util.Objects.requireNonNull;

public class IonFileWriterFactory
        implements HiveFileWriterFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;

    @Inject
    public IonFileWriterFactory(
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory);
        this.typeManager = requireNonNull(typeManager);
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
        if (!ION_OUTPUT_FORMAT.equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        IonSerDeProperties.validatePropertySupport(schema);

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            TrinoOutputFile outputFile = fileSystem.newOutputFile(location);

            Closeable rollbackAction = () -> fileSystem.deleteFile(location);

            List<String> fileColumnNames = getColumnNames(schema);
            List<Type> fileColumnTypes = getColumnTypes(schema).stream()
                    .map(hiveType -> getType(hiveType, typeManager, getTimestampPrecision(session)))
                    .toList();

            List<Column> columns = IntStream.range(0, fileColumnNames.size())
                    .mapToObj(ordinal -> new Column(fileColumnNames.get(ordinal), fileColumnTypes.get(ordinal), ordinal))
                    .toList();

            return Optional.of(new IonFileWriter(
                    outputFile,
                    rollbackAction,
                    compressionCodec.getHiveCompressionKind(),
                    IonSerDeProperties.getIonEncoding(schema),
                    columns));
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_WRITER_OPEN_ERROR, "Error creating Ion Output", e);
        }
    }
}
