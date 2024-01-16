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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveWriter;
import io.trino.plugin.hive.HiveWriterStats;
import io.trino.plugin.hive.PartitionUpdate;
import io.trino.plugin.hive.WriterFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import static io.trino.plugin.hive.HiveCompressionCodec.NONE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveWriterFactory.getFileExtension;
import static io.trino.plugin.hive.WriterKind.INSERT;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.HiveUtil.makePartName;
import static io.trino.plugin.hive.util.HiveWriteUtils.createPartitionValues;
import static java.util.Objects.requireNonNull;

public class UnloadWriterFactory
        implements WriterFactory
{
    private final ConnectorSession session;
    private final Set<HiveFileWriterFactory> fileWriterFactories;
    private final HiveWriterStats hiveWriterStats;
    private final Location location;
    private final HiveStorageFormat format;
    private final List<String> columnNames;
    private final Map<String, String> schema;
    private final List<String> partitionColumnNames;
    private final List<Type> partitionColumnTypes;
    private final Consumer<HiveWriter> onCommit;

    public UnloadWriterFactory(
            ConnectorSession session,
            Set<HiveFileWriterFactory> fileWriterFactories,
            HiveWriterStats hiveWriterStats,
            Location location,
            HiveStorageFormat format,
            List<String> columnNames,
            Map<String, String> schema,
            List<String> partitionColumnNames,
            List<Type> partitionColumnTypes,
            Consumer<HiveWriter> onCommit)
    {
        this.session = requireNonNull(session, "session is null");
        this.fileWriterFactories = ImmutableSet.copyOf(requireNonNull(fileWriterFactories, "fileWriterFactories is null"));
        this.hiveWriterStats = requireNonNull(hiveWriterStats, "hiveWriterStats is null");
        this.location = requireNonNull(location, "location is null");
        this.format = requireNonNull(format, "format is null");
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.schema = ImmutableMap.copyOf(requireNonNull(schema, "schema is null"));
        this.partitionColumnNames = ImmutableList.copyOf(requireNonNull(partitionColumnNames, "partitionColumnNames is null"));
        this.partitionColumnTypes = ImmutableList.copyOf(requireNonNull(partitionColumnTypes, "partitionColumnTypes is null"));
        this.onCommit = requireNonNull(onCommit, "onCommit is null");
    }

    @Override
    public HiveWriter createWriter(Page partitionColumns, int position, OptionalInt bucketNumber)
    {
        List<String> partitionValues = createPartitionValues(partitionColumnTypes, partitionColumns, position);

        Location location = this.location;
        Optional<String> partitionName;
        if (!partitionColumnNames.isEmpty()) {
            location = location.appendPath(makePartName(partitionColumnNames, partitionValues));
            partitionName = Optional.of(makePartName(partitionColumnNames, partitionValues));
        }
        else {
            partitionName = Optional.empty();
        }

        String fileName = session.getQueryId() + "_" + UUID.randomUUID() + getFileExtension(NONE, fromHiveStorageFormat(format));
        FileWriter hiveFileWriter = null;
        for (HiveFileWriterFactory fileWriterFactory : fileWriterFactories) {
            Optional<FileWriter> fileWriter = fileWriterFactory.createFileWriter(
                    location.appendPath(fileName),
                    columnNames,
                    fromHiveStorageFormat(format),
                    NONE,
                    schema,
                    session,
                    OptionalInt.empty(),
                    NO_ACID_TRANSACTION,
                    false,
                    INSERT);
            if (fileWriter.isPresent()) {
                hiveFileWriter = fileWriter.get();
                break;
            }
        }

        if (hiveFileWriter == null) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Writing not supported for " + format);
        }

        return new HiveWriter(
                hiveFileWriter,
                partitionName,
                PartitionUpdate.UpdateMode.NEW,
                fileName,
                location.toString(),
                location.appendPath(fileName).toString(),
                onCommit,
                hiveWriterStats);
    }
}
