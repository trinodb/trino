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
package io.prestosql.plugin.hive.parquet;

import io.prestosql.parquet.writer.ParquetSchemaConverter;
import io.prestosql.parquet.writer.ParquetWriterOptions;
import io.prestosql.plugin.hive.FileWriter;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveFileWriterFactory;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.prestosql.plugin.hive.util.HiveUtil.getColumnNames;
import static io.prestosql.plugin.hive.util.HiveUtil.getColumnTypes;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ParquetFileWriterFactory
        implements HiveFileWriterFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;

    @Inject
    public ParquetFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public Optional<FileWriter> createFileWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            JobConf conf,
            ConnectorSession session)
    {
        if (!HiveSessionProperties.isParquetOptimizedWriterEnabled(session)) {
            return Optional.empty();
        }

        if (!MapredParquetOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .setMaxPageSize(HiveSessionProperties.getParquetWriterPageSize(session))
                .setMaxBlockSize(HiveSessionProperties.getParquetWriterBlockSize(session))
                .build();

        CompressionCodecName compressionCodecName = getCompression(conf);

        List<String> fileColumnNames = getColumnNames(schema);
        List<Type> fileColumnTypes = getColumnTypes(schema).stream()
                .map(hiveType -> hiveType.getType(typeManager))
                .collect(toList());

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, conf);

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(path, false);
                return null;
            };

            ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(fileColumnTypes, fileColumnNames);

            return Optional.of(new ParquetFileWriter(
                    fileSystem.create(path),
                    rollbackAction,
                    fileColumnTypes,
                    schemaConverter.getMessageType(),
                    schemaConverter.getPrimitiveTypes(),
                    parquetWriterOptions,
                    fileInputColumnIndexes,
                    compressionCodecName));
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, "Error creating Parquet file", e);
        }
    }

    private static CompressionCodecName getCompression(JobConf configuration)
    {
        String compressionName = configuration.get(ParquetOutputFormat.COMPRESSION);
        if (compressionName == null) {
            return CompressionCodecName.GZIP;
        }
        return CompressionCodecName.valueOf(compressionName);
    }
}
