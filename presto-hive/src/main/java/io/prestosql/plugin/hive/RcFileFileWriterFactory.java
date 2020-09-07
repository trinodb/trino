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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.rcfile.HdfsRcFileDataSource;
import io.prestosql.rcfile.RcFileDataSource;
import io.prestosql.rcfile.RcFileEncoding;
import io.prestosql.rcfile.binary.BinaryRcFileEncoding;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static io.prestosql.plugin.hive.HiveSessionProperties.isRcfileOptimizedWriterValidate;
import static io.prestosql.plugin.hive.rcfile.RcFilePageSourceFactory.createTextVectorEncoding;
import static io.prestosql.plugin.hive.util.HiveUtil.getColumnNames;
import static io.prestosql.plugin.hive.util.HiveUtil.getColumnTypes;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RcFileFileWriterFactory
        implements HiveFileWriterFactory
{
    private final DateTimeZone timeZone;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final FileFormatDataSourceStats stats;

    @Inject
    public RcFileFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            HiveConfig hiveConfig,
            FileFormatDataSourceStats stats)
    {
        this(hdfsEnvironment, typeManager, nodeVersion, requireNonNull(hiveConfig, "hiveConfig is null").getRcfileDateTimeZone(), stats);
    }

    public RcFileFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            DateTimeZone timeZone,
            FileFormatDataSourceStats stats)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.stats = requireNonNull(stats, "stats is null");
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
        if (!RCFileOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        RcFileEncoding rcFileEncoding;
        if (LazyBinaryColumnarSerDe.class.getName().equals(storageFormat.getSerDe())) {
            rcFileEncoding = new BinaryRcFileEncoding(timeZone);
        }
        else if (ColumnarSerDe.class.getName().equals(storageFormat.getSerDe())) {
            rcFileEncoding = createTextVectorEncoding(schema);
        }
        else {
            return Optional.empty();
        }

        Optional<String> codecName = Optional.ofNullable(configuration.get(FileOutputFormat.COMPRESS_CODEC));

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
            OutputStream outputStream = fileSystem.create(path);

            Optional<Supplier<RcFileDataSource>> validationInputFactory = Optional.empty();
            if (isRcfileOptimizedWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> {
                    try {
                        return new HdfsRcFileDataSource(
                                path.toString(),
                                fileSystem.open(path),
                                fileSystem.getFileStatus(path).getLen(),
                                stats);
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

            return Optional.of(new RcFileFileWriter(
                    outputStream,
                    rollbackAction,
                    rcFileEncoding,
                    fileColumnTypes,
                    codecName,
                    fileInputColumnIndexes,
                    ImmutableMap.<String, String>builder()
                            .put(PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .build(),
                    validationInputFactory));
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, "Error creating RCFile file", e);
        }
    }
}
