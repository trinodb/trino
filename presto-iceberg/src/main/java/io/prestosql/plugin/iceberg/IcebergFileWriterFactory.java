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
package io.prestosql.plugin.iceberg;

import io.prestosql.plugin.hive.FileWriter;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.RecordFileWriter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;

import javax.inject.Inject;

import java.util.List;
import java.util.Properties;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.prestosql.plugin.hive.util.ParquetRecordWriterUtil.setParquetSchema;
import static io.prestosql.plugin.iceberg.TypeConverter.toHiveType;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.convert;

public class IcebergFileWriterFactory
{
    private final TypeManager typeManager;

    @Inject
    public IcebergFileWriterFactory(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    public FileWriter createFileWriter(
            Path outputPath,
            Schema icebergSchema,
            List<IcebergColumnHandle> columns,
            JobConf jobConf,
            ConnectorSession session,
            FileFormat fileFormat)
    {
        switch (fileFormat) {
            case PARQUET:
                return createParquetWriter(outputPath, icebergSchema, columns, jobConf, session);
        }
        throw new PrestoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + fileFormat);
    }

    private FileWriter createParquetWriter(
            Path outputPath,
            Schema icebergSchema,
            List<IcebergColumnHandle> columns,
            JobConf jobConf,
            ConnectorSession session)
    {
        Properties properties = new Properties();
        properties.setProperty(IOConstants.COLUMNS, columns.stream()
                .map(IcebergColumnHandle::getName)
                .collect(joining(",")));
        properties.setProperty(IOConstants.COLUMNS_TYPES, columns.stream()
                .map(column -> toHiveType(column.getType()).getHiveTypeName().toString())
                .collect(joining(":")));

        setParquetSchema(jobConf, convert(icebergSchema, "table"));

        return new RecordFileWriter(
                outputPath,
                columns.stream()
                        .map(IcebergColumnHandle::getName)
                        .collect(toImmutableList()),
                fromHiveStorageFormat(HiveStorageFormat.PARQUET),
                properties,
                HiveStorageFormat.PARQUET.getEstimatedWriterSystemMemoryUsage(),
                jobConf,
                typeManager,
                session);
    }
}
