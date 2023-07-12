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
package io.trino.plugin.hive.s3select;

import com.google.common.annotations.VisibleForTesting;
import io.trino.plugin.hive.GenericHiveRecordCursor;
import io.trino.plugin.hive.HiveColumnHandle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static java.util.Objects.requireNonNull;

class S3SelectRecordCursor<K, V extends Writable>
        extends GenericHiveRecordCursor<K, V>
{
    public S3SelectRecordCursor(
            Configuration configuration,
            Path path,
            RecordReader<K, V> recordReader,
            long totalBytes,
            Properties splitSchema,
            List<HiveColumnHandle> columns)
    {
        super(configuration, path, recordReader, totalBytes, updateSplitSchema(splitSchema, columns), columns);
    }

    // since s3select only returns the required column, not the whole columns
    // we need to update the split schema to include only the required columns
    // otherwise, Serde could not deserialize output from s3select to row data correctly
    @VisibleForTesting
    static Properties updateSplitSchema(Properties splitSchema, List<HiveColumnHandle> columns)
    {
        requireNonNull(splitSchema, "splitSchema is null");
        requireNonNull(columns, "columns is null");
        // clone split properties for update so as not to affect the original one
        Properties updatedSchema = new Properties();
        updatedSchema.putAll(splitSchema);
        updatedSchema.setProperty(LIST_COLUMNS, buildColumns(columns));
        updatedSchema.setProperty(LIST_COLUMN_TYPES, buildColumnTypes(columns));
        return updatedSchema;
    }

    private static String buildColumns(List<HiveColumnHandle> columns)
    {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        return columns.stream()
                .map(HiveColumnHandle::getName)
                .collect(Collectors.joining(","));
    }

    private static String buildColumnTypes(List<HiveColumnHandle> columns)
    {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        return columns.stream()
                .map(column -> column.getHiveType().getTypeInfo().getTypeName())
                .collect(Collectors.joining(","));
    }
}
