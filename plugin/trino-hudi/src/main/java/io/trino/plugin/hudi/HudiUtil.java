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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Column;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.avro.AvroHiveFileUtils;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNSUPPORTED_FILE_FORMAT;
import static org.apache.hudi.avro.HoodieAvroUtils.METADATA_FIELD_SCHEMA;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;

public final class HudiUtil
{
    private HudiUtil() {}

    public static HoodieFileFormat getHudiFileFormat(String path)
    {
        String extension = getFileExtension(path);
        if (extension.equals(HoodieFileFormat.PARQUET.getFileExtension())) {
            return HoodieFileFormat.PARQUET;
        }
        if (extension.equals(HoodieFileFormat.HOODIE_LOG.getFileExtension())) {
            return HoodieFileFormat.HOODIE_LOG;
        }
        if (extension.equals(HoodieFileFormat.ORC.getFileExtension())) {
            return HoodieFileFormat.ORC;
        }
        if (extension.equals(HoodieFileFormat.HFILE.getFileExtension())) {
            return HoodieFileFormat.HFILE;
        }
        throw new TrinoException(HUDI_UNSUPPORTED_FILE_FORMAT, "Hoodie InputFormat not implemented for base file of type " + extension);
    }

    private static String getFileExtension(String fullName)
    {
        String fileName = Location.of(fullName).fileName();
        int dotIndex = fileName.lastIndexOf('.');
        return dotIndex == -1 ? "" : fileName.substring(dotIndex);
    }

    public static boolean hudiMetadataExists(TrinoFileSystem trinoFileSystem, Location baseLocation)
    {
        try {
            Location metaLocation = baseLocation.appendPath(HoodieTableMetaClient.METAFOLDER_NAME);
            FileIterator iterator = trinoFileSystem.listFiles(metaLocation);
            // If there is at least one file in the .hoodie directory, it's a valid Hudi table
            return iterator.hasNext();
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, "Failed to check for Hudi table at location: " + baseLocation, e);
        }
    }

    public static boolean partitionMatchesPredicates(
            SchemaTableName tableName,
            String hivePartitionName,
            List<HiveColumnHandle> partitionColumnHandles,
            TupleDomain<HiveColumnHandle> constraintSummary)
    {
        HivePartition partition = HivePartitionManager.parsePartition(
                tableName, hivePartitionName, partitionColumnHandles);

        return partitionMatches(partitionColumnHandles, constraintSummary, partition);
    }

    public static boolean partitionMatches(List<HiveColumnHandle> partitionColumns, TupleDomain<HiveColumnHandle> constraintSummary, HivePartition partition)
    {
        if (constraintSummary.isNone()) {
            return false;
        }
        Map<HiveColumnHandle, Domain> domains = constraintSummary.getDomains().orElseGet(ImmutableMap::of);
        for (HiveColumnHandle column : partitionColumns) {
            NullableValue value = partition.getKeys().get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }
        return true;
    }

    public static List<HivePartitionKey> buildPartitionKeys(List<Column> keys, List<String> values)
    {
        checkCondition(keys.size() == values.size(), HIVE_INVALID_METADATA,
                "Expected %s partition key values, but got %s. Keys: %s, Values: %s.",
                keys.size(), values.size(), keys, values);
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            String value = values.get(i);
            partitionKeys.add(new HivePartitionKey(name, value));
        }
        return partitionKeys.build();
    }

    public static HoodieTableMetaClient buildTableMetaClient(
            TrinoFileSystem fileSystem,
            String basePath)
    {
        return HoodieTableMetaClient.builder()
                .setStorage(new HudiTrinoStorage(fileSystem, new TrinoStorageConfiguration()))
                .setBasePath(basePath)
                .build();
    }

    public static Schema constructSchema(List<String> columnNames, List<HiveType> columnTypes, boolean withMetaColumns)
    {
        // create instance of this class to keep nested record naming consistent for any given inputs
        AvroHiveFileUtils recordIncrementingUtil = new AvroHiveFileUtils();
        SchemaBuilder.RecordBuilder<Schema> schemaBuilder = SchemaBuilder.record("baseRecord");
        SchemaBuilder.FieldAssembler<Schema> fieldBuilder = schemaBuilder.fields();

        if (withMetaColumns) {
            for (String metaFieldName : HOODIE_META_COLUMNS) {
                fieldBuilder = fieldBuilder
                        .name(metaFieldName)
                        .type(METADATA_FIELD_SCHEMA)
                        .withDefault(null);
            }
        }

        for (int i = 0; i < columnNames.size(); ++i) {
            Schema fieldSchema = recordIncrementingUtil.avroSchemaForHiveType(columnTypes.get(i));
            fieldBuilder = fieldBuilder
                    .name(columnNames.get(i))
                    .type(fieldSchema)
                    .withDefault(null);
        }
        return fieldBuilder.endRecord();
    }

    public static List<HiveColumnHandle> prependHudiMetaColumns(List<HiveColumnHandle> dataColumns) {
        List<HiveColumnHandle> columns = new ArrayList<>();
        columns.addAll(IntStream.range(0, HOODIE_META_COLUMNS.size())
                .boxed()
                .map(i -> new HiveColumnHandle(
                        HOODIE_META_COLUMNS.get(i),
                        i,
                        HiveType.HIVE_STRING,
                        VarcharType.VARCHAR,
                        Optional.empty(),
                        HiveColumnHandle.ColumnType.REGULAR, Optional.empty()))
                .toList());
        columns.addAll(dataColumns);
        return columns;
    }
}
