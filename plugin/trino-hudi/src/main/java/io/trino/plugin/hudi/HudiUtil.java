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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_META_CLIENT_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNSUPPORTED_FILE_FORMAT;
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

    public static List<HivePartitionKey> buildPartitionKeys(List<HiveColumnHandle> keys, List<String> values)
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
            String tableName,
            String basePath)
    {
        try {
            return HoodieTableMetaClient.builder()
                    .setStorage(new HudiTrinoStorage(fileSystem, new TrinoStorageConfiguration()))
                    .setBasePath(basePath)
                    .build();
        }
        catch (TableNotFoundException e) {
            throw new TrinoException(HUDI_BAD_DATA,
                    "Location of table %s does not contain Hudi table metadata: %s".formatted(tableName, basePath));
        }
        catch (Throwable e) {
            throw new TrinoException(HUDI_META_CLIENT_ERROR,
                    "Unable to load Hudi meta client for table %s (%s)".formatted(tableName, basePath));
        }
    }

    public static Schema constructSchema(List<String> columnNames, List<HiveType> columnTypes)
    {
        // Convert lists into the format expected by the utility class
        String columnNamesString = String.join(",", columnNames);
        String columnTypesString = columnTypes.stream()
                .map(HiveType::getHiveTypeName)
                .map(Object::toString)
                .collect(Collectors.joining(":"));

        // Create the properties map
        Map<String, String> properties = new HashMap<>();
        properties.put(LIST_COLUMNS, columnNamesString);
        properties.put(LIST_COLUMN_TYPES, columnTypesString);

        // Call the public static method to build the schema
        try {
            // Pass null for the file system as we are not reading from a URL
            return AvroHiveFileUtils.determineSchemaOrThrowException(null, properties);
        }
        catch (IOException e) {
            // The IOException is declared on the method, but this path shouldn't throw it
            throw new UncheckedIOException("Failed to construct Avro schema", e);
        }
    }

    public static Schema constructSchema(Schema dataSchema, List<String> columnNames)
    {
        SchemaBuilder.RecordBuilder<Schema> schemaBuilder = SchemaBuilder.record("baseRecord");
        SchemaBuilder.FieldAssembler<Schema> fieldBuilder = schemaBuilder.fields();
        for (String columnName : columnNames) {
            Schema originalFieldSchema = dataSchema.getField(columnName).schema();
            Schema typeForNewField;

            // Check if the original field schema is already nullable (i.e., a UNION containing NULL)
            if (originalFieldSchema.isNullable()) {
                typeForNewField = originalFieldSchema;
            }
            else {
                typeForNewField = Schema.createUnion(Schema.create(Schema.Type.NULL), originalFieldSchema);
            }

            fieldBuilder = fieldBuilder
                    .name(columnName)
                    .type(typeForNewField)
                    .withDefault(null);
        }
        return fieldBuilder.endRecord();
    }

    public static List<HiveColumnHandle> prependHudiMetaColumns(List<HiveColumnHandle> dataColumns)
    {
        //For efficient lookup
        Set<String> dataColumnNames = dataColumns.stream()
                .map(HiveColumnHandle::getName)
                .collect(Collectors.toSet());

        // If all Hudi meta columns are already present, return the original list
        if (dataColumnNames.containsAll(HOODIE_META_COLUMNS)) {
            return dataColumns;
        }

        // Identify only the meta columns that are missing from dataColumns to avoid duplicates
        List<String> missingMetaColumns = HOODIE_META_COLUMNS.stream()
                .filter(metaColumn -> !dataColumnNames.contains(metaColumn))
                .toList();

        List<HiveColumnHandle> columns = new ArrayList<>();

        // Create and prepend the new HiveColumnHandles for the missing meta columns
        columns.addAll(IntStream.range(0, missingMetaColumns.size())
                .boxed()
                .map(i -> new HiveColumnHandle(
                        missingMetaColumns.get(i),
                        i,
                        HiveType.HIVE_STRING,
                        VarcharType.VARCHAR,
                        Optional.empty(),
                        HiveColumnHandle.ColumnType.REGULAR,
                        Optional.empty()))
                .toList());

        // Add all the original data columns after the new meta columns
        columns.addAll(dataColumns);

        return columns;
    }

    public static FileSlice convertToFileSlice(HudiSplit split, String basePath)
    {
        String dataFilePath = split.getBaseFile().isPresent()
                ? split.getBaseFile().get().getPath()
                : split.getLogFiles().getFirst().getPath();
        String fileId = FSUtils.getFileIdFromFileName(new StoragePath(dataFilePath).getName());
        HoodieBaseFile baseFile = split.getBaseFile().isPresent()
                ? new HoodieBaseFile(dataFilePath, fileId, split.getCommitTime(), null)
                : null;

        return new FileSlice(
                new HoodieFileGroupId(FSUtils.getRelativePartitionPath(new StoragePath(basePath), new StoragePath(dataFilePath)), fileId),
                split.getCommitTime(),
                baseFile,
                split.getLogFiles().stream().map(lf -> new HoodieLogFile(lf.getPath())).toList());
    }

    public static HoodieTableFileSystemView getFileSystemView(
            HoodieTableMetadata tableMetadata,
            HoodieTableMetaClient metaClient)
    {
        return new HoodieTableFileSystemView(
                tableMetadata, metaClient, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
    }
}
