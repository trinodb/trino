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
package io.trino.plugin.hive.metastore.glue.v2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.HiveType;
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import io.trino.plugin.hive.TableType;
import io.trino.spi.security.PrincipalType;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.metastore.TableInfo.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static java.lang.String.format;

public final class TestingMetastoreObjects
{
    private TestingMetastoreObjects() {}

    // --------------- Glue Objects ---------------

    public static Database getGlueTestDatabase()
    {
        return Database.builder()
                .name("test-db" + generateRandom())
                .description("database desc")
                .locationUri("/db")
                .parameters(ImmutableMap.of())
                .build();
    }

    public static Table getGlueTestTable(String dbName)
    {
        return Table.builder()
                .databaseName(dbName)
                .name("test-tbl" + generateRandom())
                .owner("owner")
                .parameters(ImmutableMap.of())
                .partitionKeys(ImmutableList.of(getGlueTestColumn()))
                .storageDescriptor(getGlueTestStorageDescriptor())
                .tableType(EXTERNAL_TABLE.name())
                .viewOriginalText("originalText")
                .viewExpandedText("expandedText")
                .build();
    }

    public static Table getGlueTestTrinoMaterializedView(String dbName)
    {
        return Table.builder()
                .databaseName(dbName)
                .name("test-mv" + generateRandom())
                .owner("owner")
                .parameters(ImmutableMap.of(PRESTO_VIEW_FLAG, "true", TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT))
                .tableType(TableType.VIRTUAL_VIEW.name())
                .viewOriginalText("/* %s: base64encodedquery */".formatted(ICEBERG_MATERIALIZED_VIEW_COMMENT))
                .viewExpandedText(ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .build();
    }

    public static Column getGlueTestColumn()
    {
        return getGlueTestColumn("string");
    }

    public static Column getGlueTestColumn(String type)
    {
        return Column.builder()
                .name("test-col" + generateRandom())
                .type(type)
                .comment("column comment")
                .build();
    }

    public static StorageDescriptor getGlueTestStorageDescriptor()
    {
        return getGlueTestStorageDescriptor(ImmutableList.of(getGlueTestColumn()), "SerdeLib");
    }

    public static StorageDescriptor getGlueTestStorageDescriptor(List<Column> columns, String serde)
    {
        return StorageDescriptor.builder()
                .bucketColumns(ImmutableList.of("test-bucket-col"))
                .columns(columns)
                .parameters(ImmutableMap.of())
                .serdeInfo(SerDeInfo.builder()
                        .serializationLibrary(serde)
                        .parameters(ImmutableMap.of())
                        .build())
                .inputFormat("InputFormat")
                .outputFormat("OutputFormat")
                .location("/test-tbl")
                .numberOfBuckets(1)
                .build();
    }

    public static Partition getGlueTestPartition(String dbName, String tblName, List<String> values)
    {
        return Partition.builder()
                .databaseName(dbName)
                .tableName(tblName)
                .values(values)
                .parameters(ImmutableMap.of())
                .storageDescriptor(getGlueTestStorageDescriptor())
                .build();
    }

    // --------------- Trino Objects ---------------

    public static io.trino.metastore.Database getTrinoTestDatabase()
    {
        return io.trino.metastore.Database.builder()
                .setDatabaseName("test-db" + generateRandom())
                .setComment(Optional.of("database desc"))
                .setLocation(Optional.of("/db"))
                .setParameters(ImmutableMap.of())
                .setOwnerName(Optional.of("PUBLIC"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
    }

    public static io.trino.metastore.Table getTrinoTestTable(String dbName)
    {
        return io.trino.metastore.Table.builder()
                .setDatabaseName(dbName)
                .setTableName("test-tbl" + generateRandom())
                .setOwner(Optional.of("owner"))
                .setParameters(ImmutableMap.of())
                .setTableType(TableType.EXTERNAL_TABLE.name())
                .setDataColumns(ImmutableList.of(getTrinoTestColumn()))
                .setPartitionColumns(ImmutableList.of(getTrinoTestColumn()))
                .setViewOriginalText(Optional.of("originalText"))
                .setViewExpandedText(Optional.of("expandedText"))
                .withStorage(STORAGE_CONSUMER).build();
    }

    public static io.trino.metastore.Partition getTrinoTestPartition(String dbName, String tblName, List<String> values)
    {
        return io.trino.metastore.Partition.builder()
                .setDatabaseName(dbName)
                .setTableName(tblName)
                .setValues(values)
                .setColumns(ImmutableList.of(getTrinoTestColumn()))
                .setParameters(ImmutableMap.of())
                .withStorage(STORAGE_CONSUMER).build();
    }

    public static io.trino.metastore.Column getTrinoTestColumn()
    {
        return new io.trino.metastore.Column("test-col" + generateRandom(), HiveType.HIVE_STRING, Optional.of("column comment"), Map.of());
    }

    private static final Consumer<Storage.Builder> STORAGE_CONSUMER = storage ->
            storage.setStorageFormat(StorageFormat.create("SerdeLib", "InputFormat", "OutputFormat"))
                    .setLocation("/test-tbl")
                    .setBucketProperty(Optional.empty())
                    .setSerdeParameters(ImmutableMap.of());

    private static String generateRandom()
    {
        return format("%04x", ThreadLocalRandom.current().nextInt());
    }
}
