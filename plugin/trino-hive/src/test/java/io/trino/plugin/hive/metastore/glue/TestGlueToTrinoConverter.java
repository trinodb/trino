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
package io.trino.plugin.hive.metastore.glue;

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter;
import io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.GluePartitionConverter;
import io.trino.spi.security.PrincipalType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static com.amazonaws.util.CollectionUtils.isNullOrEmpty;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestColumn;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestDatabase;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestPartition;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestStorageDescriptor;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestTable;
import static io.trino.plugin.hive.metastore.glue.TestingMetastoreObjects.getGlueTestTrinoMaterializedView;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getPartitionParameters;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableTypeNullable;
import static io.trino.plugin.hive.util.HiveUtil.DELTA_LAKE_PROVIDER;
import static io.trino.plugin.hive.util.HiveUtil.ICEBERG_TABLE_TYPE_NAME;
import static io.trino.plugin.hive.util.HiveUtil.ICEBERG_TABLE_TYPE_VALUE;
import static io.trino.plugin.hive.util.HiveUtil.SPARK_TABLE_PROVIDER_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestGlueToTrinoConverter
{
    private static final String PUBLIC_OWNER = "PUBLIC";

    private Database testDatabase;
    private Table testTable;
    private Partition testPartition;

    @BeforeEach
    public void setup()
    {
        testDatabase = getGlueTestDatabase();
        testTable = getGlueTestTable(testDatabase.getName());
        testPartition = getGlueTestPartition(testDatabase.getName(), testTable.getName(), ImmutableList.of("val1"));
    }

    private static GluePartitionConverter createPartitionConverter(Table table)
    {
        return new GluePartitionConverter(table.getDatabaseName(), table.getName());
    }

    @Test
    public void testConvertDatabase()
    {
        io.trino.plugin.hive.metastore.Database trinoDatabase = GlueToTrinoConverter.convertDatabase(testDatabase);
        assertThat(trinoDatabase.getDatabaseName()).isEqualTo(testDatabase.getName());
        assertThat(trinoDatabase.getLocation().get()).isEqualTo(testDatabase.getLocationUri());
        assertThat(trinoDatabase.getComment().get()).isEqualTo(testDatabase.getDescription());
        assertThat(trinoDatabase.getParameters()).isEqualTo(testDatabase.getParameters());
        assertThat(trinoDatabase.getOwnerName()).isEqualTo(Optional.of(PUBLIC_OWNER));
        assertThat(trinoDatabase.getOwnerType()).isEqualTo(Optional.of(PrincipalType.ROLE));
    }

    @Test
    public void testConvertTable()
    {
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
        assertThat(trinoTable.getTableName()).isEqualTo(testTable.getName());
        assertThat(trinoTable.getDatabaseName()).isEqualTo(testDatabase.getName());
        assertThat(trinoTable.getTableType()).isEqualTo(getTableTypeNullable(testTable));
        assertThat(trinoTable.getOwner().orElse(null)).isEqualTo(testTable.getOwner());
        assertThat(trinoTable.getParameters()).isEqualTo(getTableParameters(testTable));
        assertColumnList(trinoTable.getDataColumns(), testTable.getStorageDescriptor().getColumns());
        assertColumnList(trinoTable.getPartitionColumns(), testTable.getPartitionKeys());
        assertStorage(trinoTable.getStorage(), testTable.getStorageDescriptor());
        assertThat(trinoTable.getViewOriginalText().get()).isEqualTo(testTable.getViewOriginalText());
        assertThat(trinoTable.getViewExpandedText().get()).isEqualTo(testTable.getViewExpandedText());
    }

    @Test
    public void testConvertTableWithOpenCSVSerDe()
    {
        Table glueTable = getGlueTestTable(testDatabase.getName());
        glueTable.setStorageDescriptor(getGlueTestStorageDescriptor(
                ImmutableList.of(getGlueTestColumn("int")),
                "org.apache.hadoop.hive.serde2.OpenCSVSerde"));
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(glueTable, testDatabase.getName());

        assertThat(trinoTable.getTableName()).isEqualTo(glueTable.getName());
        assertThat(trinoTable.getDatabaseName()).isEqualTo(testDatabase.getName());
        assertThat(trinoTable.getTableType()).isEqualTo(getTableTypeNullable(glueTable));
        assertThat(trinoTable.getOwner().orElse(null)).isEqualTo(glueTable.getOwner());
        assertThat(trinoTable.getParameters()).isEqualTo(getTableParameters(glueTable));
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
        assertThat(trinoTable.getDataColumns().get(0).getType()).isEqualTo(HIVE_STRING);

        assertColumnList(trinoTable.getPartitionColumns(), glueTable.getPartitionKeys());
        assertStorage(trinoTable.getStorage(), glueTable.getStorageDescriptor());
        assertThat(trinoTable.getViewOriginalText().get()).isEqualTo(glueTable.getViewOriginalText());
        assertThat(trinoTable.getViewExpandedText().get()).isEqualTo(glueTable.getViewExpandedText());
    }

    @Test
    public void testConvertTableWithoutTableType()
    {
        Table table = getGlueTestTable(testDatabase.getName());
        table.setTableType(null);
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(table, testDatabase.getName());
        assertThat(trinoTable.getTableType()).isEqualTo(EXTERNAL_TABLE.name());
    }

    @Test
    public void testConvertTableNullPartitions()
    {
        testTable.setPartitionKeys(null);
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
        assertThat(trinoTable.getPartitionColumns().isEmpty()).isTrue();
    }

    @Test
    public void testConvertTableUppercaseColumnType()
    {
        com.amazonaws.services.glue.model.Column uppercaseColumn = getGlueTestColumn().withType("String");
        testTable.getStorageDescriptor().setColumns(ImmutableList.of(uppercaseColumn));
        GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
    }

    @Test
    public void testConvertPartition()
    {
        GluePartitionConverter converter = createPartitionConverter(testTable);
        io.trino.plugin.hive.metastore.Partition trinoPartition = converter.apply(testPartition);
        assertThat(trinoPartition.getDatabaseName()).isEqualTo(testPartition.getDatabaseName());
        assertThat(trinoPartition.getTableName()).isEqualTo(testPartition.getTableName());
        assertColumnList(trinoPartition.getColumns(), testPartition.getStorageDescriptor().getColumns());
        assertThat(trinoPartition.getValues()).isEqualTo(testPartition.getValues());
        assertStorage(trinoPartition.getStorage(), testPartition.getStorageDescriptor());
        assertThat(trinoPartition.getParameters()).isEqualTo(getPartitionParameters(testPartition));
    }

    @Test
    public void testPartitionConversionMemoization()
    {
        String fakeS3Location = "s3://some-fake-location";
        testPartition.getStorageDescriptor().setLocation(fakeS3Location);
        //  Second partition to convert with equal (but not aliased) values
        Partition partitionTwo = getGlueTestPartition("" + testDatabase.getName(), "" + testTable.getName(), new ArrayList<>(testPartition.getValues()));
        //  Ensure storage fields match as well
        partitionTwo.getStorageDescriptor().setColumns(new ArrayList<>(testPartition.getStorageDescriptor().getColumns()));
        partitionTwo.getStorageDescriptor().setBucketColumns(new ArrayList<>(testPartition.getStorageDescriptor().getBucketColumns()));
        partitionTwo.getStorageDescriptor().setLocation("" + fakeS3Location);
        partitionTwo.getStorageDescriptor().setInputFormat("" + testPartition.getStorageDescriptor().getInputFormat());
        partitionTwo.getStorageDescriptor().setOutputFormat("" + testPartition.getStorageDescriptor().getOutputFormat());
        partitionTwo.getStorageDescriptor().setParameters(new HashMap<>(testPartition.getStorageDescriptor().getParameters()));

        GluePartitionConverter converter = createPartitionConverter(testTable);
        io.trino.plugin.hive.metastore.Partition trinoPartition = converter.apply(testPartition);
        io.trino.plugin.hive.metastore.Partition trinoPartition2 = converter.apply(partitionTwo);

        assertThat(trinoPartition).isNotSameAs(trinoPartition2);
        assertThat(trinoPartition2.getDatabaseName()).isSameAs(trinoPartition.getDatabaseName());
        assertThat(trinoPartition2.getTableName()).isSameAs(trinoPartition.getTableName());
        assertThat(trinoPartition2.getColumns()).isSameAs(trinoPartition.getColumns());
        assertThat(trinoPartition2.getParameters()).isSameAs(trinoPartition.getParameters());
        assertThat(trinoPartition2.getValues()).isNotSameAs(trinoPartition.getValues());

        Storage storage = trinoPartition.getStorage();
        Storage storage2 = trinoPartition2.getStorage();

        assertThat(storage2.getStorageFormat()).isSameAs(storage.getStorageFormat());
        assertThat(storage2.getBucketProperty()).isSameAs(storage.getBucketProperty());
        assertThat(storage2.getSerdeParameters()).isSameAs(storage.getSerdeParameters());
        assertThat(storage2.getLocation()).isNotSameAs(storage.getLocation());
    }

    @Test
    public void testDatabaseNullParameters()
    {
        testDatabase.setParameters(null);
        assertThat(GlueToTrinoConverter.convertDatabase(testDatabase).getParameters()).isNotNull();
    }

    @Test
    public void testTableNullParameters()
    {
        testTable.setParameters(null);
        testTable.getStorageDescriptor().getSerdeInfo().setParameters(null);
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
        assertThat(trinoTable.getParameters()).isNotNull();
        assertThat(trinoTable.getStorage().getSerdeParameters()).isNotNull();
    }

    @Test
    public void testIcebergTableNullStorageDescriptor()
    {
        testTable.setParameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE));
        testTable.setStorageDescriptor(null);
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    public void testIcebergTableNonNullStorageDescriptor()
    {
        testTable.setParameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE));
        assertThat(testTable.getStorageDescriptor()).isNotNull();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    public void testDeltaTableNullStorageDescriptor()
    {
        testTable.setParameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER));
        testTable.setStorageDescriptor(null);
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    public void testDeltaTableNonNullStorageDescriptor()
    {
        testTable.setParameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER));
        assertThat(testTable.getStorageDescriptor()).isNotNull();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testTable, testDatabase.getName());
        assertThat(trinoTable.getDataColumns().stream()
                .map(Column::getName)
                .collect(toImmutableSet())).isEqualTo(testTable.getStorageDescriptor().getColumns().stream()
                .map(com.amazonaws.services.glue.model.Column::getName)
                .collect(toImmutableSet()));
    }

    @Test
    public void testIcebergMaterializedViewNullStorageDescriptor()
    {
        Table testMaterializedView = getGlueTestTrinoMaterializedView(testDatabase.getName());
        assertThat(testMaterializedView.getStorageDescriptor()).isNull();
        io.trino.plugin.hive.metastore.Table trinoTable = GlueToTrinoConverter.convertTable(testMaterializedView, testDatabase.getName());
        assertThat(trinoTable.getDataColumns().size()).isEqualTo(1);
    }

    @Test
    public void testPartitionNullParameters()
    {
        testPartition.setParameters(null);
        assertThat(createPartitionConverter(testTable).apply(testPartition).getParameters()).isNotNull();
    }

    private static void assertColumnList(List<Column> actual, List<com.amazonaws.services.glue.model.Column> expected)
    {
        if (expected == null) {
            assertThat(actual).isNull();
        }
        assertThat(actual.size()).isEqualTo(expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(Column actual, com.amazonaws.services.glue.model.Column expected)
    {
        assertThat(actual.getName()).isEqualTo(expected.getName());
        assertThat(actual.getType().getHiveTypeName().toString()).isEqualTo(expected.getType());
        assertThat(actual.getComment().get()).isEqualTo(expected.getComment());
    }

    private static void assertStorage(Storage actual, StorageDescriptor expected)
    {
        assertThat(actual.getLocation()).isEqualTo(expected.getLocation());
        assertThat(actual.getStorageFormat().getSerde()).isEqualTo(expected.getSerdeInfo().getSerializationLibrary());
        assertThat(actual.getStorageFormat().getInputFormat()).isEqualTo(expected.getInputFormat());
        assertThat(actual.getStorageFormat().getOutputFormat()).isEqualTo(expected.getOutputFormat());
        if (!isNullOrEmpty(expected.getBucketColumns())) {
            HiveBucketProperty bucketProperty = actual.getBucketProperty().get();
            assertThat(bucketProperty.bucketedBy()).isEqualTo(expected.getBucketColumns());
            assertThat(bucketProperty.bucketCount()).isEqualTo(expected.getNumberOfBuckets().intValue());
        }
    }
}
