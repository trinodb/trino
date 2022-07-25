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
package io.trino.plugin.hive.metastore.alluxio;

import alluxio.grpc.table.Layout;
import alluxio.shaded.client.com.google.protobuf.ByteString;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestProtoUtils
{
    @Test
    public void testDatabaseNameLocation()
    {
        Database db = ProtoUtils.fromProto(TestingAlluxioMetastoreObjects.getTestingDatabase().build());
        assertEquals(TestingAlluxioMetastoreObjects.DATABASE_NAME, db.getDatabaseName());
        assertEquals("alluxio:///", db.getLocation().get());
        // Intentionally leave location unset
        alluxio.grpc.table.Database.Builder alluxioDb = TestingAlluxioMetastoreObjects.getTestingDatabase()
                .clearLocation();
        assertEquals(Optional.empty(), ProtoUtils.fromProto(alluxioDb.build()).getLocation());
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testTableMissingLayout()
    {
        ProtoUtils.fromProto(TestingAlluxioMetastoreObjects.getTestingTableInfo().clearLayout().build());
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testTableNonHiveLayout()
    {
        alluxio.grpc.table.TableInfo.Builder alluxioTable = alluxio.grpc.table.TableInfo.newBuilder()
                .setLayout(TestingAlluxioMetastoreObjects.getTestingNonHiveLayout());
        ProtoUtils.fromProto(alluxioTable.build());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTableBadLayoutBytes()
    {
        Layout.Builder alluxioLayout = TestingAlluxioMetastoreObjects.getTestingHiveLayout()
                .setLayoutData(ByteString.copyFrom(new byte[] {'z', 'z', 'z'}));
        alluxio.grpc.table.TableInfo.Builder alluxioTable = TestingAlluxioMetastoreObjects.getTestingTableInfo()
                .setLayout(alluxioLayout);
        ProtoUtils.fromProto(alluxioTable.build());
    }

    @Test
    public void testTable()
    {
        alluxio.grpc.table.TableInfo.Builder table = TestingAlluxioMetastoreObjects.getTestingTableInfo();
        alluxio.grpc.table.FieldSchema fieldSchema = TestingAlluxioMetastoreObjects.getTestingFieldSchema().build();
        Table t = ProtoUtils.fromProto(table.build());
        Column c = t.getColumn(TestingAlluxioMetastoreObjects.COLUMN_NAME).get();
        assertEquals(table.getDbName(), t.getDatabaseName());
        assertEquals(table.getTableName(), t.getTableName());
        assertEquals(table.getOwner(), t.getOwner().orElse(null));
        assertEquals(table.getType().toString(), t.getTableType());
        assertEquals(0, t.getDataColumns().size());
        assertEquals(1, t.getPartitionColumns().size());
        assertEquals(table.getParametersMap(), t.getParameters());
        assertEquals(Optional.empty(), t.getViewOriginalText());
        assertEquals(Optional.empty(), t.getViewExpandedText());
        assertEquals(fieldSchema.getName(), c.getName());
        assertEquals(fieldSchema.getComment(), c.getComment().get());
        assertEquals(fieldSchema.getType(), c.getType().toString());
        Storage s = t.getStorage();
        alluxio.grpc.table.layout.hive.Storage storage = TestingAlluxioMetastoreObjects.getTestingPartitionInfo().getStorage();
        assertEquals(storage.getSkewed(), s.isSkewed());
        assertEquals(ProtoUtils.fromProto(storage.getStorageFormat()), s.getStorageFormat());
        assertEquals(storage.getLocation(), s.getLocation());
        assertEquals(ProtoUtils.fromProto(table.getParametersMap(), storage.getBucketProperty()), s.getBucketProperty());
        assertEquals(storage.getStorageFormat().getSerdelibParametersMap(), s.getSerdeParameters());
    }

    @Test
    public void testSortingColumn()
    {
        alluxio.grpc.table.layout.hive.SortingColumn.Builder column = TestingAlluxioMetastoreObjects.getTestingSortingColumn();
        SortingColumn c = ProtoUtils.fromProto(column.build());
        assertEquals(column.getColumnName(), c.getColumnName());
        assertEquals(SortingColumn.Order.valueOf(column.getOrder().toString()), c.getOrder());
    }

    @Test
    public void testBucketProperty()
    {
        alluxio.grpc.table.layout.hive.HiveBucketProperty.Builder bucketProperty = TestingAlluxioMetastoreObjects.getTestingHiveBucketProperty();
        Optional<HiveBucketProperty> bp = ProtoUtils.fromProto(TestingAlluxioMetastoreObjects.getTestingTableInfo().getParametersMap(), bucketProperty.build());
        assertTrue(bp.isPresent());
        assertEquals(Collections.singletonList(ProtoUtils.fromProto(TestingAlluxioMetastoreObjects.getTestingSortingColumn().build())),
                bp.get().getSortedBy());
        assertEquals(1, bp.get().getSortedBy().size());
        assertEquals(bucketProperty.getBucketedByCount(), bp.get().getBucketCount());
        assertEquals(HiveBucketing.BucketingVersion.BUCKETING_V1, bp.get().getBucketingVersion());
    }

    @Test
    public void testBucketPropertyNoBuckets()
    {
        alluxio.grpc.table.layout.hive.HiveBucketProperty.Builder bucketProperty = TestingAlluxioMetastoreObjects.getTestingHiveBucketProperty();
        bucketProperty.clearBucketCount();
        Map<String, String> tableParameters = TestingAlluxioMetastoreObjects.getTestingTableInfo().getParametersMap();
        Optional<HiveBucketProperty> bp = ProtoUtils.fromProto(tableParameters, bucketProperty.build());
        assertFalse(bp.isPresent());

        bucketProperty = TestingAlluxioMetastoreObjects.getTestingHiveBucketProperty();
        bucketProperty.setBucketCount(0);
        bp = ProtoUtils.fromProto(tableParameters, bucketProperty.build());
        assertFalse(bp.isPresent());
    }

    @Test
    public void testStorageFormat()
    {
        alluxio.grpc.table.layout.hive.StorageFormat.Builder storageFormat = TestingAlluxioMetastoreObjects.getTestingStorageFormat();
        StorageFormat fmt = ProtoUtils.fromProto(storageFormat.build());
        assertEquals(storageFormat.getSerde(), fmt.getSerde());
        assertEquals(storageFormat.getInputFormat(), fmt.getInputFormat());
        assertEquals(storageFormat.getOutputFormat(), fmt.getOutputFormat());
    }

    @Test
    public void testColumn()
    {
        alluxio.grpc.table.FieldSchema.Builder fieldSchema = TestingAlluxioMetastoreObjects.getTestingFieldSchema();
        Column column = ProtoUtils.fromProto(fieldSchema.build());
        assertTrue(column.getComment().isPresent());
        assertEquals(fieldSchema.getComment(), column.getComment().get());
        assertEquals(fieldSchema.getName(), column.getName());
        assertEquals(HiveType.valueOf(fieldSchema.getType()), column.getType());
    }

    @Test
    public void testColumnNoComment()
    {
        alluxio.grpc.table.FieldSchema.Builder fieldSchema = TestingAlluxioMetastoreObjects.getTestingFieldSchema();
        fieldSchema.clearComment();
        Column column = ProtoUtils.fromProto(fieldSchema.build());
        assertFalse(column.getComment().isPresent());
        assertEquals(fieldSchema.getName(), column.getName());
        assertEquals(HiveType.valueOf(fieldSchema.getType()), column.getType());
    }

    @Test
    public void testPartition()
    {
        alluxio.grpc.table.layout.hive.PartitionInfo.Builder partitionInfo = TestingAlluxioMetastoreObjects.getTestingPartitionInfo();
        Partition partition = ProtoUtils.fromProto(partitionInfo.build());
        assertEquals(
                partitionInfo.getDataColsList().stream().map(ProtoUtils::fromProto).collect(Collectors.toList()),
                partition.getColumns());
        assertEquals(partitionInfo.getDbName(), partition.getDatabaseName());
        assertEquals(partitionInfo.getParametersMap(), partition.getParameters());
        assertEquals(partitionInfo.getValuesList(), partition.getValues());
        assertEquals(partitionInfo.getTableName(), partition.getTableName());

        Storage s = partition.getStorage();
        alluxio.grpc.table.layout.hive.Storage storage =
                TestingAlluxioMetastoreObjects.getTestingPartitionInfo().getStorage();
        assertEquals(storage.getSkewed(), s.isSkewed());
        assertEquals(ProtoUtils.fromProto(storage.getStorageFormat()), s.getStorageFormat());
        assertEquals(storage.getLocation(), s.getLocation());
        assertEquals(ProtoUtils.fromProto(partitionInfo.getParametersMap(), storage.getBucketProperty()), s.getBucketProperty());
        assertEquals(storage.getStorageFormat().getSerdelibParametersMap(), s.getSerdeParameters());
    }
}
