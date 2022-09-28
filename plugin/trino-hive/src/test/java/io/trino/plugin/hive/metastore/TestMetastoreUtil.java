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
package io.trino.plugin.hive.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.bucketColumnHandle;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.COLUMN_NAME_DELIMITER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestMetastoreUtil
{
    private static final List<FieldSchema> TEST_SCHEMA = ImmutableList.of(
            new FieldSchema("col1", "bigint", "comment1"),
            new FieldSchema("col2", "binary", null),
            new FieldSchema("col3", "string", null));
    private static final StorageDescriptor TEST_STORAGE_DESCRIPTOR = new StorageDescriptor(
            TEST_SCHEMA,
            "hdfs://VOL1:9000/db_name/table_name",
            "com.facebook.hive.orc.OrcInputFormat",
            "com.facebook.hive.orc.OrcOutputFormat",
            false,
            100,
            new SerDeInfo("table_name", "com.facebook.hive.orc.OrcSerde", ImmutableMap.of("sdk1", "sdv1", "sdk2", "sdv2")),
            ImmutableList.of("col2", "col3"),
            ImmutableList.of(new Order("col2", 1)),
            ImmutableMap.of());
    private static final org.apache.hadoop.hive.metastore.api.Table TEST_TABLE = new org.apache.hadoop.hive.metastore.api.Table(
            "table_name",
            "db_name",
            "owner_name",
            0,
            0,
            0,
            TEST_STORAGE_DESCRIPTOR,
            ImmutableList.of(
                    new FieldSchema("pk1", "string", "comment pk1"),
                    new FieldSchema("pk2", "string", null)),
            ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"),
            "view original text",
            "view extended text",
            "MANAGED_TABLE");

    static {
        TEST_TABLE.setPrivileges(new PrincipalPrivilegeSet(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()));
    }

    private static final org.apache.hadoop.hive.metastore.api.Partition TEST_PARTITION = new org.apache.hadoop.hive.metastore.api.Partition(
            ImmutableList.of("pk1v", "pk2v"),
            "db_name",
            "table_name",
            0,
            0,
            TEST_STORAGE_DESCRIPTOR,
            ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"));
    private static final StorageDescriptor TEST_STORAGE_DESCRIPTOR_WITH_UNSUPPORTED_FIELDS = new StorageDescriptor(
            TEST_SCHEMA,
            "hdfs://VOL1:9000/db_name/table_name",
            "com.facebook.hive.orc.OrcInputFormat",
            "com.facebook.hive.orc.OrcOutputFormat",
            false,
            100,
            new SerDeInfo("table_name", "com.facebook.hive.orc.OrcSerde", ImmutableMap.of("sdk1", "sdv1", "sdk2", "sdv2")),
            ImmutableList.of("col2", "col3"),
            ImmutableList.of(new Order("col2", 0), new Order("col3", 1)),
            ImmutableMap.of("sk1", "sv1"));
    private static final org.apache.hadoop.hive.metastore.api.Table TEST_TABLE_WITH_UNSUPPORTED_FIELDS = new org.apache.hadoop.hive.metastore.api.Table(
            "table_name",
            "db_name",
            "owner_name",
            1234567890,
            1234567891,
            34,
            TEST_STORAGE_DESCRIPTOR_WITH_UNSUPPORTED_FIELDS,
            ImmutableList.of(
                    new FieldSchema("pk1", "string", "comment pk1"),
                    new FieldSchema("pk2", "string", null)),
            ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"),
            "view original text",
            "view extended text",
            "MANAGED_TABLE");
    private static final org.apache.hadoop.hive.metastore.api.Partition TEST_PARTITION_WITH_UNSUPPORTED_FIELDS = new org.apache.hadoop.hive.metastore.api.Partition(
            ImmutableList.of("pk1v", "pk2v"),
            "db_name",
            "table_name",
            1234567892,
            1234567893,
            TEST_STORAGE_DESCRIPTOR_WITH_UNSUPPORTED_FIELDS,
            ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"));

    static {
        TEST_STORAGE_DESCRIPTOR_WITH_UNSUPPORTED_FIELDS.setSkewedInfo(new SkewedInfo(
                ImmutableList.of("col1"),
                ImmutableList.of(ImmutableList.of("val1")),
                ImmutableMap.of(ImmutableList.of("val1"), "loc1")));
    }

    @Test
    public void testTableRoundTrip()
    {
        Table table = ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE, TEST_SCHEMA);
        org.apache.hadoop.hive.metastore.api.Table metastoreApiTable = ThriftMetastoreUtil.toMetastoreApiTable(table, NO_PRIVILEGES);
        assertEquals(metastoreApiTable, TEST_TABLE);
    }

    @Test
    public void testPartitionRoundTrip()
    {
        Partition partition = ThriftMetastoreUtil.fromMetastoreApiPartition(TEST_PARTITION);
        org.apache.hadoop.hive.metastore.api.Partition metastoreApiPartition = ThriftMetastoreUtil.toMetastoreApiPartition(partition);
        assertEquals(metastoreApiPartition, TEST_PARTITION);
    }

    @Test
    public void testHiveSchemaTable()
    {
        Properties expected = MetaStoreUtils.getTableMetadata(TEST_TABLE_WITH_UNSUPPORTED_FIELDS);
        expected.remove(COLUMN_NAME_DELIMITER);
        Properties actual = MetastoreUtil.getHiveSchema(ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS, TEST_SCHEMA));
        assertEquals(actual, expected);
    }

    @Test
    public void testHiveSchemaPartition()
    {
        Properties expected = MetaStoreUtils.getPartitionMetadata(TEST_PARTITION_WITH_UNSUPPORTED_FIELDS, TEST_TABLE_WITH_UNSUPPORTED_FIELDS);
        expected.remove(COLUMN_NAME_DELIMITER);
        Properties actual = MetastoreUtil.getHiveSchema(ThriftMetastoreUtil.fromMetastoreApiPartition(TEST_PARTITION_WITH_UNSUPPORTED_FIELDS), ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS, TEST_SCHEMA));
        assertEquals(actual, expected);
    }

    @Test
    public void testComputePartitionKeyFilter()
    {
        HiveColumnHandle dsColumn = partitionColumn("ds");
        HiveColumnHandle typeColumn = partitionColumn("type");
        List<HiveColumnHandle> partitionKeys = ImmutableList.of(dsColumn, typeColumn);

        Domain dsDomain = Domain.create(ValueSet.ofRanges(Range.lessThan(VARCHAR, utf8Slice("2018-05-06"))), false);
        Domain typeDomain = Domain.create(ValueSet.of(VARCHAR, utf8Slice("fruit")), false);

        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.<HiveColumnHandle, Domain>builder()
                .put(bucketColumnHandle(), Domain.create(ValueSet.of(INTEGER, 123L), false))
                .put(dsColumn, dsDomain)
                .put(typeColumn, typeDomain)
                .buildOrThrow());

        TupleDomain<String> filter = computePartitionKeyFilter(partitionKeys, tupleDomain);
        assertThat(filter.getDomains())
                .as("output contains only the partition keys")
                .contains(ImmutableMap.<String, Domain>builder()
                        .put("ds", dsDomain)
                        .put("type", typeDomain)
                        .buildOrThrow());
    }

    private static HiveColumnHandle partitionColumn(String name)
    {
        return new HiveColumnHandle(name, 0, HIVE_STRING, VARCHAR, Optional.empty(), PARTITION_KEY, Optional.empty());
    }
}
