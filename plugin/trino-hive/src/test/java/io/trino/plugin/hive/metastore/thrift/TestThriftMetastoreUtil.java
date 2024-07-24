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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.trino.hive.thrift.metastore.BinaryColumnStatsData;
import io.trino.hive.thrift.metastore.BooleanColumnStatsData;
import io.trino.hive.thrift.metastore.ColumnStatisticsObj;
import io.trino.hive.thrift.metastore.Date;
import io.trino.hive.thrift.metastore.DateColumnStatsData;
import io.trino.hive.thrift.metastore.DecimalColumnStatsData;
import io.trino.hive.thrift.metastore.DoubleColumnStatsData;
import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.hive.thrift.metastore.LongColumnStatsData;
import io.trino.hive.thrift.metastore.Order;
import io.trino.hive.thrift.metastore.PrincipalPrivilegeSet;
import io.trino.hive.thrift.metastore.SerDeInfo;
import io.trino.hive.thrift.metastore.SkewedInfo;
import io.trino.hive.thrift.metastore.StorageDescriptor;
import io.trino.hive.thrift.metastore.StringColumnStatsData;
import io.trino.plugin.hive.metastore.BooleanStatistics;
import io.trino.plugin.hive.metastore.DateStatistics;
import io.trino.plugin.hive.metastore.DecimalStatistics;
import io.trino.plugin.hive.metastore.DoubleStatistics;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.IntegerStatistics;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.binaryStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.booleanStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.dateStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.decimalStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.doubleStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.longStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.stringStats;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_OUTPUT_FORMAT;
import static io.trino.plugin.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiColumnStatistics;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreDecimal;
import static io.trino.plugin.hive.util.SerdeConstants.BIGINT_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.BINARY_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.BOOLEAN_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.DATE_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.DECIMAL_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.DOUBLE_TYPE_NAME;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_COMMENTS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.plugin.hive.util.SerdeConstants.SERIALIZATION_LIB;
import static io.trino.plugin.hive.util.SerdeConstants.STRING_TYPE_NAME;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestThriftMetastoreUtil
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
    private static final io.trino.hive.thrift.metastore.Table TEST_TABLE = new io.trino.hive.thrift.metastore.Table(
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

    private static final io.trino.hive.thrift.metastore.Partition TEST_PARTITION = new io.trino.hive.thrift.metastore.Partition(
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
    private static final io.trino.hive.thrift.metastore.Table TEST_TABLE_WITH_UNSUPPORTED_FIELDS = new io.trino.hive.thrift.metastore.Table(
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
    private static final io.trino.hive.thrift.metastore.Partition TEST_PARTITION_WITH_UNSUPPORTED_FIELDS = new io.trino.hive.thrift.metastore.Partition(
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

    // equivalent code:
    //   Properties expected = MetaStoreUtils.getTableMetadata(TEST_TABLE_WITH_UNSUPPORTED_FIELDS);
    //   expected.remove(COLUMN_NAME_DELIMITER);
    private static final Map<String, String> TEST_TABLE_METADATA = ImmutableMap.<String, String>builder()
            .put(BUCKET_COUNT_PROPERTY, "100")
            .put("bucket_field_name", "col2,col3")
            .put(LIST_COLUMNS, "col1,col2,col3")
            .put(LIST_COLUMN_COMMENTS, "comment1\0\0")
            .put(LIST_COLUMN_TYPES, "bigint:binary:string")
            .put(FILE_INPUT_FORMAT, "com.facebook.hive.orc.OrcInputFormat")
            .put(FILE_OUTPUT_FORMAT, "com.facebook.hive.orc.OrcOutputFormat")
            .put("k1", "v1")
            .put("k2", "v2")
            .put("k3", "v3")
            .put("location", "hdfs://VOL1:9000/db_name/table_name")
            .put("name", "db_name.table_name")
            .put("partition_columns", "pk1/pk2")
            .put("partition_columns.types", "string:string")
            .put("sdk1", "sdv1")
            .put("sdk2", "sdv2")
            .put(SERIALIZATION_LIB, "com.facebook.hive.orc.OrcSerde")
            .buildOrThrow();

    @Test
    public void testTableRoundTrip()
    {
        Table table = ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE, TEST_SCHEMA);
        io.trino.hive.thrift.metastore.Table metastoreApiTable = ThriftMetastoreUtil.toMetastoreApiTable(table, NO_PRIVILEGES);
        assertThat(metastoreApiTable).isEqualTo(TEST_TABLE);
    }

    @Test
    public void testPartitionRoundTrip()
    {
        Partition partition = ThriftMetastoreUtil.fromMetastoreApiPartition(TEST_PARTITION);
        io.trino.hive.thrift.metastore.Partition metastoreApiPartition = ThriftMetastoreUtil.toMetastoreApiPartition(partition);
        assertThat(metastoreApiPartition).isEqualTo(TEST_PARTITION);
    }

    @Test
    public void testHiveSchemaTable()
    {
        Map<String, String> actual = MetastoreUtil.getHiveSchema(ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS, TEST_SCHEMA));
        assertThat(actual).isEqualTo(TEST_TABLE_METADATA);
    }

    @Test
    public void testHiveSchemaPartition()
    {
        Map<String, String> actual = MetastoreUtil.getHiveSchema(ThriftMetastoreUtil.fromMetastoreApiPartition(TEST_PARTITION_WITH_UNSUPPORTED_FIELDS), ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS, TEST_SCHEMA));
        assertThat(actual).isEqualTo(TEST_TABLE_METADATA);
    }

    @Test
    public void testHiveSchemaCaseInsensitive()
    {
        List<FieldSchema> testSchema = TEST_SCHEMA.stream()
                .map(fieldSchema -> new FieldSchema(fieldSchema.getName(), fieldSchema.getType().toUpperCase(Locale.ENGLISH), fieldSchema.getComment()))
                .toList();
        Map<String, String> actualTable = MetastoreUtil.getHiveSchema(ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS, testSchema));
        assertThat(actualTable).isEqualTo(TEST_TABLE_METADATA);

        Map<String, String> actualPartition = MetastoreUtil.getHiveSchema(ThriftMetastoreUtil.fromMetastoreApiPartition(TEST_PARTITION_WITH_UNSUPPORTED_FIELDS), ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS, testSchema));
        assertThat(actualPartition).isEqualTo(TEST_TABLE_METADATA);
    }

    @Test
    public void testLongStatsToColumnStatistics()
    {
        LongColumnStatsData longColumnStatsData = new LongColumnStatsData();
        longColumnStatsData.setLowValue(0);
        longColumnStatsData.setHighValue(100);
        longColumnStatsData.setNumNulls(1);
        longColumnStatsData.setNumDVs(20);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BIGINT_TYPE_NAME, longStats(longColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setIntegerStatistics(new IntegerStatistics(OptionalLong.of(0), OptionalLong.of(100)))
                .setNullsCount(1)
                .setDistinctValuesWithNullCount(20)
                .build());
    }

    @Test
    public void testEmptyLongStatsToColumnStatistics()
    {
        LongColumnStatsData emptyLongColumnStatsData = new LongColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BIGINT_TYPE_NAME, longStats(emptyLongColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty()))
                .build());
    }

    @Test
    public void testDoubleStatsToColumnStatistics()
    {
        DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData();
        doubleColumnStatsData.setLowValue(0);
        doubleColumnStatsData.setHighValue(100);
        doubleColumnStatsData.setNumNulls(1);
        doubleColumnStatsData.setNumDVs(20);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DOUBLE_TYPE_NAME, doubleStats(doubleColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(0), OptionalDouble.of(100)))
                .setNullsCount(1)
                .setDistinctValuesWithNullCount(20)
                .build());
    }

    @Test
    public void testEmptyDoubleStatsToColumnStatistics()
    {
        DoubleColumnStatsData emptyDoubleColumnStatsData = new DoubleColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DOUBLE_TYPE_NAME, doubleStats(emptyDoubleColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()))
                .build());
    }

    @Test
    public void testDecimalStatsToColumnStatistics()
    {
        DecimalColumnStatsData decimalColumnStatsData = new DecimalColumnStatsData();
        BigDecimal low = new BigDecimal("0");
        decimalColumnStatsData.setLowValue(toMetastoreDecimal(low));
        BigDecimal high = new BigDecimal("100");
        decimalColumnStatsData.setHighValue(toMetastoreDecimal(high));
        decimalColumnStatsData.setNumNulls(1);
        decimalColumnStatsData.setNumDVs(20);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DECIMAL_TYPE_NAME, decimalStats(decimalColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setDecimalStatistics(new DecimalStatistics(Optional.of(low), Optional.of(high)))
                .setNullsCount(1)
                .setDistinctValuesWithNullCount(20)
                .build());
    }

    @Test
    public void testEmptyDecimalStatsToColumnStatistics()
    {
        DecimalColumnStatsData emptyDecimalColumnStatsData = new DecimalColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DECIMAL_TYPE_NAME, decimalStats(emptyDecimalColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty()))
                .build());
    }

    @Test
    public void testBooleanStatsToColumnStatistics()
    {
        BooleanColumnStatsData booleanColumnStatsData = new BooleanColumnStatsData();
        booleanColumnStatsData.setNumTrues(100);
        booleanColumnStatsData.setNumFalses(10);
        booleanColumnStatsData.setNumNulls(0);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BOOLEAN_TYPE_NAME, booleanStats(booleanColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setBooleanStatistics(new BooleanStatistics(OptionalLong.of(100), OptionalLong.of(10)))
                .setNullsCount(0)
                .build());
    }

    @Test
    public void testImpalaGeneratedBooleanStatistics()
    {
        BooleanColumnStatsData statsData = new BooleanColumnStatsData(1L, -1L, 2L);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BOOLEAN_TYPE_NAME, booleanStats(statsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty()))
                .setNullsCount(2)
                .build());
    }

    @Test
    public void testEmptyBooleanStatsToColumnStatistics()
    {
        BooleanColumnStatsData emptyBooleanColumnStatsData = new BooleanColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BOOLEAN_TYPE_NAME, booleanStats(emptyBooleanColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty()))
                .build());
    }

    @Test
    public void testDateStatsToColumnStatistics()
    {
        DateColumnStatsData dateColumnStatsData = new DateColumnStatsData();
        dateColumnStatsData.setLowValue(new Date(1000));
        dateColumnStatsData.setHighValue(new Date(2000));
        dateColumnStatsData.setNumNulls(1);
        dateColumnStatsData.setNumDVs(20);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DATE_TYPE_NAME, dateStats(dateColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(1000)), Optional.of(LocalDate.ofEpochDay(2000))))
                .setNullsCount(1)
                .setDistinctValuesWithNullCount(20)
                .build());
    }

    @Test
    public void testEmptyDateStatsToColumnStatistics()
    {
        DateColumnStatsData emptyDateColumnStatsData = new DateColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DATE_TYPE_NAME, dateStats(emptyDateColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty()))
                .build());
    }

    @Test
    public void testStringStatsToColumnStatistics()
    {
        StringColumnStatsData stringColumnStatsData = new StringColumnStatsData();
        stringColumnStatsData.setMaxColLen(100);
        stringColumnStatsData.setAvgColLen(23.333);
        stringColumnStatsData.setNumNulls(1);
        stringColumnStatsData.setNumDVs(20);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", STRING_TYPE_NAME, stringStats(stringColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setMaxValueSizeInBytes(100)
                .setAverageColumnLength(23.333)
                .setNullsCount(1)
                .setDistinctValuesWithNullCount(20)
                .build());
    }

    @Test
    public void testEmptyStringColumnStatsData()
    {
        StringColumnStatsData emptyStringColumnStatsData = new StringColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", STRING_TYPE_NAME, stringStats(emptyStringColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder().build());
    }

    @Test
    public void testBinaryStatsToColumnStatistics()
    {
        BinaryColumnStatsData binaryColumnStatsData = new BinaryColumnStatsData();
        binaryColumnStatsData.setMaxColLen(100);
        binaryColumnStatsData.setAvgColLen(22.2);
        binaryColumnStatsData.setNumNulls(2);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BINARY_TYPE_NAME, binaryStats(binaryColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder()
                .setMaxValueSizeInBytes(100)
                .setAverageColumnLength(22.2)
                .setNullsCount(2)
                .build());
    }

    @Test
    public void testEmptyBinaryStatsToColumnStatistics()
    {
        BinaryColumnStatsData emptyBinaryColumnStatsData = new BinaryColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BINARY_TYPE_NAME, binaryStats(emptyBinaryColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual).isEqualTo(HiveColumnStatistics.builder().build());
    }

    @Test
    public void testSingleDistinctValue()
    {
        DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData();
        doubleColumnStatsData.setNumNulls(10);
        doubleColumnStatsData.setNumDVs(1);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DOUBLE_TYPE_NAME, doubleStats(doubleColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual.getNullsCount()).isEqualTo(OptionalLong.of(10));
        assertThat(actual.getDistinctValuesWithNullCount()).isEqualTo(OptionalLong.of(1));

        doubleColumnStatsData = new DoubleColumnStatsData();
        doubleColumnStatsData.setNumNulls(10);
        doubleColumnStatsData.setNumDVs(1);
        columnStatisticsObj = new ColumnStatisticsObj("my_col", DOUBLE_TYPE_NAME, doubleStats(doubleColumnStatsData));
        actual = fromMetastoreApiColumnStatistics(columnStatisticsObj);

        assertThat(actual.getNullsCount()).isEqualTo(OptionalLong.of(10));
        assertThat(actual.getDistinctValuesWithNullCount()).isEqualTo(OptionalLong.of(1));
    }

    @Test
    public void testListApplicableRoles()
    {
        TrinoPrincipal admin = new TrinoPrincipal(USER, "admin");

        Multimap<String, String> inheritance = ImmutableMultimap.<String, String>builder()
                .put("a", "b1")
                .put("a", "b2")
                .put("b1", "d")
                .put("b1", "e")
                .put("b2", "d")
                .put("b2", "e")
                .put("d", "u")
                .put("e", "w")
                .build();

        assertThat(ThriftMetastoreUtil.listApplicableRoles(
                new HivePrincipal(ROLE, "a"),
                principal -> inheritance.get(principal.getName()).stream()
                        .map(name -> new RoleGrant(admin, name, false))
                        .collect(toImmutableSet())))
                .containsOnly(
                        new RoleGrant(admin, "b1", false),
                        new RoleGrant(admin, "b2", false),
                        new RoleGrant(admin, "d", false),
                        new RoleGrant(admin, "e", false),
                        new RoleGrant(admin, "u", false),
                        new RoleGrant(admin, "w", false));
    }
}
