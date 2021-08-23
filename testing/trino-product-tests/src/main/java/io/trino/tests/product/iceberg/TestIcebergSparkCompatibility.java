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
package io.trino.tests.product.iceberg;

import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestIcebergSparkCompatibility
        extends ProductTest
{
    // TODO: Spark SQL doesn't yet support decimal.  When it does add it to the test.
    // TODO: Spark SQL only stores TIMESTAMP WITH TIME ZONE, and Iceberg only supports
    // TIMESTAMP with no time zone.  The Spark writes/Trino reads test can pass by
    // stripping off the UTC.  However, I haven't been able to get the
    // Trino writes/Spark reads test TIMESTAMPs to match.

    // see spark-defaults.conf
    private static final String SPARK_CATALOG = "iceberg_test";
    private static final String TRINO_CATALOG = "iceberg";
    private static final String TEST_SCHEMA_NAME = "default";

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoReadingSparkData()
    {
        String baseTableName = "test_trino_reading_primitive_types";
        String sparkTableName = sparkTableName(baseTableName);

        String sparkTableDefinition =
                "CREATE TABLE %s (" +
                        "  _string STRING" +
                        ", _bigint BIGINT" +
                        ", _integer INTEGER" +
                        ", _real REAL" +
                        ", _double DOUBLE" +
                        ", _boolean BOOLEAN" +
                        ", _timestamp TIMESTAMP" +
                        ", _date DATE" +
                        ") USING ICEBERG";
        onSpark().executeQuery(format(sparkTableDefinition, sparkTableName));

        // Validate queries on an empty table created by Spark
        String snapshotsTable = trinoTableName("\"" + baseTableName + "$snapshots\"");
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", snapshotsTable))).hasNoRows();
        QueryResult emptyResult = onTrino().executeQuery(format("SELECT * FROM %s", trinoTableName(baseTableName)));
        assertThat(emptyResult).hasNoRows();

        String values = "VALUES (" +
                "'a_string'" +
                ", 1000000000000000" +
                ", 1000000000" +
                ", 10000000.123" +
                ", 100000000000.123" +
                ", true" +
                ", TIMESTAMP '2020-06-28 14:16:00.456'" +
                ", DATE '1950-06-28'" +
                ")";
        String insert = format("INSERT INTO %s %s", sparkTableName, values);
        onSpark().executeQuery(insert);

        Row row = row(
                "a_string",
                1000000000000000L,
                1000000000,
                10000000.123F,
                100000000000.123,
                true,
                Timestamp.valueOf("2020-06-28 14:16:00.456"),
                Date.valueOf("1950-06-28"));

        String startOfSelect = "SELECT _string, _bigint, _integer, _real, _double, _boolean";
        QueryResult sparkSelect = onSpark().executeQuery(format("%s, _timestamp, _date FROM %s", startOfSelect, sparkTableName));
        assertThat(sparkSelect).containsOnly(row);

        QueryResult trinoSelect = onTrino().executeQuery(format("%s, CAST(_timestamp AS TIMESTAMP), _date FROM %s", startOfSelect, trinoTableName(baseTableName)));
        assertThat(trinoSelect).containsOnly(row);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testSparkReadingTrinoData()
    {
        String baseTableName = "test_spark_reading_primitive_types";
        String trinoTableName = trinoTableName(baseTableName);

        String trinoTableDefinition =
                "CREATE TABLE %s (" +
                        "  _string VARCHAR" +
                        ", _bigint BIGINT" +
                        ", _integer INTEGER" +
                        ", _real REAL" +
                        ", _double DOUBLE" +
                        ", _boolean BOOLEAN" +
                        //", _timestamp TIMESTAMP" +
                        ", _date DATE" +
                        ") WITH (format = 'ORC')";
        onTrino().executeQuery(format(trinoTableDefinition, trinoTableName));

        String values = "VALUES (" +
                "'a_string'" +
                ", 1000000000000000" +
                ", 1000000000" +
                ", 10000000.123" +
                ", 100000000000.123" +
                ", true" +
                //", TIMESTAMP '2020-06-28 14:16:00.456'" +
                ", DATE '1950-06-28'" +
                ")";
        String insert = format("INSERT INTO %s %s", trinoTableName, values);
        onTrino().executeQuery(insert);

        Row row = row(
                "a_string",
                1000000000000000L,
                1000000000,
                10000000.123F,
                100000000000.123,
                true,
                //"2020-06-28 14:16:00.456",
                "1950-06-28");
        String startOfSelect = "SELECT _string, _bigint, _integer, _real, _double, _boolean";
        QueryResult trinoSelect = onTrino().executeQuery(format("%s, /* CAST(_timestamp AS VARCHAR),*/ CAST(_date AS VARCHAR) FROM %s", startOfSelect, trinoTableName));
        assertThat(trinoSelect).containsOnly(row);

        QueryResult sparkSelect = onSpark().executeQuery(format("%s, /* CAST(_timestamp AS STRING),*/ CAST(_date AS STRING) FROM %s", startOfSelect, sparkTableName(baseTableName)));
        assertThat(sparkSelect).containsOnly(row);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testSparkCreatesTrinoDrops()
    {
        String baseTableName = "test_spark_creates_trino_drops";
        onSpark().executeQuery(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG", sparkTableName(baseTableName)));
        onTrino().executeQuery("DROP TABLE " + trinoTableName(baseTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoCreatesSparkDrops()
    {
        String baseTableName = "test_trino_creates_spark_drops";
        onTrino().executeQuery(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT)", trinoTableName(baseTableName)));
        onSpark().executeQuery("DROP TABLE " + sparkTableName(baseTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testSparkReadsTrinoPartitionedTable()
    {
        String baseTableName = "test_spark_reads_trino_partitioned_table";
        String trinoTableName = trinoTableName(baseTableName);
        onTrino().executeQuery(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT) WITH (partitioning = ARRAY['_string'])", trinoTableName));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a', 1001), ('b', 1002), ('c', 1003)", trinoTableName));

        Row row = row("b", 1002);
        String select = "SELECT * FROM %s WHERE _string = 'b'";
        assertThat(onTrino().executeQuery(format(select, trinoTableName)))
                .containsOnly(row);
        assertThat(onSpark().executeQuery(format(select, sparkTableName(baseTableName))))
                .containsOnly(row);
        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoReadsSparkPartitionedTable()
    {
        String baseTableName = "test_trino_reads_spark_partitioned_table";
        String sparkTableName = sparkTableName(baseTableName);
        onSpark().executeQuery(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG PARTITIONED BY (_string)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s VALUES ('a', 1001), ('b', 1002), ('c', 1003)", sparkTableName));

        Row row = row("b", 1002);
        String select = "SELECT * FROM %s WHERE _string = 'b'";
        assertThat(onSpark().executeQuery(format(select, sparkTableName)))
                .containsOnly(row);
        assertThat(onTrino().executeQuery(format(select, trinoTableName(baseTableName))))
                .containsOnly(row);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoReadingCompositeSparkData()
    {
        String baseTableName = "test_trino_reading_spark_composites";
        String sparkTableName = sparkTableName(baseTableName);

        String sparkTableDefinition = "" +
                "CREATE TABLE %s (" +
                "  doc_id string,\n" +
                "  info MAP<STRING, INT>,\n" +
                "  pets ARRAY<STRING>,\n" +
                "  user_info STRUCT<name:STRING, surname:STRING, age:INT, gender:STRING>)" +
                "  USING ICEBERG";
        onSpark().executeQuery(format(sparkTableDefinition, sparkTableName));

        String insert = "" +
                "INSERT INTO TABLE %s SELECT 'Doc213', map('age', 28, 'children', 3), array('Dog', 'Cat', 'Pig'), \n" +
                "named_struct('name', 'Santa', 'surname', 'Claus','age', 1000,'gender', 'MALE')";
        onSpark().executeQuery(format(insert, sparkTableName));

        String trinoTableName = trinoTableName(baseTableName);
        String trinoSelect = "SELECT doc_id, info['age'], pets[2], user_info.surname FROM " + trinoTableName;

        QueryResult trinoResult = onTrino().executeQuery(trinoSelect);
        Row row = row("Doc213", 28, "Cat", "Claus");
        assertThat(trinoResult).containsOnly(row);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testSparkReadingCompositeTrinoData()
    {
        String baseTableName = "test_spark_reading_trino_composites";
        String trinoTableName = trinoTableName(baseTableName);

        String trinoTableDefinition = "" +
                "CREATE TABLE %s (" +
                "  doc_id VARCHAR,\n" +
                "  info MAP(VARCHAR, INTEGER),\n" +
                "  pets ARRAY(VARCHAR),\n" +
                "  user_info ROW(name VARCHAR, surname VARCHAR, age INTEGER, gender VARCHAR))";
        onTrino().executeQuery(format(trinoTableDefinition, trinoTableName));

        String insert = "INSERT INTO %s VALUES('Doc213', MAP(ARRAY['age', 'children'], ARRAY[28, 3]), ARRAY['Dog', 'Cat', 'Pig'], ROW('Santa', 'Claus', 1000, 'MALE'))";
        onTrino().executeQuery(format(insert, trinoTableName));

        String sparkTableName = sparkTableName(baseTableName);
        String sparkSelect = "SELECT doc_id, info['age'], pets[1], user_info.surname FROM " + sparkTableName;
        QueryResult sparkResult = onSpark().executeQuery(sparkSelect);
        Row row = row("Doc213", 28, "Cat", "Claus");
        assertThat(sparkResult).containsOnly(row);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoReadingNestedSparkData()
    {
        String baseTableName = "test_trino_reading_nested_spark_data";
        String sparkTableName = sparkTableName(baseTableName);

        String sparkTableDefinition = "" +
                "CREATE TABLE %s (\n" +
                "  doc_id STRING\n" +
                ", nested_map MAP<STRING, ARRAY<STRUCT<sname: STRING, snumber: INT>>>\n" +
                ", nested_array ARRAY<MAP<STRING, ARRAY<STRUCT<mname: STRING, mnumber: INT>>>>\n" +
                ", nested_struct STRUCT<name:STRING, complicated: ARRAY<MAP<STRING, ARRAY<STRUCT<mname: STRING, mnumber: INT>>>>>)\n" +
                " USING ICEBERG";
        onSpark().executeQuery(format(sparkTableDefinition, sparkTableName));

        String insert = "" +
                "INSERT INTO TABLE %s SELECT" +
                "  'Doc213'" +
                ", map('s1', array(named_struct('sname', 'ASName1', 'snumber', 201), named_struct('sname', 'ASName2', 'snumber', 202)))" +
                ", array(map('m1', array(named_struct('mname', 'MAS1Name1', 'mnumber', 301), named_struct('mname', 'MAS1Name2', 'mnumber', 302)))" +
                "       ,map('m2', array(named_struct('mname', 'MAS2Name1', 'mnumber', 401), named_struct('mname', 'MAS2Name2', 'mnumber', 402))))" +
                ", named_struct('name', 'S1'," +
                "               'complicated', array(map('m1', array(named_struct('mname', 'SAMA1Name1', 'mnumber', 301), named_struct('mname', 'SAMA1Name2', 'mnumber', 302)))" +
                "                                   ,map('m2', array(named_struct('mname', 'SAMA2Name1', 'mnumber', 401), named_struct('mname', 'SAMA2Name2', 'mnumber', 402)))))";
        onSpark().executeQuery(format(insert, sparkTableName));

        Row row = row("Doc213", "ASName2", 201, "MAS2Name1", 302, "SAMA1Name1", 402);

        String sparkSelect = "SELECT" +
                "  doc_id" +
                ", nested_map['s1'][1].sname" +
                ", nested_map['s1'][0].snumber" +
                ", nested_array[1]['m2'][0].mname" +
                ", nested_array[0]['m1'][1].mnumber" +
                ", nested_struct.complicated[0]['m1'][0].mname" +
                ", nested_struct.complicated[1]['m2'][1].mnumber" +
                "  FROM ";

        QueryResult sparkResult = onSpark().executeQuery(sparkSelect + sparkTableName);
        assertThat(sparkResult).containsOnly(row);

        String trinoTableName = trinoTableName(baseTableName);
        String trinoSelect = "SELECT" +
                "  doc_id" +
                ", nested_map['s1'][2].sname" +
                ", nested_map['s1'][1].snumber" +
                ", nested_array[2]['m2'][1].mname" +
                ", nested_array[1]['m1'][2].mnumber" +
                ", nested_struct.complicated[1]['m1'][1].mname" +
                ", nested_struct.complicated[2]['m2'][2].mnumber" +
                "  FROM ";

        QueryResult trinoResult = onTrino().executeQuery(trinoSelect + trinoTableName);
        assertThat(trinoResult).containsOnly(row);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testSparkReadingNestedTrinoData()
    {
        String baseTableName = "test_spark_reading_nested_trino_data";
        String trinoTableName = trinoTableName(baseTableName);

        String trinoTableDefinition = "" +
                "CREATE TABLE %s (\n" +
                "  doc_id VARCHAR\n" +
                ", nested_map MAP(VARCHAR, ARRAY(ROW(sname VARCHAR, snumber INT)))\n" +
                ", nested_array ARRAY(MAP(VARCHAR, ARRAY(ROW(mname VARCHAR, mnumber INT))))\n" +
                ", nested_struct ROW(name VARCHAR, complicated ARRAY(MAP(VARCHAR, ARRAY(ROW(mname VARCHAR, mnumber INT))))))";
        onTrino().executeQuery(format(trinoTableDefinition, trinoTableName));

        String insert = "" +
                "INSERT INTO %s SELECT" +
                "  'Doc213'" +
                ", map(array['s1'], array[array[row('ASName1', 201), row('ASName2', 202)]])" +
                ", array[map(array['m1'], array[array[row('MAS1Name1', 301), row('MAS1Name2', 302)]])" +
                "       ,map(array['m2'], array[array[row('MAS2Name1', 401), row('MAS2Name2', 402)]])]" +
                ", row('S1'" +
                "      ,array[map(array['m1'], array[array[row('SAMA1Name1', 301), row('SAMA1Name2', 302)]])" +
                "            ,map(array['m2'], array[array[row('SAMA2Name1', 401), row('SAMA2Name2', 402)]])])";
        onTrino().executeQuery(format(insert, trinoTableName));

        Row row = row("Doc213", "ASName2", 201, "MAS2Name1", 302, "SAMA1Name1", 402);

        String trinoSelect = "SELECT" +
                "  doc_id" +
                ", nested_map['s1'][2].sname" +
                ", nested_map['s1'][1].snumber" +
                ", nested_array[2]['m2'][1].mname" +
                ", nested_array[1]['m1'][2].mnumber" +
                ", nested_struct.complicated[1]['m1'][1].mname" +
                ", nested_struct.complicated[2]['m2'][2].mnumber" +
                "  FROM ";

        QueryResult trinoResult = onTrino().executeQuery(trinoSelect + trinoTableName);
        assertThat(trinoResult).containsOnly(row);

        String sparkTableName = sparkTableName(baseTableName);
        String sparkSelect = "SELECT" +
                "  doc_id" +
                ", nested_map['s1'][1].sname" +
                ", nested_map['s1'][0].snumber" +
                ", nested_array[1]['m2'][0].mname" +
                ", nested_array[0]['m1'][1].mnumber" +
                ", nested_struct.complicated[0]['m1'][0].mname" +
                ", nested_struct.complicated[1]['m2'][1].mnumber" +
                "  FROM ";

        QueryResult sparkResult = onSpark().executeQuery(sparkSelect + sparkTableName);
        assertThat(sparkResult).containsOnly(row);

        onTrino().executeQuery("DROP TABLE " + trinoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testIdBasedFieldMapping()
    {
        String baseTableName = "test_schema_evolution_for_nested_fields";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery(format(
                "CREATE TABLE %s (_struct STRUCT<rename:BIGINT, keep:BIGINT, drop_and_add:BIGINT, CaseSensitive:BIGINT>, _partition BIGINT)"
                        + " USING ICEBERG"
                        + " partitioned by (_partition)"
                        + " TBLPROPERTIES ('write.format.default' = 'orc')",
                sparkTableName));

        onSpark().executeQuery(format(
                "INSERT INTO TABLE %s SELECT "
                        + "named_struct('rename', 1, 'keep', 2, 'drop_and_add', 3, 'CaseSensitive', 4), "
                        + "1001",
                sparkTableName));

        // Alter nested fields using Spark. Trino does not support this yet.
        onSpark().executeQuery(format("ALTER TABLE %s RENAME COLUMN _struct.rename TO renamed", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s DROP COLUMN _struct.drop_and_add", sparkTableName));
        onSpark().executeQuery(format("ALTER TABLE %s ADD COLUMN _struct.drop_and_add BIGINT", sparkTableName));

        // TODO support Row (JAVA_OBJECT) in Tempto and switch to QueryAssert
        Assertions.assertThat(onTrino().executeQuery(format("SELECT * FROM %s", trinoTableName)).rows())
                .containsOnly(List.of(
                        rowBuilder()
                                // Rename does not change id
                                .addField("renamed", 1L)
                                .addField("keep", 2L)
                                .addField("CaseSensitive", 4L)
                                // Dropping and re-adding changes id
                                .addField("drop_and_add", null)
                                .build(),
                        1001L));

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoShowingSparkCreatedTables()
    {
        String sparkTable = "test_table_listing_for_spark";
        String trinoTable = "test_table_listing_for_trino";

        onSpark().executeQuery(format("CREATE TABLE %s (_integer INTEGER ) USING ICEBERG", sparkTableName(sparkTable)));
        onTrino().executeQuery(format("CREATE TABLE %s (_integer INTEGER )", trinoTableName(trinoTable)));

        assertThat(onTrino().executeQuery(format("SHOW TABLES FROM %s LIKE '%s'", TEST_SCHEMA_NAME, "test_table_listing_for_%")))
                .containsOnly(row(sparkTable), row(trinoTable));

        onSpark().executeQuery("DROP TABLE " + sparkTableName(sparkTable));
        onTrino().executeQuery("DROP TABLE " + trinoTableName(trinoTable));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testTrinoWritingDataWithObjectStorageLocationProvider()
    {
        String baseTableName = "test_object_storage_location_provider";
        String sparkTableName = sparkTableName(baseTableName);
        String trinoTableName = trinoTableName(baseTableName);
        String dataPath = "hdfs://hadoop-master:9000/user/hive/warehouse/test_object_storage_location_provider/obj-data";

        onSpark().executeQuery(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG TBLPROPERTIES (" +
                        "'write.object-storage.enabled'=true," +
                        "'write.object-storage.path'='%s')",
                sparkTableName, dataPath));
        onTrino().executeQuery(format("INSERT INTO %s VALUES ('a_string', 1000000000000000)", trinoTableName));

        Row result = row("a_string", 1000000000000000L);
        assertThat(onSpark().executeQuery(format("SELECT _string, _bigint FROM %s", sparkTableName))).containsOnly(result);
        assertThat(onTrino().executeQuery(format("SELECT _string, _bigint FROM %s", trinoTableName))).containsOnly(result);

        QueryResult queryResult = onTrino().executeQuery(format("SELECT file_path FROM %s", trinoTableName("\"" + baseTableName + "$files\"")));
        assertThat(queryResult).hasRowsCount(1).hasColumnsCount(1);
        assertTrue(((String) queryResult.row(0).get(0)).contains(dataPath));

        // TODO: support path override in Iceberg table creation: https://github.com/trinodb/trino/issues/8861
        assertQueryFailure(() -> onTrino().executeQuery("DROP TABLE " + trinoTableName))
                .hasMessageContaining("contains Iceberg path override properties and cannot be dropped from Trino");
        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    private static String sparkTableName(String tableName)
    {
        return format("%s.%s.%s", SPARK_CATALOG, TEST_SCHEMA_NAME, tableName);
    }

    private static String trinoTableName(String tableName)
    {
        return format("%s.%s.%s", TRINO_CATALOG, TEST_SCHEMA_NAME, tableName);
    }

    private io.trino.jdbc.Row.Builder rowBuilder()
    {
        return io.trino.jdbc.Row.builder();
    }
}
