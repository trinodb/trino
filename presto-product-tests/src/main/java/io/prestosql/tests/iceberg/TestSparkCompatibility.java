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
package io.prestosql.tests.iceberg;

import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;

import static io.prestosql.tempto.assertions.QueryAssert.Row;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tests.TestGroups.ICEBERG;
import static io.prestosql.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static io.prestosql.tests.utils.QueryExecutors.onSpark;
import static java.lang.String.format;

public class TestSparkCompatibility
        extends ProductTest
{
    // TODO: Spark SQL doesn't yet support decimal.  When it does add it to the test.
    // TODO: Spark SQL only stores TIMESTAMP WITH TIME ZONE, and Iceberg only supports
    // TIMESTAMP with no time zone.  The Spark writes/Presto reads test can pass by
    // stripping off the UTC.  However, I haven't been able to get the
    // Presto writes/Spark reads test TIMESTAMPs to match.

    // see spark-defaults.conf
    private static final String SPARK_CATALOG = "iceberg_test";
    private static final String PRESTO_CATALOG = "iceberg";

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testPrestoReadingSparkData()
    {
        String baseTableName = "test_presto_reading_primitive_types";
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

        QueryResult prestoSelect = onPresto().executeQuery(format("%s, CAST(_timestamp AS TIMESTAMP), _date FROM %s", startOfSelect, prestoTableName(baseTableName)));
        assertThat(prestoSelect).containsOnly(row);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testSparkReadingPrestoData()
    {
        String baseTableName = "test_spark_reading_primitive_types";
        String prestoTableName = prestoTableName(baseTableName);

        String prestoTableDefinition =
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
        onPresto().executeQuery(format(prestoTableDefinition, prestoTableName));

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
        String insert = format("INSERT INTO %s %s", prestoTableName, values);
        onPresto().executeQuery(insert);

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
        QueryResult prestoSelect = onPresto().executeQuery(format("%s, /* CAST(_timestamp AS VARCHAR),*/ CAST(_date AS VARCHAR) FROM %s", startOfSelect, prestoTableName));
        assertThat(prestoSelect).containsOnly(row);

        QueryResult sparkSelect = onSpark().executeQuery(format("%s, /* CAST(_timestamp AS STRING),*/ CAST(_date AS STRING) FROM %s", startOfSelect, sparkTableName(baseTableName)));
        assertThat(sparkSelect).containsOnly(row);

        onPresto().executeQuery("DROP TABLE " + prestoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testSparkCreatesPrestoDrops()
    {
        String baseTableName = "test_spark_creates_presto_drops";
        onSpark().executeQuery(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG", sparkTableName(baseTableName)));
        onPresto().executeQuery("DROP TABLE " + prestoTableName(baseTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testPrestoCreatesSparkDrops()
    {
        String baseTableName = "test_presto_creates_spark_drops";
        onPresto().executeQuery(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT)", prestoTableName(baseTableName)));
        onSpark().executeQuery("DROP TABLE " + sparkTableName(baseTableName));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testSparkReadsPrestoPartitionedTable()
    {
        String baseTableName = "test_spark_reads_presto_partitioned_table";
        String prestoTableName = prestoTableName(baseTableName);
        onPresto().executeQuery(format("CREATE TABLE %s (_string VARCHAR, _bigint BIGINT) WITH (partitioning = ARRAY['_string'])", prestoTableName));
        onPresto().executeQuery(format("INSERT INTO %s VALUES ('a', 1001), ('b', 1002), ('c', 1003)", prestoTableName));

        Row row = row("b", 1002);
        String select = "SELECT * FROM %s WHERE _string = 'b'";
        assertThat(onPresto().executeQuery(format(select, prestoTableName)))
                .containsOnly(row);
        assertThat(onSpark().executeQuery(format(select, sparkTableName(baseTableName))))
                .containsOnly(row);
        onPresto().executeQuery("DROP TABLE " + prestoTableName);
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testPrestoReadsSparkPartitionedTable()
    {
        String baseTableName = "test_spark_reads_presto_partitioned_table";
        String sparkTableName = sparkTableName(baseTableName);
        onSpark().executeQuery(format("CREATE TABLE %s (_string STRING, _bigint BIGINT) USING ICEBERG PARTITIONED BY (_string)", sparkTableName));
        onSpark().executeQuery(format("INSERT INTO %s VALUES ('a', 1001), ('b', 1002), ('c', 1003)", sparkTableName));

        Row row = row("b", 1002);
        String select = "SELECT * FROM %s WHERE _string = 'b'";
        assertThat(onSpark().executeQuery(format(select, sparkTableName)))
                .containsOnly(row);
        assertThat(onPresto().executeQuery(format(select, prestoTableName(baseTableName))))
                .containsOnly(row);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    private static String sparkTableName(String tableName)
    {
        return format("%s.default.%s", SPARK_CATALOG, tableName);
    }

    private static String prestoTableName(String tableName)
    {
        return format("%s.default.%s", PRESTO_CATALOG, tableName);
    }
}
