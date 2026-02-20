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
package io.trino.tests.product.hive;

import io.trino.tempto.assertions.QueryAssert;
import org.intellij.lang.annotations.Language;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestHiveIcebergViewTranslation
        extends HiveProductTest
{
    /**
     * Test a Hive view that spans over Hive and Iceberg table when metastore does not contain an up to date information about table schema, requiring
     * any potential view translation to follow redirections.
     */
    public void testViewReferencingHiveAndIcebergTables()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS iceberg.default.view_iceberg_table_actual_data");
        onTrino().executeQuery("DROP TABLE IF EXISTS iceberg.default.view_iceberg_table");
        onHive().executeQuery("DROP VIEW IF EXISTS hive_iceberg_view");

        @Language("SQL")
        String icebergTableData = "SELECT " +
                "  true a_boolean, " +
                "  1 an_integer, " +
                "  BIGINT '1' a_bigint," +
                "  REAL '1e0' a_real, " +
                "  1e0 a_double, " +
                "  DECIMAL '13.1' a_short_decimal, " +
                "  DECIMAL '123456789123456.123456789' a_long_decimal, " +
                "  VARCHAR 'abc' an_unbounded_varchar, " +
                "  X'abcd' a_varbinary, " +
                "  DATE '2005-09-10' a_date, " +
                // TODO this results in: column [a_timestamp] of type timestamp(6) projected from query view at position 10 cannot be coerced to column [a_timestamp] of type timestamp(3) stored in view definition
                //  This is because `HiveViewReader` unconditionally uses millisecond precision.
                //  "  TIMESTAMP '2005-09-10 13:00:00.123456' a_timestamp, " +
                // TODO Hive fails to define a view over `timestamp with time zone` column.
                //  "  TIMESTAMP '2005-09-10 13:00:00.123456 Europe/Warsaw' a_timestamp_tz, " +
                "  0 a_last_column ";
        onTrino().executeQuery("CREATE TABLE iceberg.default.view_iceberg_table_actual_data AS " + icebergTableData);
        onTrino().executeQuery("CREATE TABLE iceberg.default.view_iceberg_table AS TABLE iceberg.default.view_iceberg_table_actual_data");
        onHive().executeQuery("CREATE VIEW hive_iceberg_view AS " +
                "SELECT view_iceberg_table.*, r_regionkey, r_name " +
                "FROM view_iceberg_table JOIN region ON an_integer = r_regionkey");

        // For an Iceberg table, the table schema in the metastore is generally ignored when reading directly from the table.
        // In order to test that it's not used for Hive view translation, we desynchronize the state between metastore and the
        // actual Iceberg schema in the storage. We do this by recreating the table and registering it the second time, manually.
        String tableDescription = onHive().executeQuery("SHOW CREATE TABLE default.view_iceberg_table_actual_data").rows().stream()
                .map(row -> (String) getOnlyElement(row))
                .collect(joining());
        String location = extractMatch(tableDescription, "LOCATION\\s*'(?<location>[^']+)'", "location");
        String metadataLocation = extractMatch(tableDescription, "'metadata_location'='(?<location>[^']+\\.metadata\\.json)'", "location");
        onTrino().executeQuery("DROP TABLE iceberg.default.view_iceberg_table");
        // See https://github.com/apache/iceberg/blob/7fcc71da65a47ca3c9f6eb6e862a238389b8bdc5/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveTableOperations.java#L406-L414
        // for reference on table setup.
        // The Iceberg connector and Iceberg library would set 'org.apache.hadoop.mapred.FileOutputFormat' output format, but that's rejected by H2.
        onHive().executeQuery(format(
                """
                CREATE EXTERNAL TABLE default.view_iceberg_table (dummy_column int)
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.FileInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                LOCATION '%s'
                TBLPROPERTIES ('table_type'='iceberg', 'metadata_location'='%s')
                """,
                location,
                metadataLocation));
        // Verify the table is recreated correctly
        assertThat(onTrino().executeQuery("TABLE iceberg.default.view_iceberg_table"))
                .containsOnly(onTrino().executeQuery(icebergTableData).rows().stream()
                        .map(QueryAssert.Row::new)
                        .collect(toImmutableList()));

        // Test querying the view
        assertQueryFailure(() -> onHive().executeQuery("SELECT * FROM hive_iceberg_view"))
                // Testing Hive is not set up for Iceberg support. TODO when this changes, switch to use `assertViewQuery`
                .hasMessageContaining("SemanticException")
                .hasMessageContaining("Invalid column reference 'an_integer' in definition of VIEW hive_iceberg_view");
        assertThat(onTrino().executeQuery("SELECT * FROM hive_iceberg_view"))
                .containsOnly(
                        row(
                                true,
                                1,
                                1L,
                                1.0f,
                                1d,
                                new BigDecimal("13.1"),
                                new BigDecimal("123456789123456.123456789"),
                                "abc",
                                new byte[] {(byte) 0xAB, (byte) 0xCD},
                                Date.valueOf(LocalDate.of(2005, 9, 10)),
                                0, // view_iceberg_table.a_last_column,
                                1L,
                                "AMERICA"));

        onHive().executeQuery("DROP VIEW hive_iceberg_view");
        onTrino().executeQuery("DROP TABLE iceberg.default.view_iceberg_table_actual_data");
        onHive().executeQuery("DROP TABLE default.view_iceberg_table");
    }

    protected void setSessionProperty(String key, String value)
    {
        // We need to setup sessions for both "trino" and "default" executors in tempto
        onTrino().executeQuery(format("SET SESSION %s = %s", key, value));
        onTrino().executeQuery(format("SET SESSION %s = %s", key, value));
    }

    private static String extractMatch(String value, @Language("RegExp") String pattern, String groupName)
    {
        Matcher matcher = Pattern.compile(pattern).matcher(value);
        verify(matcher.find(), "Did not find match in [%s] for [%s]", value, pattern);
        String extract = matcher.group(groupName);
        verify(!matcher.find(), "Match ambiguous in [%s] for [%s]", value, pattern);
        return extract;
    }
}
