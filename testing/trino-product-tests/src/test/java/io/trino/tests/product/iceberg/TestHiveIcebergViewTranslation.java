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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.hive.HiveIcebergRedirectionsEnvironment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(HiveIcebergRedirectionsEnvironment.class)
@TestGroup.HiveIcebergRedirections
class TestHiveIcebergViewTranslation
{
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testViewReferencingHiveAndIcebergTables(boolean legacyTranslation, HiveIcebergRedirectionsEnvironment env)
    {
        QueryResult expectedIcebergRows = env.executeTrino(
                """
                SELECT
                  true,
                  1,
                  BIGINT '1',
                  REAL '1e0',
                  1e0,
                  DECIMAL '13.1',
                  DECIMAL '123456789123456.123456789',
                  VARCHAR 'abc',
                  X'abcd',
                  DATE '2005-09-10',
                  0
                """);

        try {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.default.view_iceberg_table_actual_data");
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.default.view_iceberg_table");
            env.executeHiveUpdate("DROP VIEW IF EXISTS hive_iceberg_view");

            String icebergTableData =
                    """
                    SELECT
                      true a_boolean,
                      1 an_integer,
                      BIGINT '1' a_bigint,
                      REAL '1e0' a_real,
                      1e0 a_double,
                      DECIMAL '13.1' a_short_decimal,
                      DECIMAL '123456789123456.123456789' a_long_decimal,
                      VARCHAR 'abc' an_unbounded_varchar,
                      X'abcd' a_varbinary,
                      DATE '2005-09-10' a_date,
                      0 a_last_column
                    """;
            env.executeTrinoUpdate("CREATE TABLE iceberg.default.view_iceberg_table_actual_data AS " + icebergTableData);
            env.executeTrinoUpdate("CREATE TABLE iceberg.default.view_iceberg_table AS TABLE iceberg.default.view_iceberg_table_actual_data");
            env.executeHiveUpdate(
                    """
                    CREATE VIEW hive_iceberg_view AS
                    SELECT view_iceberg_table.*, r_regionkey, r_name
                    FROM view_iceberg_table
                    JOIN region ON an_integer = r_regionkey
                    """);

            String tableDescription = env.executeHive("SHOW CREATE TABLE default.view_iceberg_table_actual_data").getRows().stream()
                    .map(row -> (String) row.getValue(0))
                    .collect(joining());
            String location = extractMatch(tableDescription, "LOCATION\\s*'(?<location>[^']+)'");
            String metadataLocation = extractMatch(tableDescription, "'metadata_location'='(?<location>[^']+\\.metadata\\.json)'");

            env.executeTrinoUpdate("DROP TABLE iceberg.default.view_iceberg_table");
            env.executeHiveUpdate(
                    "CREATE EXTERNAL TABLE default.view_iceberg_table (dummy_column int) " +
                            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
                            "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.FileInputFormat' " +
                            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' " +
                            "LOCATION '" + location + "' " +
                            "TBLPROPERTIES ('table_type'='iceberg', 'metadata_location'='" + metadataLocation + "')");

            assertThat(env.executeTrino("TABLE iceberg.default.view_iceberg_table"))
                    .containsOnly(expectedIcebergRows.getRows().toArray(Row[]::new));

            assertThatThrownBy(() -> env.executeHive("SELECT * FROM hive_iceberg_view"))
                    .rootCause()
                    .hasMessageContaining("SemanticException")
                    .hasMessageContaining("Invalid column reference 'an_integer' in definition of VIEW hive_iceberg_view");
            assertThat(queryHiveView(env, legacyTranslation))
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
                                    0,
                                    1L,
                                    "AMERICA"));
        }
        finally {
            env.executeHiveUpdate("DROP VIEW IF EXISTS hive_iceberg_view");
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.default.view_iceberg_table_actual_data");
            env.executeHiveUpdate("DROP TABLE IF EXISTS default.view_iceberg_table");
        }
    }

    private static QueryResult queryHiveView(HiveIcebergRedirectionsEnvironment env, boolean legacyTranslation)
    {
        if (!legacyTranslation) {
            return env.executeTrino("SELECT * FROM hive.default.hive_iceberg_view");
        }

        QueryResult[] result = new QueryResult[1];
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.hive_views_legacy_translation = true");
            result[0] = session.executeQuery("SELECT * FROM hive.default.hive_iceberg_view");
        });
        return result[0];
    }

    private static String extractMatch(String text, String pattern)
    {
        Matcher matcher = Pattern.compile(pattern).matcher(text);
        if (!matcher.find()) {
            throw new IllegalStateException("Pattern not found: " + pattern);
        }
        return matcher.group("location");
    }
}
