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

import com.google.common.collect.ImmutableList;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

@ProductTest
@RequiresEnvironment(HiveSparkNoStatsFallbackEnvironment.class)
@TestGroup.HiveSparkNoStatsFallback
class TestHiveSparkNoStatsFallback
{
    @Test
    void testIgnoringSparkStatisticsWithDisabledFallback(HiveSparkNoStatsFallbackEnvironment env)
    {
        String tableName = "test_trino_reading_spark_statistics_table_" + randomNameSuffix();
        try {
            env.executeSparkUpdate(format("CREATE TABLE %s(" +
                    "c_tinyint            BYTE, " +
                    "c_smallint           SMALLINT, " +
                    "c_int                INT, " +
                    "c_bigint             BIGINT, " +
                    "c_float              REAL, " +
                    "c_double             DOUBLE, " +
                    "c_decimal            DECIMAL(10,0), " +
                    "c_decimal_w_params   DECIMAL(10,5), " +
                    "c_timestamp          TIMESTAMP, " +
                    "c_date               DATE, " +
                    "c_string             STRING, " +
                    "c_varchar            VARCHAR(10), " +
                    "c_char               CHAR(10), " +
                    "c_boolean            BOOLEAN, " +
                    "c_binary             BINARY" +
                    ")", tableName));

            env.executeSparkUpdate(format("INSERT INTO %s VALUES " +
                    "(120, 32760, 2147483640, 9223372036854775800, 123.340, 234.560, CAST(343.0 AS DECIMAL(10, 0)), CAST(345.670 AS DECIMAL(10, 5)), TIMESTAMP '2015-05-10 12:15:30', DATE '2015-05-08', 'p1 varchar', CAST('varchar10' AS VARCHAR(10)), CAST('p1 char10' AS CHAR(10)), false, CAST('p1 binary' as BINARY))," +
                    "(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)", tableName));
            env.executeSparkUpdate(format("ANALYZE TABLE %s COMPUTE STATISTICS FOR ALL COLUMNS", tableName));
            assertThat(env.executeTrino(format("SHOW STATS FOR %s", tableName))).containsOnly(ImmutableList.of(
                    row("c_tinyint", null, null, null, null, null, null),
                    row("c_smallint", null, null, null, null, null, null),
                    row("c_int", null, null, null, null, null, null),
                    row("c_bigint", null, null, null, null, null, null),
                    row("c_float", null, null, null, null, null, null),
                    row("c_double", null, null, null, null, null, null),
                    row("c_decimal", null, null, null, null, null, null),
                    row("c_decimal_w_params", null, null, null, null, null, null),
                    row("c_timestamp", null, null, null, null, null, null),
                    row("c_date", null, null, null, null, null, null),
                    row("c_string", null, null, null, null, null, null),
                    row("c_varchar", null, null, null, null, null, null),
                    row("c_char", null, null, null, null, null, null),
                    row("c_boolean", null, null, null, null, null, null),
                    row("c_binary", null, null, null, null, null, null),
                    row(null, null, null, null, null, null, null)));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }
}
