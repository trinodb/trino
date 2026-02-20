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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.dropDeltaTableWithRetry;
import static java.lang.String.format;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks
{
    @Test
    @TestGroup.DeltaLakeDatabricks143
    @TestGroup.DeltaLakeDatabricks154
    @TestGroup.DeltaLakeDatabricks164
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testTrinoTypesWithDatabricks(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_ctas_" + randomNameSuffix();

        try {
            assertThat(env.executeTrinoSql("CREATE TABLE delta.default.\"" + tableName + "\" " +
                    "(id, boolean, tinyint, smallint, integer, bigint, real, double, short_decimal, long_decimal, varchar_25, varchar, date) " +
                    "WITH (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "') AS VALUES " +
                    "(1, BOOLEAN 'false', TINYINT '-128', SMALLINT '-32768', INTEGER '-2147483648', BIGINT '-9223372036854775808', REAL '-0x1.fffffeP+127f', DOUBLE '-0x1.fffffffffffffP+1023', CAST('-123456789.123456789' AS DECIMAL(18,9)), CAST('-12345678901234567890.123456789012345678' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '1901-01-01')" +
                    ", (2, BOOLEAN 'true', TINYINT '127', SMALLINT '32767', INTEGER '2147483647', BIGINT '9223372036854775807', REAL '0x1.fffffeP+127f', DOUBLE '0x1.fffffffffffffP+1023', CAST('123456789.123456789' AS DECIMAL(18,9)), CAST('12345678901234567890.123456789012345678' AS DECIMAL(38,18)), CAST('Canada' AS VARCHAR(25)), CAST('Germany' AS VARCHAR), DATE '9999-12-31')" +
                    ", (3, BOOLEAN 'true', TINYINT '0', SMALLINT '0', INTEGER '0', BIGINT '0', REAL '0', DOUBLE '0', CAST('0' AS DECIMAL(18,9)), CAST('0' AS DECIMAL(38,18)), CAST('' AS VARCHAR(25)), CAST('' AS VARCHAR), DATE '2020-08-22')" +
                    ", (4, BOOLEAN 'true', TINYINT '1', SMALLINT '1', INTEGER '1', BIGINT '1', REAL '1', DOUBLE '1', CAST('1' AS DECIMAL(18,9)), CAST('1' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '2001-08-22')" +
                    ", (5, BOOLEAN 'true', TINYINT '37', SMALLINT '12242', INTEGER '2524', BIGINT '132', REAL '3.141592653', DOUBLE '2.718281828459045', CAST('3.141592653' AS DECIMAL(18,9)), CAST('2.718281828459045' AS DECIMAL(38,18)), CAST('\u653b\u6bbb\u6a5f\u52d5\u968a' AS VARCHAR(25)), CAST('\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!' AS VARCHAR), DATE '2020-08-22')" +
                    ", (6, BOOLEAN 'true', TINYINT '0', SMALLINT '0', INTEGER '0', BIGINT '0', REAL '0x0.000002P-126f', DOUBLE '0x0.0000000000001P-1022', CAST('0' AS DECIMAL(18,9)), CAST('0' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '2001-08-22')" +
                    ", (7, BOOLEAN 'true', TINYINT '0', SMALLINT '0', INTEGER '0', BIGINT '0', REAL '0x1.0p-126f', DOUBLE '0x1.0p-1022', CAST('0' AS DECIMAL(18,9)), CAST('0' AS DECIMAL(38,18)), CAST('Romania' AS VARCHAR(25)), CAST('Poland' AS VARCHAR), DATE '2001-08-22')"))
                    .containsOnly(row(7));

            QueryResult databricksResult = env.executeDatabricksSql(format("SELECT * FROM default.%s", tableName));
            QueryResult trinoResult = env.executeTrinoSql(format("SELECT * FROM delta.default.\"%s\"", tableName));
            assertThat(databricksResult).containsOnly(trinoResult.getRows());
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testTrinoTimestampsWithDatabricks(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_ctas_timestamps_" + randomNameSuffix();

        try {
            assertThat(env.executeTrinoSql("CREATE TABLE delta.default.\"" + tableName + "\" " +
                    "(id, timestamp_in_utc, timestamp_in_new_york, timestamp_in_warsaw) " +
                    "WITH (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "') AS VALUES " +
                    "(1, TIMESTAMP '1901-01-01 00:00:00 UTC', TIMESTAMP '1902-01-01 00:00:00 America/New_York', TIMESTAMP '1902-01-01 00:00:00 Europe/Warsaw')" +
                    ", (2, TIMESTAMP '9999-12-31 23:59:59.999 UTC', TIMESTAMP '9998-12-31 23:59:59.999 America/New_York', TIMESTAMP '9998-12-31 23:59:59.999 Europe/Warsaw')" +
                    ", (3, TIMESTAMP '2020-06-10 15:55:23 UTC', TIMESTAMP '2020-06-10 15:55:23.123 America/New_York', TIMESTAMP '2020-06-10 15:55:23.123 Europe/Warsaw')" +
                    ", (4, TIMESTAMP '2001-08-22 03:04:05.321 UTC', TIMESTAMP '2001-08-22 03:04:05.321 America/New_York', TIMESTAMP '2001-08-22 03:04:05.321 Europe/Warsaw')"))
                    .containsOnly(row(4));

            QueryResult databricksResult = env.executeDatabricksSql("SELECT id, date_format(timestamp_in_utc, \"yyyy-MM-dd HH:mm:ss.SSS\"), date_format(timestamp_in_new_york, \"yyyy-MM-dd HH:mm:ss.SSS\"), date_format(timestamp_in_warsaw, \"yyyy-MM-dd HH:mm:ss.SSS\") FROM default." + tableName);
            QueryResult trinoResult = env.executeTrinoSql("SELECT id, format('%1$tF %1$tT.%1$tL', timestamp_in_utc), format('%1$tF %1$tT.%1$tL', timestamp_in_new_york), format('%1$tF %1$tT.%1$tL', timestamp_in_warsaw) FROM delta.default.\"" + tableName + "\"");
            assertThat(databricksResult).containsOnly(trinoResult.getRows());
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }
}
