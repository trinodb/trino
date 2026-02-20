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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Hive compression support.
 * <p>
 * Ported from the Tempto-based TestHiveCompression.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
@TestGroup.HiveCompression
class TestHiveCompression
{
    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testReadTextfileWithLzop(HiveStorageFormatsEnvironment env)
    {
        testReadCompressedTextfileTable(
                env,
                "STORED AS TEXTFILE",
                "com.hadoop.compression.lzo.LzopCodec",
                ".*\\.lzo"); // LZOP compression uses .lzo file extension by default
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testReadSequencefileWithLzo(HiveStorageFormatsEnvironment env)
    {
        testReadCompressedTextfileTable(
                env,
                "STORED AS SEQUENCEFILE",
                "com.hadoop.compression.lzo.LzoCodec",
                // sequencefile stores compression information in the file header, so no suffix expected
                "\\d+_0");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSnappyCompressedParquetTableCreatedInHive(HiveStorageFormatsEnvironment env)
    {
        String tableName = "table_hive_parquet_snappy";
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        env.executeHiveUpdate(format(
                "CREATE TABLE %s (" +
                        "   c_bigint BIGINT," +
                        "   c_varchar VARCHAR(255))" +
                        "STORED AS PARQUET " +
                        "TBLPROPERTIES(\"parquet.compression\"=\"SNAPPY\")",
                tableName));
        env.executeHiveUpdate(format("INSERT INTO %s VALUES(1, 'test data')", tableName));

        assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName))
                .containsExactlyInOrder(row(1L, "test data"));

        env.executeHiveUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSnappyCompressedParquetTableCreatedInTrino(HiveStorageFormatsEnvironment env)
    {
        String tableName = "table_trino_parquet_snappy";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        env.executeTrinoUpdate(format(
                "CREATE TABLE hive.default.%s (" +
                        "   c_bigint BIGINT," +
                        "   c_varchar VARCHAR(255))" +
                        "WITH (format='PARQUET')",
                tableName));

        String catalog = (String) env.executeTrino("SELECT CURRENT_CATALOG").getOnlyValue();
        env.executeTrinoUpdate("SET SESSION " + catalog + ".compression_codec = 'SNAPPY'");
        env.executeTrinoUpdate(format("INSERT INTO hive.default.%s VALUES(1, 'test data')", tableName));

        assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName))
                .containsExactlyInOrder(row(1L, "test data"));
        assertThat(env.executeHive("SELECT * FROM " + tableName))
                .containsExactlyInOrder(row(1L, "test data"));

        env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
    }

    private void testReadCompressedTextfileTable(
            HiveStorageFormatsEnvironment env,
            String tableStorageDefinition,
            String compressionCodec,
            @Language("RegExp") String expectedFileNamePattern)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_read_compressed");
        env.executeHiveUpdate("DROP TABLE IF EXISTS orders");

        try {
            // First create the orders table with TPCH data using Trino
            // (the original Tempto tests used immutableTable(ORDERS) to pre-populate this)
            // Use column aliases to match TPCH standard column naming (with o_ prefix)
            env.executeTrinoUpdate("""
                    CREATE TABLE hive.default.orders AS
                    SELECT orderkey AS o_orderkey,
                           custkey AS o_custkey,
                           orderstatus AS o_orderstatus,
                           totalprice AS o_totalprice,
                           orderdate AS o_orderdate,
                           orderpriority AS o_orderpriority,
                           clerk AS o_clerk,
                           shippriority AS o_shippriority,
                           comment AS o_comment
                    FROM tpch.sf1.orders
                    """);

            // Use a single Hive connection for all SET commands and the CTAS query
            // (SET commands only affect the current session)
            try (Connection conn = env.createHiveConnection();
                    Statement stmt = conn.createStatement()) {
                stmt.execute("SET hive.exec.compress.output=true");
                stmt.execute("SET mapreduce.output.fileoutputformat.compress=true");
                stmt.execute("SET mapreduce.output.fileoutputformat.compress.codec=" + compressionCodec);
                stmt.execute("SET mapreduce.map.output.compress=true");
                stmt.execute("SET mapreduce.map.output.compress.codec=" + compressionCodec);

                // Create compressed table from the orders table
                stmt.execute("CREATE TABLE test_read_compressed " + tableStorageDefinition + " AS SELECT * FROM orders");
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed to create compressed table", e);
            }

            assertThat(env.executeTrino("SELECT count(*) FROM hive.default.test_read_compressed"))
                    .containsExactlyInOrder(row(1500000L));
            assertThat(env.executeTrino("SELECT sum(o_orderkey) FROM hive.default.test_read_compressed"))
                    .containsExactlyInOrder(row(4499987250000L));

            String path = (String) env.executeTrino("SELECT regexp_replace(\"$path\", '.*/') FROM hive.default.test_read_compressed LIMIT 1").getOnlyValue();
            assertThat(path).matches(expectedFileNamePattern);
        }
        finally {
            // Reset Hive settings
            env.executeHiveUpdate("RESET");
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_read_compressed");
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.orders");
        }
    }
}
