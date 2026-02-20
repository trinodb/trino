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
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Tests for INSERT INTO operations on Hive tables.
 * <p>
 * Ported from the Tempto-based TestInsertIntoHiveTable.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestInsertIntoHiveTable
{
    private static final String TARGET_TABLE = "test_insert_target_table";
    private static final String PARTITIONED_TABLE_WITH_SERDE = "test_insert_partitioned_with_serde";
    private static final String SOURCE_TABLE = "test_insert_source_textfile_all_types";

    @BeforeEach
    void setUp(HiveBasicEnvironment env)
    {
        // Create source table with data (equivalent to immutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE))
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + SOURCE_TABLE);
        env.executeHiveUpdate(
                "CREATE TABLE " + SOURCE_TABLE + "(" +
                        "   c_tinyint            TINYINT," +
                        "   c_smallint           SMALLINT," +
                        "   c_int                INT," +
                        "   c_bigint             BIGINT," +
                        "   c_float              FLOAT," +
                        "   c_double             DOUBLE," +
                        "   c_decimal            DECIMAL," +
                        "   c_decimal_w_params   DECIMAL(10,5)," +
                        "   c_timestamp          TIMESTAMP," +
                        "   c_date               DATE," +
                        "   c_string             STRING," +
                        "   c_varchar            VARCHAR(10)," +
                        "   c_char               CHAR(10)," +
                        "   c_boolean            BOOLEAN," +
                        "   c_binary             BINARY" +
                        ") " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                        "STORED AS TEXTFILE");

        // Insert the data using Trino (since we don't have the original data file)
        env.executeTrinoUpdate(
                "INSERT INTO hive.default." + SOURCE_TABLE + " VALUES(" +
                        "TINYINT '127', " +
                        "SMALLINT '32767', " +
                        "2147483647, " +
                        "9223372036854775807, " +
                        "REAL '123.345', " +
                        "234.567, " +
                        "CAST(346 as DECIMAL(10,0))," +
                        "CAST(345.67800 as DECIMAL(10,5))," +
                        "timestamp '2015-05-10 12:15:35.123', " +
                        "date '2015-05-10', " +
                        "'ala ma kota', " +
                        "'ala ma kot', " +
                        "CAST('ala ma    ' as CHAR(10)), " +
                        "true, " +
                        "from_base64('a290IGJpbmFybnk=')" +
                        ")");

        // Create target table (equivalent to mutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE, TABLE_NAME, CREATED))
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + TARGET_TABLE);
        env.executeHiveUpdate(
                "CREATE TABLE " + TARGET_TABLE + "(" +
                        "   c_tinyint            TINYINT," +
                        "   c_smallint           SMALLINT," +
                        "   c_int                INT," +
                        "   c_bigint             BIGINT," +
                        "   c_float              FLOAT," +
                        "   c_double             DOUBLE," +
                        "   c_decimal            DECIMAL," +
                        "   c_decimal_w_params   DECIMAL(10,5)," +
                        "   c_timestamp          TIMESTAMP," +
                        "   c_date               DATE," +
                        "   c_string             STRING," +
                        "   c_varchar            VARCHAR(10)," +
                        "   c_char               CHAR(10)," +
                        "   c_boolean            BOOLEAN," +
                        "   c_binary             BINARY" +
                        ") " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                        "STORED AS TEXTFILE");

        // Create partitioned table with SERDE properties
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + PARTITIONED_TABLE_WITH_SERDE);
        env.executeHiveUpdate(
                "CREATE TABLE " + PARTITIONED_TABLE_WITH_SERDE + "( " +
                        "id int, " +
                        "name string " +
                        ") " +
                        "PARTITIONED BY (dt string) " +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
                        "WITH SERDEPROPERTIES ( " +
                        "'field.delim'='\\t', " +
                        "'line.delim'='\\n', " +
                        "'serialization.format'='\\t' " +
                        ")");
    }

    @AfterEach
    void tearDown(HiveBasicEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + TARGET_TABLE);
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + PARTITIONED_TABLE_WITH_SERDE);
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + SOURCE_TABLE);
    }

    @Test
    void testInsertIntoValuesToHiveTableAllHiveSimpleTypes(HiveBasicEnvironment env)
    {
        assertThat(env.executeTrino("SELECT * FROM hive.default." + TARGET_TABLE)).hasNoRows();

        env.executeTrinoUpdate(
                "INSERT INTO hive.default." + TARGET_TABLE + " VALUES(" +
                        "TINYINT '127', " +
                        "SMALLINT '32767', " +
                        "2147483647, " +
                        "9223372036854775807, " +
                        "REAL '123.345', " +
                        "234.567, " +
                        "CAST(346 as DECIMAL(10,0))," +
                        "CAST(345.67800 as DECIMAL(10,5))," +
                        "timestamp '2015-05-10 12:15:35.123', " +
                        "date '2015-05-10', " +
                        "'ala ma kota', " +
                        "'ala ma kot', " +
                        "CAST('ala ma    ' as CHAR(10)), " +
                        "true, " +
                        "from_base64('a290IGJpbmFybnk=')" +
                        ")");

        assertThat(env.executeTrino("SELECT * FROM hive.default." + TARGET_TABLE)).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345f,
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000)),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes(UTF_8)));
    }

    @Test
    void testInsertIntoSelectToHiveTableAllHiveSimpleTypes(HiveBasicEnvironment env)
    {
        assertThat(env.executeTrino("SELECT * FROM hive.default." + TARGET_TABLE)).hasNoRows();

        assertThat(env.executeTrino(
                "INSERT INTO hive.default." + TARGET_TABLE + " SELECT * from hive.default." + SOURCE_TABLE))
                .containsExactlyInOrder(row(1));

        assertThat(env.executeTrino("SELECT * FROM hive.default." + TARGET_TABLE)).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345f,
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000)),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes(UTF_8)));
    }

    @Test
    void testInsertIntoPartitionedWithSerdeProperty(HiveBasicEnvironment env)
    {
        assertThat(env.executeTrino(
                "INSERT INTO hive.default." + PARTITIONED_TABLE_WITH_SERDE + " SELECT 1, 'Trino', '2018-01-01'"))
                .containsExactlyInOrder(row(1));

        assertThat(env.executeTrino("SELECT * FROM hive.default." + PARTITIONED_TABLE_WITH_SERDE))
                .containsExactlyInOrder(row(1, "Trino", "2018-01-01"));
    }
}
