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
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARBINARY;
import static java.sql.JDBCType.VARCHAR;

/**
 * Tests for reading all Hive datatypes through the Hive connector.
 * <p>
 * Ported from the Tempto-based TestAllDatatypesFromHiveConnector.
 * <p>
 * This test verifies that Trino can correctly read data from tables
 * using different Hive storage formats (TextFile, ORC, RCFile, Parquet, Avro).
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
class TestAllDatatypesFromHiveConnector
{
    @Test
    void testSelectAllDatatypesTextFile(HiveStorageFormatsEnvironment env)
    {
        String tableName = "textfile_all_types_" + randomNameSuffix();

        try {
            // Create table with TEXTFILE format
            env.executeHiveUpdate("""
                    CREATE TABLE %s (
                        c_tinyint TINYINT,
                        c_smallint SMALLINT,
                        c_int INT,
                        c_bigint BIGINT,
                        c_float FLOAT,
                        c_double DOUBLE,
                        c_decimal DECIMAL,
                        c_decimal_w_params DECIMAL(10,5),
                        c_timestamp TIMESTAMP,
                        c_date DATE,
                        c_string STRING,
                        c_varchar VARCHAR(10),
                        c_char CHAR(10),
                        c_boolean BOOLEAN,
                        c_binary BINARY
                    )
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
                    STORED AS TEXTFILE
                    """.formatted(tableName));

            // Insert test data
            env.executeHiveUpdate("""
                    INSERT INTO %s VALUES (
                        127,
                        32767,
                        2147483647,
                        9223372036854775807,
                        123.345,
                        234.567,
                        346,
                        345.67800,
                        '2015-05-10 12:15:35.123',
                        '2015-05-10',
                        'ala ma kota',
                        'ala ma kot',
                        'ala ma    ',
                        true,
                        'kot binarny'
                    )
                    """.formatted(tableName));

            assertProperAllDatatypesSchema(env, tableName);
            QueryResult queryResult = env.executeTrino("SELECT * FROM hive.default.%s".formatted(tableName));

            assertColumnTypes(queryResult);
            assertThat(queryResult).containsOnly(
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
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testSelectAllDatatypesOrc(HiveStorageFormatsEnvironment env)
    {
        String tableName = "orc_all_types_" + randomNameSuffix();
        String textTableName = "textfile_source_" + randomNameSuffix();

        try {
            // Create ORC table
            env.executeHiveUpdate("""
                    CREATE TABLE %s (
                        c_tinyint TINYINT,
                        c_smallint SMALLINT,
                        c_int INT,
                        c_bigint BIGINT,
                        c_float FLOAT,
                        c_double DOUBLE,
                        c_decimal DECIMAL,
                        c_decimal_w_params DECIMAL(10,5),
                        c_timestamp TIMESTAMP,
                        c_date DATE,
                        c_string STRING,
                        c_varchar VARCHAR(10),
                        c_char CHAR(10),
                        c_boolean BOOLEAN,
                        c_binary BINARY
                    )
                    STORED AS ORC
                    """.formatted(tableName));

            // Create source TextFile table and populate ORC table from it
            env.executeHiveUpdate("""
                    CREATE TABLE %s (
                        c_tinyint TINYINT,
                        c_smallint SMALLINT,
                        c_int INT,
                        c_bigint BIGINT,
                        c_float FLOAT,
                        c_double DOUBLE,
                        c_decimal DECIMAL,
                        c_decimal_w_params DECIMAL(10,5),
                        c_timestamp TIMESTAMP,
                        c_date DATE,
                        c_string STRING,
                        c_varchar VARCHAR(10),
                        c_char CHAR(10),
                        c_boolean BOOLEAN,
                        c_binary BINARY
                    )
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
                    STORED AS TEXTFILE
                    """.formatted(textTableName));

            env.executeHiveUpdate("""
                    INSERT INTO %s VALUES (
                        127,
                        32767,
                        2147483647,
                        9223372036854775807,
                        123.345,
                        234.567,
                        346,
                        345.67800,
                        '2015-05-10 12:15:35.123',
                        '2015-05-10',
                        'ala ma kota',
                        'ala ma kot',
                        'ala ma    ',
                        true,
                        'kot binarny'
                    )
                    """.formatted(textTableName));

            // Populate ORC from TextFile
            env.executeHiveUpdate("INSERT INTO TABLE %s SELECT * FROM %s".formatted(tableName, textTableName));

            assertProperAllDatatypesSchema(env, tableName);

            QueryResult queryResult = env.executeTrino("SELECT * FROM hive.default.%s".formatted(tableName));
            assertColumnTypes(queryResult);
            assertThat(queryResult).containsOnly(
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
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + textTableName);
        }
    }

    @Test
    void testSelectAllDatatypesRcfile(HiveStorageFormatsEnvironment env)
    {
        String tableName = "rcfile_all_types_" + randomNameSuffix();
        String textTableName = "textfile_source_" + randomNameSuffix();

        try {
            // Create RCFILE table
            env.executeHiveUpdate("""
                    CREATE TABLE %s (
                        c_tinyint TINYINT,
                        c_smallint SMALLINT,
                        c_int INT,
                        c_bigint BIGINT,
                        c_float FLOAT,
                        c_double DOUBLE,
                        c_decimal DECIMAL,
                        c_decimal_w_params DECIMAL(10,5),
                        c_timestamp TIMESTAMP,
                        c_date DATE,
                        c_string STRING,
                        c_varchar VARCHAR(10),
                        c_char CHAR(10),
                        c_boolean BOOLEAN,
                        c_binary BINARY
                    )
                    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
                    STORED AS RCFILE
                    """.formatted(tableName));

            // Create source TextFile table and populate RCFile table from it
            env.executeHiveUpdate("""
                    CREATE TABLE %s (
                        c_tinyint TINYINT,
                        c_smallint SMALLINT,
                        c_int INT,
                        c_bigint BIGINT,
                        c_float FLOAT,
                        c_double DOUBLE,
                        c_decimal DECIMAL,
                        c_decimal_w_params DECIMAL(10,5),
                        c_timestamp TIMESTAMP,
                        c_date DATE,
                        c_string STRING,
                        c_varchar VARCHAR(10),
                        c_char CHAR(10),
                        c_boolean BOOLEAN,
                        c_binary BINARY
                    )
                    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
                    STORED AS TEXTFILE
                    """.formatted(textTableName));

            env.executeHiveUpdate("""
                    INSERT INTO %s VALUES (
                        127,
                        32767,
                        2147483647,
                        9223372036854775807,
                        123.345,
                        234.567,
                        346,
                        345.67800,
                        '2015-05-10 12:15:35.123',
                        '2015-05-10',
                        'ala ma kota',
                        'ala ma kot',
                        'ala ma    ',
                        true,
                        'kot binarny'
                    )
                    """.formatted(textTableName));

            // Populate RCFile from TextFile
            env.executeHiveUpdate("INSERT INTO TABLE %s SELECT * FROM %s".formatted(tableName, textTableName));

            assertProperAllDatatypesSchema(env, tableName);

            QueryResult queryResult = env.executeTrino("SELECT * FROM hive.default.%s".formatted(tableName));
            assertColumnTypes(queryResult);
            assertThat(queryResult).containsOnly(
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
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + textTableName);
        }
    }

    @Test
    void testSelectAllDatatypesAvro(HiveStorageFormatsEnvironment env)
    {
        String tableName = "avro_all_types_" + randomNameSuffix();

        try {
            // Create AVRO table (note: Avro doesn't support TINYINT or SMALLINT)
            env.executeHiveUpdate("""
                    CREATE TABLE %s (
                        c_int INT,
                        c_bigint BIGINT,
                        c_float FLOAT,
                        c_double DOUBLE,
                        c_decimal DECIMAL,
                        c_decimal_w_params DECIMAL(10,5),
                        c_timestamp TIMESTAMP,
                        c_date DATE,
                        c_string STRING,
                        c_varchar VARCHAR(10),
                        c_char CHAR(10),
                        c_boolean BOOLEAN,
                        c_binary BINARY
                    )
                    STORED AS AVRO
                    """.formatted(tableName));

            env.executeHiveUpdate("""
                    INSERT INTO %s VALUES (
                        2147483647,
                        9223372036854775807,
                        123.345,
                        234.567,
                        346,
                        345.67800,
                        '%s',
                        '%s',
                        'ala ma kota',
                        'ala ma kot',
                        'ala ma    ',
                        true,
                        'kot binarny'
                    )
                    """.formatted(
                    tableName,
                    Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000)).toString(),
                    Date.valueOf("2015-05-10")));

            assertThat(env.executeTrino("SHOW COLUMNS FROM hive.default." + tableName).project(1, 2)).containsExactlyInOrder(
                    row("c_int", "integer"),
                    row("c_bigint", "bigint"),
                    row("c_float", "real"),
                    row("c_double", "double"),
                    row("c_decimal", "decimal(10,0)"),
                    row("c_decimal_w_params", "decimal(10,5)"),
                    row("c_timestamp", "timestamp(3)"),
                    row("c_date", "date"),
                    row("c_string", "varchar"),
                    row("c_varchar", "varchar(10)"),
                    row("c_char", "char(10)"),
                    row("c_boolean", "boolean"),
                    row("c_binary", "varbinary"));

            QueryResult queryResult = env.executeTrino("SELECT * FROM hive.default." + tableName);
            assertThat(queryResult).hasColumns(toJdbcTypes(
                    INTEGER,
                    BIGINT,
                    REAL,
                    DOUBLE,
                    DECIMAL,
                    DECIMAL,
                    TIMESTAMP,
                    DATE,
                    VARCHAR,
                    VARCHAR,
                    CHAR,
                    BOOLEAN,
                    VARBINARY));

            // In 3.1.0 timestamp semantics in hive changed in backward incompatible way,
            // which was fixed for Parquet and Avro in 3.1.2 (https://issues.apache.org/jira/browse/HIVE-21002)
            Timestamp expectedTimestamp = env.isHiveWithBrokenAvroTimestamps()
                    ? Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 6, 30, 35, 123_000_000))
                    : Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000));
            assertThat(queryResult).containsOnly(
                    row(
                            2147483647,
                            9223372036854775807L,
                            123.345f,
                            234.567,
                            new BigDecimal("346"),
                            new BigDecimal("345.67800"),
                            expectedTimestamp,
                            Date.valueOf("2015-05-10"),
                            "ala ma kota",
                            "ala ma kot",
                            "ala ma    ",
                            true,
                            "kot binarny".getBytes(UTF_8)));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues?q=is%3Aissue+4936+5427",
            match = "Error committing write to Hive|could only be replicated to 0 nodes instead of minReplication")
    void testSelectAllDatatypesParquetFile(HiveStorageFormatsEnvironment env)
    {
        String tableName = "parquet_all_types_" + randomNameSuffix();

        try {
            // Create PARQUET table (note: Parquet doesn't support DATE in the original test)
            env.executeHiveUpdate("""
                    CREATE TABLE %s (
                        c_tinyint TINYINT,
                        c_smallint SMALLINT,
                        c_int INT,
                        c_bigint BIGINT,
                        c_float FLOAT,
                        c_double DOUBLE,
                        c_decimal DECIMAL,
                        c_decimal_w_params DECIMAL(10,5),
                        c_timestamp TIMESTAMP,
                        c_string STRING,
                        c_varchar VARCHAR(10),
                        c_char CHAR(10),
                        c_boolean BOOLEAN,
                        c_binary BINARY
                    )
                    STORED AS PARQUET
                    """.formatted(tableName));

            env.executeHiveUpdate("""
                    INSERT INTO %s VALUES (
                        127,
                        32767,
                        2147483647,
                        9223372036854775807,
                        123.345,
                        234.567,
                        346,
                        345.67800,
                        '%s',
                        'ala ma kota',
                        'ala ma kot',
                        'ala ma    ',
                        true,
                        'kot binarny'
                    )
                    """.formatted(
                    tableName,
                    Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000)).toString()));

            assertThat(env.executeTrino("SHOW COLUMNS FROM hive.default.%s".formatted(tableName)).project(1, 2)).containsExactlyInOrder(
                    row("c_tinyint", "tinyint"),
                    row("c_smallint", "smallint"),
                    row("c_int", "integer"),
                    row("c_bigint", "bigint"),
                    row("c_float", "real"),
                    row("c_double", "double"),
                    row("c_decimal", "decimal(10,0)"),
                    row("c_decimal_w_params", "decimal(10,5)"),
                    row("c_timestamp", "timestamp(3)"),
                    row("c_string", "varchar"),
                    row("c_varchar", "varchar(10)"),
                    row("c_char", "char(10)"),
                    row("c_boolean", "boolean"),
                    row("c_binary", "varbinary"));

            QueryResult queryResult = env.executeTrino("SELECT * FROM hive.default.%s".formatted(tableName));
            assertColumnTypesParquet(queryResult);
            assertThat(queryResult).containsOnly(
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
                            "ala ma kota",
                            "ala ma kot",
                            "ala ma    ",
                            true,
                            "kot binarny".getBytes(UTF_8)));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private void assertProperAllDatatypesSchema(HiveStorageFormatsEnvironment env, String tableName)
    {
        assertThat(env.executeTrino("SHOW COLUMNS FROM hive.default." + tableName).project(1, 2)).containsExactlyInOrder(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "real"),
                row("c_double", "double"),
                row("c_decimal", "decimal(10,0)"),
                row("c_decimal_w_params", "decimal(10,5)"),
                row("c_timestamp", "timestamp(3)"),
                row("c_date", "date"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_char", "char(10)"),
                row("c_boolean", "boolean"),
                row("c_binary", "varbinary"));
    }

    private void assertColumnTypes(QueryResult queryResult)
    {
        assertThat(queryResult).hasColumns(toJdbcTypes(
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                TIMESTAMP,
                DATE,
                VARCHAR,
                VARCHAR,
                CHAR,
                BOOLEAN,
                VARBINARY));
    }

    private void assertColumnTypesParquet(QueryResult queryResult)
    {
        assertThat(queryResult).hasColumns(toJdbcTypes(
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                TIMESTAMP,
                VARCHAR,
                VARCHAR,
                CHAR,
                BOOLEAN,
                VARBINARY));
    }

    /**
     * Converts JDBCType varargs to a List of Integer JDBC type codes.
     */
    private static List<Integer> toJdbcTypes(JDBCType... types)
    {
        return Arrays.stream(types)
                .map(JDBCType::getVendorTypeNumber)
                .toList();
    }
}
