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
import com.google.common.io.Resources;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HIVE_OPENX;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BINARY;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.FLOAT;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARBINARY;
import static java.sql.JDBCType.VARCHAR;

public class TestOpenXJsonHiveTable
        extends HiveProductTest
{
    private static final LocalDateTime LOCAL_DATE_TIME_1 = LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_456_000);
    private static final LocalDateTime LOCAL_DATE_TIME_2 = LocalDateTime.of(2021, 5, 10, 12, 15, 35, 123_456_000);
    private static final List<QueryAssert.Row> EXPECTED_ROWS = createExpectedRows(LOCAL_DATE_TIME_1, LOCAL_DATE_TIME_2);
    private static final List<QueryAssert.Row> EXPECTED_ROWS_MILLISECONDS = createExpectedRows(
            LOCAL_DATE_TIME_1.truncatedTo(ChronoUnit.MILLIS),
            LOCAL_DATE_TIME_2.truncatedTo(ChronoUnit.MILLIS));

    private static List<QueryAssert.Row> createExpectedRows(LocalDateTime localDateTime1, LocalDateTime localDateTime2)
    {
        return ImmutableList.of(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345,
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        new BigDecimal("12345678901234567890.123456789012345678"),
                        Timestamp.valueOf(localDateTime1),
                        Date.valueOf("2015-05-10"),
                        "plain row - ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes(UTF_8)),
                row(
                        0,
                        1,
                        2,
                        3L,
                        (float) Math.PI,
                        Math.E,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        new BigDecimal("123.123"),
                        Timestamp.valueOf(localDateTime1),
                        Date.valueOf("2015-05-10"),
                        "plain row - ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes(UTF_8)),
                row(
                        -128,
                        -32768,
                        -2147483648,
                        -9223372036854775808L,
                        -123.345,
                        -234.567,
                        new BigDecimal("-346"),
                        new BigDecimal("-345.67800"),
                        new BigDecimal("-12345678901234567890.123456789012345678"),
                        Timestamp.valueOf(localDateTime2),
                        Date.valueOf("2021-05-10"),
                        "numbers quoted - ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        false,
                        "kot binarny".getBytes(UTF_8)),
                row(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null),
                row(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null));
    }

    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @BeforeTestWithContext
    public void setup()
            throws Exception
    {
        hdfsClient.createDirectory(warehouseDirectory + "/TestOpenXJsonHiveTable/openx_types");
        try (InputStream inputStream = Resources.asByteSource(Resources.getResource("io/trino/tests/product/hive/openx.json")).openStream()) {
            hdfsClient.saveFile(warehouseDirectory + "/TestOpenXJsonHiveTable/openx_types/openx.json", inputStream);
        }
        String createTableTemplate = "CREATE EXTERNAL TABLE openx_types (" +
                "   c_tinyint            TINYINT," +
                "   c_smallint           SMALLINT," +
                "   c_int                INT," +
                "   c_bigint             BIGINT," +
                "   c_float              FLOAT," +
                "   c_double             DOUBLE," +
                "   c_decimal            DECIMAL," +
                "   c_decimal_short      DECIMAL(10,5)," +
                "   c_decimal_full       DECIMAL(38,18)," +
                "   c_timestamp          TIMESTAMP," +
                "   c_date               DATE," +
                "   c_string             STRING," +
                "   c_varchar            VARCHAR(10)," +
                "   c_char               CHAR(10)," +
                "   c_boolean            BOOLEAN," +
                "   c_binary             BINARY" +
                ") ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' " +
                "STORED AS TEXTFILE " +
                "LOCATION 'hdfs://hadoop-master:9000%s/TestOpenXJsonHiveTable/openx_types'";
        onHive().executeQuery(format(createTableTemplate, warehouseDirectory));
    }

    @AfterTestWithContext
    public void cleanup()
    {
        onHive().executeQuery("DROP TABLE openx_types");
        hdfsClient.delete(warehouseDirectory + "/TestOpenXJsonHiveTable");
    }

    @Test(groups = {HIVE_OPENX, PROFILE_SPECIFIC_TESTS})
    public void testSelectOpenXDatatypes()
    {
        assertThat(onTrino().executeQuery("DESCRIBE hive_timestamp_nanos.default.openx_types").project(1, 2)).containsExactlyInOrder(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "real"),
                row("c_double", "double"),
                row("c_decimal", "decimal(10,0)"),
                row("c_decimal_short", "decimal(10,5)"),
                row("c_decimal_full", "decimal(38,18)"),
                row("c_timestamp", "timestamp(9)"),
                row("c_date", "date"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_char", "char(10)"),
                row("c_boolean", "boolean"),
                row("c_binary", "varbinary"));

        assertThat(onTrino().executeQuery(format("SELECT * FROM hive_timestamp_nanos.default.openx_types")))
                .hasColumns(
                        TINYINT,
                        SMALLINT,
                        INTEGER,
                        BIGINT,
                        REAL,
                        DOUBLE,
                        DECIMAL,
                        DECIMAL,
                        DECIMAL,
                        TIMESTAMP,
                        DATE,
                        VARCHAR,
                        VARCHAR,
                        CHAR,
                        BOOLEAN,
                        VARBINARY)
                .containsExactlyInOrder(EXPECTED_ROWS);

        assertThat(onTrino().executeQuery(format("SELECT * FROM openx_types")))
                .hasColumns(
                        TINYINT,
                        SMALLINT,
                        INTEGER,
                        BIGINT,
                        REAL,
                        DOUBLE,
                        DECIMAL,
                        DECIMAL,
                        DECIMAL,
                        TIMESTAMP,
                        DATE,
                        VARCHAR,
                        VARCHAR,
                        CHAR,
                        BOOLEAN,
                        VARBINARY)
                .containsExactlyInOrder(EXPECTED_ROWS_MILLISECONDS);
    }

    @Test(groups = {HIVE_OPENX, PROFILE_SPECIFIC_TESTS})
    public void testSelectOpenXDatatypesOnHive()
    {
        QueryExecutor hive = onHive();
        assertThat(hive.executeQuery("DESCRIBE openx_types").project(1, 2)).containsExactlyInOrder(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "int"),
                row("c_bigint", "bigint"),
                row("c_float", "float"),
                row("c_double", "double"),
                row("c_decimal", "decimal(10,0)"),
                row("c_decimal_short", "decimal(10,5)"),
                row("c_decimal_full", "decimal(38,18)"),
                row("c_timestamp", "timestamp"),
                row("c_date", "date"),
                row("c_string", "string"),
                row("c_varchar", "varchar(10)"),
                row("c_char", "char(10)"),
                row("c_boolean", "boolean"),
                row("c_binary", "binary"));

        if (getHiveVersionMajor() > 2) {
            assertThat(hive.executeQuery(format("SELECT * FROM openx_types")))
                    .hasColumns(
                            TINYINT,
                            SMALLINT,
                            INTEGER,
                            BIGINT,
                            FLOAT,
                            DOUBLE,
                            DECIMAL,
                            DECIMAL,
                            DECIMAL,
                            TIMESTAMP,
                            DATE,
                            VARCHAR,
                            VARCHAR,
                            CHAR,
                            BOOLEAN,
                            BINARY)
                    .containsExactlyInOrder(EXPECTED_ROWS);
        }
        else {
            assertQueryFailure(() -> hive.executeQuery(format("SELECT * FROM openx_types")))
                    .hasMessageMatching("Error retrieving next row|" +
                            "java.lang.NoClassDefFoundError: org/apache/hadoop/hive/common/type/Timestamp|" +
                            "java.lang.NoSuchMethodError: org.apache.hadoop.hive.common.type.HiveDecimal.create.*");
        }
    }
}
