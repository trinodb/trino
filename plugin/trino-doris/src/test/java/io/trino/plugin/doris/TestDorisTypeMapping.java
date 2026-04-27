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
package io.trino.plugin.doris;

import io.trino.Session;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
final class TestDorisTypeMapping
        extends AbstractTestQueryFramework
{
    private static final SchemaTableName ALL_TYPES = new SchemaTableName("type_mapping", "all_types");
    private static final SchemaTableName INTEGRAL_BOUNDARIES = new SchemaTableName("type_mapping", "integral_boundaries");
    private static final SchemaTableName DECIMAL_BOUNDARIES = new SchemaTableName("type_mapping", "decimal_boundaries");
    private static final SchemaTableName CHARACTER_BOUNDARIES = new SchemaTableName("type_mapping", "character_boundaries");
    private static final SchemaTableName TEMPORAL_BOUNDARIES = new SchemaTableName("type_mapping", "temporal_boundaries");

    private static final List<DorisRemoteColumn> ALL_TYPES_COLUMNS = List.of(
            column("bool_col", "BOOLEAN", null, null, 1),
            column("bool_alias_col", "TINYINT", 0, null, 2),
            column("tiny_alias_col", "TINYINT", 4, null, 3),
            column("tiny_col", "TINYINT", 4, null, 4),
            column("small_col", "SMALLINT", 6, null, 5),
            column("int_col", "INT", 11, null, 6),
            column("big_col", "BIGINT", 20, null, 7),
            column("unsigned_big_col", "BIGINT UNSIGNED", 20, 0, 8),
            column("decimal_col", "DECIMAL", 18, 2, 9),
            column("largeint_col", "LARGEINT", 39, 0, 10),
            column("char_col", "CHAR", 4, null, 11),
            column("varchar_col", "VARCHAR", 20, null, 12),
            column("string_col", "STRING", null, null, 13),
            column("date_col", "DATEV2", null, null, 14),
            column("timestamp_col", "DATETIMEV2", null, 6, 15),
            column("json_col", "JSON", null, null, 16),
            column("oversized_decimal_col", "DECIMAL256", 76, 6, 17));
    private static final List<DorisRemoteColumn> INTEGRAL_BOUNDARY_COLUMNS = List.of(
            column("row_id", "INT", 11, null, 1),
            column("bool_alias_col", "TINYINT", 0, null, 2),
            column("tiny_col", "TINYINT", 4, null, 3),
            column("small_col", "SMALLINT", 6, null, 4),
            column("int_col", "INT", 11, null, 5),
            column("big_col", "BIGINT", 20, null, 6),
            column("unsigned_big_col", "BIGINT UNSIGNED", 20, 0, 7),
            column("largeint_col", "LARGEINT", 39, 0, 8));
    private static final List<DorisRemoteColumn> DECIMAL_BOUNDARY_COLUMNS = List.of(
            column("row_id", "INT", 11, null, 1),
            column("decimal_38_0_col", "DECIMAL", 38, 0, 2),
            column("decimal_38_18_col", "DECIMAL", 38, 18, 3),
            column("oversized_decimal_col", "DECIMAL256", 76, 6, 4));
    private static final List<DorisRemoteColumn> CHARACTER_BOUNDARY_COLUMNS = List.of(
            column("row_id", "INT", 11, null, 1),
            column("char_col", "CHAR", 4, null, 2),
            column("varchar_col", "VARCHAR", 20, null, 3),
            column("string_col", "STRING", null, null, 4));
    private static final List<DorisRemoteColumn> TEMPORAL_BOUNDARY_COLUMNS = List.of(
            column("row_id", "INT", 11, null, 1),
            column("date_col", "DATEV2", null, null, 2),
            column("timestamp_col", "DATETIMEV2", null, 6, 3));

    private final TestingDorisEnvironment environment = new TestingDorisEnvironment(List.of(
            new TestingDorisEnvironment.TableDefinition(
                    ALL_TYPES,
                    ALL_TYPES_COLUMNS,
                    List.of(nonNullRow(), nullRow(ALL_TYPES_COLUMNS))),
            new TestingDorisEnvironment.TableDefinition(
                    INTEGRAL_BOUNDARIES,
                    INTEGRAL_BOUNDARY_COLUMNS,
                    List.of(
                            row(
                                    "row_id",
                                    1L,
                                    "bool_alias_col",
                                    0L,
                                    "tiny_col",
                                    (long) Byte.MIN_VALUE,
                                    "small_col",
                                    (long) Short.MIN_VALUE,
                                    "int_col",
                                    (long) Integer.MIN_VALUE,
                                    "big_col",
                                    Long.MIN_VALUE,
                                    "unsigned_big_col",
                                    new BigDecimal("0"),
                                    "largeint_col",
                                    new BigInteger("-170141183460469231731687303715884105728")),
                            row(
                                    "row_id",
                                    2L,
                                    "bool_alias_col",
                                    1L,
                                    "tiny_col",
                                    (long) Byte.MAX_VALUE,
                                    "small_col",
                                    (long) Short.MAX_VALUE,
                                    "int_col",
                                    (long) Integer.MAX_VALUE,
                                    "big_col",
                                    Long.MAX_VALUE,
                                    "unsigned_big_col",
                                    new BigDecimal("18446744073709551615"),
                                    "largeint_col",
                                    new BigInteger("170141183460469231731687303715884105727")),
                            nullRow(INTEGRAL_BOUNDARY_COLUMNS, "row_id", 3L))),
            new TestingDorisEnvironment.TableDefinition(
                    DECIMAL_BOUNDARIES,
                    DECIMAL_BOUNDARY_COLUMNS,
                    List.of(
                            row(
                                    "row_id",
                                    1L,
                                    "decimal_38_0_col",
                                    new BigDecimal("-99999999999999999999999999999999999999"),
                                    "decimal_38_18_col",
                                    new BigDecimal("-99999999999999999999.999999999999999999"),
                                    "oversized_decimal_col",
                                    new BigDecimal("-1234567890123456789012345678901234567890.123456")),
                            row(
                                    "row_id",
                                    2L,
                                    "decimal_38_0_col",
                                    new BigDecimal("99999999999999999999999999999999999999"),
                                    "decimal_38_18_col",
                                    new BigDecimal("99999999999999999999.999999999999999999"),
                                    "oversized_decimal_col",
                                    new BigDecimal("1234567890123456789012345678901234567890.123456")),
                            nullRow(DECIMAL_BOUNDARY_COLUMNS, "row_id", 3L))),
            new TestingDorisEnvironment.TableDefinition(
                    CHARACTER_BOUNDARIES,
                    CHARACTER_BOUNDARY_COLUMNS,
                    List.of(
                            row(
                                    "row_id",
                                    1L,
                                    "char_col",
                                    "",
                                    "varchar_col",
                                    "",
                                    "string_col",
                                    ""),
                            row(
                                    "row_id",
                                    2L,
                                    "char_col",
                                    "xy  ",
                                    "varchar_col",
                                    "trailing spaces  ",
                                    "string_col",
                                    "中文 café 日本語"),
                            row(
                                    "row_id",
                                    3L,
                                    "char_col",
                                    "abcd",
                                    "varchar_col",
                                    "hello doris",
                                    "string_col",
                                    "emoji 😀 and accents é"),
                            nullRow(CHARACTER_BOUNDARY_COLUMNS, "row_id", 4L))),
            new TestingDorisEnvironment.TableDefinition(
                    TEMPORAL_BOUNDARIES,
                    TEMPORAL_BOUNDARY_COLUMNS,
                    List.of(
                            row(
                                    "row_id",
                                    1L,
                                    "date_col",
                                    LocalDate.of(1582, 10, 4),
                                    "timestamp_col",
                                    LocalDateTime.of(2024, 3, 10, 1, 59, 59, 999_999_000)),
                            row(
                                    "row_id",
                                    2L,
                                    "date_col",
                                    LocalDate.of(1582, 10, 15),
                                    "timestamp_col",
                                    LocalDateTime.of(2024, 3, 10, 3, 0, 0, 0)),
                            row(
                                    "row_id",
                                    3L,
                                    "date_col",
                                    LocalDate.of(2024, 11, 3),
                                    "timestamp_col",
                                    LocalDateTime.of(2024, 11, 3, 1, 30, 0, 0)),
                            nullRow(TEMPORAL_BOUNDARY_COLUMNS, "row_id", 4L)))));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DorisQueryRunner.builder(environment).build();
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSchema(ALL_TYPES.getSchemaName())
                .build();
    }

    @Test
    void testShowColumns()
    {
        assertQuery(
                "SHOW COLUMNS FROM all_types",
                """
                VALUES
                    ('bool_col', 'boolean', '', ''),
                    ('bool_alias_col', 'boolean', '', ''),
                    ('tiny_alias_col', 'tinyint', '', ''),
                    ('tiny_col', 'tinyint', '', ''),
                    ('small_col', 'smallint', '', ''),
                    ('int_col', 'integer', '', ''),
                    ('big_col', 'bigint', '', ''),
                    ('unsigned_big_col', 'decimal(20,0)', '', ''),
                    ('decimal_col', 'decimal(18,2)', '', ''),
                    ('largeint_col', 'number', '', ''),
                    ('char_col', 'char(4)', '', ''),
                    ('varchar_col', 'varchar(20)', '', ''),
                    ('string_col', 'varchar', '', ''),
                    ('date_col', 'date', '', ''),
                    ('timestamp_col', 'timestamp(6)', '', ''),
                    ('json_col', 'varchar', '', ''),
                    ('oversized_decimal_col', 'varchar', '', '')
                """);
    }

    @Test
    void testReadPrimitiveAndDecimalTypes()
    {
        assertQuery(
                """
                SELECT bool_col, bool_alias_col, tiny_alias_col, tiny_col, small_col, int_col, big_col, unsigned_big_col, decimal_col
                FROM all_types
                ORDER BY big_col NULLS LAST
                """,
                """
                VALUES
                    (true, false, CAST(1 AS tinyint), CAST(7 AS tinyint), CAST(12 AS smallint), 34567, CAST(9876543210 AS bigint), CAST('18446744073709551615' AS decimal(20,0)), CAST('1234567890123456.78' AS decimal(18,2))),
                    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """);
    }

    @Test
    void testReadCharacterTemporalAndFallbackTypes()
    {
        assertThat(query(
                """
                SELECT varchar_col, string_col, date_col, timestamp_col, json_col, oversized_decimal_col, largeint_col
                FROM all_types
                ORDER BY big_col NULLS LAST
                """))
                .ordered()
                .matches(
                        """
                        VALUES
                            (CAST('hello doris' AS varchar(20)), CAST('wide text' AS varchar), DATE '2024-03-20', TIMESTAMP '2024-07-25 10:02:23.586123', CAST('{"k":1}' AS varchar), CAST('1234567890123456789012345678901234567890.123456' AS varchar), NUMBER '170141183460469231731687303715884105'),
                            (CAST(NULL AS varchar(20)), CAST(NULL AS varchar), CAST(NULL AS date), CAST(NULL AS timestamp(6)), CAST(NULL AS varchar), CAST(NULL AS varchar), CAST(NULL AS NUMBER))
                        """);
    }

    @Test
    void testIntegralBoundaries()
    {
        assertThat(query(
                """
                SELECT bool_alias_col, tiny_col, small_col, int_col, big_col, unsigned_big_col, largeint_col
                FROM integral_boundaries
                ORDER BY row_id
                """))
                .ordered()
                .matches(
                        """
                        VALUES
                            (false, CAST(-128 AS tinyint), CAST(-32768 AS smallint), CAST(-2147483648 AS integer), BIGINT '-9223372036854775808', CAST('0' AS decimal(20,0)), NUMBER '-170141183460469231731687303715884105728'),
                            (true, CAST(127 AS tinyint), CAST(32767 AS smallint), CAST(2147483647 AS integer), BIGINT '9223372036854775807', CAST('18446744073709551615' AS decimal(20,0)), NUMBER '170141183460469231731687303715884105727'),
                            (CAST(NULL AS boolean), CAST(NULL AS tinyint), CAST(NULL AS smallint), CAST(NULL AS integer), CAST(NULL AS bigint), CAST(NULL AS decimal(20,0)), CAST(NULL AS NUMBER))
                        """);
    }

    @Test
    void testDecimalBoundaries()
    {
        assertThat(query(
                """
                SELECT decimal_38_0_col, decimal_38_18_col, oversized_decimal_col
                FROM decimal_boundaries
                ORDER BY row_id
                """))
                .ordered()
                .matches(
                        """
                        VALUES
                            (CAST('-99999999999999999999999999999999999999' AS decimal(38,0)), CAST('-99999999999999999999.999999999999999999' AS decimal(38,18)), CAST('-1234567890123456789012345678901234567890.123456' AS varchar)),
                            (CAST('99999999999999999999999999999999999999' AS decimal(38,0)), CAST('99999999999999999999.999999999999999999' AS decimal(38,18)), CAST('1234567890123456789012345678901234567890.123456' AS varchar)),
                            (CAST(NULL AS decimal(38,0)), CAST(NULL AS decimal(38,18)), CAST(NULL AS varchar))
                        """);
    }

    @Test
    void testCharacterBoundaries()
    {
        assertThat(query(
                """
                SELECT char_col, varchar_col, string_col, length(varchar_col), length(string_col)
                FROM character_boundaries
                ORDER BY row_id
                """))
                .ordered()
                .matches(
                        """
                        VALUES
                            (CAST('' AS char(4)), CAST('' AS varchar(20)), CAST('' AS varchar), BIGINT '0', BIGINT '0'),
                            (CAST('xy' AS char(4)), CAST('trailing spaces  ' AS varchar(20)), CAST('中文 café 日本語' AS varchar), BIGINT '17', BIGINT '11'),
                            (CAST('abcd' AS char(4)), CAST('hello doris' AS varchar(20)), CAST('emoji 😀 and accents é' AS varchar), BIGINT '11', BIGINT '21'),
                            (CAST(NULL AS char(4)), CAST(NULL AS varchar(20)), CAST(NULL AS varchar), CAST(NULL AS bigint), CAST(NULL AS bigint))
                        """);
    }

    @Test
    void testTemporalBoundaries()
    {
        assertThat(query(
                """
                SELECT date_col, timestamp_col
                FROM temporal_boundaries
                ORDER BY row_id
                """))
                .ordered()
                .matches(
                        """
                        VALUES
                            (DATE '1582-10-04', TIMESTAMP '2024-03-10 01:59:59.999999'),
                            (DATE '1582-10-15', TIMESTAMP '2024-03-10 03:00:00.000000'),
                            (DATE '2024-11-03', TIMESTAMP '2024-11-03 01:30:00.000000'),
                            (CAST(NULL AS date), CAST(NULL AS timestamp(6)))
                        """);
    }

    private static DorisRemoteColumn column(String name, String type, Integer size, Integer scale, int ordinalPosition)
    {
        return new DorisRemoteColumn(
                name,
                type,
                Optional.ofNullable(size),
                Optional.ofNullable(scale),
                ordinalPosition);
    }

    private static Map<String, Object> nonNullRow()
    {
        return row(
                "bool_col",
                true,
                "bool_alias_col",
                false,
                "tiny_alias_col",
                1L,
                "tiny_col",
                7L,
                "small_col",
                12L,
                "int_col",
                34567L,
                "big_col",
                9_876_543_210L,
                "unsigned_big_col",
                new BigDecimal("18446744073709551615"),
                "decimal_col",
                new BigDecimal("1234567890123456.78"),
                "largeint_col",
                new BigInteger("170141183460469231731687303715884105"),
                "char_col",
                "xy  ",
                "varchar_col",
                "hello doris",
                "string_col",
                "wide text",
                "date_col",
                LocalDate.of(2024, 3, 20),
                "timestamp_col",
                LocalDateTime.of(2024, 7, 25, 10, 2, 23, 586_123_000),
                "json_col",
                "{\"k\":1}",
                "oversized_decimal_col",
                new BigDecimal("1234567890123456789012345678901234567890.123456"));
    }

    private static Map<String, Object> nullRow(List<DorisRemoteColumn> columns)
    {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        for (DorisRemoteColumn column : columns) {
            row.put(column.columnName(), null);
        }
        return row;
    }

    private static Map<String, Object> nullRow(List<DorisRemoteColumn> columns, String key, Object value)
    {
        Map<String, Object> row = nullRow(columns);
        row.put(key, value);
        return row;
    }

    private static Map<String, Object> row(Object... values)
    {
        requireNonNull(values, "values is null");
        if (values.length % 2 != 0) {
            throw new IllegalArgumentException("Row values must contain key/value pairs");
        }

        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            row.put((String) values[index], values[index + 1]);
        }
        return row;
    }
}
