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

@Execution(ExecutionMode.SAME_THREAD)
final class TestDorisTypeMapping
        extends AbstractTestQueryFramework
{
    private static final SchemaTableName ALL_TYPES = new SchemaTableName("type_mapping", "all_types");
    private static final List<DorisRemoteColumn> ALL_TYPES_COLUMNS = List.of(
            column("bool_col", "BOOLEAN", null, null, 1),
            column("bool_alias_col", "TINYINT", 0, null, 2),
            column("tiny_col", "TINYINT", 4, null, 3),
            column("small_col", "SMALLINT", 6, null, 4),
            column("int_col", "INT", 11, null, 5),
            column("big_col", "BIGINT", 20, null, 6),
            column("unsigned_big_col", "BIGINT UNSIGNED", 20, 0, 7),
            column("decimal_col", "DECIMAL", 18, 2, 8),
            column("largeint_col", "LARGEINT", 39, 0, 9),
            column("char_col", "CHAR", 4, null, 10),
            column("varchar_col", "VARCHAR", 20, null, 11),
            column("string_col", "STRING", null, null, 12),
            column("date_col", "DATEV2", null, null, 13),
            column("timestamp_col", "DATETIMEV2", null, 6, 14),
            column("json_col", "JSON", null, null, 15),
            column("oversized_decimal_col", "DECIMAL256", 76, 6, 16));

    private final TestingDorisEnvironment environment = new TestingDorisEnvironment(List.of(
            new TestingDorisEnvironment.TableDefinition(
                    ALL_TYPES,
                    ALL_TYPES_COLUMNS,
                    List.of(nonNullRow(), nullRow()))));

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
                    ('tiny_col', 'tinyint', '', ''),
                    ('small_col', 'smallint', '', ''),
                    ('int_col', 'integer', '', ''),
                    ('big_col', 'bigint', '', ''),
                    ('unsigned_big_col', 'decimal(20,0)', '', ''),
                    ('decimal_col', 'decimal(18,2)', '', ''),
                    ('largeint_col', 'varchar', '', ''),
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
                SELECT bool_col, bool_alias_col, tiny_col, small_col, int_col, big_col, unsigned_big_col, decimal_col
                FROM all_types
                ORDER BY big_col NULLS LAST
                """,
                """
                VALUES
                    (true, false, CAST(7 AS tinyint), CAST(12 AS smallint), 34567, CAST(9876543210 AS bigint), CAST('18446744073709551615' AS decimal(20,0)), CAST('1234567890123456.78' AS decimal(18,2))),
                    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """);
    }

    @Test
    void testReadCharacterTemporalAndFallbackTypes()
    {
        assertQuery(
                """
                SELECT varchar_col, string_col, date_col, timestamp_col, json_col, oversized_decimal_col, largeint_col
                FROM all_types
                ORDER BY big_col NULLS LAST
                """,
                """
                VALUES
                    ('hello doris', 'wide text', DATE '2024-03-20', TIMESTAMP '2024-07-25 10:02:23.586123', '{"k":1}', '1234567890123456789012345678901234567890.123456', '170141183460469231731687303715884105'),
                    (NULL, NULL, NULL, NULL, NULL, NULL, NULL)
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

    private static Map<String, Object> nullRow()
    {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        for (DorisRemoteColumn column : ALL_TYPES_COLUMNS) {
            row.put(column.columnName(), null);
        }
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
