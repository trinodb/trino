package tech.ydb.trino;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.ydb.test.junit5.YdbHelperExtension;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.type.DateTimes.parseTimestamp;
import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ResourceLock("YDB_HELPER")
public class TestYdbJoinPushdown extends AbstractTestQueryFramework {
    @RegisterExtension
    static final YdbHelperExtension ydb = new YdbHelperExtension().failIfUnavailable();

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        return YdbQueryRunner.builder(ydb).useProductionClient().build();
    }

    @ParameterizedTest
    @MethodSource("scalarKeys")
    public void testScalarJoinPushdown(String type, List<String> keys) {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable left = nativeTable(executor, "join_scalar_left_", "(id Int64, k " + type + ", PRIMARY KEY (id))", "id, k", rows(keys, 0));
                TestTable right = nativeTable(executor, "join_scalar_right_", "(id Int64, k " + type + ", PRIMARY KEY (id))", "id, k", rows(keys, 100))) {
            for (String joinType : List.of("INNER", "LEFT", "RIGHT", "FULL")) {
                // QueryAssert compares with the same query with connector pushdown disabled.
                assertThat(query(joinSession(), "SELECT l.id, r.id FROM " + left.getName() + " l " +
                        joinType + " JOIN " + right.getName() + " r ON l.k = r.k")).isFullyPushedDown();
            }
            assertThat(query(joinSession(), "SELECT l.id, r.id FROM " + left.getName() + " l JOIN " +
                    right.getName() + " r ON l.k IS NOT DISTINCT FROM r.k")).joinIsNotFullyPushedDown();
        }
    }

    public static Stream<Arguments> scalarKeys() {
        return Stream.of(
                arguments("Bool", List.of("true", "false", "NULL")),
                arguments("Int8", List.of("CAST(-128 AS Int8)", "CAST(127 AS Int8)", "NULL")),
                arguments("Int16", List.of("CAST(-32768 AS Int16)", "CAST(32767 AS Int16)", "NULL")),
                arguments("Int32", List.of("CAST(-2147483648 AS Int32)", "CAST(2147483647 AS Int32)", "NULL")),
                arguments("Int64", List.of("-9223372036854775808l", "9223372036854775807l", "NULL")),
                arguments("Uint8", List.of("CAST(0 AS Uint8)", "CAST(255 AS Uint8)", "NULL")),
                arguments("Uint16", List.of("CAST(0 AS Uint16)", "CAST(65535 AS Uint16)", "NULL")),
                arguments("Uint32", List.of("CAST(0 AS Uint32)", "CAST(4294967295ul AS Uint32)", "NULL")),
                arguments("Uint64", List.of("0ul", "9223372036854775808ul", "18446744073709551615ul", "NULL")),
                arguments("Float", List.of("Float('0')", "Float('-0')", "Float('nan')", "Float('inf')", "Float('-inf')", "NULL")),
                arguments("Double", List.of("Double('0')", "Double('-0')", "Double('nan')", "Double('inf')", "Double('-inf')", "NULL")),
                arguments("Utf8", List.of("'A'u", "'a'u", "'a 'u", "'\u00e9'u", "'e\u0301'u", "NULL")),
                arguments("String", List.of("String('')", "String('\\x00\\x80\\xff')", "NULL")),
                arguments("Date", List.of("Date('1970-01-01')", "Date('2105-12-31')", "NULL")),
                arguments("Date32", List.of("Date32('1969-12-31')", "Date32('9999-12-31')", "NULL")),
                arguments("Datetime", List.of("Datetime('1970-01-01T00:00:00Z')", "Datetime('2020-02-29T12:34:56Z')", "NULL")),
                arguments("Datetime64", List.of("Datetime64('1969-12-31T23:59:59Z')", "Datetime64('2020-02-29T12:34:56Z')", "NULL")),
                arguments("Timestamp", List.of("Timestamp('1970-01-01T00:00:00Z')", "Timestamp('2020-02-29T12:34:56.123456Z')", "NULL")),
                arguments("Timestamp64", List.of("Timestamp64('1969-12-31T23:59:59.999999Z')", "Timestamp64('2020-02-29T12:34:56.123456Z')", "NULL")));
    }

    @Test
    public void testDecimalAndComputedKeys() {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        List<String> values = List.of("1, 1, Decimal('1.25', 22, 9)", "2, 2, NULL");
        try (TestTable left = nativeTable(executor, "join_fallback_left_", "(id Int64, k Int64, d Decimal(22,9), PRIMARY KEY (id))", "id, k, d", values);
                TestTable right = nativeTable(executor, "join_fallback_right_", "(id Int64, k Int64, d Decimal(22,9), PRIMARY KEY (id))", "id, k, d", values)) {
            String join = "SELECT l.id, r.id FROM " + left.getName() + " l JOIN " + right.getName() + " r ON ";
            assertThat(query(joinSession(), join + "l.d = r.d")).joinIsNotFullyPushedDown();
            assertThat(query(joinSession(), join + "l.k + 1 = r.k")).joinIsNotFullyPushedDown();
            assertThat(query(joinSession(), join + "CAST(l.k AS SMALLINT) = CAST(r.k AS SMALLINT)")).joinIsNotFullyPushedDown();
            assertThat(query(joinSession(), join + "l.k < r.k")).joinIsNotFullyPushedDown();
        }
    }

    @Test
    public void testMixedNativeIntegerKeys() {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable left = nativeTable(executor, "join_mixed_left_", "(id Int64, k Int16, PRIMARY KEY (id))", "id, k",
                List.of("1, -1", "2, 255", "3, NULL"));
                TestTable right = nativeTable(executor, "join_mixed_right_", "(id Int64, k Uint8, PRIMARY KEY (id))", "id, k",
                        List.of("10, 255", "11, 0", "12, NULL"))) {
            for (String joinType : List.of("INNER", "LEFT", "RIGHT", "FULL")) {
                assertThat(query(joinSession(), "SELECT l.id, r.id FROM " + left.getName() + " l " +
                        joinType + " JOIN " + right.getName() + " r ON l.k = r.k")).isFullyPushedDown();
            }
        }
    }

    @Test
    public void testUnicodeProjectionAndNulls() {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable table = nativeTable(executor, "unicode_position_", "(id Int64, s Utf8, sub Utf8, PRIMARY KEY (id))", "id, s, sub",
                List.of("1, '\u00e9x'u, 'x'u", "2, NULL, 'x'u", "3, 'x'u, NULL", "4, 'x'u, 'missing'u"))) {
            assertThat(query("SELECT id, strpos(s, sub) FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1', BIGINT '2'), (2, NULL), (3, NULL), (4, 0)")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testFloatingPointTopN() {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable table = nativeTable(executor, "float_topn_", "(id Int64, k Double, PRIMARY KEY (id))", "id, k",
                rows(List.of("Double('nan')", "Double('-0')", "Double('0')", "Double('-inf')", "Double('inf')", "NULL"), 0))) {
            for (String order : List.of("ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST")) {
                assertThat(query("SELECT id, k FROM " + table.getName() + " ORDER BY k " + order + ", id LIMIT 5"))
                        .ordered()
                        .isFullyPushedDown();
            }
        }
    }

    @Test
    public void testOverflowIsEvaluatedByTrino() {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable left = nativeTable(executor, "join_overflow_left_", "(id Int64, k Int64, PRIMARY KEY (id))", "id, k",
                List.of("1, 9223372036854775807l", "2, -9223372036854775808l"));
                TestTable right = nativeTable(executor, "join_overflow_right_", "(id Int64, k Int64, PRIMARY KEY (id))", "id, k", List.of("1, 0"))) {
            String join = "SELECT l.id FROM " + left.getName() + " l JOIN " + right.getName() + " r ON ";
            assertThat(query(joinSession(), join + "l.k + 1 = r.k")).failure().hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            assertThat(query(joinSession(), join + "l.k / -1 = r.k")).failure().hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            assertThat(query(joinSession(), join + "CAST(l.k AS SMALLINT) = r.k")).failure().hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            assertThat(query("SELECT id, k % BIGINT '-1' FROM " + left.getName()))
                    .matches("VALUES (BIGINT '1', BIGINT '0'), (2, 0)");
            assertThat(query("SELECT id FROM " + left.getName() + " WHERE k % BIGINT '-1' = 0"))
                    .matches("VALUES BIGINT '1', BIGINT '2'");
            assertThat(query(joinSession(), join + "l.k % BIGINT '-1' = r.k"))
                    .matches("VALUES BIGINT '1', BIGINT '2'")
                    .joinIsNotFullyPushedDown();
        }
    }

    @Test
    public void testTimestamp64PredicateBoundaries() {
        long minimum = -4611669897600000000L;
        long maximum = 4611669811199999999L;
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable table = nativeTable(executor, "timestamp_bounds_", "(id Int64, k Timestamp64, PRIMARY KEY (id))", "id, k",
                List.of("1, CAST(" + minimum + "l AS Timestamp64)", "2, CAST(0l AS Timestamp64)",
                        "3, CAST(" + maximum + "l AS Timestamp64)", "4, NULL"))) {
            String select = "SELECT id FROM " + table.getName();
            assertThat(query(select + " WHERE k = " + timestampLiteral(minimum))).matches("VALUES BIGINT '1'").isFullyPushedDown();
            assertThat(query(select + " WHERE k = " + timestampLiteral(maximum))).matches("VALUES BIGINT '3'").isFullyPushedDown();
            assertThat(query(select + " WHERE k IN (" + timestampLiteral(minimum) + ", " + timestampLiteral(maximum) + ")"))
                    .matches("VALUES BIGINT '1', BIGINT '3'").isFullyPushedDown();
            assertThat(query(select + " WHERE k >= " + timestampLiteral(minimum))).matches("VALUES BIGINT '1', BIGINT '2', BIGINT '3'").isFullyPushedDown();
            assertThat(query(select + " WHERE k <= " + timestampLiteral(maximum))).matches("VALUES BIGINT '1', BIGINT '2', BIGINT '3'").isFullyPushedDown();
            assertThat(query(select + " WHERE k IS NULL")).matches("VALUES BIGINT '4'").isFullyPushedDown();
            assertThat(query(select + " WHERE k IS NOT NULL")).matches("VALUES BIGINT '1', BIGINT '2', BIGINT '3'").isFullyPushedDown();
            assertThat(query(select + " WHERE k < " + timestampLiteral(maximum + 1))).matches("VALUES BIGINT '1', BIGINT '2', BIGINT '3'");
            assertThat(query(select + " WHERE k > " + timestampLiteral(minimum - 1))).matches("VALUES BIGINT '1', BIGINT '2', BIGINT '3'");
            assertThat(query(select + " WHERE k = " + timestampLiteral(maximum + 1))).returnsEmptyResult();
            assertThat(query(select + " WHERE k = " + timestampLiteral(minimum - 1))).returnsEmptyResult();
            assertThat(query(select + " WHERE k < " + timestampLiteral(maximum + 1) + " OR id = 4"))
                    .matches("VALUES BIGINT '1', BIGINT '2', BIGINT '3', BIGINT '4'");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (5, NULL), (6, " + timestampLiteral(maximum) +
                    "), (7, " + timestampLiteral(minimum) + ")", 3);
            assertThat(query("SELECT id, k IS NULL FROM " + table.getName() + " WHERE id >= 5"))
                    .matches("VALUES (BIGINT '5', true), (6, false), (7, false)");
            assertThat(query(select + " WHERE id = 6 AND k = " + timestampLiteral(maximum))).matches("VALUES BIGINT '6'");
        }
        try (TestTable table = new TestTable(getQueryRunner()::execute, "timestamp_notnull_",
                "(id bigint, k timestamp(6) NOT NULL) WITH (primary_key = ARRAY['id'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, " + timestampLiteral(maximum) +
                    "), (2, " + timestampLiteral(minimum) + ")", 2);
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE k = " + timestampLiteral(maximum)))
                    .matches("VALUES BIGINT '1'");
        }
    }

    private static String timestampLiteral(long micros) {
        String value = LocalDateTime.ofEpochSecond(Math.floorDiv(micros, 1_000_000),
                        (int) Math.floorMod(micros, 1_000_000) * 1_000, UTC)
                .format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS", Locale.ROOT));
        assertThat(parseTimestamp(6, value)).isEqualTo(micros);
        return "TIMESTAMP '" + value + "'";
    }

    private Session joinSession() {
        return Session.builder(getSession()).setCatalogSessionProperty("local", "join_pushdown_enabled", "true").build();
    }

    private static TestTable nativeTable(JdbcSqlExecutor executor, String prefix, String definition, String columns, List<String> rows) {
        TestTable table = new TestTable(executor, prefix, definition);
        try {
            executor.execute("INSERT INTO " + table.getName() + " (" + columns + ") VALUES " +
                    rows.stream().map(row -> "(" + row + ")").collect(joining(", ")));
            return table;
        }
        catch (RuntimeException e) {
            try {
                table.close();
            }
            catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    private static List<String> rows(List<String> keys, int offset) {
        return IntStream.range(0, keys.size()).mapToObj(index -> (offset + index + 1) + ", " + keys.get(index)).toList();
    }
}
