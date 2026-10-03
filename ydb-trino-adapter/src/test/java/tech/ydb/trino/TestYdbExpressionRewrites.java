package tech.ydb.trino;

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.type.BigintOperators;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.DIVIDE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MODULO_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NULLIF_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestYdbExpressionRewrites {
    private final YdbClient client = new YdbClient(
            new BaseJdbcConfig(),
            _ -> {
                throw new SQLException("This test must not open a connection");
            },
            new YdbQueryBuilder(RemoteQueryModifier.NONE),
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

    @Test
    public void testNullIfParameterOrder() {
        Call expression = new Call(BIGINT, NULLIF_FUNCTION_NAME, List.of(
                new Constant(11L, BIGINT), new Constant(22L, BIGINT)));
        var result = client.convertPredicate(SESSION, expression, Map.of()).orElseThrow();
        assertThat(result.parameters()).extracting(parameter -> parameter.getValue().orElseThrow())
                .containsExactly(11L, 22L, 11L);
    }

    @Test
    public void testStringPositionParameterOrder() {
        Call expression = new Call(BIGINT, new FunctionName("strpos"), List.of(
                new Constant(utf8Slice("hello"), VARCHAR), new Constant(utf8Slice("ll"), VARCHAR)));
        JdbcTableHandle table = new JdbcTableHandle(
                new SchemaTableName("default", "test"),
                new RemoteTableName(Optional.empty(), Optional.empty(), "test"),
                Optional.empty());
        var result = client.convertProjection(SESSION, table, expression, Map.of()).orElseThrow();
        assertThat(result.getParameters()).extracting(parameter -> parameter.getValue().orElseThrow())
                .containsExactly(utf8Slice("hello"), utf8Slice("ll"), utf8Slice("hello"), utf8Slice("ll"));
    }

    @Test
    public void testUnicodeCaseRewritesAndTrimFallback() {
        JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(VARCHAR).orElseThrow(), VARCHAR);
        JdbcTableHandle table = new JdbcTableHandle(new SchemaTableName("default", "test"),
                new RemoteTableName(Optional.empty(), Optional.empty(), "test"), Optional.empty());
        for (String function : List.of("upper", "lower")) {
            assertThat(client.convertProjection(SESSION, table,
                    new Call(VARCHAR, new FunctionName(function), List.of(new Variable("v", VARCHAR))), Map.of("v", column))).isPresent();
        }
        assertThat(client.convertProjection(SESSION, table,
                new Call(VARCHAR, new FunctionName("trim"), List.of(new Variable("v", VARCHAR))), Map.of("v", column))).isEmpty();
    }

    @Test
    public void testIntegralDivisionAndModulusPushdown() {
        for (var entry : Map.of(DIVIDE_FUNCTION_NAME, "(?) / (?)", MODULO_FUNCTION_NAME, "(?) % (?)").entrySet()) {
            var result = client.convertPredicate(SESSION, new Call(BIGINT, entry.getKey(), List.of(
                    new Constant(11L, BIGINT), new Constant(2L, BIGINT))), Map.of()).orElseThrow();
            assertThat(result.expression()).isEqualTo(entry.getValue());
            assertThat(result.parameters()).extracting(parameter -> parameter.getValue().orElseThrow())
                    .containsExactly(11L, 2L);
            assertThat(client.convertPredicate(SESSION, new Call(BIGINT, entry.getKey(), List.of(
                    new Constant(11L, BIGINT), new Constant(0L, BIGINT))), Map.of())).isEmpty();
        }
    }

    @Test
    public void testSignedMinimumModuloFallsBack() {
        assertThat(BigintOperators.modulo(Long.MIN_VALUE, -1)).isZero();
        JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(BIGINT).orElseThrow(), BIGINT);
        assertThat(client.convertPredicate(SESSION, new Call(BIGINT, MODULO_FUNCTION_NAME, List.of(
                new Variable("v", BIGINT), new Constant(-1L, BIGINT))), Map.of("v", column))).isEmpty();
    }

    @Test
    public void testTimestampLiteralPredicatesAreNotRewritten() {
        // Literal rewriting is unsupported even in range; domain and writer bounds have separate tests.
        JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(TIMESTAMP_MICROS).orElseThrow(), TIMESTAMP_MICROS);
        for (long bound : List.of(-4611669897600000001L, 0L, 4611669811200000000L)) {
            assertThat(client.convertPredicate(SESSION, new Call(BOOLEAN, LESS_THAN_OPERATOR_FUNCTION_NAME, List.of(
                    new Variable("v", TIMESTAMP_MICROS), new Constant(bound, TIMESTAMP_MICROS))), Map.of("v", column))).isEmpty();
        }
    }

    @Test
    public void testOverflowingArithmeticIsNotPushedDown() {
        JdbcColumnHandle column = new JdbcColumnHandle("value", YdbTypeUtils.toTypeHandle(BIGINT).orElseThrow(), BIGINT);
        assertThat(client.convertPredicate(SESSION, new Call(BIGINT, ADD_FUNCTION_NAME, List.of(
                new Variable("v", BIGINT), new Constant(1L, BIGINT))), Map.of("v", column))).isEmpty();
        assertThat(client.convertPredicate(SESSION, new Call(BIGINT, DIVIDE_FUNCTION_NAME, List.of(
                new Variable("v", BIGINT), new Constant(-1L, BIGINT))), Map.of("v", column))).isEmpty();
    }
}
