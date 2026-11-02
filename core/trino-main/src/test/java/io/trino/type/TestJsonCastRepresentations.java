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
package io.trino.type;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.json.Json;
import io.trino.json.JsonItems;
import io.trino.json.TypedValue;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.operator.scalar.JsonOperators;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TrinoNumber;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonCastRepresentations
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestJsonCastRepresentations.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testNegativeZero()
    {
        Map<String, Double> reciprocals = ImmutableMap.<String, Double>builder()
                .put("0", POSITIVE_INFINITY)
                .put("-0", POSITIVE_INFINITY)
                .put("-0.0", POSITIVE_INFINITY)
                .put("0e0", POSITIVE_INFINITY)
                .put("-0e0", NEGATIVE_INFINITY)
                .put("-0E0", NEGATIVE_INFINITY)
                .put("-1e-999", NEGATIVE_INFINITY)
                .put("-0." + "0".repeat(399) + "1", NEGATIVE_INFINITY)
                .buildOrThrow();
        for (boolean encoded : List.of(false, true)) {
            for (Map.Entry<String, Double> entry : reciprocals.entrySet()) {
                String number = entry.getKey();
                assertThat(assertions.expression("1e0 / CAST(a AS ARRAY(DOUBLE))[1]")
                        .binding("a", "cast_test_json('[" + number + "]', " + encoded + ")"))
                        .isEqualTo(entry.getValue());
                assertThat(assertions.expression("1e0 / CAST(a AS MAP(VARCHAR, DOUBLE))['x']")
                        .binding("a", "cast_test_json('{\"x\":" + number + "}', " + encoded + ")"))
                        .isEqualTo(entry.getValue());
                assertThat(assertions.expression("1e0 / CAST(a AS ARRAY(ARRAY(DOUBLE)))[1][1]")
                        .binding("a", "cast_test_json('[[" + number + "]]', " + encoded + ")"))
                        .isEqualTo(entry.getValue());
                assertThat(assertions.expression("1e0 / CAST(a AS ROW(x DOUBLE)).x")
                        .binding("a", "cast_test_json('{\"x\":" + number + "}', " + encoded + ")"))
                        .isEqualTo(entry.getValue());
            }
        }
    }

    @Test
    public void testSmallintOverflow()
    {
        for (String source : List.of("cast_test_json('%s', false)", "cast_test_json('%s', true)", "cast_test_json_tree('%s')")) {
            for (int value : List.of(32768, -32769, 123456)) {
                for (String targetType : List.of("ARRAY(SMALLINT)", "ROW(x SMALLINT)")) {
                    assertTrinoExceptionThrownBy(assertions.expression("CAST(a AS " + targetType + ")")
                            .binding("a", source.formatted("[" + value + "]"))::evaluate)
                            .hasErrorCode(INVALID_CAST_ARGUMENT)
                            .hasMessageContaining("Out of range for smallint: " + value);
                }
                assertTrinoExceptionThrownBy(assertions.expression("CAST(a AS MAP(VARCHAR, SMALLINT))")
                        .binding("a", source.formatted("{\"x\":" + value + "}"))::evaluate)
                        .hasErrorCode(INVALID_CAST_ARGUMENT)
                        .hasMessageContaining("Out of range for smallint: " + value);
            }
        }
    }

    @Test
    public void testCharScalar()
    {
        for (boolean encoded : List.of(false, true)) {
            for (String targetType : List.of("VARCHAR", "VARCHAR(1)")) {
                assertThat(assertions.expression("CAST(a AS " + targetType + ")")
                        .binding("a", "cast_test_char_json('a', " + encoded + ")"))
                        .isEqualTo("a");
            }
            assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                    .binding("a", "cast_test_char_json('true', " + encoded + ")"))
                    .isEqualTo(true);
            for (String targetType : List.of("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "DECIMAL(5,2)", "DECIMAL(30,2)", "NUMBER")) {
                assertThat(assertions.expression("CAST(a AS " + targetType + ")")
                        .binding("a", "cast_test_char_json('1', " + encoded + ")"))
                        .matches("CAST(1 AS " + targetType + ")");
            }
        }
    }

    @Test
    public void testNumberToBoolean()
    {
        Map<String, Boolean> numbers = ImmutableMap.<String, Boolean>builder()
                .put("0." + "0".repeat(39), false)
                .put("-0." + "0".repeat(39), false)
                .put("0." + "0".repeat(38) + "1", true)
                .put("-0." + "0".repeat(38) + "1", true)
                .put("1" + "0".repeat(40), true)
                .put("-1" + "0".repeat(40), true)
                .buildOrThrow();
        for (String source : List.of("cast_test_json('%s', false)", "cast_test_json('%s', true)", "cast_test_json_tree('%s')")) {
            for (Map.Entry<String, Boolean> entry : numbers.entrySet()) {
                String number = entry.getKey();
                assertThat(assertions.expression("CAST(a AS BOOLEAN)")
                        .binding("a", source.formatted(number)))
                        .isEqualTo(entry.getValue());
                assertThat(assertions.expression("CAST(a AS ARRAY(BOOLEAN))[1]")
                        .binding("a", source.formatted("[" + number + "]")))
                        .isEqualTo(entry.getValue());
                assertThat(assertions.expression("CAST(a AS MAP(VARCHAR, BOOLEAN))['x']")
                        .binding("a", source.formatted("{\"x\":" + number + "}")))
                        .isEqualTo(entry.getValue());
                assertThat(assertions.expression("CAST(a AS ROW(x BOOLEAN)).x")
                        .binding("a", source.formatted("{\"x\":" + number + "}")))
                        .isEqualTo(entry.getValue());
            }
        }
    }

    @Test
    public void testNonFiniteNumberToBoolean()
    {
        for (boolean negative : List.of(false, true)) {
            Json infinity = new TypedValue(NUMBER, TrinoNumber.from(new TrinoNumber.Infinity(negative)));
            assertThat(JsonOperators.castToBoolean(infinity)).isTrue();
            assertThat(JsonOperators.castToBoolean(Json.of(infinity.encoding()))).isTrue();
        }
        Json nan = new TypedValue(NUMBER, TrinoNumber.from(new TrinoNumber.NotANumber()));
        for (Json value : List.of(nan, Json.of(nan.encoding()))) {
            assertTrinoExceptionThrownBy(() -> JsonOperators.castToBoolean(value))
                    .hasErrorCode(INVALID_CAST_ARGUMENT);
        }
    }

    @ScalarFunction(deterministic = false)
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Json castTestJsonTree(@SqlType("varchar(x)") Slice value)
    {
        return JsonItems.parseToTree(value);
    }

    // Prevent constant folding from encoding the tree value before it reaches the cast.
    @ScalarFunction(deterministic = false)
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Json castTestCharJson(@SqlType("varchar(x)") Slice value, @SqlType(StandardTypes.BOOLEAN) boolean encoded)
    {
        Json json = new TypedValue(createCharType(10), value);
        return encoded ? Json.of(json.encoding()) : json;
    }

    @ScalarFunction(deterministic = false)
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Json castTestJson(@SqlType("varchar(x)") Slice value, @SqlType(StandardTypes.BOOLEAN) boolean encoded)
    {
        return encoded ? JsonItems.fromText(value) : Json.unchecked(value);
    }
}
