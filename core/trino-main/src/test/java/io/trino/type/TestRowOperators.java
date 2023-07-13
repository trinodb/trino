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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.MoreMaps.asMap;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;

@TestInstance(PER_CLASS)
public class TestRowOperators
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();

        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestRowOperators.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.JSON)
    public static Slice uncheckedToJson(@SqlType("varchar(x)") Slice slice)
    {
        return slice;
    }

    @Test
    public void testRowTypeLookup()
    {
        TypeSignature signature = RowType.from(ImmutableList.of(field("b", BIGINT))).getTypeSignature();
        Type type = ((LocalQueryRunner) assertions.getQueryRunner()).getPlannerContext().getTypeManager().getType(signature);
        assertEquals(type.getTypeSignature().getParameters().size(), 1);
        assertEquals(type.getTypeSignature().getParameters().get(0).getNamedTypeSignature().getName().get(), "b");
    }

    @Test
    public void testRowToJson()
    {
        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "CAST(null as ROW(BIGINT, VARCHAR))"))
                .isNull(JSON);

        assertThat(assertions.expression("CAST(a as json)")
                .binding("a", "ROW(null, null)"))
                .hasType(JSON)
                .isEqualTo("{\"\":null,\"\":null}");

        assertThat(assertions.expression("CAST(a as JSON)")
                .binding("a", "ROW(true, false, null)"))
                .hasType(JSON)
                .isEqualTo("{\"\":true,\"\":false,\"\":null}");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "CAST(ROW(12, 12345, 123456789, 1234567890123456789, null, null, null, null) as ROW(TINYINT, SMALLINT, INTEGER, BIGINT, TINYINT, SMALLINT, INTEGER, BIGINT))"))
                .hasType(JSON)
                .isEqualTo("{\"\":12,\"\":12345,\"\":123456789,\"\":1234567890123456789,\"\":null,\"\":null,\"\":null,\"\":null}");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ROW(CAST(3.14E0 as REAL), 3.1415E0, 1e308, DECIMAL '3.14', DECIMAL '12345678901234567890.123456789012345678', CAST(null AS REAL), CAST(null AS DOUBLE), CAST(null AS DECIMAL))"))
                .hasType(JSON)
                .isEqualTo("{\"\":3.14,\"\":3.1415,\"\":1.0E308,\"\":3.14,\"\":12345678901234567890.123456789012345678,\"\":null,\"\":null,\"\":null}");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ROW('a', 'bb', CAST(null as VARCHAR), JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', CAST(null AS JSON))"))
                .hasType(JSON)
                .isEqualTo("{\"\":\"a\",\"\":\"bb\",\"\":null,\"\":123,\"\":3.14,\"\":false,\"\":\"abc\",\"\":[1,\"a\",null],\"\":{\"a\":1,\"b\":\"str\",\"c\":null},\"\":null,\"\":null}");

        assertThat(assertions.expression("CAST(a as JSON)")
                .binding("a", "ROW(DATE '2001-08-22', DATE '2001-08-23', null)"))
                .hasType(JSON)
                .isEqualTo("{\"\":\"2001-08-22\",\"\":\"2001-08-23\",\"\":null}");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ROW(TIMESTAMP '1970-01-01 00:00:01', CAST(null as TIMESTAMP))"))
                .hasType(JSON)
                .isEqualTo(format("{\"\":\"%s\",\"\":null}", sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0)));

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ROW(ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], CAST(null as ARRAY(BIGINT)))"))
                .hasType(JSON)
                .isEqualTo("{\"\":[1,2],\"\":[3,null],\"\":[],\"\":[null,null],\"\":null}");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ROW(MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), CAST(NULL as MAP(VARCHAR, BIGINT)))"))
                .hasType(JSON)
                .isEqualTo("{\"\":{\"a\":1,\"b\":2},\"\":{\"none\":null,\"three\":3},\"\":{},\"\":{\"h1\":null,\"h2\":null},\"\":null}");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ROW(ROW(1, 2), ROW(3, CAST(null as INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null)"))
                .hasType(JSON)
                .isEqualTo("{\"\":{\"\":1,\"\":2},\"\":{\"\":3,\"\":null},\"\":{\"\":null,\"\":null},\"\":null}");

        // other miscellaneous tests
        assertThat(assertions.expression("CAST(a as JSON)")
                .binding("a", "ROW(1, 2)"))
                .hasType(JSON)
                .isEqualTo("{\"\":1,\"\":2}");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "CAST(ROW(1, 2) as ROW(a BIGINT, b BIGINT))"))
                .hasType(JSON)
                .isEqualTo("{\"a\":1,\"b\":2}");

        assertThat(assertions.expression("CAST(a as JSON)")
                .binding("a", "ROW(1, NULL)"))
                .hasType(JSON)
                .isEqualTo("{\"\":1,\"\":null}");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ROW(1, CAST(NULL as INTEGER))"))
                .hasType(JSON)
                .isEqualTo("{\"\":1,\"\":null}");

        assertThat(assertions.expression("CAST(a as JSON)")
                .binding("a", "ROW(1, 2.0E0)"))
                .hasType(JSON)
                .isEqualTo("{\"\":1,\"\":2.0}");

        assertThat(assertions.expression("CAST(a as JSON)")
                .binding("a", "ROW(1.0E0, 2.5E0)"))
                .hasType(JSON)
                .isEqualTo("{\"\":1.0,\"\":2.5}");

        assertThat(assertions.expression("CAST(a as JSON)")
                .binding("a", "ROW(1.0E0, 'kittens')"))
                .hasType(JSON)
                .isEqualTo("{\"\":1.0,\"\":\"kittens\"}");

        assertThat(assertions.expression("CAST(a as JSON)")
                .binding("a", "ROW(TRUE, FALSE)"))
                .hasType(JSON)
                .isEqualTo("{\"\":true,\"\":false}");

        assertThat(assertions.expression("CAST(a as JSON)")
                .binding("a", "ROW(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0]))"))
                .hasType(JSON)
                .isEqualTo("{\"\":false,\"\":[1,2],\"\":{\"1\":2.0,\"3\":4.0}}");

        assertThat(assertions.expression("CAST(a as JSON)")
                .binding("a", "row(1.0, 123123123456.6549876543)"))
                .hasType(JSON)
                .isEqualTo("{\"\":1.0,\"\":123123123456.6549876543}");
    }

    @Test
    public void testJsonToRow()
    {
        // special values
        assertThat(assertions.expression("CAST(a AS ROW(BIGINT))")
                .binding("a", "CAST(null as JSON)"))
                .isNull(RowType.anonymous(ImmutableList.of(BIGINT)));

        assertThat(assertions.expression("CAST(a as ROW(BIGINT))")
                .binding("a", "JSON 'null'"))
                .isNull(RowType.anonymous(ImmutableList.of(BIGINT)));

        assertThat(assertions.expression("CAST(a as ROW(VARCHAR, BIGINT))")
                .binding("a", "JSON '[null, null]'"))
                .hasType(RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)))
                .isEqualTo(Lists.newArrayList(null, null));

        assertThat(assertions.expression("CAST(a as ROW(k1 VARCHAR, k2 BIGINT))")
                .binding("a", "JSON '{\"k2\": null, \"k1\": null}'"))
                .hasType(RowType.from(ImmutableList.of(
                        RowType.field("k1", VARCHAR),
                        RowType.field("k2", BIGINT))))
                .isEqualTo(Lists.newArrayList(null, null));

        // allow json object contains non-exist field names
        assertThat(assertions.expression("CAST(a as ROW(used BIGINT))")
                .binding("a", "JSON '{\"k1\": [1, 2], \"used\": 3, \"k2\": [4, 5]}'"))
                .hasType(RowType.from(ImmutableList.of(
                        RowType.field("used", BIGINT))))
                .isEqualTo(ImmutableList.of(3L));

        assertThat(assertions.expression("CAST(a as ARRAY(ROW(used BIGINT)))")
                .binding("a", "JSON '[{\"k1\": [1, 2], \"used\": 3, \"k2\": [4, 5]}]'"))
                .hasType(new ArrayType(RowType.from(ImmutableList.of(
                        RowType.field("used", BIGINT)))))
                .isEqualTo(ImmutableList.of(ImmutableList.of(3L)));

        // allow non-exist fields in json object
        assertThat(assertions.expression("CAST(a as ROW(a BIGINT, b BIGINT, c BIGINT, d BIGINT))")
                .binding("a", "JSON '{\"a\":1,\"c\":3}'"))
                .hasType(RowType.from(ImmutableList.of(
                        RowType.field("a", BIGINT),
                        RowType.field("b", BIGINT),
                        RowType.field("c", BIGINT),
                        RowType.field("d", BIGINT))))
                .isEqualTo(asList(1L, null, 3L, null));

        assertThat(assertions.expression("CAST(a as ARRAY(ROW(a BIGINT, b BIGINT, c BIGINT, d BIGINT)))")
                .binding("a", "JSON '[{\"a\":1,\"c\":3}]'"))
                .hasType(new ArrayType(
                        RowType.from(ImmutableList.of(
                                RowType.field("a", BIGINT),
                                RowType.field("b", BIGINT),
                                RowType.field("c", BIGINT),
                                RowType.field("d", BIGINT)))))
                .isEqualTo(ImmutableList.of(asList(1L, null, 3L, null)));

        // fields out of order
        assertThat(assertions.expression("CAST(a as ROW(k1 BIGINT, k2 BIGINT, k3 BIGINT, k4 BIGINT))")
                .binding("a", "unchecked_to_json('{\"k4\": 4, \"k2\": 2, \"k3\": 3, \"k1\": 1}')"))
                .hasType(RowType.from(ImmutableList.of(
                        RowType.field("k1", BIGINT),
                        RowType.field("k2", BIGINT),
                        RowType.field("k3", BIGINT),
                        RowType.field("k4", BIGINT))))
                .isEqualTo(ImmutableList.of(1L, 2L, 3L, 4L));

        assertThat(assertions.expression("CAST(a as ARRAY(ROW(k1 BIGINT, k2 BIGINT, k3 BIGINT, k4 BIGINT)))")
                .binding("a", "unchecked_to_json('[{\"k4\": 4, \"k2\": 2, \"k3\": 3, \"k1\": 1}]')"))
                .hasType(new ArrayType(
                        RowType.from(ImmutableList.of(
                                RowType.field("k1", BIGINT),
                                RowType.field("k2", BIGINT),
                                RowType.field("k3", BIGINT),
                                RowType.field("k4", BIGINT)))))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1L, 2L, 3L, 4L)));

        // boolean
        assertThat(assertions.expression("CAST(a as ROW(BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN))")
                .binding("a", "JSON '[true, false, 12, 0, 12.3, 0.0, \"true\", \"false\", null]'"))
                .hasType(RowType.anonymous(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN)))
                .isEqualTo(asList(true, false, true, false, true, false, true, false, null));

        assertThat(assertions.expression("CAST(a as ROW(k1 BOOLEAN, k2 BOOLEAN, k3 BOOLEAN, k4 BOOLEAN, k5 BOOLEAN, k6 BOOLEAN, k7 BOOLEAN, k8 BOOLEAN, k9 BOOLEAN))")
                .binding("a", "JSON '{\"k1\": true, \"k2\": false, \"k3\": 12, \"k4\": 0, \"k5\": 12.3, \"k6\": 0.0, \"k7\": \"true\", \"k8\": \"false\", \"k9\": null}'"))
                .hasType(RowType.from(ImmutableList.of(
                        RowType.field("k1", BOOLEAN),
                        RowType.field("k2", BOOLEAN),
                        RowType.field("k3", BOOLEAN),
                        RowType.field("k4", BOOLEAN),
                        RowType.field("k5", BOOLEAN),
                        RowType.field("k6", BOOLEAN),
                        RowType.field("k7", BOOLEAN),
                        RowType.field("k8", BOOLEAN),
                        RowType.field("k9", BOOLEAN))))
                .isEqualTo(asList(true, false, true, false, true, false, true, false, null));

        // tinyint, smallint, integer, bigint
        assertThat(assertions.expression("CAST(a as ROW(TINYINT, SMALLINT, INTEGER, BIGINT, TINYINT, SMALLINT, INTEGER, BIGINT))")
                .binding("a", "JSON '[12,12345,123456789,1234567890123456789,null,null,null,null]'"))
                .hasType(RowType.anonymous(ImmutableList.of(TINYINT, SMALLINT, INTEGER, BIGINT, TINYINT, SMALLINT, INTEGER, BIGINT)))
                .isEqualTo(asList((byte) 12, (short) 12345, 123456789, 1234567890123456789L, null, null, null, null));

        assertThat(assertions.expression("CAST(a AS ROW(tinyint_value TINYINT, smallint_value SMALLINT, integer_value INTEGER, bigint_value BIGINT, tinyint_null TINYINT, smallint_null SMALLINT, integer_null INTEGER, bigint_null BIGINT))")
                .binding("a", "JSON '{\"tinyint_value\": 12, \"tinyint_null\":null, " +
                        "\"smallint_value\":12345, \"smallint_null\":null, " +
                        " \"integer_value\":123456789, \"integer_null\": null, " +
                        "\"bigint_value\":1234567890123456789, \"bigint_null\": null}'"))
                .hasType(RowType.from(ImmutableList.of(
                        RowType.field("tinyint_value", TINYINT),
                        RowType.field("smallint_value", SMALLINT),
                        RowType.field("integer_value", INTEGER),
                        RowType.field("bigint_value", BIGINT),
                        RowType.field("tinyint_null", TINYINT),
                        RowType.field("smallint_null", SMALLINT),
                        RowType.field("integer_null", INTEGER),
                        RowType.field("bigint_null", BIGINT))))
                .isEqualTo(asList((byte) 12, (short) 12345, 123456789, 1234567890123456789L, null, null, null, null));

        // real, double, decimal
        assertThat(assertions.expression("CAST(a as ROW(REAL, DOUBLE, DECIMAL(10, 5), DECIMAL(38, 8), REAL, DOUBLE, DECIMAL(7, 7)))")
                .binding("a", "JSON '[12345.67,1234567890.1,123.456,12345678.12345678,null,null,null]'"))
                .hasType(RowType.anonymous(ImmutableList.of(REAL, DOUBLE, createDecimalType(10, 5), createDecimalType(38, 8), REAL, DOUBLE, createDecimalType(7, 7))))
                .isEqualTo(asList(
                        12345.67f,
                        1234567890.1,
                        decimal("123.45600", createDecimalType(10, 5)),
                        decimal("12345678.12345678", createDecimalType(38, 8)),
                        null,
                        null,
                        null));

        assertThat(assertions.expression("CAST(a AS ROW(real_value REAL, double_value DOUBLE, decimal_value1 DECIMAL(10, 5), decimal_value2 DECIMAL(38, 8), real_null REAL, double_null DOUBLE, decimal_null DECIMAL(7, 7)))")
                .binding("a", "JSON '{" +
                        "\"real_value\": 12345.67, \"real_null\": null, " +
                        "\"double_value\": 1234567890.1, \"double_null\": null, " +
                        "\"decimal_value1\": 123.456, \"decimal_value2\": 12345678.12345678, \"decimal_null\": null}'"))
                .hasType(RowType.from(ImmutableList.of(
                        RowType.field("real_value", REAL),
                        RowType.field("double_value", DOUBLE),
                        RowType.field("decimal_value1", createDecimalType(10, 5)),
                        RowType.field("decimal_value2", createDecimalType(38, 8)),
                        RowType.field("real_null", REAL),
                        RowType.field("double_null", DOUBLE),
                        RowType.field("decimal_null", createDecimalType(7, 7)))))
                .isEqualTo(asList(
                        12345.67f,
                        1234567890.1,
                        decimal("123.45600", createDecimalType(10, 5)),
                        decimal("12345678.12345678", createDecimalType(38, 8)),
                        null,
                        null,
                        null));

        // varchar, json
        assertThat(assertions.expression("CAST(a as ROW(VARCHAR, JSON, VARCHAR, JSON))")
                .binding("a", "JSON '[\"puppies\", [1, 2, 3], null, null]'"))
                .hasType(RowType.anonymous(ImmutableList.of(VARCHAR, JSON, VARCHAR, JSON)))
                .isEqualTo(asList("puppies", "[1,2,3]", null, "null"));

        assertThat(assertions.expression("CAST(a AS ROW(varchar_value VARCHAR, json_value_field JSON, varchar_null VARCHAR, json_null JSON))")
                .binding("a", "JSON '{\"varchar_value\": \"puppies\", \"json_value_field\": [1, 2, 3], \"varchar_null\": null, \"json_null\": null}'"))
                .hasType(RowType.from(ImmutableList.of(
                        RowType.field("varchar_value", VARCHAR),
                        RowType.field("json_value_field", JSON),
                        RowType.field("varchar_null", VARCHAR),
                        RowType.field("json_null", JSON))))
                .isEqualTo(asList("puppies", "[1,2,3]", null, "null"));

        // nested array/map/row
        assertThat(assertions.expression("CAST(a AS ROW(ARRAY(BIGINT), ARRAY(BIGINT), ARRAY(BIGINT), MAP(VARCHAR, BIGINT), MAP(VARCHAR, BIGINT), MAP(VARCHAR, BIGINT), ROW(BIGINT, BIGINT, BIGINT, BIGINT), ROW(BIGINT),ROW(a BIGINT, b BIGINT, three BIGINT, none BIGINT), ROW(nothing BIGINT)))")
                .binding("a", "JSON '[" +
                        "[1, 2, null, 3], [], null, " +
                        "{\"a\": 1, \"b\": 2, \"none\": null, \"three\": 3}, {}, null, " +
                        "[1, 2, null, 3], null, " +
                        "{\"a\": 1, \"b\": 2, \"none\": null, \"three\": 3}, null]' "))
                .hasType(RowType.anonymous(
                        ImmutableList.of(
                                new ArrayType(BIGINT), new ArrayType(BIGINT), new ArrayType(BIGINT),
                                mapType(VARCHAR, BIGINT), mapType(VARCHAR, BIGINT), mapType(VARCHAR, BIGINT),
                                RowType.anonymous(ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT)), RowType.anonymous(ImmutableList.of(BIGINT)),
                                RowType.from(ImmutableList.of(
                                        RowType.field("a", BIGINT),
                                        RowType.field("b", BIGINT),
                                        RowType.field("three", BIGINT),
                                        RowType.field("none", BIGINT))),
                                RowType.from(ImmutableList.of(RowType.field("nothing", BIGINT))))))
                .isEqualTo(asList(
                        asList(1L, 2L, null, 3L), emptyList(), null,
                        asMap(ImmutableList.of("a", "b", "none", "three"), asList(1L, 2L, null, 3L)), ImmutableMap.of(), null,
                        asList(1L, 2L, null, 3L), null,
                        asList(1L, 2L, 3L, null), null));

        assertThat(assertions.expression("CAST(a AS ROW(array1 ARRAY(BIGINT), array2 ARRAY(BIGINT), array3 ARRAY(BIGINT), " +
                        "map1 MAP(VARCHAR, BIGINT), map2 MAP(VARCHAR, BIGINT), map3 MAP(VARCHAR, BIGINT), " +
                        "rowAsJsonArray1 ROW(BIGINT, BIGINT, BIGINT, BIGINT), rowAsJsonArray2 ROW(BIGINT)," +
                        "rowAsJsonObject1 ROW(nothing BIGINT), rowAsJsonObject2 ROW(a BIGINT, b BIGINT, three BIGINT, none BIGINT)))")
                .binding("a", "JSON '{" +
                        "\"array2\": [1, 2, null, 3], " +
                        "\"array1\": [], " +
                        "\"array3\": null, " +
                        "\"map3\": {\"a\": 1, \"b\": 2, \"none\": null, \"three\": 3}, " +
                        "\"map1\": {}, " +
                        "\"map2\": null, " +
                        "\"rowAsJsonArray1\": [1, 2, null, 3], " +
                        "\"rowAsJsonArray2\": null, " +
                        "\"rowAsJsonObject2\": {\"a\": 1, \"b\": 2, \"none\": null, \"three\": 3}, " +
                        "\"rowAsJsonObject1\": null}' "))
                .hasType(RowType.from(ImmutableList.of(
                        RowType.field("array1", new ArrayType(BIGINT)),
                        RowType.field("array2", new ArrayType(BIGINT)),
                        RowType.field("array3", new ArrayType(BIGINT)),
                        RowType.field("map1", mapType(VARCHAR, BIGINT)),
                        RowType.field("map2", mapType(VARCHAR, BIGINT)),
                        RowType.field("map3", mapType(VARCHAR, BIGINT)),
                        RowType.field("rowasjsonarray1", RowType.anonymous(ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT))),
                        RowType.field("rowasjsonarray2", RowType.anonymous(ImmutableList.of(BIGINT))),
                        RowType.field("rowasjsonobject1", RowType.from(ImmutableList.of(RowType.field("nothing", BIGINT)))),
                        RowType.field("rowasjsonobject2", RowType.from(ImmutableList.of(
                                RowType.field("a", BIGINT),
                                RowType.field("b", BIGINT),
                                RowType.field("three", BIGINT),
                                RowType.field("none", BIGINT)))))))
                .isEqualTo(asList(
                        emptyList(), asList(1L, 2L, null, 3L), null,
                        ImmutableMap.of(), null, asMap(ImmutableList.of("a", "b", "none", "three"), asList(1L, 2L, null, 3L)),
                        asList(1L, 2L, null, 3L), null,
                        null, asList(1L, 2L, 3L, null)));

        // invalid cast
        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a as ROW(a BIGINT, b BIGINT))")
                .binding("a", "unchecked_to_json('{\"a\":1,\"b\":2,\"a\":3}')")
                .evaluate())
                .hasMessage("Cannot cast to row(a bigint, b bigint). Duplicate field: a\n{\"a\":1,\"b\":2,\"a\":3}")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a as ARRAY(ROW(a BIGINT, b BIGINT)))")
                .binding("a", "unchecked_to_json('[{\"a\":1,\"b\":2,\"a\":3}]')")
                .evaluate())
                .hasMessage("Cannot cast to array(row(a bigint, b bigint)). Duplicate field: a\n[{\"a\":1,\"b\":2,\"a\":3}]")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testFieldAccessor()
    {
        assertThat(assertions.expression("a[2]")
                .binding("a", "row(1, CAST(NULL AS DOUBLE))"))
                .isNull(DOUBLE);

        assertThat(assertions.expression("a[2]")
                .binding("a", "row(TRUE, CAST(NULL AS BOOLEAN))"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a[2]")
                .binding("a", "row(TRUE, CAST(NULL AS ARRAY(INTEGER)))"))
                .isNull(new ArrayType(INTEGER));

        assertThat(assertions.expression("a[2]")
                .binding("a", "row(1.0E0, CAST(NULL AS VARCHAR))"))
                .isNull(createUnboundedVarcharType());

        assertThat(assertions.expression("a[1]")
                .binding("a", "row(1, 2)"))
                .isEqualTo(1);

        assertThat(assertions.expression("a[2]")
                .binding("a", "row(1, 'kittens')"))
                .hasType(createVarcharType(7))
                .isEqualTo("kittens");

        assertThat(assertions.expression("a[2]")
                .binding("a", "row(1, 2)"))
                .isEqualTo(2);

        assertThat(assertions.expression("a[2]")
                .binding("a", "row(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0]))"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));

        assertThat(assertions.expression("a[3]")
                .binding("a", "row(FALSE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0]))"))
                .hasType(mapType(INTEGER, DOUBLE))
                .isEqualTo(ImmutableMap.of(1, 2.0, 3, 4.0));

        assertThat(assertions.expression("a[2][2][1]")
                .binding("a", "row(1.0E0, ARRAY[row(31, 4.1E0), row(32, 4.2E0)], row(3, 4.0E0))"))
                .isEqualTo(32);

        // Using ROW constructor
        assertThat(assertions.expression("a[1]")
                .binding("a", "cast(ROW(1, 2) AS ROW(a BIGINT, b DOUBLE))"))
                .isEqualTo(1L);

        assertThat(assertions.expression("a[2]")
                .binding("a", "cast(ROW(1, 2) AS ROW(a BIGINT, b DOUBLE))"))
                .isEqualTo(2.0);

        assertThat(assertions.expression("a[1][1]")
                .binding("a", "row(row('aa'))"))
                .hasType(createVarcharType(2))
                .isEqualTo("aa");

        assertThat(assertions.expression("a[1][1]")
                .binding("a", "row(row('ab'))"))
                .hasType(createVarcharType(2))
                .isEqualTo("ab");

        assertThat(assertions.expression("a[1]")
                .binding("a", "cast(ROW(ARRAY[NULL]) AS ROW(a ARRAY(BIGINT)))"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(asList((Integer) null));

        assertThat(assertions.expression("a[1]")
                .binding("a", "cast(row(1.0, 123123123456.6549876543) AS ROW(col0 decimal(2,1), col1 decimal(22,10)))"))
                .isEqualTo(decimal("1.0", createDecimalType(2, 1)));

        assertThat(assertions.expression("a[2]")
                .binding("a", "cast(row(1.0, 123123123456.6549876543) AS ROW(col0 decimal(2,1), col1 decimal(22,10)))"))
                .isEqualTo(decimal("123123123456.6549876543", createDecimalType(22, 10)));
    }

    @Test
    public void testRowCast()
    {
        assertThat(assertions.expression("cast(a AS row(aa bigint, bb bigint))[1]")
                .binding("a", "row(2, 3)"))
                .isEqualTo(2L);

        assertThat(assertions.expression("cast(a AS row(aa bigint, bb bigint))[2]")
                .binding("a", "row(2, 3)"))
                .isEqualTo(3L);

        assertThat(assertions.expression("cast(a AS row(aa bigint, bb boolean))[2]")
                .binding("a", "row(2, 3)"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a AS row(aa bigint, bb double))[2]")
                .binding("a", "row(2, CAST(null as double))"))
                .isNull(DOUBLE);

        assertThat(assertions.expression("cast(a AS row(aa bigint, bb varchar))[2]")
                .binding("a", "row(2, 'test_str')"))
                .hasType(VARCHAR)
                .isEqualTo("test_str");

        // ROW casting with NULLs
        assertThat(assertions.expression("cast(a AS row(aa bigint, bb boolean, cc boolean))[2]")
                .binding("a", "row(1,null,3)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("cast(a AS row(aa bigint, bb boolean, cc boolean))[1]")
                .binding("a", "row(1,null,3)"))
                .isEqualTo(1L);

        assertThat(assertions.expression("cast(a AS row(aa bigint, bb boolean, cc boolean))[1]")
                .binding("a", "row(null,null,null)"))
                .isNull(BIGINT);

        // there are totally 7 field names
        String longFieldNameCast = "CAST(a AS ROW(%s VARCHAR, %s ARRAY(ROW(%s VARCHAR, %s VARCHAR)), %s ROW(%s VARCHAR, %s VARCHAR)))[2][1][1]";
        int fieldCount = 7;
        char[] chars = new char[9333];
        String[] fields = new String[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            Arrays.fill(chars, (char) ('a' + i));
            fields[i] = new String(chars);
        }
        assertThat(assertions.expression(format(longFieldNameCast, fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]))
                .binding("a", "row(1.2E0, ARRAY[row(233, 6.9E0)], row(1000, 6.3E0))"))
                .hasType(VARCHAR)
                .isEqualTo("233");

        assertThat(assertions.expression("CAST(a as row(a BIGINT, b DOUBLE, c BOOLEAN, d VARCHAR, e ARRAY(BIGINT)))")
                .binding("a", "row(json '2', json '1.5', json 'true', json '\"abc\"', json '[1, 2]')"))
                .hasType(RowType.from(ImmutableList.of(
                        RowType.field("a", BIGINT),
                        RowType.field("b", DOUBLE),
                        RowType.field("c", BOOLEAN),
                        RowType.field("d", VARCHAR),
                        RowType.field("e", new ArrayType(BIGINT)))))
                .isEqualTo(asList(2L, 1.5, true, "abc", ImmutableList.of(1L, 2L)));
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS ROW(UNKNOWN))", "CAST(NULL AS ROW(UNKNOWN))"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "row(NULL)", "row(NULL)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "row(1, 'cat')", "row(1, 'cat')"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "row(1, ARRAY [1])", "row(1, ARRAY [1])"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "row(1, ARRAY [1, 2])", "row(1, ARRAY [1, NULL])"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "row(1, 2.0E0, TRUE, 'cat', from_unixtime(1))", "row(1, 2.0E0, TRUE, 'cat', from_unixtime(1))"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "row(1, 2.0E0, TRUE, 'cat', from_unixtime(1))", "row(1, 2.0E0, TRUE, 'cat', from_unixtime(2))"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "row(1, 2.0E0, TRUE, 'cat', CAST(NULL AS INTEGER))", "row(1, 2.0E0, TRUE, 'cat', 2)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "row(1, 2.0E0, TRUE, 'cat', CAST(NULL AS INTEGER))", "row(1, 2.0E0, TRUE, 'cat', CAST(NULL AS INTEGER))"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "row(1, 2.0E0, TRUE, 'cat')", "row(1, 2.0E0, TRUE, CAST(NULL AS VARCHAR(3)))"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "row(1, 2.0E0, TRUE, CAST(NULL AS VARCHAR(3)))", "row(1, 2.0E0, TRUE, CAST(NULL AS VARCHAR(3)))"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[ROW(1)]", "ARRAY[ROW(1)]"))
                .isEqualTo(false);
    }

    @Test
    public void testRowComparison()
    {
        assertThat(assertions.operator(EQUAL, "row(TIMESTAMP '2002-01-02 03:04:05.321 +08:10', TIMESTAMP '2002-01-02 03:04:05.321 +08:10')", "row(TIMESTAMP '2002-01-02 02:04:05.321 +07:10', TIMESTAMP '2002-01-02 03:05:05.321 +08:11')"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "row(1.0E0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10'))", "row(1.0E0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:11'))"))
                .isEqualTo(false);

        assertComparisonCombination(
                "row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10')",
                "row(TIMESTAMP '2002-01-02 03:04:05.321 +07:09', TIMESTAMP '2002-01-02 03:04:05.321 +07:09')");

        assertComparisonCombination(
                "row(1.0E0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10'))",
                "row(2.0E0, row(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 03:04:05.321 +07:10'))");

        assertComparisonCombination("row(1.0E0, 'kittens')", "row(1.0E0, 'puppies')");
        assertComparisonCombination("row(1, 2.0E0)", "row(5, 2.0E0)");
        assertComparisonCombination("row(TRUE, FALSE, TRUE, FALSE)", "row(TRUE, TRUE, TRUE, FALSE)");
        assertComparisonCombination("row(1, 2.0E0, TRUE, 'kittens', from_unixtime(1))", "row(1, 3.0E0, TRUE, 'kittens', from_unixtime(1))");

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(row(CAST(CAST('' as varbinary) as hyperloglog)) as row(col0 hyperloglog)) = CAST(row(CAST(CAST('' as varbinary) as hyperloglog)) as row(col0 hyperloglog))").evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:91: Cannot apply operator: row(col0 HyperLogLog) = row(col0 HyperLogLog)");

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(row(CAST(CAST('' as varbinary) as hyperloglog)) as row(col0 hyperloglog)) > CAST(row(CAST(CAST('' as varbinary) as hyperloglog)) as row(col0 hyperloglog))").evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:91: Cannot apply operator: row(col0 HyperLogLog) < row(col0 HyperLogLog)");

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(row(CAST(CAST('' as varbinary) as qdigest(double))) as row(col0 qdigest(double))) = CAST(row(CAST(CAST('' as varbinary) as qdigest(double))) as row(col0 qdigest(double)))").evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:99: Cannot apply operator: row(col0 qdigest(double)) = row(col0 qdigest(double))");

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(row(CAST(CAST('' as varbinary) as qdigest(double))) as row(col0 qdigest(double))) > CAST(row(CAST(CAST('' as varbinary) as qdigest(double))) as row(col0 qdigest(double)))").evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:99: Cannot apply operator: row(col0 qdigest(double)) < row(col0 qdigest(double))");

        assertThat(assertions.operator(EQUAL, "row(TRUE, ARRAY [1], MAP(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0]))", "row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0]))"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0]))", "row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0]))"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "row(1, CAST(NULL AS INTEGER))", "row(1, 2)"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a != b")
                .binding("a", "row(1, CAST(NULL AS INTEGER))")
                .binding("b", "row(1, 2)"))
                .isNull(BOOLEAN);

        assertThat(assertions.operator(EQUAL, "row(2, CAST(NULL AS INTEGER))", "row(1, 2)"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "row(2, CAST(NULL AS INTEGER))")
                .binding("b", "row(1, 2)"))
                .isEqualTo(true);

        assertTrinoExceptionThrownBy(() -> assertions.expression("row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0])) > row(TRUE, ARRAY [1, 2], MAP(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0]))").evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:75: Cannot apply operator: row(boolean, array(integer), map(integer, double)) < row(boolean, array(integer), map(integer, double))");

        assertTrinoExceptionThrownBy(() -> assertions.expression("row(1, CAST(NULL AS INTEGER)) < row(1, 2)").evaluate())
                .hasErrorCode(StandardErrorCode.NOT_SUPPORTED);

        assertComparisonCombination("row(1.0E0, ARRAY [1,2,3], row(2, 2.0E0))", "row(1.0E0, ARRAY [1,3,3], row(2, 2.0E0))");
        assertComparisonCombination("row(TRUE, ARRAY [1])", "row(TRUE, ARRAY [1, 2])");
        assertComparisonCombination("ROW(1, 2)", "ROW(2, 1)");

        assertThat(assertions.operator(EQUAL, "ROW(1, 2)", "ROW(1, 2)"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ROW(2, 1)")
                .binding("b", "ROW(1, 2)"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "ROW(1.0, 123123123456.6549876543)", "ROW(1.0, 123123123456.6549876543)"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "ROW(1.0, 123123123456.6549876543)", "ROW(1.0, 123123123456.6549876542)"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ROW(1.0, 123123123456.6549876543)")
                .binding("b", "ROW(1.0, 123123123456.6549876543)"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ROW(1.0, 123123123456.6549876543)")
                .binding("b", "ROW(1.0, 123123123456.6549876542)"))
                .isEqualTo(true);

        for (int i = 1; i < 128; i += 10) {
            assertEqualOperator(i);
        }
    }

    @Test
    public void testRowHashOperator()
    {
        assertRowHashOperator("ROW(1, 2)", ImmutableList.of(INTEGER, INTEGER), ImmutableList.of(1, 2));
        assertRowHashOperator("ROW(true, 2)", ImmutableList.of(BOOLEAN, INTEGER), ImmutableList.of(true, 2));

        for (int i = 1; i < 128; i += 5) {
            assertRowHashOperator(i, false);
            assertRowHashOperator(i, true);
        }
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(null as row(col0 bigint))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(1)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "row(null)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(1,2)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "row(1,null)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(null,2)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(null,null)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row('111',null)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(null,'222')"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row('111','222')"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "row(row(1), row(2), row(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "row(row(1), row(null), row(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(row(1), row(CAST(null as bigint)), row(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(row(row(1)), row(2), row(3))"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "row(row(row(null)), row(2), row(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(row(row(CAST(null as boolean))), row(2), row(3))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(row(1,2),row(array[3,4,5]))"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "row(row(1,2),row(array[row(3,4)]))"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "row(row(null,2),row(array[row(3,4)]))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(row(1,null),row(array[row(3,4)]))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(row(1,2),row(array[CAST(row(3,4) as row(a integer, b integer)), CAST(null as row(a integer, b integer))]))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(row(1,2),row(array[row(null,4)]))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(row(1,2),row(array[row(map(array[8], array[9]),4)]))"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "row(row(1,2),row(array[row(map(array[8], array[null]),4)]))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(1E0,2E0)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "row(1E0,null)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "row(true,false)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "row(true,null)"))
                .isEqualTo(true);
    }

    private void assertEqualOperator(int fieldCount)
    {
        String rowLiteral = toRowLiteral(largeRow(fieldCount, false));
        assertThat(assertions.expression("a = b")
                .binding("a", rowLiteral)
                .binding("b", rowLiteral))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", rowLiteral)
                .binding("b", rowLiteral))
                .isEqualTo(false);

        String alternateRowLiteral = toRowLiteral(largeRow(fieldCount, false, true));
        assertThat(assertions.expression("a = b")
                .binding("a", rowLiteral)
                .binding("b", alternateRowLiteral))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", rowLiteral)
                .binding("b", alternateRowLiteral))
                .isEqualTo(true);

        if (fieldCount > 1) {
            String rowLiteralWithNulls = toRowLiteral(largeRow(fieldCount, true));
            assertThat(assertions.expression("a = b")
                    .binding("a", rowLiteralWithNulls)
                    .binding("b", rowLiteralWithNulls))
                    .isNull(BOOLEAN);

            assertThat(assertions.expression("a != b")
                    .binding("a", rowLiteralWithNulls)
                    .binding("b", rowLiteralWithNulls))
                    .isNull(BOOLEAN);

            String alternateRowLiteralWithNulls = toRowLiteral(largeRow(fieldCount, true, true));
            assertThat(assertions.expression("a = b")
                    .binding("a", rowLiteralWithNulls)
                    .binding("b", alternateRowLiteralWithNulls))
                    .isEqualTo(false);

            assertThat(assertions.expression("a != b")
                    .binding("a", rowLiteralWithNulls)
                    .binding("b", alternateRowLiteralWithNulls))
                    .isEqualTo(true);
        }
    }

    private void assertRowHashOperator(int fieldCount, boolean nulls)
    {
        List<Object> data = largeRow(fieldCount, nulls);
        assertRowHashOperator(toRowLiteral(data), largeRowDataTypes(fieldCount), data);
    }

    private static String toRowLiteral(List<Object> data)
    {
        return "ROW(" + Joiner.on(", ").useForNull("null").join(data) + ")";
    }

    private void assertRowHashOperator(String inputString, List<Type> types, List<Object> elements)
    {
        checkArgument(types.size() == elements.size(), "types and elements must have the same size");
        assertThat(assertions.operator(HASH_CODE, inputString))
                .isEqualTo(hashFields(types, elements));

        assertThat(assertions.operator(XX_HASH_64, inputString))
                .isEqualTo(hashFields(types, elements));
    }

    private long hashFields(List<Type> types, List<Object> elements)
    {
        checkArgument(types.size() == elements.size(), "types and elements must have the same size");

        long result = 1;
        for (int i = 0; i < types.size(); i++) {
            Object fieldValue = elements.get(i);

            long fieldHashCode = 0;
            if (fieldValue != null) {
                Type fieldType = types.get(i);
                try {
                    fieldHashCode = (long) ((LocalQueryRunner) assertions.getQueryRunner()).getTypeOperators().getHashCodeOperator(fieldType, simpleConvention(FAIL_ON_NULL, NEVER_NULL))
                            .invoke(fieldValue);
                }
                catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }
            }

            result = CombineHashFunction.getHash(result, fieldHashCode);
        }
        return result;
    }

    private static List<Object> largeRow(int fieldCount, boolean nulls)
    {
        return largeRow(fieldCount, nulls, false);
    }

    private static List<Object> largeRow(int fieldCount, boolean nulls, boolean alternateLastElement)
    {
        List<Object> data = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            if (nulls && i % 7 == 0) {
                data.add(null);
            }
            else if (i % 10 == 0) {
                data.add(true);
            }
            else {
                data.add(i);
            }
        }
        if (alternateLastElement) {
            int lastPosition = fieldCount - 1;
            if (nulls && lastPosition % 7 == 0) {
                lastPosition--;
            }
            if (lastPosition % 10 == 0) {
                data.set(lastPosition, false);
            }
            else {
                data.set(lastPosition, -lastPosition);
            }
        }
        return data;
    }

    private static List<Type> largeRowDataTypes(int fieldCount)
    {
        List<Type> types = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            if (i % 10 == 0) {
                types.add(BOOLEAN);
            }
            else {
                types.add(INTEGER);
            }
        }
        return types;
    }

    private void assertComparisonCombination(String base, String greater)
    {
        Set<String> equalOperators = new HashSet<>(ImmutableSet.of("=", ">=", "<="));
        Set<String> greaterOrInequalityOperators = new HashSet<>(ImmutableSet.of(">=", ">", "!="));
        Set<String> lessOrInequalityOperators = new HashSet<>(ImmutableSet.of("<=", "<", "!="));
        for (String operator : ImmutableList.of(">", "=", "<", ">=", "<=", "!=")) {
            assertThat(assertions.expression("a %s b".formatted(operator))
                    .binding("a", base)
                    .binding("b", base))
                    .isEqualTo(equalOperators.contains(operator));

            assertThat(assertions.expression("a %s b".formatted(operator))
                    .binding("a", base)
                    .binding("b", greater))
                    .isEqualTo(lessOrInequalityOperators.contains(operator));

            assertThat(assertions.expression("a %s b".formatted(operator))
                    .binding("a", greater)
                    .binding("b", base))
                    .isEqualTo(greaterOrInequalityOperators.contains(operator));
        }
    }
}
