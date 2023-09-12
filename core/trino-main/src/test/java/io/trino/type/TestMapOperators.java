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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
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
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.SqlVarbinaryTestingUtil.sqlVarbinary;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.MoreMaps.asMap;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestMapOperators
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();

        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestMapOperators.class)
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
    public void testConstructor()
    {
        assertThat(assertions.function("map", "ARRAY['1','3']", "ARRAY[2,4]"))
                .hasType(mapType(createVarcharType(1), INTEGER))
                .isEqualTo(ImmutableMap.of("1", 2, "3", 4));

        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 2);
        map.put(3, null);
        assertThat(assertions.expression("MAP(ARRAY[1, 3], ARRAY[2, NULL])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(map);

        assertThat(assertions.expression("MAP(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0])"))
                .hasType(mapType(INTEGER, DOUBLE))
                .isEqualTo(ImmutableMap.of(1, 2.0, 3, 4.0));

        assertThat(assertions.function("map", "ARRAY[1.0, 383838383838383.12324234234234]", "ARRAY[2.2, 3.3]"))
                .hasType(mapType(createDecimalType(29, 14), createDecimalType(2, 1)))
                .isEqualTo(ImmutableMap.of(
                        decimal("000000000000001.00000000000000", createDecimalType(29, 14)),
                        decimal("2.2", createDecimalType(2, 1)),
                        decimal("383838383838383.12324234234234", createDecimalType(29, 14)),
                        decimal("3.3", createDecimalType(2, 1))));

        assertThat(assertions.function("map", "ARRAY[1.0E0, 2.0E0]", "ARRAY[ARRAY[BIGINT '1', BIGINT '2'], ARRAY[BIGINT '3' ]]"))
                .hasType(mapType(DOUBLE, new ArrayType(BIGINT)))
                .isEqualTo(ImmutableMap.of(1.0, ImmutableList.of(1L, 2L), 2.0, ImmutableList.of(3L)));

        assertThat(assertions.function("map", "ARRAY['puppies']", "ARRAY['kittens']"))
                .hasType(mapType(createVarcharType(7), createVarcharType(7)))
                .isEqualTo(ImmutableMap.of("puppies", "kittens"));

        assertThat(assertions.function("map", "ARRAY[TRUE, FALSE]", "ARRAY[2,4]"))
                .hasType(mapType(BOOLEAN, INTEGER))
                .isEqualTo(ImmutableMap.of(true, 2, false, 4));

        assertThat(assertions.function("map", "ARRAY['1', '100']", "ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01']"))
                .hasType(mapType(createVarcharType(3), createTimestampType(0)))
                .isEqualTo(ImmutableMap.of(
                        "1",
                        sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0),
                        "100",
                        sqlTimestampOf(0, 1973, 7, 8, 22, 0, 1, 0)));

        assertThat(assertions.function("map", "ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01']", "ARRAY[1.0E0, 100.0E0]"))
                .hasType(mapType(createTimestampType(0), DOUBLE))
                .isEqualTo(ImmutableMap.of(
                        sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0),
                        1.0,
                        sqlTimestampOf(0, 1973, 7, 8, 22, 0, 1, 0),
                        100.0));

        assertTrinoExceptionThrownBy(assertions.function("map", "ARRAY[1]", "ARRAY[2, 4]")::evaluate)
                .hasMessage("Key and value arrays must be the same length");

        assertTrinoExceptionThrownBy(assertions.function("map", "ARRAY[1, 2, 3, 2]", "ARRAY[4, 5, 6, 7]")::evaluate)
                .hasMessage("Duplicate map keys (2) are not allowed");

        assertTrinoExceptionThrownBy(assertions.function("map", "ARRAY[ARRAY[1, 2], ARRAY[1, 3], ARRAY[1, 2]]", "ARRAY[1, 2, 3]")::evaluate)
                .hasMessage("Duplicate map keys ([1, 2]) are not allowed");

        assertThat(assertions.function("map", "ARRAY[ARRAY[1]]", "ARRAY[2]"))
                .hasType(mapType(new ArrayType(INTEGER), INTEGER))
                .isEqualTo(ImmutableMap.of(ImmutableList.of(1), 2));

        assertTrinoExceptionThrownBy(assertions.function("map", "ARRAY[NULL]", "ARRAY[2]")::evaluate)
                .hasMessage("map key cannot be null");

        assertTrinoExceptionThrownBy(assertions.function("map", "ARRAY[ARRAY[NULL]]", "ARRAY[2]")::evaluate)
                .hasMessage("map key cannot be indeterminate: [null]");
    }

    @Test
    public void testEmptyMapConstructor()
    {
        assertThat(assertions.expression("MAP()"))
                .hasType(mapType(UNKNOWN, UNKNOWN))
                .isEqualTo(ImmutableMap.of());
    }

    @Test
    public void testCardinality()
    {
        assertThat(assertions.function("cardinality", "MAP(ARRAY['1','3'], ARRAY[2,4])"))
                .isEqualTo(2L);

        assertThat(assertions.function("cardinality", "MAP(ARRAY[1, 3], ARRAY[2, NULL])"))
                .isEqualTo(2L);

        assertThat(assertions.function("cardinality", "MAP(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0])"))
                .isEqualTo(2L);

        assertThat(assertions.function("cardinality", "MAP(ARRAY[1.0E0, 2.0E0], ARRAY[ARRAY[1, 2], ARRAY[3]])"))
                .isEqualTo(2L);

        assertThat(assertions.function("cardinality", "MAP(ARRAY['puppies'], ARRAY['kittens'])"))
                .isEqualTo(1L);

        assertThat(assertions.function("cardinality", "MAP(ARRAY[TRUE], ARRAY[2])"))
                .isEqualTo(1L);

        assertThat(assertions.function("cardinality", "MAP(ARRAY['1'], ARRAY[from_unixtime(1)])"))
                .isEqualTo(1L);

        assertThat(assertions.function("cardinality", "MAP(ARRAY[from_unixtime(1)], ARRAY[1.0E0])"))
                .isEqualTo(1L);

        assertThat(assertions.function("cardinality", "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3])"))
                .isEqualTo(2L);

        assertThat(assertions.function("cardinality", "MAP(ARRAY[1.0], ARRAY[2.2])"))
                .isEqualTo(1L);
    }

    @Test
    public void testMapToJson()
    {
        // Test key ordering
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[7,5,3,1], ARRAY[8,6,4,2])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":2,\"3\":4,\"5\":6,\"7\":8}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1,3,5,7], ARRAY[2,4,6,8])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":2,\"3\":4,\"5\":6,\"7\":8}");

        // Test null value
        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "CAST(null as MAP(BIGINT, BIGINT))"))
                .isNull(JSON);

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP()"))
                .hasType(JSON)
                .isEqualTo("{}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[null, null])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":null,\"2\":null}");

        // Test key types
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[true, false], ARRAY[1, 2])"))
                .hasType(JSON)
                .isEqualTo("{\"false\":2,\"true\":1}");

        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "MAP(cast(ARRAY[1, 2, 3] as ARRAY(TINYINT)), ARRAY[5, 8, null])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":5,\"2\":8,\"3\":null}");

        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "MAP(cast(ARRAY[12345, 12346, 12347] as ARRAY(SMALLINT)), ARRAY[5, 8, null])"))
                .hasType(JSON)
                .isEqualTo("{\"12345\":5,\"12346\":8,\"12347\":null}");

        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "MAP(cast(ARRAY[123456789,123456790,123456791] as ARRAY(INTEGER)), ARRAY[5, 8, null])"))
                .hasType(JSON)
                .isEqualTo("{\"123456789\":5,\"123456790\":8,\"123456791\":null}");

        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "MAP(cast(ARRAY[1234567890123456111,1234567890123456222,1234567890123456777] as ARRAY(BIGINT)), ARRAY[111, 222, null])"))
                .hasType(JSON)
                .isEqualTo("{\"1234567890123456111\":111,\"1234567890123456222\":222,\"1234567890123456777\":null}");

        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "MAP(cast(ARRAY[3.14E0, 1e10, 1e20] as ARRAY(REAL)), ARRAY[null, 10, 20])"))
                .hasType(JSON)
                .isEqualTo("{\"1.0E10\":10,\"1.0E20\":20,\"3.14\":null}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1e-323,1e308,nan()], ARRAY[-323,308,null])"))
                .hasType(JSON)
                .isEqualTo(Runtime.version().feature() >= 19
                        ? "{\"1.0E308\":308,\"9.9E-324\":-323,\"NaN\":null}"
                        : "{\"1.0E-323\":-323,\"1.0E308\":308,\"NaN\":null}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[DECIMAL '3.14', DECIMAL '0.01'], ARRAY[0.14, null])"))
                .hasType(JSON)
                .isEqualTo("{\"0.01\":null,\"3.14\":0.14}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[DECIMAL '12345678901234567890.1234567890666666', DECIMAL '0.0'], ARRAY[666666, null])"))
                .hasType(JSON)
                .isEqualTo("{\"0.0000000000000000\":null,\"12345678901234567890.1234567890666666\":666666}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY['a', 'bb', 'ccc'], ARRAY[1, 2, 3])"))
                .hasType(JSON)
                .isEqualTo("{\"a\":1,\"bb\":2,\"ccc\":3}");

        // Test value types
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 3], ARRAY[true, false, null])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":true,\"2\":false,\"3\":null}");

        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 3], cast(ARRAY[5, 8, null] as ARRAY(TINYINT)))"))
                .hasType(JSON)
                .isEqualTo("{\"1\":5,\"2\":8,\"3\":null}");

        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 3], cast(ARRAY[12345, -12345, null] as ARRAY(SMALLINT)))"))
                .hasType(JSON)
                .isEqualTo("{\"1\":12345,\"2\":-12345,\"3\":null}");

        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 3], cast(ARRAY[123456789, -123456789, null] as ARRAY(INTEGER)))"))
                .hasType(JSON)
                .isEqualTo("{\"1\":123456789,\"2\":-123456789,\"3\":null}");

        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 3], cast(ARRAY[1234567890123456789, -1234567890123456789, null] as ARRAY(BIGINT)))"))
                .hasType(JSON)
                .isEqualTo("{\"1\":1234567890123456789,\"2\":-1234567890123456789,\"3\":null}");

        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 3, 5, 8], CAST(ARRAY[3.14E0, nan(), infinity(), -infinity(), null] as ARRAY(REAL)))"))
                .hasType(JSON)
                .isEqualTo("{\"1\":3.14,\"2\":\"NaN\",\"3\":\"Infinity\",\"5\":\"-Infinity\",\"8\":null}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 3, 5, 8, 13, 21], ARRAY[3.14E0, 1e-323, 1e308, nan(), infinity(), -infinity(), null])"))
                .hasType(JSON)
                .isEqualTo(Runtime.version().feature() >= 19
                        ? "{\"1\":3.14,\"13\":\"-Infinity\",\"2\":9.9E-324,\"21\":null,\"3\":1.0E308,\"5\":\"NaN\",\"8\":\"Infinity\"}"
                        : "{\"1\":3.14,\"13\":\"-Infinity\",\"2\":1.0E-323,\"21\":null,\"3\":1.0E308,\"5\":\"NaN\",\"8\":\"Infinity\"}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[DECIMAL '3.14', null])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":3.14,\"2\":null}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[DECIMAL '12345678901234567890.123456789012345678', null])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":12345678901234567890.123456789012345678,\"2\":null}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 3], ARRAY['a', 'bb', null])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":\"a\",\"2\":\"bb\",\"3\":null}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 3, 5, 8, 13, 21, 34], ARRAY[JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', null])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":123,\"13\":{\"a\":1,\"b\":\"str\",\"c\":null},\"2\":3.14,\"21\":null,\"3\":false,\"34\":null,\"5\":\"abc\",\"8\":[1,\"a\",null]}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[TIMESTAMP '1970-01-01 00:00:01', null])"))
                .hasType(JSON)
                .isEqualTo(format("{\"1\":\"%s\",\"2\":null}", sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0)));

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[2, 5, 3], ARRAY[DATE '2001-08-22', DATE '2001-08-23', null])"))
                .hasType(JSON)
                .isEqualTo("{\"2\":\"2001-08-22\",\"3\":null,\"5\":\"2001-08-23\"}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 3, 5, 8], ARRAY[ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], null])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":[1,2],\"2\":[3,null],\"3\":[],\"5\":[null,null],\"8\":null}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 8, 5, 3], ARRAY[MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), null])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":{\"a\":1,\"b\":2},\"2\":{\"none\":null,\"three\":3},\"3\":null,\"5\":{\"h1\":null,\"h2\":null},\"8\":{}}");

        assertThat(assertions.expression("cast(a AS JSON)")
                .binding("a", "MAP(ARRAY[1, 2, 3, 5], ARRAY[ROW(1, 2), ROW(3, CAST(null as INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null])"))
                .hasType(JSON)
                .isEqualTo("{\"1\":{\"\":1,\"\":2},\"2\":{\"\":3,\"\":null},\"3\":{\"\":null,\"\":null},\"5\":null}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3])"))
                .hasType(JSON)
                .isEqualTo("{\"1.00000000000000\":2.2,\"383838383838383.12324234234234\":3.3}");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "MAP(ARRAY[1.0], ARRAY[2.2])"))
                .hasType(JSON)
                .isEqualTo("{\"1.0\":2.2}");
    }

    @Test
    public void testJsonToMap()
    {
        // special values
        assertThat(assertions.expression("cast(a AS MAP(BIGINT, BIGINT))")
                .binding("a", "CAST(null as JSON)"))
                .isNull(mapType(BIGINT, BIGINT));

        assertThat(assertions.expression("cast(a as MAP(BIGINT, BIGINT))")
                .binding("a", "JSON 'null'"))
                .isNull(mapType(BIGINT, BIGINT));

        assertThat(assertions.expression("cast(a as MAP(BIGINT, BIGINT))")
                .binding("a", "JSON '{}'"))
                .hasType(mapType(BIGINT, BIGINT))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.expression("cast(a as MAP(BIGINT, BIGINT))")
                .binding("a", "JSON '{\"1\": null, \"2\": null}'"))
                .hasType(mapType(BIGINT, BIGINT))
                .isEqualTo(asMap(ImmutableList.of(1L, 2L), asList(null, null)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a AS MAP(DECIMAL(10,5), DECIMAL(2,1)))")
                .binding("a", "CAST(MAP(ARRAY[12345.12345], ARRAY[12345.12345]) as JSON)").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        // key type: boolean
        assertThat(assertions.expression("cast(a as MAP(BOOLEAN, BIGINT))")
                .binding("a", "JSON '{\"true\": 1, \"false\": 0}'"))
                .hasType(mapType(BOOLEAN, BIGINT))
                .isEqualTo(ImmutableMap.of(true, 1L, false, 0L));

        // key type: tinyint, smallint, integer, bigint
        assertThat(assertions.expression("cast(a as MAP(TINYINT, BIGINT))")
                .binding("a", "JSON '{\"1\": 5, \"2\": 8, \"3\": 13}'"))
                .hasType(mapType(TINYINT, BIGINT))
                .isEqualTo(ImmutableMap.of((byte) 1, 5L, (byte) 2, 8L, (byte) 3, 13L));

        assertThat(assertions.expression("cast(a as MAP(SMALLINT, BIGINT))")
                .binding("a", "JSON '{\"12345\": 5, \"12346\": 8, \"12347\": 13}'"))
                .hasType(mapType(SMALLINT, BIGINT))
                .isEqualTo(ImmutableMap.of((short) 12345, 5L, (short) 12346, 8L, (short) 12347, 13L));

        assertThat(assertions.expression("cast(a as MAP(INTEGER, BIGINT))")
                .binding("a", "JSON '{\"123456789\": 5, \"123456790\": 8, \"123456791\": 13}'"))
                .hasType(mapType(INTEGER, BIGINT))
                .isEqualTo(ImmutableMap.of(123456789, 5L, 123456790, 8L, 123456791, 13L));

        assertThat(assertions.expression("cast(a as MAP(BIGINT, BIGINT))")
                .binding("a", "JSON '{\"1234567890123456111\": 5, \"1234567890123456222\": 8, \"1234567890123456777\": 13}'"))
                .hasType(mapType(BIGINT, BIGINT))
                .isEqualTo(ImmutableMap.of(1234567890123456111L, 5L, 1234567890123456222L, 8L, 1234567890123456777L, 13L));

        // key type: real, double, decimal
        assertThat(assertions.expression("cast(a as MAP(REAL, BIGINT))")
                .binding("a", "JSON '{\"3.14\": 5, \"NaN\": 8, \"Infinity\": 13, \"-Infinity\": 21}'"))
                .hasType(mapType(REAL, BIGINT))
                .isEqualTo(ImmutableMap.of(3.14f, 5L, Float.NaN, 8L, Float.POSITIVE_INFINITY, 13L, Float.NEGATIVE_INFINITY, 21L));

        assertThat(assertions.expression("cast(a as MAP(DOUBLE, BIGINT))")
                .binding("a", "JSON '{\"3.1415926\": 5, \"NaN\": 8, \"Infinity\": 13, \"-Infinity\": 21}'"))
                .hasType(mapType(DOUBLE, BIGINT))
                .isEqualTo(ImmutableMap.of(3.1415926, 5L, Double.NaN, 8L, Double.POSITIVE_INFINITY, 13L, Double.NEGATIVE_INFINITY, 21L));

        assertThat(assertions.expression("cast(a as MAP(DECIMAL(10, 5), BIGINT))")
                .binding("a", "JSON '{\"123.456\": 5, \"3.14\": 8}'"))
                .hasType(mapType(createDecimalType(10, 5), BIGINT))
                .isEqualTo(ImmutableMap.of(
                        decimal("123.45600", createDecimalType(10, 5)),
                        5L,
                        decimal("3.14000", createDecimalType(10, 5)),
                        8L));

        assertThat(assertions.expression("cast(a as MAP(DECIMAL(38, 8), BIGINT))")
                .binding("a", "JSON '{\"12345678.12345678\": 5, \"3.1415926\": 8}'"))
                .hasType(mapType(createDecimalType(38, 8), BIGINT))
                .isEqualTo(ImmutableMap.of(
                        decimal("12345678.12345678", createDecimalType(38, 8)),
                        5L,
                        decimal("3.14159260", createDecimalType(38, 8)),
                        8L));

        // key type: varchar
        assertThat(assertions.expression("cast(a as MAP(VARCHAR, BIGINT))")
                .binding("a", "JSON '{\"a\": 5, \"bb\": 8, \"ccc\": 13}'"))
                .hasType(mapType(VARCHAR, BIGINT))
                .isEqualTo(ImmutableMap.of("a", 5L, "bb", 8L, "ccc", 13L));

        // value type: boolean
        assertThat(assertions.expression("cast(a as MAP(BIGINT, BOOLEAN))")
                .binding("a", "JSON '{\"1\": true, \"2\": false, \"3\": 12, \"5\": 0, \"8\": 12.3, \"13\": 0.0, \"21\": \"true\", \"34\": \"false\", \"55\": null}'"))
                .hasType(mapType(BIGINT, BOOLEAN))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L),
                        asList(true, false, true, false, true, false, true, false, null)));

        // value type: tinyint, smallint, integer, bigint
        assertThat(assertions.expression("cast(a as MAP(BIGINT, TINYINT))")
                .binding("a", "JSON '{\"1\": true, \"2\": false, \"3\": 12, \"5\": 12.7, \"8\": \"12\", \"13\": null}'"))
                .hasType(mapType(BIGINT, TINYINT))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList((byte) 1, (byte) 0, (byte) 12, (byte) 13, (byte) 12, null)));

        assertThat(assertions.expression("cast(a as MAP(BIGINT, SMALLINT))")
                .binding("a", "JSON '{\"1\": true, \"2\": false, \"3\": 12345, \"5\": 12345.6, \"8\": \"12345\", \"13\": null}'"))
                .hasType(mapType(BIGINT, SMALLINT))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList((short) 1, (short) 0, (short) 12345, (short) 12346, (short) 12345, null)));

        assertThat(assertions.expression("cast(a as MAP(BIGINT, INTEGER))")
                .binding("a", "JSON '{\"1\": true, \"2\": false, \"3\": 12345678, \"5\": 12345678.9, \"8\": \"12345678\", \"13\": null}'"))
                .hasType(mapType(BIGINT, INTEGER))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList(1, 0, 12345678, 12345679, 12345678, null)));

        assertThat(assertions.expression("cast(a as MAP(BIGINT, BIGINT))")
                .binding("a", "JSON '{\"1\": true, \"2\": false, \"3\": 1234567891234567, \"5\": 1234567891234567.8, \"8\": \"1234567891234567\", \"13\": null}'"))
                .hasType(mapType(BIGINT, BIGINT))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList(1L, 0L, 1234567891234567L, 1234567891234568L, 1234567891234567L, null)));

        // value type: real, double, decimal
        assertThat(assertions.expression("cast(a as MAP(BIGINT, REAL))")
                .binding("a", "JSON '{\"1\": true, \"2\": false, \"3\": 12345, \"5\": 12345.67, \"8\": \"3.14\", \"13\": \"NaN\", \"21\": \"Infinity\", \"34\": \"-Infinity\", \"55\": null}'"))
                .hasType(mapType(BIGINT, REAL))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L),
                        asList(1.0f, 0.0f, 12345.0f, 12345.67f, 3.14f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, null)));

        assertThat(assertions.expression("cast(a as MAP(BIGINT, DOUBLE))")
                .binding("a", "JSON '{\"1\": true, \"2\": false, \"3\": 1234567890, \"5\": 1234567890.1, \"8\": \"3.14\", \"13\": \"NaN\", \"21\": \"Infinity\", \"34\": \"-Infinity\", \"55\": null}'"))
                .hasType(mapType(BIGINT, DOUBLE))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L),
                        asList(1.0, 0.0, 1234567890.0, 1234567890.1, 3.14, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, null)));

        assertThat(assertions.expression("cast(a as MAP(BIGINT, DECIMAL(10, 5)))")
                .binding("a", "JSON '{\"1\": true, \"2\": false, \"3\": 128, \"5\": 123.456, \"8\": \"3.14\", \"13\": null}'"))
                .hasType(mapType(BIGINT, createDecimalType(10, 5)))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList(
                                decimal("1.00000", createDecimalType(10, 5)),
                                decimal("0.00000", createDecimalType(10, 5)),
                                decimal("128.00000", createDecimalType(10, 5)),
                                decimal("123.45600", createDecimalType(10, 5)),
                                decimal("3.14000", createDecimalType(10, 5)),
                                null)));

        assertThat(assertions.expression("cast(a as MAP(BIGINT, DECIMAL(38, 8)))")
                .binding("a", "JSON '{\"1\": true, \"2\": false, \"3\": 128, \"5\": 12345678.12345678, \"8\": \"3.14\", \"13\": null}'"))
                .hasType(mapType(BIGINT, createDecimalType(38, 8)))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L),
                        asList(
                                decimal("1.00000000", createDecimalType(38, 8)),
                                decimal("0.00000000", createDecimalType(38, 8)),
                                decimal("128.00000000", createDecimalType(38, 8)),
                                decimal("12345678.12345678", createDecimalType(38, 8)),
                                decimal("3.14000000", createDecimalType(38, 8)),
                                null)));

        // varchar, json
        assertThat(assertions.expression("cast(a as MAP(BIGINT, VARCHAR))")
                .binding("a", "JSON '{\"1\": true, \"2\": false, \"3\": 12, \"5\": 12.3, \"8\": \"puppies\", \"13\": \"kittens\", \"21\": \"null\", \"34\": \"\", \"55\": null}'"))
                .hasType(mapType(BIGINT, VARCHAR))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L),
                        asList("true", "false", "12", "1.23E1", "puppies", "kittens", "null", "", null)));

        assertThat(assertions.expression("cast(a as MAP(VARCHAR, JSON))")
                .binding("a", "JSON '{\"k1\": 5, \"k2\": 3.14, \"k3\":[1, 2, 3], \"k4\":\"e\", \"k5\":{\"a\": \"b\"}, \"k6\":null, \"k7\":\"null\", \"k8\":[null]}'"))
                .hasType(mapType(VARCHAR, JSON))
                .isEqualTo(ImmutableMap.builder()
                        .put("k1", "5")
                        .put("k2", "3.14")
                        .put("k3", "[1,2,3]")
                        .put("k4", "\"e\"")
                        .put("k5", "{\"a\":\"b\"}")
                        .put("k6", "null")
                        .put("k7", "\"null\"")
                        .put("k8", "[null]")
                        .buildOrThrow());

        // These two tests verifies that partial json cast preserves input order
        // The second test should never happen in real life because valid json in Trino requires natural key ordering.
        // However, it is added to make sure that the order in the first test is not a coincidence.
        assertThat(assertions.expression("cast(a as MAP(VARCHAR, JSON))")
                .binding("a", "JSON '{\"k1\": {\"1klmnopq\":1, \"2klmnopq\":2, \"3klmnopq\":3, \"4klmnopq\":4, \"5klmnopq\":5, \"6klmnopq\":6, \"7klmnopq\":7}}'"))
                .hasType(mapType(VARCHAR, JSON))
                .isEqualTo(ImmutableMap.of("k1", "{\"1klmnopq\":1,\"2klmnopq\":2,\"3klmnopq\":3,\"4klmnopq\":4,\"5klmnopq\":5,\"6klmnopq\":6,\"7klmnopq\":7}"));

        assertThat(assertions.expression("cast(a as MAP(VARCHAR, JSON))")
                .binding("a", "unchecked_to_json('{\"k1\": {\"7klmnopq\":7, \"6klmnopq\":6, \"5klmnopq\":5, \"4klmnopq\":4, \"3klmnopq\":3, \"2klmnopq\":2, \"1klmnopq\":1}}')"))
                .hasType(mapType(VARCHAR, JSON))
                .isEqualTo(ImmutableMap.of("k1", "{\"7klmnopq\":7,\"6klmnopq\":6,\"5klmnopq\":5,\"4klmnopq\":4,\"3klmnopq\":3,\"2klmnopq\":2,\"1klmnopq\":1}"));

        // nested array/map
        assertThat(assertions.expression("cast(a as MAP(BIGINT, ARRAY(BIGINT)))")
                .binding("a", "JSON '{\"1\": [1, 2], \"2\": [3, null], \"3\": [], \"5\": [null, null], \"8\": null}'"))
                .hasType(mapType(BIGINT, new ArrayType(BIGINT)))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L),
                        asList(asList(1L, 2L), asList(3L, null), emptyList(), asList(null, null), null)));

        assertThat(assertions.expression("CAST(JSON '{" +
                "\"1\": {\"a\": 1, \"b\": 2}, " +
                "\"2\": {\"none\": null, \"three\": 3}, " +
                "\"3\": {}, " +
                "\"5\": {\"h1\": null,\"h2\": null}, " +
                "\"8\": null}' " +
                "AS MAP(BIGINT, MAP(VARCHAR, BIGINT)))"))
                .hasType(mapType(BIGINT, mapType(VARCHAR, BIGINT)))
                .isEqualTo(asMap(
                        ImmutableList.of(1L, 2L, 3L, 5L, 8L),
                        asList(
                                ImmutableMap.of("a", 1L, "b", 2L),
                                asMap(ImmutableList.of("none", "three"), asList(null, 3L)),
                                ImmutableMap.of(),
                                asMap(ImmutableList.of("h1", "h2"), asList(null, null)),
                                null)));

        assertThat(assertions.expression("CAST(JSON '{" +
                "\"row1\": [1, \"two\"], " +
                "\"row2\": [3, null], " +
                "\"row3\": {\"k1\": 1, \"k2\": \"two\"}, " +
                "\"row4\": {\"k2\": null, \"k1\": 3}, " +
                "\"row5\": null}' " +
                "AS MAP(VARCHAR, ROW(k1 BIGINT, k2 VARCHAR)))"))
                .hasType(mapType(VARCHAR, RowType.from(ImmutableList.of(
                        RowType.field("k1", BIGINT),
                        RowType.field("k2", VARCHAR)))))
                .isEqualTo(asMap(
                        ImmutableList.of("row1", "row2", "row3", "row4", "row5"),
                        asList(
                                asList(1L, "two"),
                                asList(3L, null),
                                asList(1L, "two"),
                                asList(3L, null),
                                null)));

        // invalid cast
        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(ARRAY(BIGINT), BIGINT))")
                .binding("a", "JSON '{\"[]\": 1}'").evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:8: Cannot cast json to map(array(bigint), bigint)");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(BIGINT, BIGINT))")
                .binding("a", "JSON '[1, 2]'").evaluate())
                .hasMessage("Cannot cast to map(bigint, bigint). Expected a json object, but got [\n[1,2]")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(VARCHAR, MAP(VARCHAR, BIGINT)))")
                .binding("a", "JSON '{\"a\": 1, \"b\": 2}'").evaluate())
                .hasMessage("Cannot cast to map(varchar, map(varchar, bigint)). Expected a json object, but got 1\n{\"a\":1,\"b\":2}")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(VARCHAR, BIGINT))")
                .binding("a", "JSON '{\"a\": 1, \"b\": []}'").evaluate())
                .hasMessage("Cannot cast to map(varchar, bigint). Unexpected token when cast to bigint: [\n{\"a\":1,\"b\":[]}")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(VARCHAR, MAP(VARCHAR, BIGINT)))")
                .binding("a", "JSON '{\"1\": {\"a\": 1}, \"2\": []}'").evaluate())
                .hasMessage("Cannot cast to map(varchar, map(varchar, bigint)). Expected a json object, but got [\n{\"1\":{\"a\":1},\"2\":[]}")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(VARCHAR, BIGINT))")
                .binding("a", "unchecked_to_json('\"a\": 1, \"b\": 2')").evaluate())
                .hasMessage("Cannot cast to map(varchar, bigint). Expected a json object, but got a\n\"a\": 1, \"b\": 2")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(VARCHAR, BIGINT))")
                .binding("a", "unchecked_to_json('{\"a\": 1} 2')").evaluate())
                .hasMessage("Cannot cast to map(varchar, bigint). Unexpected trailing token: 2\n{\"a\": 1} 2")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(VARCHAR, BIGINT))")
                .binding("a", "unchecked_to_json('{\"a\": 1')").evaluate())
                .hasMessage("Cannot cast to map(varchar, bigint).\n{\"a\": 1")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(VARCHAR, BIGINT))")
                .binding("a", "JSON '{\"a\": \"b\"}'").evaluate())
                .hasMessage("Cannot cast to map(varchar, bigint). Cannot cast 'b' to BIGINT\n{\"a\":\"b\"}")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(VARCHAR, INTEGER))")
                .binding("a", "JSON '{\"a\": 1234567890123.456}'").evaluate())
                .hasMessage("Cannot cast to map(varchar, integer). Out of range for integer: 1.234567890123456E12\n{\"a\":1.234567890123456E12}")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(BIGINT, BIGINT))")
                .binding("a", "JSON '{\"1\":1, \"01\": 2}'").evaluate())
                .hasMessage("Cannot cast to map(bigint, bigint). Duplicate keys are not allowed\n{\"01\":2,\"1\":1}")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as ARRAY(MAP(BIGINT, BIGINT)))")
                .binding("a", "JSON '[{\"1\":1, \"01\": 2}]'").evaluate())
                .hasMessage("Cannot cast to array(map(bigint, bigint)). Duplicate keys are not allowed\n[{\"01\":2,\"1\":1}]")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        // some other key/value type combinations
        assertThat(assertions.expression("cast(a as MAP(VARCHAR, VARCHAR))")
                .binding("a", "JSON '{\"puppies\":\"kittens\"}'"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("puppies", "kittens"));

        assertThat(assertions.expression("cast(a as MAP(BOOLEAN, VARCHAR))")
                .binding("a", "JSON '{\"true\":\"kittens\"}'"))
                .hasType(mapType(BOOLEAN, VARCHAR))
                .isEqualTo(ImmutableMap.of(true, "kittens"));

        assertThat(assertions.expression("cast(a as MAP(BOOLEAN, VARCHAR))")
                .binding("a", "JSON 'null'"))
                .isNull(mapType(BOOLEAN, VARCHAR));

        // cannot use JSON literal containing DECIMAL values right now.
        // Decimal literal are interpreted internally by JSON parser as double and precision is lost.

        assertThat(assertions.expression("cast(a AS MAP(DECIMAL(29,14), DECIMAL(2,1)))")
                .binding("a", "CAST(MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3]) as JSON)"))
                .hasType(mapType(createDecimalType(29, 14), createDecimalType(2, 1)))
                .isEqualTo(ImmutableMap.of(
                        decimal("000000000000001.00000000000000", createDecimalType(29, 14)),
                        decimal("2.2", createDecimalType(2, 1)),
                        decimal("383838383838383.12324234234234", createDecimalType(29, 14)),
                        decimal("3.3", createDecimalType(2, 1))));

        assertThat(assertions.expression("cast(a AS MAP(DECIMAL(2,1), DECIMAL(29,14)))")
                .binding("a", "CAST(MAP(ARRAY[2.2, 3.3], ARRAY[1.0, 383838383838383.12324234234234]) as JSON)"))
                .hasType(mapType(createDecimalType(2, 1), createDecimalType(29, 14)))
                .isEqualTo(ImmutableMap.of(
                        decimal("2.2", createDecimalType(2, 1)),
                        decimal("000000000000001.00000000000000", createDecimalType(29, 14)),
                        decimal("3.3", createDecimalType(2, 1)),
                        decimal("383838383838383.12324234234234", createDecimalType(29, 14))));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a AS MAP(DECIMAL(2,1), DECIMAL(10,5)))")
                .binding("a", "CAST(MAP(ARRAY[12345.12345], ARRAY[12345.12345]) as JSON)").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a AS MAP(DECIMAL(10,5), DECIMAL(2,1)))")
                .binding("a", "CAST(MAP(ARRAY[12345.12345], ARRAY[12345.12345]) as JSON)").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testElementAt()
    {
        // empty map
        assertThat(assertions.function("element_at", "map(CAST(ARRAY[] AS ARRAY(BIGINT)), CAST(ARRAY[] AS ARRAY(BIGINT)))", "1"))
                .isNull(BIGINT);

        // missing key
        assertThat(assertions.function("element_at", "map(ARRAY[1], ARRAY[1e0])", "2"))
                .isNull(DOUBLE);

        assertThat(assertions.function("element_at", "map(ARRAY[1.0], ARRAY['a'])", "2.0"))
                .isNull(createVarcharType(1));

        assertThat(assertions.function("element_at", "map(ARRAY['a'], ARRAY[true])", "'b'"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("element_at", "map(ARRAY[true], ARRAY[ARRAY[1]])", "false"))
                .isNull(new ArrayType(INTEGER));

        assertThat(assertions.function("element_at", "map(ARRAY[ARRAY[1]], ARRAY[1])", "ARRAY[2]"))
                .isNull(INTEGER);

        // null value associated with the requested key
        assertThat(assertions.function("element_at", "map(ARRAY[1], ARRAY[null])", "1"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("element_at", "map(ARRAY[1.0E0], ARRAY[null])", "1.0E0"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("element_at", "map(ARRAY[TRUE], ARRAY[null])", "TRUE"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("element_at", "map(ARRAY['puppies'], ARRAY[null])", "'puppies'"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("element_at", "map(ARRAY[ARRAY[1]], ARRAY[null])", "ARRAY[1]"))
                .isNull(UNKNOWN);

        // general tests
        assertThat(assertions.function("element_at", "map(ARRAY[1, 3], ARRAY[2, 4])", "3"))
                .isEqualTo(4);

        assertThat(assertions.function("element_at", "map(ARRAY[BIGINT '1', 3], ARRAY[BIGINT '2', 4])", "3"))
                .isEqualTo(4L);

        assertThat(assertions.function("element_at", "map(ARRAY[1, 3], ARRAY[2, NULL])", "3"))
                .isNull(INTEGER);

        assertThat(assertions.function("element_at", "map(ARRAY[BIGINT '1', 3], ARRAY[2, NULL])", "3"))
                .isNull(INTEGER);

        assertThat(assertions.function("element_at", "map(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0])", "1"))
                .isEqualTo(2.0);

        assertThat(assertions.function("element_at", "map(ARRAY[1.0E0, 2.0E0], ARRAY[ARRAY[1, 2], ARRAY[3]])", "1.0E0"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));

        assertThat(assertions.function("element_at", "map(ARRAY['puppies'], ARRAY['kittens'])", "'puppies'"))
                .hasType(createVarcharType(7))
                .isEqualTo("kittens");

        assertThat(assertions.function("element_at", "map(ARRAY[TRUE, FALSE], ARRAY[2, 4])", "TRUE"))
                .isEqualTo(2);

        assertThat(assertions.function("element_at", "map(ARRAY[ARRAY[1, 2], ARRAY[3]], ARRAY[1e0, 2e0])", "ARRAY[1, 2]"))
                .isEqualTo(1.0);

        assertThat(assertions.function("element_at", "map(ARRAY['1', '100'], ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '2005-09-10 13:00:00'])", "'1'"))
                .hasType(createTimestampType(0))
                .isEqualTo(sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0));

        assertThat(assertions.function("element_at", "map(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[1.0E0, 100.0E0])", "from_unixtime(1)"))
                .isEqualTo(1.0);

        assertThat(assertions.function("element_at", "map(ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '2222-05-10 12:34:56.123456789'], ARRAY[1, 2])", "TIMESTAMP '2222-05-10 12:34:56.123456789'"))
                .isEqualTo(2);
    }

    @Test
    public void testSubscript()
    {
        assertThat(assertions.expression("a[1]")
                .binding("a", "map(ARRAY[1], ARRAY[null])"))
                .isNull(UNKNOWN);

        assertThat(assertions.expression("a[1.0E0]")
                .binding("a", "map(ARRAY[1.0E0], ARRAY[null])"))
                .isNull(UNKNOWN);

        assertThat(assertions.expression("a[TRUE]")
                .binding("a", "map(ARRAY[TRUE], ARRAY[null])"))
                .isNull(UNKNOWN);

        assertThat(assertions.expression("a['puppies']")
                .binding("a", "map(ARRAY['puppies'], ARRAY[null])"))
                .isNull(UNKNOWN);

        assertTrinoExceptionThrownBy(assertions.function("map", "ARRAY[CAST(null as bigint)]", "ARRAY[1]")::evaluate)
                .hasMessage("map key cannot be null");

        assertTrinoExceptionThrownBy(assertions.function("map", "ARRAY[CAST(null as bigint)]", "ARRAY[CAST(null as bigint)]")::evaluate)
                .hasMessage("map key cannot be null");

        assertTrinoExceptionThrownBy(assertions.function("map", "ARRAY[1,null]", "ARRAY[null,2]")::evaluate)
                .hasMessage("map key cannot be null");

        assertThat(assertions.expression("a[3]")
                .binding("a", "map(ARRAY[1, 3], ARRAY[2, 4])"))
                .isEqualTo(4);

        assertThat(assertions.expression("a[3]")
                .binding("a", "map(ARRAY[BIGINT '1', 3], ARRAY[BIGINT '2', 4])"))
                .isEqualTo(4L);

        assertThat(assertions.expression("a[3]")
                .binding("a", "map(ARRAY[1, 3], ARRAY[2, NULL])"))
                .isNull(INTEGER);

        assertThat(assertions.expression("a[3]")
                .binding("a", "map(ARRAY[BIGINT '1', 3], ARRAY[2, NULL])"))
                .isNull(INTEGER);

        assertThat(assertions.expression("a[1]")
                .binding("a", "map(ARRAY[1, 3], ARRAY[2.0E0, 4.0E0])"))
                .isEqualTo(2.0);

        assertThat(assertions.expression("a[1.0E0]")
                .binding("a", "map(ARRAY[1.0E0, 2.0E0], ARRAY[ARRAY[1, 2], ARRAY[3]])"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));

        assertThat(assertions.expression("a['puppies']")
                .binding("a", "map(ARRAY['puppies'], ARRAY['kittens'])"))
                .hasType(createVarcharType(7))
                .isEqualTo("kittens");

        assertThat(assertions.expression("a[TRUE]")
                .binding("a", "map(ARRAY[TRUE,FALSE],ARRAY[2,4])"))
                .isEqualTo(2);

        assertThat(assertions.expression("a['1']")
                .binding("a", "map(ARRAY['1', '100'], ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'])"))
                .hasType(createTimestampType(0))
                .isEqualTo(sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0));

        assertThat(assertions.expression("a[from_unixtime(1)]")
                .binding("a", "map(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[1.0E0, 100.0E0])"))
                .isEqualTo(1.0);

        assertTrinoExceptionThrownBy(() -> assertions.expression("a[3]")
                .binding("a", "map(ARRAY[BIGINT '1'], ARRAY[BIGINT '2'])").evaluate())
                .hasMessage("Key not present in map: 3");

        assertTrinoExceptionThrownBy(() -> assertions.expression("a['missing']")
                .binding("a", "map(ARRAY['hi'], ARRAY[2])").evaluate())
                .hasMessage("Key not present in map: missing");

        assertThat(assertions.expression("a[ARRAY[1,1]]")
                .binding("a", "map(ARRAY[array[1,1]], ARRAY['a'])"))
                .hasType(createVarcharType(1))
                .isEqualTo("a");

        assertThat(assertions.expression("a[('a', 'b')]")
                .binding("a", "map(ARRAY[('a', 'b')], ARRAY[ARRAY[100, 200]])"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(100, 200));

        assertThat(assertions.expression("a[1.0]")
                .binding("a", "map(ARRAY[1.0], ARRAY[2.2])"))
                .hasType(createDecimalType(2, 1))
                .isEqualTo(decimal("2.2", createDecimalType(2, 1)));

        assertThat(assertions.expression("a[000000000000001.00000000000000]")
                .binding("a", "map(ARRAY[000000000000001.00000000000000], ARRAY[2.2])"))
                .hasType(createDecimalType(2, 1))
                .isEqualTo(decimal("2.2", createDecimalType(2, 1)));

        assertThat(assertions.expression("a[TIMESTAMP '2222-05-10 12:34:56.123456789']")
                .binding("a", "map(ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '2222-05-10 12:34:56.123456789'], ARRAY[1, 2])"))
                .isEqualTo(2);
    }

    @Test
    public void testMapKeys()
    {
        assertThat(assertions.function("map_keys", "map(ARRAY['1', '3'], ARRAY['2', '4'])"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("1", "3"));

        assertThat(assertions.function("map_keys", "map(ARRAY[1.0E0, 2.0E0], ARRAY[ARRAY[1, 2], ARRAY[3]])"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0, 2.0));

        assertThat(assertions.function("map_keys", "map(ARRAY['puppies'], ARRAY['kittens'])"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("puppies"));

        assertThat(assertions.function("map_keys", "map(ARRAY[TRUE], ARRAY[2])"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true));

        assertThat(assertions.function("map_keys", "map(ARRAY[TIMESTAMP '1970-01-01 00:00:01'], ARRAY[1.0E0])"))
                .hasType(new ArrayType(createTimestampType(0)))
                .isEqualTo(ImmutableList.of(sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0)));

        assertThat(assertions.function("map_keys", "map(ARRAY[CAST('puppies' as varbinary)], ARRAY['kittens'])"))
                .hasType(new ArrayType(VARBINARY))
                .isEqualTo(ImmutableList.of(sqlVarbinary("puppies")));

        assertThat(assertions.function("map_keys", "map(ARRAY[1,2],  ARRAY[ARRAY[1, 2], ARRAY[3]])"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));

        assertThat(assertions.function("map_keys", "map(ARRAY[1,4], ARRAY[MAP(ARRAY[2], ARRAY[3]), MAP(ARRAY[5], ARRAY[6])])"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 4));

        assertThat(assertions.function("map_keys", "map(ARRAY[ARRAY[1], ARRAY[2, 3]],  ARRAY[ARRAY[3, 4], ARRAY[5]])"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2, 3)));

        assertThat(assertions.function("map_keys", "map(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3])"))
                .hasType(new ArrayType(createDecimalType(29, 14)))
                .isEqualTo(ImmutableList.of(
                        decimal("000000000000001.00000000000000", createDecimalType(29, 14)),
                        decimal("383838383838383.12324234234234", createDecimalType(29, 14))));

        assertThat(assertions.function("map_keys", "map(ARRAY[1.0, 2.01], ARRAY[2.2, 3.3])"))
                .hasType(new ArrayType(createDecimalType(3, 2)))
                .isEqualTo(ImmutableList.of(
                        decimal("1.00", createDecimalType(3, 2)),
                        decimal("2.01", createDecimalType(3, 2))));
    }

    @Test
    public void testMapValues()
    {
        assertThat(assertions.function("map_values", "map(ARRAY['1'], ARRAY[ARRAY[TRUE, FALSE, NULL]])"))
                .hasType(new ArrayType(new ArrayType(BOOLEAN)))
                .isEqualTo(ImmutableList.of(Lists.newArrayList(true, false, null)));

        assertThat(assertions.function("map_values", "map(ARRAY['1'], ARRAY[ARRAY[ARRAY[1, 2]]])"))
                .hasType(new ArrayType(new ArrayType(new ArrayType(INTEGER))))
                .isEqualTo(ImmutableList.of(ImmutableList.of(ImmutableList.of(1, 2))));

        assertThat(assertions.function("map_values", "map(ARRAY[1, 3], ARRAY['2', '4'])"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("2", "4"));

        assertThat(assertions.function("map_values", "map(ARRAY[1.0E0,2.0E0], ARRAY[ARRAY[1, 2], ARRAY[3]])"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3)));

        assertThat(assertions.function("map_values", "map(ARRAY['puppies'], ARRAY['kittens'])"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("kittens"));

        assertThat(assertions.function("map_values", "map(ARRAY[TRUE], ARRAY[2])"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(2));

        assertThat(assertions.function("map_values", "map(ARRAY['1'], ARRAY[NULL])"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(Lists.newArrayList((Object) null));

        assertThat(assertions.function("map_values", "map(ARRAY['1'], ARRAY[TRUE])"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true));

        assertThat(assertions.function("map_values", "map(ARRAY['1'], ARRAY[1.0E0])"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0));

        assertThat(assertions.function("map_values", "map(ARRAY['1', '2'], ARRAY[ARRAY[1.0E0, 2.0E0], ARRAY[3.0E0, 4.0E0]])"))
                .hasType(new ArrayType(new ArrayType(DOUBLE)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1.0, 2.0), ImmutableList.of(3.0, 4.0)));

        assertThat(assertions.function("map_values", "map(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3])"))
                .hasType(new ArrayType(createDecimalType(2, 1)))
                .isEqualTo(ImmutableList.of(
                        decimal("2.2", createDecimalType(2, 1)),
                        decimal("3.3", createDecimalType(2, 1))));

        assertThat(assertions.function("map_values", "map(ARRAY[1.0, 2.01], ARRAY[383838383838383.12324234234234, 3.3])"))
                .hasType(new ArrayType(createDecimalType(29, 14)))
                .isEqualTo(ImmutableList.of(
                        decimal("383838383838383.12324234234234", createDecimalType(29, 14)),
                        decimal("000000000000003.30000000000000", createDecimalType(29, 14))));
    }

    @Test
    public void testEquals()
    {
        // single item
        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1], ARRAY[2])", "MAP(ARRAY[1], ARRAY[2])"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1], ARRAY[2])", "MAP(ARRAY[1], ARRAY[4])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[3], ARRAY[1])", "MAP(ARRAY[2], ARRAY[1])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[2.2], ARRAY[3.1])", "MAP(ARRAY[2.2], ARRAY[3.1])"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[2.2], ARRAY[3.1])", "MAP(ARRAY[2.2], ARRAY[3.0])"))
                .isEqualTo(false);

        assertThat(assertions.expression("MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000])"))
                .isEqualTo(true);

        assertThat(assertions.expression("MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000013.30000000000000])"))
                .isEqualTo(false);

        // multiple items
        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1], ARRAY[2])", "MAP(ARRAY[1, 3], ARRAY[2, 4])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1, 3], ARRAY[2, 4])", "MAP(ARRAY[1], ARRAY[2])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1, 3], ARRAY[2, 4])", "MAP(ARRAY[3, 1], ARRAY[4, 2])"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1, 3], ARRAY[2, 4])", "MAP(ARRAY[3, 1], ARRAY[2, 4])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0])", "MAP(ARRAY['3', '1'], ARRAY[4.0E0, 2.0E0])"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0])", "MAP(ARRAY['3', '1'], ARRAY[2.0E0, 4.0E0])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4'])", "MAP(ARRAY[FALSE, TRUE], ARRAY['4', '2'])"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4'])", "MAP(ARRAY[FALSE, TRUE], ARRAY['2', '4'])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE])", "MAP(ARRAY[3.0E0, 1.0E0], ARRAY[FALSE, TRUE])"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE])", "MAP(ARRAY[3.0E0, 1.0E0], ARRAY[TRUE, FALSE])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)])", "MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(100), from_unixtime(1)])"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)])", "MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(1), from_unixtime(100)])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens', 'puppies'])", "MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['puppies', 'kittens'])"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens', 'puppies'])", "MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['kittens', 'puppies'])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])", "MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])", "MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[3], ARRAY[1, 2]])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]])", "MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]])"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]])", "MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[3], ARRAY[1, 2]])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['cat', 'dog']], ARRAY[ARRAY[1, 2], ARRAY[3]])", "MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3])", "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3])"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3])", "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.2])"))
                .isEqualTo(false);

        // nulls
        assertThat(assertions.operator(EQUAL, "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])", "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])"))
                .isNull(BOOLEAN);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY['kittens', 'puppies'], ARRAY[3, 3])", "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])"))
                .isNull(BOOLEAN);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])", "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 2])"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])", "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])"))
                .isNull(BOOLEAN);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[NULL, FALSE])", "MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[FALSE, NULL])"))
                .isNull(BOOLEAN);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[TRUE, NULL])", "MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[TRUE, NULL])"))
                .isNull(BOOLEAN);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, null])", "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, null])"))
                .isNull(BOOLEAN);

        assertThat(assertions.operator(EQUAL, "MAP(ARRAY[1.0, 2.1], ARRAY[null, null])", "MAP(ARRAY[1.0, 2.1], ARRAY[null, null])"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testNotEquals()
    {
        // single item
        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1], ARRAY[2])")
                .binding("b", "MAP(ARRAY[1], ARRAY[2])"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1], ARRAY[2])")
                .binding("b", "MAP(ARRAY[1], ARRAY[4])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[3], ARRAY[1])")
                .binding("b", "MAP(ARRAY[2], ARRAY[1])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[2.2], ARRAY[3.1])")
                .binding("b", "MAP(ARRAY[2.2], ARRAY[3.1])"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[2.2], ARRAY[3.1])")
                .binding("b", "MAP(ARRAY[2.2], ARRAY[3.0])"))
                .isEqualTo(true);

        assertThat(assertions.expression("MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "!= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000])"))
                .isEqualTo(false);

        assertThat(assertions.expression("MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "!= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000013.30000000000000])"))
                .isEqualTo(true);

        // multiple items
        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1], ARRAY[2])")
                .binding("b", "MAP(ARRAY[1, 3], ARRAY[2, 4])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1, 3], ARRAY[2, 4])")
                .binding("b", "MAP(ARRAY[1], ARRAY[2])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1, 3], ARRAY[2, 4])")
                .binding("b", "MAP(ARRAY[3, 1], ARRAY[4, 2])"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1, 3], ARRAY[2, 4])")
                .binding("b", "MAP(ARRAY[3, 1], ARRAY[2, 4])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0])")
                .binding("b", "MAP(ARRAY['3', '1'], ARRAY[4.0E0, 2.0E0])"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0])")
                .binding("b", "MAP(ARRAY['3', '1'], ARRAY[2.0E0, 4.0E0])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4'])")
                .binding("b", "MAP(ARRAY[FALSE, TRUE], ARRAY['4', '2'])"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4'])")
                .binding("b", "MAP(ARRAY[FALSE, TRUE], ARRAY['2', '4'])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE])")
                .binding("b", "MAP(ARRAY[3.0E0, 1.0E0], ARRAY[FALSE, TRUE])"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE])")
                .binding("b", "MAP(ARRAY[3.0E0, 1.0E0], ARRAY[TRUE, FALSE])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)])")
                .binding("b", "MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(100), from_unixtime(1)])"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)])")
                .binding("b", "MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(1), from_unixtime(100)])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens','puppies'])")
                .binding("b", "MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['puppies', 'kittens'])"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens','puppies'])")
                .binding("b", "MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['kittens', 'puppies'])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])")
                .binding("b", "MAP(ARRAY['kittens','puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])")
                .binding("b", "MAP(ARRAY['kittens','puppies'], ARRAY[ARRAY[3], ARRAY[1, 2]])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3])")
                .binding("b", "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3])"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3])")
                .binding("b", "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.2])"))
                .isEqualTo(true);

        // nulls
        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])")
                .binding("b", "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY['kittens', 'puppies'], ARRAY[3, 3])")
                .binding("b", "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])")
                .binding("b", "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 2])"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])")
                .binding("b", "MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[NULL, FALSE])")
                .binding("b", "MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[FALSE, NULL])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[TRUE, NULL])")
                .binding("b", "MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[TRUE, NULL])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, null])")
                .binding("b", "MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, null])"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a != b")
                .binding("a", "MAP(ARRAY[1.0, 2.1], ARRAY[null, null])")
                .binding("b", "MAP(ARRAY[1.0, 2.1], ARRAY[null, null])"))
                .isNull(BOOLEAN);
    }

    @Test
    public void testDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS MAP(INTEGER, VARCHAR))", "CAST(NULL AS MAP(INTEGER, VARCHAR))"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1], ARRAY[2])", "NULL"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "MAP(ARRAY[1], ARRAY[2])"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1], ARRAY[2])", "MAP(ARRAY[1], ARRAY[2])"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1], ARRAY[NULL])", "MAP(ARRAY[1], ARRAY[NULL])"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1], ARRAY[0])", "MAP(ARRAY[1], ARRAY[NULL])"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1], ARRAY[NULL])", "MAP(ARRAY[1], ARRAY[0])"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1, 2], ARRAY['kittens','puppies'])", "MAP(ARRAY[1, 2], ARRAY['puppies', 'kittens'])"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1, 2], ARRAY['kittens','puppies'])", "MAP(ARRAY[1, 2], ARRAY['kittens', 'puppies'])"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1, 3], ARRAY['kittens','puppies'])", "MAP(ARRAY[1, 2], ARRAY['kittens', 'puppies'])"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1, 3], ARRAY['kittens','puppies'])", "MAP(ARRAY[1, 2], ARRAY['kittens', 'pupp111'])"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1, 3], ARRAY['kittens','puppies'])", "MAP(ARRAY[1, 2], ARRAY['kittens', NULL])"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1, 3], ARRAY['kittens','puppies'])", "MAP(ARRAY[1, 2], ARRAY[NULL, NULL])"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[3e0])])", "MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[3e0])])"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[3e0])])", "MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[4e0])])"))
                .isEqualTo(true);
    }

    @Test
    public void testMapConcat()
    {
        assertThat(assertions.function("map_concat", "map(ARRAY[TRUE], ARRAY[1])", "map(CAST(ARRAY[] AS ARRAY(BOOLEAN)), CAST(ARRAY[] AS ARRAY(INTEGER)))"))
                .hasType(mapType(BOOLEAN, INTEGER))
                .isEqualTo(ImmutableMap.of(true, 1));

        // <BOOLEAN, INTEGER> Tests
        assertThat(assertions.function("map_concat", "map(ARRAY[TRUE], ARRAY[1])", "map(ARRAY[TRUE, FALSE], ARRAY[10, 20])"))
                .hasType(mapType(BOOLEAN, INTEGER))
                .isEqualTo(ImmutableMap.of(true, 10, false, 20));

        assertThat(assertions.function("map_concat", "map(ARRAY[TRUE, FALSE], ARRAY[1, 2])", "map(ARRAY[TRUE, FALSE], ARRAY[10, 20])"))
                .hasType(mapType(BOOLEAN, INTEGER))
                .isEqualTo(ImmutableMap.of(true, 10, false, 20));

        assertThat(assertions.function("map_concat", "map(ARRAY[TRUE, FALSE], ARRAY[1, 2])", "map(ARRAY[TRUE], ARRAY[10])"))
                .hasType(mapType(BOOLEAN, INTEGER))
                .isEqualTo(ImmutableMap.of(true, 10, false, 2));

        // <VARCHAR, INTEGER> Tests
        assertThat(assertions.function("map_concat", "map(ARRAY['1', '2', '3'], ARRAY[1, 2, 3])", "map(ARRAY['1', '2', '3', '4'], ARRAY[10, 20, 30, 40])"))
                .hasType(mapType(createVarcharType(1), INTEGER))
                .isEqualTo(ImmutableMap.of("1", 10, "2", 20, "3", 30, "4", 40));

        assertThat(assertions.function("map_concat", "map(ARRAY['1', '2', '3', '4'], ARRAY[1, 2, 3, 4])", "map(ARRAY['1', '2', '3', '4'], ARRAY[10, 20, 30, 40])"))
                .hasType(mapType(createVarcharType(1), INTEGER))
                .isEqualTo(ImmutableMap.of("1", 10, "2", 20, "3", 30, "4", 40));

        assertThat(assertions.function("map_concat", "map(ARRAY['1', '2', '3', '4'], ARRAY[1, 2, 3, 4])", "map(ARRAY['1', '2', '3'], ARRAY[10, 20, 30])"))
                .hasType(mapType(createVarcharType(1), INTEGER))
                .isEqualTo(ImmutableMap.of("1", 10, "2", 20, "3", 30, "4", 4));

        // <BIGINT, ARRAY<DOUBLE>> Tests
        assertThat(assertions.function("map_concat", "map(ARRAY[1, 2, 3], ARRAY[ARRAY[1.0E0], ARRAY[2.0E0], ARRAY[3.0E0]])", "map(ARRAY[1, 2, 3, 4], ARRAY[ARRAY[10.0E0], ARRAY[20.0E0], ARRAY[30.0E0], ARRAY[40.0E0]])"))
                .hasType(mapType(INTEGER, new ArrayType(DOUBLE)))
                .isEqualTo(ImmutableMap.of(1, ImmutableList.of(10.0), 2, ImmutableList.of(20.0), 3, ImmutableList.of(30.0), 4, ImmutableList.of(40.0)));

        assertThat(assertions.function("map_concat", "map(ARRAY[1, 2, 3, 4], ARRAY[ARRAY[1.0E0], ARRAY[2.0E0], ARRAY[3.0E0], ARRAY[4.0E0]])", "map(ARRAY[1, 2, 3, 4], ARRAY[ARRAY[10.0E0], ARRAY[20.0E0], ARRAY[30.0E0], ARRAY[40.0E0]])"))
                .hasType(mapType(INTEGER, new ArrayType(DOUBLE)))
                .isEqualTo(ImmutableMap.of(1, ImmutableList.of(10.0), 2, ImmutableList.of(20.0), 3, ImmutableList.of(30.0), 4, ImmutableList.of(40.0)));

        assertThat(assertions.function("map_concat", "map(ARRAY[1, 2, 3, 4], ARRAY[ARRAY[1.0E0], ARRAY[2.0E0], ARRAY[3.0E0], ARRAY[4.0E0]])", "map(ARRAY[1, 2, 3], ARRAY[ARRAY[10.0E0], ARRAY[20.0E0], ARRAY[30.0E0]])"))
                .hasType(mapType(INTEGER, new ArrayType(DOUBLE)))
                .isEqualTo(ImmutableMap.of(1, ImmutableList.of(10.0), 2, ImmutableList.of(20.0), 3, ImmutableList.of(30.0), 4, ImmutableList.of(4.0)));

        // <ARRAY<DOUBLE>, VARCHAR> Tests
        assertThat(assertions.function("map_concat", "map(ARRAY[ARRAY[1.0E0], ARRAY[2.0E0], ARRAY[3.0E0]], ARRAY['1', '2', '3'])", "map(ARRAY[ARRAY[1.0E0], ARRAY[2.0E0], ARRAY[3.0E0], ARRAY[4.0E0]], ARRAY['10', '20', '30', '40'])"))
                .hasType(mapType(new ArrayType(DOUBLE), createVarcharType(2)))
                .isEqualTo(ImmutableMap.of(ImmutableList.of(1.0), "10", ImmutableList.of(2.0), "20", ImmutableList.of(3.0), "30", ImmutableList.of(4.0), "40"));

        assertThat(assertions.function("map_concat", "map(ARRAY[ARRAY[1.0E0], ARRAY[2.0E0], ARRAY[3.0E0]], ARRAY['1', '2', '3'])", "map(ARRAY[ARRAY[1.0E0], ARRAY[2.0E0], ARRAY[3.0E0], ARRAY[4.0E0]], ARRAY['10', '20', '30', '40'])"))
                .hasType(mapType(new ArrayType(DOUBLE), createVarcharType(2)))
                .isEqualTo(ImmutableMap.of(ImmutableList.of(1.0), "10", ImmutableList.of(2.0), "20", ImmutableList.of(3.0), "30", ImmutableList.of(4.0), "40"));

        assertThat(assertions.function("map_concat", "map(ARRAY[ARRAY[1.0E0], ARRAY[2.0E0], ARRAY[3.0E0], ARRAY[4.0E0]], ARRAY['1', '2', '3', '4'])", "map(ARRAY[ARRAY[1.0E0], ARRAY[2.0E0], ARRAY[3.0E0]], ARRAY['10', '20', '30'])"))
                .hasType(mapType(new ArrayType(DOUBLE), createVarcharType(2)))
                .isEqualTo(ImmutableMap.of(ImmutableList.of(1.0), "10", ImmutableList.of(2.0), "20", ImmutableList.of(3.0), "30", ImmutableList.of(4.0), "4"));

        // Tests for concatenating multiple maps
        assertThat(assertions.function("map_concat", "MAP(ARRAY[1], ARRAY[-1])", "NULL", "MAP(ARRAY[3], ARRAY[-3])"))
                .isNull(mapType(INTEGER, INTEGER));

        assertThat(assertions.function("map_concat", "MAP(ARRAY[1], ARRAY[-1])", "MAP(ARRAY[2], ARRAY[-2])", "MAP(ARRAY[3], ARRAY[-3])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(1, -1, 2, -2, 3, -3));

        assertThat(assertions.function("map_concat", "map(ARRAY[1], ARRAY[-1])", "map(ARRAY[1], ARRAY[-2])", "MAP(ARRAY[1], ARRAY[-3])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(1, -3));

        assertThat(assertions.function("map_concat", "map(ARRAY[1], ARRAY[-1])", "map(ARRAY[], ARRAY[])", "MAP(ARRAY[3], ARRAY[-3])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(1, -1, 3, -3));

        assertThat(assertions.function("map_concat", "map(ARRAY[], ARRAY[])", "map(ARRAY['a_string'], ARRAY['b_string'])", "cast(MAP(ARRAY[], ARRAY[]) AS MAP(VARCHAR, VARCHAR))"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("a_string", "b_string"));

        assertThat(assertions.function("map_concat", "map(ARRAY[], ARRAY[])", "map(ARRAY[], ARRAY[])", "MAP(ARRAY[], ARRAY[])"))
                .hasType(mapType(UNKNOWN, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.function("map_concat", "map()", "map()", "map()"))
                .hasType(mapType(UNKNOWN, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.function("map_concat", "map(ARRAY[1], ARRAY[-1])", "map()", "map(ARRAY[3], ARRAY[-3])"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(1, -1, 3, -3));

        assertThat(assertions.function("map_concat", "map(ARRAY[TRUE], ARRAY[1])", "map(ARRAY[TRUE, FALSE], ARRAY[10, 20])", "map(ARRAY[FALSE], ARRAY[0])"))
                .hasType(mapType(BOOLEAN, INTEGER))
                .isEqualTo(ImmutableMap.of(true, 10, false, 0));

        // <DECIMAL, DECIMAL>
        assertThat(assertions.function("map_concat", "map(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3])", "map(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.1, 3.2])"))
                .hasType(mapType(createDecimalType(29, 14), createDecimalType(2, 1)))
                .isEqualTo(ImmutableMap.of(
                        decimal("000000000000001.00000000000000", createDecimalType(29, 14)),
                        decimal("2.1", createDecimalType(2, 1)),
                        decimal("383838383838383.12324234234234", createDecimalType(29, 14)),
                        decimal("3.2", createDecimalType(2, 1))));

        assertThat(assertions.function("map_concat", "map(ARRAY[1.0], ARRAY[2.2])", "map(ARRAY[5.1], ARRAY[3.2])"))
                .hasType(mapType(createDecimalType(2, 1), createDecimalType(2, 1)))
                .isEqualTo(ImmutableMap.of(
                        decimal("1.0", createDecimalType(2, 1)),
                        decimal("2.2", createDecimalType(2, 1)),
                        decimal("5.1", createDecimalType(2, 1)),
                        decimal("3.2", createDecimalType(2, 1))));

        // Decimal with type only coercion
        assertThat(assertions.function("map_concat", "map(ARRAY[1.0], ARRAY[2.2])", "map(ARRAY[5.1], ARRAY[33.2])"))
                .hasType(mapType(createDecimalType(2, 1), createDecimalType(3, 1)))
                .isEqualTo(ImmutableMap.of(
                        decimal("1.0", createDecimalType(2, 1)),
                        decimal("2.2", createDecimalType(3, 1)),
                        decimal("5.1", createDecimalType(2, 1)),
                        decimal("33.2", createDecimalType(3, 1))));

        assertThat(assertions.function("map_concat", "map(ARRAY[1.0], ARRAY[2.2])", "map(ARRAY[55.1], ARRAY[33.2])"))
                .hasType(mapType(createDecimalType(3, 1), createDecimalType(3, 1)))
                .isEqualTo(ImmutableMap.of(
                        decimal("01.0", createDecimalType(3, 1)),
                        decimal("2.2", createDecimalType(3, 1)),
                        decimal("55.1", createDecimalType(3, 1)),
                        decimal("33.2", createDecimalType(3, 1))));

        assertThat(assertions.function("map_concat", "map(ARRAY[1.0], ARRAY[2.2])", "map(ARRAY[5.1], ARRAY[33.22])"))
                .hasType(mapType(createDecimalType(2, 1), createDecimalType(4, 2)))
                .isEqualTo(ImmutableMap.of(
                        decimal("5.1", createDecimalType(2, 1)),
                        decimal("33.22", createDecimalType(4, 2)),
                        decimal("1.0", createDecimalType(2, 1)),
                        decimal("2.20", createDecimalType(4, 2))));

        assertThat(assertions.function("map_concat", "map(ARRAY[1.0], ARRAY[2.2])", "map(ARRAY[5.1], ARRAY[00000000000000002.2])"))
                .hasType(mapType(createDecimalType(2, 1), createDecimalType(2, 1)))
                .isEqualTo(ImmutableMap.of(
                        decimal("1.0", createDecimalType(2, 1)),
                        decimal("2.2", createDecimalType(2, 1)),
                        decimal("5.1", createDecimalType(2, 1)),
                        decimal("2.2", createDecimalType(2, 1))));

        // Compare keys with IS DISTINCT semantics
        assertThat(assertions.function("map_concat", "map(ARRAY[NaN()], ARRAY[1])", "map(ARRAY[NaN()], ARRAY[2])", "map(ARRAY[NaN()], ARRAY[3])"))
                .hasType(mapType(DOUBLE, INTEGER))
                .isEqualTo(ImmutableMap.of(Double.NaN, 3));
    }

    @Test
    public void testMapToMapCast()
    {
        assertThat(assertions.expression("cast(a as MAP(varchar, MAP(bigint,bigint)))")
                .binding("a", "MAP(ARRAY[1], ARRAY[MAP(ARRAY[1.0E0], ARRAY[false])])"))
                .hasType(mapType(VARCHAR, mapType(BIGINT, BIGINT)))
                .isEqualTo(ImmutableMap.of("1", ImmutableMap.of(1L, 0L)));

        assertThat(assertions.expression("cast(a as MAP(varchar,bigint))")
                .binding("a", "MAP(ARRAY['1', '100'], ARRAY[true, false])"))
                .hasType(mapType(VARCHAR, BIGINT))
                .isEqualTo(ImmutableMap.of("1", 1L, "100", 0L));

        assertThat(assertions.expression("cast(a as MAP(bigint, boolean))")
                .binding("a", "MAP(ARRAY[1,2], ARRAY[1,2])"))
                .hasType(mapType(BIGINT, BOOLEAN))
                .isEqualTo(ImmutableMap.of(1L, true, 2L, true));

        assertThat(assertions.expression("cast(a as MAP(bigint, ARRAY(boolean)))")
                .binding("a", "MAP(ARRAY[1,2], ARRAY[array[1],array[2]])"))
                .hasType(mapType(BIGINT, new ArrayType(BOOLEAN)))
                .isEqualTo(ImmutableMap.of(1L, ImmutableList.of(true), 2L, ImmutableList.of(true)));

        assertThat(assertions.expression("cast(a as MAP(varchar, MAP(bigint,bigint)))")
                .binding("a", "MAP(ARRAY[1], ARRAY[MAP(ARRAY[1.0E0], ARRAY[false])])"))
                .hasType(mapType(VARCHAR, mapType(BIGINT, BIGINT)))
                .isEqualTo(ImmutableMap.of("1", ImmutableMap.of(1L, 0L)));

        assertThat(assertions.expression("cast(a as MAP(bigint, varchar))")
                .binding("a", "MAP(ARRAY[1,2], ARRAY[DATE '2016-01-02', DATE '2016-02-03'])"))
                .hasType(mapType(BIGINT, VARCHAR))
                .isEqualTo(ImmutableMap.of(1L, "2016-01-02", 2L, "2016-02-03"));

        assertThat(assertions.expression("cast(a as MAP(bigint, varchar))")
                .binding("a", "MAP(ARRAY[1,2], ARRAY[TIMESTAMP '2016-01-02 01:02:03', TIMESTAMP '2016-02-03 03:04:05'])"))
                .hasType(mapType(BIGINT, VARCHAR))
                .isEqualTo(ImmutableMap.of(1L, "2016-01-02 01:02:03", 2L, "2016-02-03 03:04:05"));

        assertThat(assertions.expression("cast(a as MAP(integer, real))")
                .binding("a", "MAP(ARRAY['123', '456'], ARRAY[1.23456E0, 2.34567E0])"))
                .hasType(mapType(INTEGER, REAL))
                .isEqualTo(ImmutableMap.of(123, 1.23456F, 456, 2.34567F));

        assertThat(assertions.expression("cast(a as MAP(smallint, decimal(6,5)))")
                .binding("a", "MAP(ARRAY['123', '456'], ARRAY[1.23456E0, 2.34567E0])"))
                .hasType(mapType(SMALLINT, createDecimalType(6, 5)))
                .isEqualTo(ImmutableMap.of(
                        (short) 123,
                        decimal("1.23456", createDecimalType(6, 5)),
                        (short) 456,
                        decimal("2.34567", createDecimalType(6, 5))));

        assertThat(assertions.expression("cast(a as MAP(bigint, bigint))")
                .binding("a", "MAP(ARRAY[json '1'], ARRAY[1])"))
                .hasType(mapType(BIGINT, BIGINT))
                .isEqualTo(ImmutableMap.of(1L, 1L));

        assertThat(assertions.expression("cast(a as MAP(bigint, bigint))")
                .binding("a", "MAP(ARRAY['1'], ARRAY[json '1'])"))
                .hasType(mapType(BIGINT, BIGINT))
                .isEqualTo(ImmutableMap.of(1L, 1L));

        // null values
        Map<Long, Double> expected = new HashMap<>();
        expected.put(0L, 1.0);
        expected.put(1L, null);
        expected.put(2L, null);
        expected.put(3L, 2.0);
        assertThat(assertions.expression("cast(a as MAP(BIGINT, DOUBLE))")
                .binding("a", "MAP(ARRAY[0, 1, 2, 3], ARRAY[1,NULL, NULL, 2])"))
                .hasType(mapType(BIGINT, DOUBLE))
                .isEqualTo(expected);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(boolean, bigint))")
                .binding("a", "MAP(ARRAY[1, 2], ARRAY[6, 9])").evaluate())
                .hasMessage("duplicate keys")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(bigint, bigint))")
                .binding("a", "MAP(ARRAY[json 'null'], ARRAY[1])").evaluate())
                .hasMessage("map key is null")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as MAP(double, integer))")
                .binding("a", "MAP(ARRAY['NaN', ' NaN '], ARRAY[1, 2])").evaluate())
                .hasMessage("duplicate keys")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testMapFromEntries()
    {
        assertThat(assertions.function("map_from_entries", "null"))
                .isNull(mapType(UNKNOWN, UNKNOWN));

        assertThat(assertions.function("map_from_entries", "ARRAY[]"))
                .hasType(mapType(UNKNOWN, UNKNOWN))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.function("map_from_entries", "CAST(ARRAY[] AS ARRAY(ROW(DOUBLE, BIGINT)))"))
                .hasType(mapType(DOUBLE, BIGINT))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.function("map_from_entries", "ARRAY[(1, 3)]"))
                .hasType(mapType(INTEGER, INTEGER))
                .isEqualTo(ImmutableMap.of(1, 3));

        assertThat(assertions.function("map_from_entries", "ARRAY[(1, 'x'), (2, 'y')]"))
                .hasType(mapType(INTEGER, createVarcharType(1)))
                .isEqualTo(ImmutableMap.of(1, "x", 2, "y"));

        assertThat(assertions.function("map_from_entries", "ARRAY[('x', 1.0E0), ('y', 2.0E0)]"))
                .hasType(mapType(createVarcharType(1), DOUBLE))
                .isEqualTo(ImmutableMap.of("x", 1.0, "y", 2.0));

        assertThat(assertions.function("map_from_entries", "ARRAY[('x', ARRAY[1, 2]), ('y', ARRAY[3, 4])]"))
                .hasType(mapType(createVarcharType(1), new ArrayType(INTEGER)))
                .isEqualTo(ImmutableMap.of("x", ImmutableList.of(1, 2), "y", ImmutableList.of(3, 4)));

        assertThat(assertions.function("map_from_entries", "ARRAY[(ARRAY[1, 2], 'x'), (ARRAY[3, 4], 'y')]"))
                .hasType(mapType(new ArrayType(INTEGER), createVarcharType(1)))
                .isEqualTo(ImmutableMap.of(ImmutableList.of(1, 2), "x", ImmutableList.of(3, 4), "y"));

        assertThat(assertions.function("map_from_entries", "ARRAY[('x', MAP(ARRAY[1], ARRAY[2])), ('y', MAP(ARRAY[3], ARRAY[4]))]"))
                .hasType(mapType(createVarcharType(1), mapType(INTEGER, INTEGER)))
                .isEqualTo(ImmutableMap.of("x", ImmutableMap.of(1, 2), "y", ImmutableMap.of(3, 4)));

        assertThat(assertions.function("map_from_entries", "ARRAY[(MAP(ARRAY[1], ARRAY[2]), 'x'), (MAP(ARRAY[3], ARRAY[4]), 'y')]"))
                .hasType(mapType(mapType(INTEGER, INTEGER), createVarcharType(1)))
                .isEqualTo(ImmutableMap.of(ImmutableMap.of(1, 2), "x", ImmutableMap.of(3, 4), "y"));

        // null values
        Map<String, Integer> expectedNullValueMap = new HashMap<>();
        expectedNullValueMap.put("x", null);
        expectedNullValueMap.put("y", null);
        assertThat(assertions.function("map_from_entries", "ARRAY[('x', null), ('y', null)]"))
                .hasType(mapType(createVarcharType(1), UNKNOWN))
                .isEqualTo(expectedNullValueMap);

        // invalid invocation
        assertTrinoExceptionThrownBy(assertions.function("map_from_entries", "ARRAY[('a', 1), ('a', 2)]")::evaluate)
                .hasMessage("Duplicate map keys (a) are not allowed");

        assertTrinoExceptionThrownBy(assertions.function("map_from_entries", "ARRAY[(1, 1), (1, 2)]")::evaluate)
                .hasMessage("Duplicate map keys (1) are not allowed");

        assertTrinoExceptionThrownBy(assertions.function("map_from_entries", "ARRAY[(1.0, 1), (1.0, 2)]")::evaluate)
                .hasMessage("Duplicate map keys (1.0) are not allowed");

        assertTrinoExceptionThrownBy(assertions.function("map_from_entries", "ARRAY[(ARRAY[1, 2], 1), (ARRAY[1, 2], 2)]")::evaluate)
                .hasMessage("Duplicate map keys ([1, 2]) are not allowed");

        assertTrinoExceptionThrownBy(assertions.function("map_from_entries", "ARRAY[(MAP(ARRAY[1], ARRAY[2]), 1), (MAP(ARRAY[1], ARRAY[2]), 2)]")::evaluate)
                .hasMessage("Duplicate map keys ({1=2}) are not allowed");

        assertTrinoExceptionThrownBy(assertions.function("map_from_entries", "ARRAY[(NaN(), 1), (NaN(), 2)]")::evaluate)
                .hasMessage("Duplicate map keys (NaN) are not allowed");

        assertTrinoExceptionThrownBy(assertions.function("map_from_entries", "ARRAY[(null, 1), (null, 2)]")::evaluate)
                .hasMessage("map key cannot be null");

        assertTrinoExceptionThrownBy(assertions.function("map_from_entries", "ARRAY[null]")::evaluate)
                .hasMessage("map entry cannot be null");

        assertTrinoExceptionThrownBy(assertions.function("map_from_entries", "ARRAY[(1, 2), null]")::evaluate)
                .hasMessage("map entry cannot be null");
    }

    @Test
    public void testMultimapFromEntries()
    {
        assertThat(assertions.function("multimap_from_entries", "null"))
                .isNull(mapType(UNKNOWN, new ArrayType(UNKNOWN)));

        assertThat(assertions.function("multimap_from_entries", "ARRAY[]"))
                .hasType(mapType(UNKNOWN, new ArrayType(UNKNOWN)))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.function("multimap_from_entries", "CAST(ARRAY[] AS ARRAY(ROW(DOUBLE, BIGINT)))"))
                .hasType(mapType(DOUBLE, new ArrayType(BIGINT)))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.function("multimap_from_entries", "ARRAY[(1, 3), (2, 4), (1, 6), (1, 8), (2, 10)]"))
                .hasType(mapType(INTEGER, new ArrayType(INTEGER)))
                .isEqualTo(ImmutableMap.of(
                        1, ImmutableList.of(3, 6, 8),
                        2, ImmutableList.of(4, 10)));

        assertThat(assertions.function("multimap_from_entries", "ARRAY[(1, 'x'), (2, 'y'), (1, 'a'), (3, 'b'), (2, 'c'), (3, null)]"))
                .hasType(mapType(INTEGER, new ArrayType(createVarcharType(1))))
                .isEqualTo(ImmutableMap.of(
                        1, ImmutableList.of("x", "a"),
                        2, ImmutableList.of("y", "c"),
                        3, asList("b", null)));

        assertThat(assertions.function("multimap_from_entries", "ARRAY[('x', 1.0E0), ('y', 2.0E0), ('z', null), ('x', 1.5E0), ('y', 2.5E0)]"))
                .hasType(mapType(createVarcharType(1), new ArrayType(DOUBLE)))
                .isEqualTo(ImmutableMap.of(
                        "x", ImmutableList.of(1.0, 1.5),
                        "y", ImmutableList.of(2.0, 2.5),
                        "z", singletonList(null)));

        assertThat(assertions.function("multimap_from_entries", "ARRAY[(NaN(), 1), (NaN(), 2)]"))
                .hasType(mapType(DOUBLE, new ArrayType(INTEGER)))
                .isEqualTo(ImmutableMap.of(Double.NaN, ImmutableList.of(1, 2)));

        // invalid invocation
        assertTrinoExceptionThrownBy(assertions.function("multimap_from_entries", "ARRAY[(null, 1), (null, 2)]")::evaluate)
                .hasMessage("map key cannot be null");

        assertTrinoExceptionThrownBy(assertions.function("multimap_from_entries", "ARRAY[null]")::evaluate)
                .hasMessage("map entry cannot be null");

        assertTrinoExceptionThrownBy(assertions.function("multimap_from_entries", "ARRAY[(1, 2), null]")::evaluate)
                .hasMessage("map entry cannot be null");
    }

    @Test
    public void testMapEntries()
    {
        assertThat(assertions.function("map_entries", "null"))
                .isNull(entryType(UNKNOWN, UNKNOWN));

        assertThat(assertions.function("map_entries", "MAP(ARRAY[], null)"))
                .isNull(entryType(UNKNOWN, UNKNOWN));

        assertThat(assertions.function("map_entries", "MAP(null, ARRAY[])"))
                .isNull(entryType(UNKNOWN, UNKNOWN));

        assertThat(assertions.function("map_entries", "MAP(ARRAY[1, 2, 3], null)"))
                .isNull(entryType(INTEGER, UNKNOWN));

        assertThat(assertions.function("map_entries", "MAP(null, ARRAY[1, 2, 3])"))
                .isNull(entryType(UNKNOWN, INTEGER));

        assertThat(assertions.function("map_entries", "MAP(ARRAY[], ARRAY[])"))
                .hasType(entryType(UNKNOWN, UNKNOWN))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("map_entries", "MAP(ARRAY[1], ARRAY['x'])"))
                .hasType(entryType(INTEGER, createVarcharType(1)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1, "x")));

        assertThat(assertions.function("map_entries", "MAP(ARRAY[1, 2], ARRAY['x', 'y'])"))
                .hasType(entryType(INTEGER, createVarcharType(1)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1, "x"), ImmutableList.of(2, "y")));

        assertThat(assertions.function("map_entries", "MAP(ARRAY['x', 'y'], ARRAY[ARRAY[1, 2], ARRAY[3, 4]])"))
                .hasType(entryType(createVarcharType(1), new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of("x", ImmutableList.of(1, 2)), ImmutableList.of("y", ImmutableList.of(3, 4))));

        assertThat(assertions.function("map_entries", "MAP(ARRAY[ARRAY[1.0E0, 2.0E0], ARRAY[3.0E0, 4.0E0]], ARRAY[5.0E0, 6.0E0])"))
                .hasType(entryType(new ArrayType(DOUBLE), DOUBLE))
                .isEqualTo(ImmutableList.of(ImmutableList.of(ImmutableList.of(1.0, 2.0), 5.0), ImmutableList.of(ImmutableList.of(3.0, 4.0), 6.0)));

        assertThat(assertions.function("map_entries", "MAP(ARRAY['x', 'y'], ARRAY[MAP(ARRAY[1], ARRAY[2]), MAP(ARRAY[3], ARRAY[4])])"))
                .hasType(entryType(createVarcharType(1), mapType(INTEGER, INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of("x", ImmutableMap.of(1, 2)), ImmutableList.of("y", ImmutableMap.of(3, 4))));

        assertThat(assertions.function("map_entries", "MAP(ARRAY[MAP(ARRAY[1], ARRAY[2]), MAP(ARRAY[3], ARRAY[4])], ARRAY['x', 'y'])"))
                .hasType(entryType(mapType(INTEGER, INTEGER), createVarcharType(1)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(ImmutableMap.of(1, 2), "x"), ImmutableList.of(ImmutableMap.of(3, 4), "y")));

        // null values
        List<Object> expectedEntries = ImmutableList.of(asList("x", null), asList("y", null));
        assertThat(assertions.function("map_entries", "MAP(ARRAY['x', 'y'], ARRAY[null, null])"))
                .hasType(entryType(createVarcharType(1), UNKNOWN))
                .isEqualTo(expectedEntries);
    }

    @Test
    public void testEntryMappings()
    {
        assertThat(assertions.function("map_from_entries", "map_entries(MAP(ARRAY[1, 2, 3], ARRAY['x', 'y', 'z']))"))
                .hasType(mapType(INTEGER, createVarcharType(1)))
                .isEqualTo(ImmutableMap.of(1, "x", 2, "y", 3, "z"));

        assertThat(assertions.function("map_entries", "map_from_entries(ARRAY[(1, 'x'), (2, 'y'), (3, 'z')])"))
                .hasType(entryType(INTEGER, createVarcharType(1)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1, "x"), ImmutableList.of(2, "y"), ImmutableList.of(3, "z")));
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as map(bigint, bigint))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "map(array[1,2], array[3,4])"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "map(array[1,2], array[1.0,2.0])"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "map(array[1,2], array[null, 3])"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "map(array[1,2], array[null, 3.0])"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "map(array[1,2], array[array[11], array[22]])"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "map(array[1,2], array[array[11], array[null]])"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "map(array[1,2], array[array[11], array[22,null]])"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "map(array[1,2], array[array[11, null], array[22,null]])"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "map(array[1,2], array[array[null], array[null]])"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "map(array[array[1], array[2]], array[array[11], array[22]])"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "map(array[row(1), row(2)], array[array[11], array[22]])"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "map(array[row(1), row(2)], array[array[11], array[22, null]])"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "map(array[1E0, 2E0], array[11E0, null])"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "map(array[1E0, 2E0], array[11E0, 12E0])"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "map(array['a', 'b'], array['c', null])"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "map(array['a', 'b'], array['c', 'd'])"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "map(array[true,false], array[false,true])"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "map(array[true,false], array[false,null])"))
                .isEqualTo(true);
    }

    @Test
    public void testMapHashOperator()
    {
        assertThat(assertions.operator(HASH_CODE, "MAP(ARRAY[1], ARRAY[2])"))
                .isEqualTo(-6461599496541202183L);

        assertThat(assertions.operator(HASH_CODE, "MAP(ARRAY[1, 2147483647], ARRAY[2147483647, 2])"))
                .isEqualTo(3917017315680083193L);

        assertThat(assertions.operator(HASH_CODE, "MAP(ARRAY[8589934592], ARRAY[2])"))
                .isEqualTo(5432374638989305986L);

        assertThat(assertions.operator(HASH_CODE, "MAP(ARRAY[true], ARRAY[false])"))
                .isEqualTo(-6061667139012558290L);

        assertThat(assertions.operator(HASH_CODE, "MAP(ARRAY['123'], ARRAY['456'])"))
                .isEqualTo(370638291674634983L);

        assertThat(assertions.operator(XX_HASH_64, "MAP(ARRAY[1], ARRAY[2])"))
                .isEqualTo(8497999304769451045L);

        assertThat(assertions.operator(XX_HASH_64, "MAP(ARRAY[1, 2147483647], ARRAY[2147483647, 2])"))
                .isEqualTo(-8786792975300342875L);

        assertThat(assertions.operator(XX_HASH_64, "MAP(ARRAY[8589934592], ARRAY[2])"))
                .isEqualTo(3099409451015666579L);

        assertThat(assertions.operator(XX_HASH_64, "MAP(ARRAY[true], ARRAY[false])"))
                .isEqualTo(-6061667139012558290L);

        assertThat(assertions.operator(XX_HASH_64, "MAP(ARRAY['123'], ARRAY['456'])"))
                .isEqualTo(370638291674634983L);
    }

    private static Type entryType(Type keyType, Type valueType)
    {
        return new ArrayType(RowType.anonymous(ImmutableList.of(keyType, valueType)));
    }
}
