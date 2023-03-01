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
import com.google.common.primitives.Ints;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collections;

import static io.trino.block.BlockSerdeUtil.writeBlock;
import static io.trino.operator.aggregation.TypedSet.MAX_FUNCTION_MEMORY;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static io.trino.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.MoreMaps.asMap;
import static io.trino.util.StructuralTestUtil.arrayBlockOf;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;

@TestInstance(PER_CLASS)
public class TestArrayOperators
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();

        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestArrayOperators.class)
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
    public void testStackRepresentation()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        Block actualBlock = arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 1L, 2L), arrayBlockOf(BIGINT, 3L));
        DynamicSliceOutput actualSliceOutput = new DynamicSliceOutput(100);
        writeBlock(((LocalQueryRunner) assertions.getQueryRunner()).getPlannerContext().getBlockEncodingSerde(), actualSliceOutput, actualBlock);

        BlockBuilder expectedBlockBuilder = arrayType.createBlockBuilder(null, 3);
        arrayType.writeObject(expectedBlockBuilder, BIGINT.createBlockBuilder(null, 2).writeLong(1).writeLong(2).build());
        arrayType.writeObject(expectedBlockBuilder, BIGINT.createBlockBuilder(null, 1).writeLong(3).build());
        Block expectedBlock = expectedBlockBuilder.build();
        DynamicSliceOutput expectedSliceOutput = new DynamicSliceOutput(100);
        writeBlock(((LocalQueryRunner) assertions.getQueryRunner()).getPlannerContext().getBlockEncodingSerde(), expectedSliceOutput, expectedBlock);

        assertEquals(actualSliceOutput.slice(), expectedSliceOutput.slice());
    }

    @Test
    public void testTypeConstructor()
    {
        assertThat(assertions.expression("ARRAY[a]")
                .binding("a", "7"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(7));

        assertThat(assertions.expression("ARRAY[a, b]")
                .binding("a", "12.34E0")
                .binding("b", "56.78E0"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(12.34, 56.78));
    }

    @Test
    public void testArrayToArrayCast()
    {
        assertThat(assertions.expression("CAST(a AS array(INTEGER))")
                .binding("a", "array[null]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList((Integer) null));

        assertThat(assertions.expression("CAST(a AS array(INTEGER))")
                .binding("a", "array[1, 2, 3]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3));

        assertThat(assertions.expression("CAST(a AS array(INTEGER))")
                .binding("a", "array[1, null, 3]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(1, null, 3));

        assertThat(assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "array[null]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(asList((Long) null));

        assertThat(assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "array[1, 2, 3]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(1L, 2L, 3L));

        assertThat(assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "array[1, null, 3]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(asList(1L, null, 3L));

        assertThat(assertions.expression("CAST(a AS array(DOUBLE))")
                .binding("a", "array[1, 2, 3]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0, 2.0, 3.0));

        assertThat(assertions.expression("CAST(a AS array(DOUBLE))")
                .binding("a", "array[1, null, 3]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(asList(1.0, null, 3.0));

        assertThat(assertions.expression("CAST(a AS array(VARCHAR))")
                .binding("a", "array['1', '2']"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("1", "2"));

        assertThat(assertions.expression("CAST(a AS array(DOUBLE))")
                .binding("a", "array['1', '2']"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0, 2.0));

        assertThat(assertions.expression("CAST(a AS array(BOOLEAN))")
                .binding("a", "array[true, false]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));

        assertThat(assertions.expression("CAST(a AS array(VARCHAR))")
                .binding("a", "array[true, false]"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("true", "false"));

        assertThat(assertions.expression("CAST(a AS array(BOOLEAN))")
                .binding("a", "array[1, 0]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));

        assertThat(assertions.expression("CAST(a AS array(ARRAY(DOUBLE)))")
                .binding("a", "array[ARRAY[1], ARRAY[2, 3]]"))
                .hasType(new ArrayType(new ArrayType(DOUBLE)))
                .isEqualTo(asList(asList(1.0), asList(2.0, 3.0)));

        assertThat(assertions.expression("CAST(a AS array(ARRAY(DOUBLE)))")
                .binding("a", "array[ARRAY[1.0], ARRAY[2.0, 3.0]]"))
                .hasType(new ArrayType(new ArrayType(DOUBLE)))
                .isEqualTo(asList(asList(1.0), asList(2.0, 3.0)));

        assertThat(assertions.expression("CAST(a AS array(ARRAY(DECIMAL(2,1))))")
                .binding("a", "array[ARRAY[1.0E0], ARRAY[2.0E0, 3.0E0]]"))
                .hasType(new ArrayType(new ArrayType(createDecimalType(2, 1))))
                .isEqualTo(asList(
                        asList(decimal("1.0", createDecimalType(2, 1))),
                        asList(
                                decimal("2.0", createDecimalType(2, 1)),
                                decimal("3.0", createDecimalType(2, 1)))));

        assertThat(assertions.expression("CAST(a AS array(ARRAY(DECIMAL(20,10))))")
                .binding("a", "array[ARRAY[1.0E0], ARRAY[2.0E0, 3.0E0]]"))
                .hasType(new ArrayType(new ArrayType(createDecimalType(20, 10))))
                .isEqualTo(asList(
                        asList(decimal("0000000001.0000000000", createDecimalType(20, 10))),
                        asList(
                                decimal("0000000002.0000000000", createDecimalType(20, 10)),
                                decimal("0000000003.0000000000", createDecimalType(20, 10)))));

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(TIMESTAMP))")
                .binding("a", "array[1, null, 3]")
                .evaluate())
                .hasErrorCode(TYPE_MISMATCH);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(ARRAY(TIMESTAMP)))")
                .binding("a", "array[1, null, 3]")
                .evaluate())
                .hasErrorCode(TYPE_MISMATCH);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "array['puppies', 'kittens']")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testArraySize()
    {
        int size = toIntExact(MAX_FUNCTION_MEMORY.toBytes() + 1);
        assertTrinoExceptionThrownBy(() -> assertions.expression("array_distinct(ARRAY['" +
                                                                 "x".repeat(size) + "', '" +
                                                                 "y".repeat(size) + "', '" +
                                                                 "z".repeat(size) +
                                                                 "'])").evaluate())
                .hasErrorCode(EXCEEDED_FUNCTION_MEMORY_LIMIT);
    }

    @Test
    public void testArrayToJson()
    {
        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "CAST(null as ARRAY(BIGINT))"))
                .isNull(JSON);

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[]"))
                .hasType(JSON)
                .isEqualTo("[]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[null, null]"))
                .hasType(JSON)
                .isEqualTo("[null,null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[true, false, null]"))
                .hasType(JSON)
                .isEqualTo("[true,false,null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "CAST(ARRAY[1, 2, null] AS ARRAY(TINYINT))"))
                .hasType(JSON)
                .isEqualTo("[1,2,null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "CAST(ARRAY[12345, -12345, null] AS ARRAY(SMALLINT))"))
                .hasType(JSON)
                .isEqualTo("[12345,-12345,null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "CAST(ARRAY[123456789, -123456789, null] AS ARRAY(INTEGER))"))
                .hasType(JSON)
                .isEqualTo("[123456789,-123456789,null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "CAST(ARRAY[1234567890123456789, -1234567890123456789, null] AS ARRAY(BIGINT))"))
                .hasType(JSON)
                .isEqualTo("[1234567890123456789,-1234567890123456789,null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "CAST(ARRAY[3.14E0, nan(), infinity(), -infinity(), null] AS ARRAY(REAL))"))
                .hasType(JSON)
                .isEqualTo("[3.14,\"NaN\",\"Infinity\",\"-Infinity\",null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[3.14E0, 1e-323, 1e308, nan(), infinity(), -infinity(), null]"))
                .hasType(JSON)
                .isEqualTo(Runtime.version().feature() >= 19
                        ? "[3.14,9.9E-324,1.0E308,\"NaN\",\"Infinity\",\"-Infinity\",null]"
                        : "[3.14,1.0E-323,1.0E308,\"NaN\",\"Infinity\",\"-Infinity\",null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[DECIMAL '3.14', null]"))
                .hasType(JSON)
                .isEqualTo("[3.14,null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[DECIMAL '12345678901234567890.123456789012345678', null]"))
                .hasType(JSON)
                .isEqualTo("[12345678901234567890.123456789012345678,null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY['a', 'bb', null]"))
                .hasType(JSON)
                .isEqualTo("[\"a\",\"bb\",null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', null]"))
                .hasType(JSON)
                .isEqualTo("[123,3.14,false,\"abc\",[1,\"a\",null],{\"a\":1,\"b\":\"str\",\"c\":null},null,null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[TIMESTAMP '1970-01-01 00:00:01', null]"))
                .hasType(JSON)
                .isEqualTo(format("[\"%s\",null]", sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0)));

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[DATE '2001-08-22', DATE '2001-08-23', null]"))
                .hasType(JSON)
                .isEqualTo("[\"2001-08-22\",\"2001-08-23\",null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], null]"))
                .hasType(JSON)
                .isEqualTo("[[1,2],[3,null],[],[null,null],null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), null]"))
                .hasType(JSON)
                .isEqualTo("[{\"a\":1,\"b\":2},{\"none\":null,\"three\":3},{},{\"h1\":null,\"h2\":null},null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[ROW(1, 2), ROW(3, CAST(null as INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null]"))
                .hasType(JSON)
                .isEqualTo("[{\"\":1,\"\":2},{\"\":3,\"\":null},{\"\":null,\"\":null},null]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[12345.12345, 12345.12345, 3.0]"))
                .hasType(JSON)
                .isEqualTo("[12345.12345,12345.12345,3.00000]");

        assertThat(assertions.expression("CAST(a AS JSON)")
                .binding("a", "ARRAY[123456789012345678901234567890.87654321, 123456789012345678901234567890.12345678]"))
                .hasType(JSON)
                .isEqualTo("[123456789012345678901234567890.87654321,123456789012345678901234567890.12345678]");
    }

    @Test
    public void testJsonToArray()
    {
        // special values
        assertThat(assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "CAST(null AS JSON)"))
                .isNull(new ArrayType(BIGINT));

        assertThat(assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "JSON 'null'"))
                .isNull(new ArrayType(BIGINT));

        assertThat(assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "JSON '[]'"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "JSON '[null, null]'"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(Lists.newArrayList(null, null));

        // boolean
        assertThat(assertions.expression("CAST(a AS array(BOOLEAN))")
                .binding("a", "JSON '[true, false, 12, 0, 12.3, 0.0, \"true\", \"false\", null]'"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(asList(true, false, true, false, true, false, true, false, null));

        // tinyint, smallint, integer, bigint
        assertThat(assertions.expression("CAST(a AS array(TINYINT))")
                .binding("a", "JSON '[true, false, 12, 12.7, \"12\", null]'"))
                .hasType(new ArrayType(TINYINT))
                .isEqualTo(asList((byte) 1, (byte) 0, (byte) 12, (byte) 13, (byte) 12, null));

        assertThat(assertions.expression("CAST(a AS array(SMALLINT))")
                .binding("a", "JSON '[true, false, 12345, 12345.6, \"12345\", null]'"))
                .hasType(new ArrayType(SMALLINT))
                .isEqualTo(asList((short) 1, (short) 0, (short) 12345, (short) 12346, (short) 12345, null));

        assertThat(assertions.expression("CAST(a AS array(INTEGER))")
                .binding("a", "JSON '[true, false, 12345678, 12345678.9, \"12345678\", null]'"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(1, 0, 12345678, 12345679, 12345678, null));

        assertThat(assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "JSON '[true, false, 1234567891234567, 1234567891234567.8, \"1234567891234567\", null]'"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(asList(1L, 0L, 1234567891234567L, 1234567891234568L, 1234567891234567L, null));

        // real, double, decimal
        assertThat(assertions.expression("CAST(a AS array(REAL))")
                .binding("a", "JSON '[true, false, 12345, 12345.67, \"3.14\", \"NaN\", \"Infinity\", \"-Infinity\", null]'"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(asList(1.0f, 0.0f, 12345.0f, 12345.67f, 3.14f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, null));

        assertThat(assertions.expression("CAST(a AS array(DOUBLE))")
                .binding("a", "JSON '[true, false, 1234567890, 1234567890.1, \"3.14\", \"NaN\", \"Infinity\", \"-Infinity\", null]'"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(asList(1.0, 0.0, 1234567890.0, 1234567890.1, 3.14, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, null));

        assertThat(assertions.expression("CAST(a AS array(DECIMAL(10, 5)))")
                .binding("a", "JSON '[true, false, 128, 123.456, \"3.14\", null]'"))
                .hasType(new ArrayType(createDecimalType(10, 5)))
                .isEqualTo(asList(
                        decimal("1.00000", createDecimalType(10, 5)),
                        decimal("0.00000", createDecimalType(10, 5)),
                        decimal("128.00000", createDecimalType(10, 5)),
                        decimal("123.45600", createDecimalType(10, 5)),
                        decimal("3.14000", createDecimalType(10, 5)),
                        null));

        assertThat(assertions.expression("CAST(a AS array(DECIMAL(38, 8)))")
                .binding("a", "JSON '[true, false, 128, 12345678.12345678, \"3.14\", null]'"))
                .hasType(new ArrayType(createDecimalType(38, 8)))
                .isEqualTo(asList(
                        decimal("1.00000000", createDecimalType(38, 8)),
                        decimal("0.00000000", createDecimalType(38, 8)),
                        decimal("128.00000000", createDecimalType(38, 8)),
                        decimal("12345678.12345678", createDecimalType(38, 8)),
                        decimal("3.14000000", createDecimalType(38, 8)),
                        null));

        // varchar, json
        assertThat(assertions.expression("CAST(a AS array(VARCHAR))")
                .binding("a", "JSON '[true, false, 12, 12.3, \"puppies\", \"kittens\", \"null\", \"\", null]'"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(asList("true", "false", "12", "1.23E1", "puppies", "kittens", "null", "", null));

        assertThat(assertions.expression("CAST(a AS array(JSON))")
                .binding("a", "JSON '[5, 3.14, [1, 2, 3], \"e\", {\"a\": \"b\"}, null, \"null\", [null]]'"))
                .hasType(new ArrayType(JSON))
                .isEqualTo(ImmutableList.of("5", "3.14", "[1,2,3]", "\"e\"", "{\"a\":\"b\"}", "null", "\"null\"", "[null]"));

        // nested array/map
        assertThat(assertions.expression("CAST(a AS array(ARRAY(BIGINT)))")
                .binding("a", "JSON '[[1, 2], [3, null], [], [null, null], null]'"))
                .hasType(new ArrayType(new ArrayType(BIGINT)))
                .isEqualTo(asList(asList(1L, 2L), asList(3L, null), emptyList(), asList(null, null), null));

        assertThat(assertions.expression("CAST(a AS ARRAY(MAP(VARCHAR, BIGINT)))")
                .binding("a", """
                        JSON '[
                            {"a": 1, "b": 2},
                            {"none": null, "three": 3},
                            {},
                            {"h1": null,"h2": null},
                            null
                        ]'
                        """))
                .hasType(new ArrayType(mapType(VARCHAR, BIGINT)))
                .isEqualTo(asList(
                        ImmutableMap.of("a", 1L, "b", 2L),
                        asMap(ImmutableList.of("none", "three"), asList(null, 3L)),
                        ImmutableMap.of(),
                        asMap(ImmutableList.of("h1", "h2"), asList(null, null)),
                        null));

        assertThat(assertions.expression("CAST(a AS ARRAY(ROW(k1 BIGINT, k2 VARCHAR)))")
                .binding("a", """
                        JSON '[
                            [1, "two"],
                            [3, null],
                            {"k1": 1, "k2": "two"},
                            {"k2": null, "k1": 3},
                            null
                        ]'
                        """))
                .hasType(new ArrayType(RowType.from(ImmutableList.of(
                        RowType.field("k1", BIGINT),
                        RowType.field("k2", VARCHAR)))))
                .isEqualTo(asList(
                        asList(1L, "two"),
                        asList(3L, null),
                        asList(1L, "two"),
                        asList(3L, null),
                        null));

        // invalid cast
        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "JSON '{\"a\": 1}'").evaluate())
                .hasMessage("Cannot cast to array(bigint). Expected a json array, but got {\n{\"a\":1}")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(ARRAY(BIGINT)))")
                .binding("a", "JSON '[1, 2, 3]'").evaluate())
                .hasMessage("Cannot cast to array(array(bigint)). Expected a json array, but got 1\n[1,2,3]")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "JSON '[1, {}]'").evaluate())
                .hasMessage("Cannot cast to array(bigint). Unexpected token when cast to bigint: {\n[1,{}]")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(ARRAY(BIGINT)))")
                .binding("a", "JSON '[[1], {}]'").evaluate())
                .hasMessage("Cannot cast to array(array(bigint)). Expected a json array, but got {\n[[1],{}]")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "unchecked_to_json('1, 2, 3')").evaluate())
                .hasMessage("Cannot cast to array(bigint).\n1, 2, 3")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "unchecked_to_json('[1] 2')").evaluate())
                .hasMessage("Cannot cast to array(bigint). Unexpected trailing token: 2\n[1] 2")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "unchecked_to_json('[1, 2, 3')").evaluate())
                .hasMessage("Cannot cast to array(bigint).\n[1, 2, 3")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(BIGINT))")
                .binding("a", "JSON '[\"a\", \"b\"]'").evaluate())
                .hasMessage("Cannot cast to array(bigint). Cannot cast 'a' to BIGINT\n[\"a\",\"b\"]")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(INTEGER))")
                .binding("a", "JSON '[1234567890123.456]'").evaluate())
                .hasMessage("Cannot cast to array(integer). Out of range for integer: 1.234567890123456E12\n[1.234567890123456E12]")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("CAST(a AS array(DECIMAL(10,5)))")
                .binding("a", "JSON '[1, 2.0, 3]'"))
                .hasType(new ArrayType(createDecimalType(10, 5)))
                .isEqualTo(ImmutableList.of(
                        decimal("1.00000", createDecimalType(10, 5)),
                        decimal("2.00000", createDecimalType(10, 5)),
                        decimal("3.00000", createDecimalType(10, 5))));

        assertThat(assertions.expression("CAST(a AS array(DECIMAL(10,5)))")
                .binding("a", "CAST(ARRAY[1, 2.0, 3] as JSON)"))
                .hasType(new ArrayType(createDecimalType(10, 5)))
                .isEqualTo(ImmutableList.of(
                        decimal("1.00000", createDecimalType(10, 5)),
                        decimal("2.00000", createDecimalType(10, 5)),
                        decimal("3.00000", createDecimalType(10, 5))));

        assertThat(assertions.expression("CAST(a AS array(DECIMAL(38,8)))")
                .binding("a", "CAST(ARRAY[123456789012345678901234567890.12345678, 1.2] as JSON)"))
                .hasType(new ArrayType(createDecimalType(38, 8)))
                .isEqualTo(ImmutableList.of(
                        decimal("123456789012345678901234567890.12345678", createDecimalType(38, 8)),
                        decimal("1.20000000", createDecimalType(38, 8))));

        assertThat(assertions.expression("CAST(a AS array(DECIMAL(7,2)))")
                .binding("a", "CAST(ARRAY[12345.87654] as JSON)"))
                .hasType(new ArrayType(createDecimalType(7, 2)))
                .isEqualTo(ImmutableList.of(decimal("12345.88", createDecimalType(7, 2))));

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS array(DECIMAL(6,2)))")
                .binding("a", "CAST(ARRAY[12345.12345] as JSON)").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testConstructor() // TODO
    {
        assertThat(assertions.expression("ARRAY[]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.expression("ARRAY[a]")
                .binding("a", "NULL"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(Lists.newArrayList((Object) null));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "1")
                .binding("b", "2")
                .binding("c", "3"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "1")
                .binding("b", "NULL")
                .binding("c", "3"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(Lists.newArrayList(1, null, 3));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "NULL")
                .binding("b", "2")
                .binding("c", "3"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(Lists.newArrayList(null, 2, 3));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "1")
                .binding("b", "2.0E0")
                .binding("c", "3"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0, 2.0, 3.0));

        assertThat(assertions.expression("ARRAY[a, b]")
                .binding("a", "ARRAY[1, 2]")
                .binding("b", "ARRAY[3]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3)));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "ARRAY[1, 2]")
                .binding("b", "NULL")
                .binding("c", "ARRAY[3]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(Lists.newArrayList(ImmutableList.of(1, 2), null, ImmutableList.of(3)));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "BIGINT '1'")
                .binding("b", "2")
                .binding("c", "3"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(1L, 2L, 3L));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "1")
                .binding("b", "CAST(NULL AS BIGINT)")
                .binding("c", "3"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(Lists.newArrayList(1L, null, 3L));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "NULL")
                .binding("b", "20000000000")
                .binding("c", "30000000000"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(Lists.newArrayList(null, 20000000000L, 30000000000L));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "1")
                .binding("b", "2.0E0")
                .binding("c", "3"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0, 2.0, 3.0));

        assertThat(assertions.expression("ARRAY[a, b]")
                .binding("a", "ARRAY[1, 2]")
                .binding("b", "ARRAY[3]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3)));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "ARRAY[1, 2]")
                .binding("b", "NULL")
                .binding("c", "ARRAY[3]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(Lists.newArrayList(ImmutableList.of(1, 2), null, ImmutableList.of(3)));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "ARRAY[1, 2]")
                .binding("b", "NULL")
                .binding("c", "ARRAY[BIGINT '3']"))
                .hasType(new ArrayType(new ArrayType(BIGINT)))
                .isEqualTo(Lists.newArrayList(ImmutableList.of(1L, 2L), null, ImmutableList.of(3L)));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "1.0E0")
                .binding("b", "2.5E0")
                .binding("c", "3.0E0"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0, 2.5, 3.0));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "1")
                .binding("b", "2.5E0")
                .binding("c", "3"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0, 2.5, 3.0));

        assertThat(assertions.expression("ARRAY[a, b]")
                .binding("a", "'puppies'")
                .binding("b", "'kittens'"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("puppies", "kittens"));

        assertThat(assertions.expression("ARRAY[a, b]")
                .binding("a", "TRUE")
                .binding("b", "FALSE"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));

        assertThat(assertions.expression("ARRAY[a, b]")
                .binding("a", "TIMESTAMP '1970-01-01 00:00:01'")
                .binding("b", "TIMESTAMP '1973-07-08 22:00:01'"))
                .hasType(new ArrayType(createTimestampType(0)))
                .isEqualTo(ImmutableList.of(
                        sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0),
                        sqlTimestampOf(0, 1973, 7, 8, 22, 0, 1, 0)));

        assertThat(assertions.expression("ARRAY[a]")
                .binding("a", "sqrt(-1)"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(NaN));

        assertThat(assertions.expression("ARRAY[a]")
                .binding("a", "pow(infinity(), 2)"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(POSITIVE_INFINITY));

        assertThat(assertions.expression("ARRAY[a]")
                .binding("a", "pow(-infinity(), 1)"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(NEGATIVE_INFINITY));

        assertThat(assertions.expression("ARRAY[a, b]")
                .binding("a", "ARRAY[]")
                .binding("b", "NULL"))
                .hasType(new ArrayType(new ArrayType(UNKNOWN)))
                .isEqualTo(asList(ImmutableList.of(), null));

        assertThat(assertions.expression("ARRAY[a, b]")
                .binding("a", "ARRAY[1.0]")
                .binding("b", "ARRAY[2.0, 3.0]"))
                .hasType(new ArrayType(new ArrayType(createDecimalType(2, 1))))
                .isEqualTo(asList(
                        asList(decimal("1.0", createDecimalType(2, 1))),
                        asList(
                                decimal("2.0", createDecimalType(2, 1)),
                                decimal("3.0", createDecimalType(2, 1)))));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "1.0")
                .binding("b", "2.0")
                .binding("c", "3.11"))
                .hasType(new ArrayType(createDecimalType(3, 2)))
                .isEqualTo(asList(
                        decimal("1.00", createDecimalType(3, 2)),
                        decimal("2.00", createDecimalType(3, 2)),
                        decimal("3.11", createDecimalType(3, 2))));

        assertThat(assertions.expression("ARRAY[a, b, c]")
                .binding("a", "1")
                .binding("b", "2.0")
                .binding("c", "3.11"))
                .hasType(new ArrayType(createDecimalType(12, 2)))
                .isEqualTo(asList(
                        decimal("0000000001.00", createDecimalType(12, 2)),
                        decimal("0000000002.00", createDecimalType(12, 2)),
                        decimal("0000000003.11", createDecimalType(12, 2))));

        assertThat(assertions.expression("ARRAY[a, b]")
                .binding("a", "ARRAY[1.0]")
                .binding("b", "ARRAY[2.0, 123456789123456.789]"))
                .hasType(new ArrayType(new ArrayType(createDecimalType(18, 3))))
                .isEqualTo(asList(
                        asList(decimal("000000000000001.000", createDecimalType(18, 3))),
                        asList(
                                decimal("000000000000002.000", createDecimalType(18, 3)),
                                decimal("123456789123456.789", createDecimalType(18, 3)))));
    }

    @Test
    public void testArrayToArrayConcat()
    {
        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[1, NULL]")
                .binding("b", "ARRAY[3]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(Lists.newArrayList(1, null, 3));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[1, 2]")
                .binding("b", "ARRAY[3, 4]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3, 4));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[1, 2]")
                .binding("b", "ARRAY[3, BIGINT '4']"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(1L, 2L, 3L, 4L));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[1, 2]")
                .binding("b", "ARRAY[3, 40000000000]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(1L, 2L, 3L, 40000000000L));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[NULL]")
                .binding("b", "ARRAY[NULL]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(Lists.newArrayList(null, null));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY['puppies']")
                .binding("b", "ARRAY['kittens']"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("puppies", "kittens"));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[TRUE]")
                .binding("b", "ARRAY[FALSE]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));

        assertThat(assertions.function("concat", "ARRAY[1]", "ARRAY[2,3]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[TIMESTAMP '1970-01-01 00:00:01']")
                .binding("b", "ARRAY[TIMESTAMP '1973-07-08 22:00:01']"))
                .matches("ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01']");

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[ARRAY[ARRAY[1]]]")
                .binding("b", "ARRAY[ARRAY[ARRAY[2]]]"))
                .hasType(new ArrayType(new ArrayType(new ArrayType(INTEGER))))
                .isEqualTo(asList(singletonList(Ints.asList(1)), singletonList(Ints.asList(2))));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[]")
                .binding("b", "ARRAY[]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.expression("a || b || c")
                .binding("a", "ARRAY[TRUE]")
                .binding("b", "ARRAY[FALSE]")
                .binding("c", "ARRAY[TRUE]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false, true));

        assertThat(assertions.expression("a || b || c || d")
                .binding("a", "ARRAY[1]")
                .binding("b", "ARRAY[2]")
                .binding("c", "ARRAY[3]")
                .binding("d", "ARRAY[4]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3, 4));

        assertThat(assertions.expression("a || b || c || d")
                .binding("a", "ARRAY[1]")
                .binding("b", "ARRAY[2.0E0]")
                .binding("c", "ARRAY[3]")
                .binding("d", "ARRAY[4.0E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0, 2.0, 3.0, 4.0));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[ARRAY[1], ARRAY[2, 8]]")
                .binding("b", "ARRAY[ARRAY[3, 6], ARRAY[4]]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2, 8), ImmutableList.of(3, 6), ImmutableList.of(4)));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[1.0]")
                .binding("b", "ARRAY[2.0, 3.11]"))
                .matches("ARRAY[1.0, 2.0, 3.11]");

        assertThat(assertions.expression("a || b || c")
                .binding("a", "ARRAY[1.0]")
                .binding("b", "ARRAY[2.0]")
                .binding("c", "ARRAY[123456789123456.789]"))
                .matches("ARRAY[1.0, 2.0, 123456789123456.789]");

        // Tests for concatenating multiple arrays
        assertThat(assertions.function("concat", "ARRAY[]", "ARRAY[NULL]", "ARRAY[]", "ARRAY[NULL]", "ARRAY[]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(Collections.nCopies(2, null));

        assertThat(assertions.function("concat", "ARRAY[]", "ARRAY[]", "ARRAY[]", "NULL", "ARRAY[]"))
                .isNull(new ArrayType(UNKNOWN));

        assertThat(assertions.function("concat", "ARRAY[]", "ARRAY[]", "ARRAY[]", "ARRAY[]", "ARRAY[]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("concat", "ARRAY[]", "ARRAY[]", "ARRAY[333]", "ARRAY[]", "ARRAY[]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(333));

        assertThat(assertions.function("concat", "ARRAY[1]", "ARRAY[2,3]", "ARRAY[]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3));

        assertThat(assertions.function("concat", "ARRAY[1]", "ARRAY[2,3,3]", "ARRAY[2,1]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3, 3, 2, 1));

        assertThat(assertions.function("concat", "ARRAY[1]", "ARRAY[]", "ARRAY[1,2]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 1, 2));

        assertThat(assertions.function("concat", "ARRAY[]", "ARRAY[1]", "ARRAY[]", "ARRAY[3]", "ARRAY[]", "ARRAY[5]", "ARRAY[]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 3, 5));

        assertThat(assertions.function("concat", "ARRAY[]", "ARRAY['123456']", "CAST(ARRAY[1,2] AS ARRAY(varchar))", "ARRAY[]"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(ImmutableList.of("123456", "1", "2"));

        assertTrinoExceptionThrownBy(() -> assertions.expression("a || b")
                .binding("a", "ARRAY[ARRAY[1]]")
                .binding("b", "ARRAY[ARRAY[true], ARRAY[false]]").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);

        // This query is ambiguous. The result can be [[1], NULL] or [[1], [NULL]] depending on interpretation
        assertTrinoExceptionThrownBy(() -> assertions.expression("a || b")
                .binding("a", "ARRAY[ARRAY[1]]")
                .binding("b", "ARRAY[NULL]").evaluate())
                .hasErrorCode(AMBIGUOUS_FUNCTION_CALL);

        assertTrinoExceptionThrownBy(() -> assertions.expression("a || b")
                .binding("a", "ARRAY[ARRAY[1]]")
                .binding("b", "ARRAY[ARRAY['x']]")
                .evaluate())
                .hasMessage("line 1:10: Unexpected parameters (array(array(integer)), array(array(varchar(1)))) for function concat. Expected: concat(char(x), char(y)), concat(array(E), E) E, concat(E, array(E)) E, concat(array(E)) E, concat(varchar), concat(varbinary)");
    }

    @Test
    public void testElementArrayConcat()
    {
        assertThat(assertions.expression("CAST(a || b AS JSON)")
                .binding("a", "ARRAY[DATE '2001-08-22']")
                .binding("b", "DATE '2001-08-23'"))
                .hasType(JSON)
                .isEqualTo("[\"2001-08-22\",\"2001-08-23\"]");

        assertThat(assertions.expression("CAST(a || b AS JSON)")
                .binding("a", "DATE '2001-08-23'")
                .binding("b", "ARRAY[DATE '2001-08-22']"))
                .hasType(JSON)
                .isEqualTo("[\"2001-08-23\",\"2001-08-22\"]");

        assertThat(assertions.expression("a || b")
                .binding("a", "1")
                .binding("b", "ARRAY[2]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(Lists.newArrayList(1, 2));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[2]")
                .binding("b", "1"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(Lists.newArrayList(2, 1));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[2]")
                .binding("b", "BIGINT '1'"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(Lists.newArrayList(2L, 1L));

        assertThat(assertions.expression("a || b")
                .binding("a", "TRUE")
                .binding("b", "ARRAY[FALSE]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(Lists.newArrayList(true, false));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[FALSE]")
                .binding("b", "TRUE"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(Lists.newArrayList(false, true));

        assertThat(assertions.expression("a || b")
                .binding("a", "1.0E0")
                .binding("b", "ARRAY[2.0E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(Lists.newArrayList(1.0, 2.0));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[2.0E0]")
                .binding("b", "1.0E0"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(Lists.newArrayList(2.0, 1.0));

        assertThat(assertions.expression("a || b")
                .binding("a", "'puppies'")
                .binding("b", "ARRAY['kittens']"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(Lists.newArrayList("puppies", "kittens"));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY['kittens']")
                .binding("b", "'puppies'"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(Lists.newArrayList("kittens", "puppies"));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[TIMESTAMP '1970-01-01 00:00:01']")
                .binding("b", "TIMESTAMP '1973-07-08 22:00:01'"))
                .matches("ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01']");

        assertThat(assertions.expression("a || b")
                .binding("a", "TIMESTAMP '1973-07-08 22:00:01'")
                .binding("b", "ARRAY[TIMESTAMP '1970-01-01 00:00:01']"))
                .matches("ARRAY[TIMESTAMP '1973-07-08 22:00:01', TIMESTAMP '1970-01-01 00:00:01']");

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[2, 8]")
                .binding("b", "ARRAY[ARRAY[3, 6], ARRAY[4]]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(2, 8), ImmutableList.of(3, 6), ImmutableList.of(4)));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[ARRAY[1], ARRAY[2, 8]]")
                .binding("b", "ARRAY[3, 6]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2, 8), ImmutableList.of(3, 6)));

        assertThat(assertions.expression("a || b")
                .binding("a", "ARRAY[2.0, 3.11]")
                .binding("b", "1.0"))
                .matches("ARRAY[2.0, 3.11, 1.0]");

        assertThat(assertions.expression("a || b || c")
                .binding("a", "ARRAY[1.0]")
                .binding("b", "2.0")
                .binding("c", "123456789123456.789"))
                .matches("ARRAY[1.0, 2.0, 123456789123456.789]");

        assertTrinoExceptionThrownBy(() -> assertions.expression("a || b")
                .binding("a", "ARRAY[ARRAY[1]]")
                .binding("b", "ARRAY['x']")
                .evaluate())
                .hasMessage("line 1:10: Unexpected parameters (array(array(integer)), array(varchar(1))) for function concat. Expected: concat(char(x), char(y)), concat(array(E), E) E, concat(E, array(E)) E, concat(array(E)) E, concat(varchar), concat(varbinary)");
    }

    @Test
    public void testArrayContains()
    {
        assertThat(assertions.function("contains", "array['puppies', 'dogs']", "'dogs'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[1, 2, 3]", "2"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[1, BIGINT '2', 3]", "2"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[1, 2, 3]", "BIGINT '2'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[1, 2, 3]", "5"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "array[1, NULL, 3]", "1"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[NULL, 2, 3]", "1"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("contains", "array[NULL, 2, 3]", "NULL"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("contains", "array[1, 2.0E0, 3]", "3.0E0"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[1.0E0, 2.5E0, 3.0E0]", "2.2E0"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "array['puppies', 'dogs']", "'dogs'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array['puppies', 'dogs']", "'sharks'"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "array[TRUE, FALSE]", "TRUE"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[FALSE]", "TRUE"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "array[ARRAY[1, 2], ARRAY[3, 4]]", "ARRAY[3, 4]"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[ARRAY[1, 2], ARRAY[3, 4]]", "ARRAY[3]"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "array[CAST(NULL AS BIGINT)]", "1"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("contains", "array[CAST(NULL AS BIGINT)]", "NULL"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("contains", "array[]", "NULL"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("contains", "array[]", "1"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "array[2.2, 1.1]", "1.1"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[2.2, 1.1]", "1.1"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[2.2, NULL]", "1.1"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("contains", "array[2.2, 1.1]", "1.2"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "array[2.2, 1.1]", "0000000000001.100"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[2.2, 001.20]", "1.2"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[ARRAY[1.1, 2.2], ARRAY[3.3, 4.3]]", "ARRAY[3.3, 4.300]"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "array[ARRAY[1.1, 2.2], ARRAY[3.3, 4.3]]", "ARRAY[1.3, null]"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "array[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-05-10 12:34:56.123456789']", "TIMESTAMP '1111-05-10 12:34:56.123456789'"))
                .isEqualTo(true);

        assertTrinoExceptionThrownBy(() -> assertions.function("contains", "array[ARRAY[1.1, 2.2], ARRAY[3.3, 4.3]]", "ARRAY[1.1, null]").evaluate())
                .hasErrorCode(NOT_SUPPORTED);

        assertTrinoExceptionThrownBy(() -> assertions.function("contains", "array[ARRAY[1.1, null], ARRAY[3.3, 4.3]]", "ARRAY[1.1, null]").evaluate())
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testArrayJoin()
    {
        assertThat(assertions.function("array_join", "ARRAY[NULL, 1, 2]", "','"))
                .hasType(VARCHAR)
                .isEqualTo("1,2");

        assertThat(assertions.function("array_join", "ARRAY[1, NULL, 2]", "','"))
                .hasType(VARCHAR)
                .isEqualTo("1,2");

        assertThat(assertions.function("array_join", "ARRAY[1, 2, NULL]", "','"))
                .hasType(VARCHAR)
                .isEqualTo("1,2");

        assertThat(assertions.function("array_join", "ARRAY[1, 2, 3]", "';'", "'N/A'"))
                .hasType(VARCHAR)
                .isEqualTo("1;2;3");

        assertThat(assertions.function("array_join", "ARRAY[1, 2, null]", "';'", "'N/A'"))
                .hasType(VARCHAR)
                .isEqualTo("1;2;N/A");

        assertThat(assertions.function("array_join", "ARRAY[1, 2, 3]", "'x'"))
                .hasType(VARCHAR)
                .isEqualTo("1x2x3");

        assertThat(assertions.function("array_join", "ARRAY[BIGINT '1', 2, 3]", "'x'"))
                .hasType(VARCHAR)
                .isEqualTo("1x2x3");

        assertThat(assertions.function("array_join", "ARRAY[null]", "'='"))
                .hasType(VARCHAR)
                .isEqualTo("");

        assertThat(assertions.function("array_join", "ARRAY[null,null]", "'='"))
                .hasType(VARCHAR)
                .isEqualTo("");

        assertThat(assertions.function("array_join", "ARRAY[]", "'S'"))
                .hasType(VARCHAR)
                .isEqualTo("");

        assertThat(assertions.function("array_join", "ARRAY['']", "''", "''"))
                .hasType(VARCHAR)
                .isEqualTo("");

        assertThat(assertions.function("array_join", "ARRAY[1, 2, 3, null, 5]", "','", "'*'"))
                .hasType(VARCHAR)
                .isEqualTo("1,2,3,*,5");

        assertThat(assertions.function("array_join", "ARRAY['a', 'b', 'c', null, null, 'd']", "'-'", "'N/A'"))
                .hasType(VARCHAR)
                .isEqualTo("a-b-c-N/A-N/A-d");

        assertThat(assertions.function("array_join", "ARRAY['a', 'b', 'c', null, null, 'd']", "'-'"))
                .hasType(VARCHAR)
                .isEqualTo("a-b-c-d");

        assertThat(assertions.function("array_join", "ARRAY[null, null, null, null]", "'X'"))
                .hasType(VARCHAR)
                .isEqualTo("");

        assertThat(assertions.function("array_join", "ARRAY[true, false]", "'XX'"))
                .hasType(VARCHAR)
                .isEqualTo("trueXXfalse");

        assertThat(assertions.function("array_join", "ARRAY[sqrt(-1), infinity()]", "','"))
                .hasType(VARCHAR)
                .isEqualTo("NaN,Infinity");

        assertThat(assertions.function("array_join", "ARRAY[JSON '\"a\"', JSON '\"b\"']", "','"))
                .hasType(VARCHAR)
                .isEqualTo("a,b");

        assertThat(assertions.function("array_join", "ARRAY[JSON '\"a\"', JSON 'null']", "','"))
                .hasType(VARCHAR)
                .isEqualTo("a");

        assertThat(assertions.function("array_join", "ARRAY[JSON '\"a\"', JSON 'null']", "','", "'N/A'"))
                .hasType(VARCHAR)
                .isEqualTo("a,N/A");

        assertThat(assertions.function("array_join", "ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01']", "'|'"))
                .hasType(VARCHAR)
                .isEqualTo(format(
                        "%s|%s",
                        sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0),
                        sqlTimestampOf(0, 1973, 7, 8, 22, 0, 1, 0)));

        assertThat(assertions.function("array_join", "ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-05-10 12:34:56.123456789']", "'|'"))
                .hasType(VARCHAR)
                .isEqualTo("2020-05-10 12:34:56.123456789|1111-05-10 12:34:56.123456789");

        assertThat(assertions.function("array_join", "ARRAY[null, TIMESTAMP '1970-01-01 00:00:01']", "'|'"))
                .hasType(VARCHAR)
                .isEqualTo(sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0).toString());

        assertThat(assertions.function("array_join", "ARRAY[null, TIMESTAMP '1970-01-01 00:00:01']", "'|'", "'XYZ'"))
                .hasType(VARCHAR)
                .isEqualTo("XYZ|" + sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0).toString());

        assertThat(assertions.function("array_join", "ARRAY[1.0, 2.1, 3.3]", "'x'"))
                .hasType(VARCHAR)
                .isEqualTo("1.0x2.1x3.3");

        assertThat(assertions.function("array_join", "ARRAY[1.0, 2.100, 3.3]", "'x'"))
                .hasType(VARCHAR)
                .isEqualTo("1.000x2.100x3.300");

        assertThat(assertions.function("array_join", "ARRAY[1.0, 2.100, NULL]", "'x'", "'N/A'"))
                .hasType(VARCHAR)
                .isEqualTo("1.000x2.100xN/A");

        assertThat(assertions.function("array_join", "ARRAY[1.0, DOUBLE '002.100', 3.3]", "'x'"))
                .hasType(VARCHAR)
                .isEqualTo("1.0E0x2.1E0x3.3E0");

        assertTrinoExceptionThrownBy(() -> assertions.function("array_join", "ARRAY[ARRAY[1], ARRAY[2]]", "'-'").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);

        assertTrinoExceptionThrownBy(() -> assertions.function("array_join", "ARRAY[MAP(ARRAY[1], ARRAY[2])]", "'-'").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);

        assertTrinoExceptionThrownBy(() -> assertions.function("array_join", "ARRAY[CAST(row(1, 2) AS row(col0 bigint, col1 bigint))]", "'-'").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testCardinality()
    {
        assertThat(assertions.function("cardinality", "ARRAY[]"))
                .isEqualTo(0L);

        assertThat(assertions.function("cardinality", "ARRAY[NULL]"))
                .isEqualTo(1L);

        assertThat(assertions.function("cardinality", "ARRAY[1, 2, 3]"))
                .isEqualTo(3L);

        assertThat(assertions.function("cardinality", "ARRAY[1, NULL, 3]"))
                .isEqualTo(3L);

        assertThat(assertions.function("cardinality", "ARRAY[1, 2.0E0, 3]"))
                .isEqualTo(3L);

        assertThat(assertions.function("cardinality", "ARRAY[ARRAY[1, 2], ARRAY[3]]"))
                .isEqualTo(2L);

        assertThat(assertions.function("cardinality", "ARRAY[1.0E0, 2.5E0, 3.0E0]"))
                .isEqualTo(3L);

        assertThat(assertions.function("cardinality", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(2L);

        assertThat(assertions.function("cardinality", "ARRAY[TRUE, FALSE]"))
                .isEqualTo(2L);

        assertThat(assertions.function("cardinality", "ARRAY[1.1, 2.2, 3.3]"))
                .isEqualTo(3L);

        assertThat(assertions.function("cardinality", "ARRAY[1.1, 33832293522235.23522]"))
                .isEqualTo(2L);
    }

    @Test
    public void testArrayMin()
    {
        assertThat(assertions.function("array_min", "ARRAY[]"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("array_min", "ARRAY[NULL]"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("array_min", "ARRAY[NaN()]"))
                .isEqualTo(NaN);

        assertThat(assertions.function("array_min", "ARRAY[CAST(NaN() AS REAL)]"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.function("array_min", "ARRAY[NULL, NULL, NULL]"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("array_min", "ARRAY[NaN(), NaN(), NaN()]"))
                .isEqualTo(NaN);

        assertThat(assertions.function("array_min", "ARRAY[CAST(NaN() AS REAL), CAST(NaN() AS REAL)]"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.function("array_min", "ARRAY[NULL, 2, 3]"))
                .isNull(INTEGER);

        assertThat(assertions.function("array_min", "ARRAY[NaN(), 2, 3]"))
                .isEqualTo(2.0);

        assertThat(assertions.function("array_min", "ARRAY[2, NaN(), 3]"))
                .isEqualTo(2.0);

        assertThat(assertions.function("array_min", "ARRAY[2, 3, NaN()]"))
                .isEqualTo(2.0);

        assertThat(assertions.function("array_min", "ARRAY[NULL, NaN(), 1]"))
                .isNull(DOUBLE);

        assertThat(assertions.function("array_min", "ARRAY[NaN(), NULL, 3.0]"))
                .isNull(DOUBLE);

        assertThat(assertions.function("array_min", "ARRAY[1.0E0, NULL, 3]"))
                .isNull(DOUBLE);

        assertThat(assertions.function("array_min", "ARRAY[1.0, NaN(), 3]"))
                .isEqualTo(1.0);

        assertThat(assertions.function("array_min", "ARRAY[CAST(NaN() AS REAL), REAL '2', REAL '3']"))
                .isEqualTo(2.0f);

        assertThat(assertions.function("array_min", "ARRAY[REAL '2', CAST(NaN() AS REAL), REAL '3']"))
                .isEqualTo(2.0f);

        assertThat(assertions.function("array_min", "ARRAY[REAL '2', REAL '3', CAST(NaN() AS REAL)]"))
                .isEqualTo(2.0f);

        assertThat(assertions.function("array_min", "ARRAY[NULL, CAST(NaN() AS REAL), REAL '1']"))
                .isNull(REAL);

        assertThat(assertions.function("array_min", "ARRAY[CAST(NaN() AS REAL), NULL, REAL '3']"))
                .isNull(REAL);

        assertThat(assertions.function("array_min", "ARRAY[REAL '1', NULL, REAL '3']"))
                .isNull(REAL);

        assertThat(assertions.function("array_min", "ARRAY[REAL '1', CAST(NaN() AS REAL), REAL '3']"))
                .isEqualTo(1.0f);

        assertThat(assertions.function("array_min", "ARRAY['1', '2', NULL]"))
                .isNull(createVarcharType(1));

        assertThat(assertions.function("array_min", "ARRAY[3, 2, 1]"))
                .isEqualTo(1);

        assertThat(assertions.function("array_min", "ARRAY[1, 2, 3]"))
                .isEqualTo(1);

        assertThat(assertions.function("array_min", "ARRAY[BIGINT '3', 2, 1]"))
                .isEqualTo(1L);

        assertThat(assertions.function("array_min", "ARRAY[1, 2.0E0, 3]"))
                .isEqualTo(1.0);

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[1, 2], ARRAY[3]]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));

        assertThat(assertions.function("array_min", "ARRAY[1.0E0, 2.5E0, 3.0E0]"))
                .isEqualTo(1.0);

        assertThat(assertions.function("array_min", "ARRAY['puppies', 'kittens']"))
                .hasType(createVarcharType(7))
                .isEqualTo("kittens");

        assertThat(assertions.function("array_min", "ARRAY[TRUE, FALSE]"))
                .isEqualTo(false);

        assertThat(assertions.function("array_min", "ARRAY[NULL, FALSE]"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("array_min", "ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '2222-05-10 12:34:56.123456789']"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.123456789'");

        assertThat(assertions.function("array_min", "ARRAY[2.1, 2.2, 2.3]"))
                .isEqualTo(decimal("2.1", createDecimalType(2, 1)));

        assertThat(assertions.function("array_min", "ARRAY[2.111111222111111114111, 2.22222222222222222, 2.222222222222223]"))
                .isEqualTo(decimal("2.111111222111111114111", createDecimalType(22, 21)));

        assertThat(assertions.function("array_min", "ARRAY[1.9, 2, 2.3]"))
                .isEqualTo(decimal("0000000001.9", createDecimalType(11, 1)));

        assertThat(assertions.function("array_min", "ARRAY[2.22222222222222222, 2.3]"))
                .isEqualTo(decimal("2.22222222222222222", createDecimalType(18, 17)));

        assertThat(assertions.function("array_min", "ARRAY[ROW(NaN()), ROW(2), ROW(3)]"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(2.0));

        assertThat(assertions.function("array_min", "ARRAY[ROW(2), ROW(NaN()), ROW(3)]"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(2.0));

        assertThat(assertions.function("array_min", "ARRAY[ROW(2), ROW(3), ROW(NaN())]"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(2.0));

        assertThat(assertions.function("array_min", "ARRAY[NULL, ROW(NaN()), ROW(1)]"))
                .isNull(anonymousRow(DOUBLE));

        assertThat(assertions.function("array_min", "ARRAY[ROW(NaN()), NULL, ROW(3.0)]"))
                .isNull(anonymousRow(DOUBLE));

        assertThat(assertions.function("array_min", "ARRAY[ROW(1.0E0), NULL, ROW(3)]"))
                .isNull(anonymousRow(DOUBLE));

        assertThat(assertions.function("array_min", "ARRAY[ROW(1.0), ROW(NaN()), ROW(3)]"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0));

        assertThat(assertions.function("array_min", "ARRAY[ROW(CAST(NaN() AS REAL)), ROW(REAL '2'), ROW(REAL '3')]"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(2.0f));

        assertThat(assertions.function("array_min", "ARRAY[ROW(REAL '2'), ROW(CAST(NaN() AS REAL)), ROW(REAL '3')]"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(2.0f));

        assertThat(assertions.function("array_min", "ARRAY[ROW(REAL '2'), ROW(REAL '3'), ROW(CAST(NaN() AS REAL))]"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(2.0f));

        assertThat(assertions.function("array_min", "ARRAY[NULL, ROW(CAST(NaN() AS REAL)), ROW(REAL '1')]"))
                .isNull(anonymousRow(REAL));

        assertThat(assertions.function("array_min", "ARRAY[ROW(CAST(NaN() AS REAL)), NULL, ROW(REAL '3')]"))
                .isNull(anonymousRow(REAL));

        assertThat(assertions.function("array_min", "ARRAY[ROW(REAL '1'), NULL, ROW(REAL '3')]"))
                .isNull(anonymousRow(REAL));

        assertThat(assertions.function("array_min", "ARRAY[ROW(REAL '1'), ROW(CAST(NaN() AS REAL)), ROW(REAL '3')]"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(1.0f));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[NaN()], ARRAY[2], ARRAY[3]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(2.0));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[2], ARRAY[NaN()], ARRAY[3]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(2.0));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[2], ARRAY[3], ARRAY[NaN()]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(2.0));

        assertThat(assertions.function("array_min", "ARRAY[NULL, ARRAY[NaN()], ARRAY[1]]"))
                .isNull(new ArrayType(DOUBLE));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[NaN()], NULL, ARRAY[3.0]]"))
                .isNull(new ArrayType(DOUBLE));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[1.0E0], NULL, ARRAY[3]]"))
                .isNull(new ArrayType(DOUBLE));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[1.0], ARRAY[NaN()], ARRAY[3]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[CAST(NaN() AS REAL)], ARRAY[REAL '2'], ARRAY[REAL '3']]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(2.0f));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[REAL '2'], ARRAY[CAST(NaN() AS REAL)], ARRAY[REAL '3']]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(2.0f));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[REAL '2'], ARRAY[REAL '3'], ARRAY[CAST(NaN() AS REAL)]]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(2.0f));

        assertThat(assertions.function("array_min", "ARRAY[NULL, ARRAY[CAST(NaN() AS REAL)], ARRAY[REAL '1']]"))
                .isNull(new ArrayType(REAL));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[CAST(NaN() AS REAL)], NULL, ARRAY[REAL '3']]"))
                .isNull(new ArrayType(REAL));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[REAL '1'], NULL, ARRAY[REAL '3']]"))
                .isNull(new ArrayType(REAL));

        assertThat(assertions.function("array_min", "ARRAY[ARRAY[REAL '1'], ARRAY[CAST(NaN() AS REAL)], ARRAY[REAL '3']]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(1.0f));
    }

    @Test
    public void testArrayMax()
    {
        assertThat(assertions.function("array_max", "ARRAY[]"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("array_max", "ARRAY[NULL]"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("array_max", "ARRAY[NaN()]"))
                .isEqualTo(NaN);

        assertThat(assertions.function("array_max", "ARRAY[CAST(NaN() AS REAL)]"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.function("array_max", "ARRAY[NULL, NULL, NULL]"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("array_max", "ARRAY[NaN(), NaN(), NaN()]"))
                .isEqualTo(NaN);

        assertThat(assertions.function("array_max", "ARRAY[CAST(NaN() AS REAL), CAST(NaN() AS REAL)]"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.function("array_max", "ARRAY[NULL, 2, 3]"))
                .isNull(INTEGER);

        assertThat(assertions.function("array_max", "ARRAY[NaN(), 2, 3]"))
                .isEqualTo(3.0);

        assertThat(assertions.function("array_max", "ARRAY[2, NaN(), 3]"))
                .isEqualTo(3.0);

        assertThat(assertions.function("array_max", "ARRAY[2, 3, NaN()]"))
                .isEqualTo(3.0);

        assertThat(assertions.function("array_max", "ARRAY[NULL, NaN(), 1]"))
                .isNull(DOUBLE);

        assertThat(assertions.function("array_max", "ARRAY[NaN(), NULL, 3.0]"))
                .isNull(DOUBLE);

        assertThat(assertions.function("array_max", "ARRAY[1.0E0, NULL, 3]"))
                .isNull(DOUBLE);

        assertThat(assertions.function("array_max", "ARRAY[1.0, NaN(), 3]"))
                .isEqualTo(3.0);

        assertThat(assertions.function("array_max", "ARRAY[CAST(NaN() AS REAL), REAL '2', REAL '3']"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("array_max", "ARRAY[REAL '2', CAST(NaN() AS REAL), REAL '3']"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("array_max", "ARRAY[REAL '2', REAL '3', CAST(NaN() AS REAL)]"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("array_max", "ARRAY[NULL, CAST(NaN() AS REAL), REAL '1']"))
                .isNull(REAL);

        assertThat(assertions.function("array_max", "ARRAY[CAST(NaN() AS REAL), NULL, REAL '3']"))
                .isNull(REAL);

        assertThat(assertions.function("array_max", "ARRAY[REAL '1', NULL, REAL '3']"))
                .isNull(REAL);

        assertThat(assertions.function("array_max", "ARRAY[REAL '1', CAST(NaN() AS REAL), REAL '3']"))
                .isEqualTo(3.0f);

        assertThat(assertions.function("array_max", "ARRAY['1', '2', NULL]"))
                .isNull(createVarcharType(1));

        assertThat(assertions.function("array_max", "ARRAY[3, 2, 1]"))
                .isEqualTo(3);

        assertThat(assertions.function("array_max", "ARRAY[1, 2, 3]"))
                .isEqualTo(3);

        assertThat(assertions.function("array_max", "ARRAY[BIGINT '1', 2, 3]"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_max", "ARRAY[1, 2.0E0, 3]"))
                .isEqualTo(3.0);

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[1, 2], ARRAY[3]]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(3));

        assertThat(assertions.function("array_max", "ARRAY[1.0E0, 2.5E0, 3.0E0]"))
                .isEqualTo(3.0);

        assertThat(assertions.function("array_max", "ARRAY['puppies', 'kittens']"))
                .hasType(createVarcharType(7))
                .isEqualTo("puppies");

        assertThat(assertions.function("array_max", "ARRAY[TRUE, FALSE]"))
                .isEqualTo(true);

        assertThat(assertions.function("array_max", "ARRAY[NULL, FALSE]"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("array_max", "ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-05-10 12:34:56.123456789']"))
                .matches("TIMESTAMP '2020-05-10 12:34:56.123456789'");

        assertThat(assertions.function("array_max", "ARRAY[2.1, 2.2, 2.3]"))
                .isEqualTo(decimal("2.3", createDecimalType(2, 1)));

        assertThat(assertions.function("array_max", "ARRAY[2.111111222111111114111, 2.22222222222222222, 2.222222222222223]"))
                .isEqualTo(decimal("2.222222222222223000000", createDecimalType(22, 21)));

        assertThat(assertions.function("array_max", "ARRAY[1.9, 2, 2.3]"))
                .isEqualTo(decimal("0000000002.3", createDecimalType(11, 1)));

        assertThat(assertions.function("array_max", "ARRAY[2.22222222222222222, 2.3]"))
                .isEqualTo(decimal("2.30000000000000000", createDecimalType(18, 17)));

        assertThat(assertions.function("array_max", "ARRAY[ROW(NaN()), ROW(2), ROW(3)]"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("array_max", "ARRAY[ROW(2), ROW(NaN()), ROW(3)]"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("array_max", "ARRAY[ROW(2), ROW(3), ROW(NaN())]"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("array_max", "ARRAY[NULL, ROW(NaN()), ROW(1)]"))
                .isNull(anonymousRow(DOUBLE));

        assertThat(assertions.function("array_max", "ARRAY[ROW(NaN()), NULL, ROW(3.0)]"))
                .isNull(anonymousRow(DOUBLE));

        assertThat(assertions.function("array_max", "ARRAY[ROW(1.0E0), NULL, ROW(3)]"))
                .isNull(anonymousRow(DOUBLE));

        assertThat(assertions.function("array_max", "ARRAY[ROW(1.0), ROW(NaN()), ROW(3)]"))
                .hasType(anonymousRow(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("array_max", "ARRAY[ROW(CAST(NaN() AS REAL)), ROW(REAL '2'), ROW(REAL '3')]"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("array_max", "ARRAY[ROW(REAL '2'), ROW(CAST(NaN() AS REAL)), ROW(REAL '3')]"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("array_max", "ARRAY[ROW(REAL '2'), ROW(REAL '3'), ROW(CAST(NaN() AS REAL))]"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("array_max", "ARRAY[NULL, ROW(CAST(NaN() AS REAL)), ROW(REAL '1')]"))
                .isNull(anonymousRow(REAL));

        assertThat(assertions.function("array_max", "ARRAY[ROW(CAST(NaN() AS REAL)), NULL, ROW(REAL '3')]"))
                .isNull(anonymousRow(REAL));

        assertThat(assertions.function("array_max", "ARRAY[ROW(REAL '1'), NULL, ROW(REAL '3')]"))
                .isNull(anonymousRow(REAL));

        assertThat(assertions.function("array_max", "ARRAY[ROW(REAL '1'), ROW(CAST(NaN() AS REAL)), ROW(REAL '3')]"))
                .hasType(anonymousRow(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[NaN()], ARRAY[2], ARRAY[3]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[2], ARRAY[NaN()], ARRAY[3]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[2], ARRAY[3], ARRAY[NaN()]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("array_max", "ARRAY[NULL, ARRAY[NaN()], ARRAY[1]]"))
                .isNull(new ArrayType(DOUBLE));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[NaN()], NULL, ARRAY[3.0]]"))
                .isNull(new ArrayType(DOUBLE));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[1.0E0], NULL, ARRAY[3]]"))
                .isNull(new ArrayType(DOUBLE));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[1.0], ARRAY[NaN()], ARRAY[3]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(3.0));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[CAST(NaN() AS REAL)], ARRAY[REAL '2'], ARRAY[REAL '3']]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[REAL '2'], ARRAY[CAST(NaN() AS REAL)], ARRAY[REAL '3']]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[REAL '2'], ARRAY[REAL '3'], ARRAY[CAST(NaN() AS REAL)]]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(3.0f));

        assertThat(assertions.function("array_max", "ARRAY[NULL, ARRAY[CAST(NaN() AS REAL)], ARRAY[REAL '1']]"))
                .isNull(new ArrayType(REAL));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[CAST(NaN() AS REAL)], NULL, ARRAY[REAL '3']]"))
                .isNull(new ArrayType(REAL));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[REAL '1'], NULL, ARRAY[REAL '3']]"))
                .isNull(new ArrayType(REAL));

        assertThat(assertions.function("array_max", "ARRAY[ARRAY[REAL '1'], ARRAY[CAST(NaN() AS REAL)], ARRAY[REAL '3']]"))
                .hasType(new ArrayType(REAL))
                .isEqualTo(ImmutableList.of(3.0f));
    }

    @Test
    public void testArrayPosition()
    {
        assertThat(assertions.function("array_position", "ARRAY[10, 20, 30, 40]", "30"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_position", "CAST(JSON '[]' as array(bigint))", "30"))
                .isEqualTo(0L);

        assertThat(assertions.function("array_position", "ARRAY[CAST(NULL as bigint)]", "30"))
                .isEqualTo(0L);

        assertThat(assertions.function("array_position", "ARRAY[CAST(NULL as bigint), NULL, NULL]", "30"))
                .isEqualTo(0L);

        assertThat(assertions.function("array_position", "ARRAY[NULL, NULL, 30, NULL]", "30"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_position", "ARRAY[1.1E0, 2.1E0, 3.1E0, 4.1E0]", "3.1E0"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_position", "ARRAY[false, false, true, true]", "true"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_position", "ARRAY['10', '20', '30', '40']", "'30'"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_position", "ARRAY[DATE '2000-01-01', DATE '2000-01-02', DATE '2000-01-03', DATE '2000-01-04']", "DATE '2000-01-03'"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_position", "ARRAY[ARRAY[1, 11], ARRAY[2, 12], ARRAY[3, 13], ARRAY[4, 14]]", "ARRAY[3, 13]"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_position", "ARRAY[]", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.function("array_position", "ARRAY[NULL]", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.function("array_position", "ARRAY[1, NULL, 2]", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.function("array_position", "ARRAY[1, CAST(NULL AS BIGINT), 2]", "CAST(NULL AS BIGINT)"))
                .isNull(BIGINT);

        assertThat(assertions.function("array_position", "ARRAY[1, NULL, 2]", "CAST(NULL AS BIGINT)"))
                .isNull(BIGINT);

        assertThat(assertions.function("array_position", "ARRAY[1, CAST(NULL AS BIGINT), 2]", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.function("array_position", "ARRAY[1.0, 2.0, 3.0, 4.0]", "3.0"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_position", "ARRAY[1.0, 2.0, 000000000000000000000003.000, 4.0]", "3.0"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_position", "ARRAY[1.0, 2.0, 3.0, 4.0]", "000000000000000000000003.000"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_position", "ARRAY[1.0, 2.0, 3.0, 4.0]", "3"))
                .isEqualTo(3L);

        assertThat(assertions.function("array_position", "ARRAY[1.0, 2.0, 3, 4.0]", "4.0"))
                .isEqualTo(4L);

        assertThat(assertions.function("array_position", "ARRAY[ARRAY[1]]", "ARRAY[1]"))
                .isEqualTo(1L);

        assertThat(assertions.function("array_position", "ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-05-10 12:34:56.123456789']", "TIMESTAMP '1111-05-10 12:34:56.123456789'"))
                .isEqualTo(2L);

        assertTrinoExceptionThrownBy(() -> assertions.function("array_position", "ARRAY[ARRAY[null]]", "ARRAY[1]").evaluate())
                .hasErrorCode(NOT_SUPPORTED);

        assertTrinoExceptionThrownBy(() -> assertions.function("array_position", "ARRAY[ARRAY[null]]", "ARRAY[null]").evaluate())
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testSubscript()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("a[1]")
                .binding("a", "ARRAY[]").evaluate())
                .hasMessage("Array subscript must be less than or equal to array length: 1 > 0");

        assertTrinoExceptionThrownBy(() -> assertions.expression("a[-1]")
                .binding("a", "ARRAY[null]").evaluate())
                .hasMessage("Array subscript is negative: -1");

        assertTrinoExceptionThrownBy(() -> assertions.expression("a[0]")
                .binding("a", "ARRAY[1, 2, 3]").evaluate())
                .hasMessage("SQL array indices start at 1");

        assertTrinoExceptionThrownBy(() -> assertions.expression("a[-1]")
                .binding("a", "ARRAY[1, 2, 3]").evaluate())
                .hasMessage("Array subscript is negative: -1");

        assertTrinoExceptionThrownBy(() -> assertions.expression("a[4]")
                .binding("a", "ARRAY[1, 2, 3]").evaluate())
                .hasMessage("Array subscript must be less than or equal to array length: 4 > 3");

        assertTrinoExceptionThrownBy(() -> assertions.expression("a[1.1E0]")
                .binding("a", "ARRAY[1, 2, 3]").evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:8: Cannot use double for subscript of array(integer)");

        assertThat(assertions.expression("a[1]")
                .binding("a", "ARRAY[NULL]"))
                .isNull(UNKNOWN);

        assertThat(assertions.expression("a[3]")
                .binding("a", "ARRAY[NULL, NULL, NULL]"))
                .isNull(UNKNOWN);

        assertThat(assertions.expression("a[2]")
                .binding("a", "ARRAY[2, 1, 3]"))
                .isEqualTo(1);

        assertThat(assertions.expression("a[2]")
                .binding("a", "ARRAY[2, NULL, 3]"))
                .isNull(INTEGER);

        assertThat(assertions.expression("a[3]")
                .binding("a", "ARRAY[1.0E0, 2.5E0, 3.5E0]"))
                .isEqualTo(3.5);

        assertThat(assertions.expression("a[2]")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3]]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(3));

        assertThat(assertions.expression("a[2]")
                .binding("a", "ARRAY[ARRAY[1, 2], NULL, ARRAY[3]]"))
                .isNull(new ArrayType(INTEGER));

        assertThat(assertions.expression("a[2][1]")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3]]"))
                .isEqualTo(3);

        assertThat(assertions.expression("a[2]")
                .binding("a", "ARRAY['puppies', 'kittens']"))
                .hasType(createVarcharType(7))
                .isEqualTo("kittens");

        assertThat(assertions.expression("a[3]")
                .binding("a", "ARRAY['puppies', 'kittens', NULL]"))
                .isNull(createVarcharType(7));

        assertThat(assertions.expression("a[2]")
                .binding("a", "ARRAY[TRUE, FALSE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a[1]")
                .binding("a", "ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01']"))
                .hasType(createTimestampType(0))
                .isEqualTo(sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0));

        assertThat(assertions.expression("a[2]")
                .binding("a", "ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-05-10 12:34:56.123456789']"))
                .matches("TIMESTAMP '1111-05-10 12:34:56.123456789'");

        assertThat(assertions.expression("a[1]")
                .binding("a", "ARRAY[infinity()]"))
                .isEqualTo(POSITIVE_INFINITY);

        assertThat(assertions.expression("a[1]")
                .binding("a", "ARRAY[-infinity()]"))
                .isEqualTo(NEGATIVE_INFINITY);

        assertThat(assertions.expression("a[1]")
                .binding("a", "ARRAY[sqrt(-1)]"))
                .isEqualTo(NaN);

        assertThat(assertions.expression("a[3]")
                .binding("a", "ARRAY[2.1, 2.2, 2.3]"))
                .isEqualTo(decimal("2.3", createDecimalType(2, 1)));

        assertThat(assertions.expression("a[3]")
                .binding("a", "ARRAY[2.111111222111111114111, 2.22222222222222222, 2.222222222222223]"))
                .isEqualTo(decimal("2.222222222222223000000", createDecimalType(22, 21)));

        assertThat(assertions.expression("a[3]")
                .binding("a", "ARRAY[1.9, 2, 2.3]"))
                .isEqualTo(decimal("0000000002.3", createDecimalType(11, 1)));

        assertThat(assertions.expression("a[1]")
                .binding("a", "ARRAY[2.22222222222222222, 2.3]"))
                .isEqualTo(decimal("2.22222222222222222", createDecimalType(18, 17)));
    }

    @Test
    public void testSubscriptReturnType()
    {
        // Test return type of array subscript by passing it to another operation
        // One test for each specialization of the operator, as well as a Block-based type
        // boolean
        assertThat(assertions.expression("CAST(a[1] AS JSON)")
                .binding("a", "ARRAY[true]"))
                .hasType(JSON)
                .isEqualTo("true");

        // long
        assertThat(assertions.expression("CAST(a[1] AS JSON)")
                .binding("a", "ARRAY[1234]"))
                .hasType(JSON)
                .isEqualTo("1234");

        // double
        assertThat(assertions.expression("CAST(a[1] AS JSON)")
                .binding("a", "ARRAY[1.23]"))
                .hasType(JSON)
                .isEqualTo("1.23");

        // Slice
        assertThat(assertions.expression("CAST(a[1] AS JSON)")
                .binding("a", "ARRAY['vc']"))
                .hasType(JSON)
                .isEqualTo("\"vc\"");

        // Object (LongTimestamp)
        assertThat(assertions.expression("CAST(a[1] AS JSON)")
                .binding("a", "ARRAY[TIMESTAMP '1970-01-01 00:00:00.000000001']"))
                .hasType(JSON)
                .isEqualTo("\"1970-01-01 00:00:00.000000001\"");

        // Block
        assertThat(assertions.expression("CAST(a[1] AS JSON)")
                .binding("a", "ARRAY[ARRAY[1]]"))
                .hasType(JSON)
                .isEqualTo("[1]");
    }

    @Test
    public void testElementAt()
    {
        assertTrinoExceptionThrownBy(() -> assertions.function("element_at", "ARRAY[]", "0").evaluate())
                .hasMessage("SQL array indices start at 1");

        assertTrinoExceptionThrownBy(() -> assertions.function("element_at", "ARRAY[1, 2, 3]", "0").evaluate())
                .hasMessage("SQL array indices start at 1");

        assertThat(assertions.function("element_at", "ARRAY[]", "1"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("element_at", "ARRAY[]", "-1"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("element_at", "ARRAY[1, 2, 3]", "4"))
                .isNull(INTEGER);

        assertThat(assertions.function("element_at", "ARRAY[1, 2, 3]", "-4"))
                .isNull(INTEGER);

        assertThat(assertions.function("element_at", "ARRAY[NULL]", "1"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("element_at", "ARRAY[NULL]", "-1"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("element_at", "ARRAY[NULL, NULL, NULL]", "3"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("element_at", "ARRAY[NULL, NULL, NULL]", "-1"))
                .isNull(UNKNOWN);

        assertThat(assertions.function("element_at", "ARRAY[2, 1, 3]", "2"))
                .isEqualTo(1);

        assertThat(assertions.function("element_at", "ARRAY[2, 1, 3]", "-2"))
                .isEqualTo(1);

        assertThat(assertions.function("element_at", "ARRAY[2, NULL, 3]", "2"))
                .isNull(INTEGER);

        assertThat(assertions.function("element_at", "ARRAY[2, NULL, 3]", "-2"))
                .isNull(INTEGER);

        assertThat(assertions.function("element_at", "ARRAY[BIGINT '2', 1, 3]", "-2"))
                .isEqualTo(1L);

        assertThat(assertions.function("element_at", "ARRAY[2, NULL, BIGINT '3']", "-2"))
                .isNull(BIGINT);

        assertThat(assertions.function("element_at", "ARRAY[1.0E0, 2.5E0, 3.5E0]", "3"))
                .isEqualTo(3.5);

        assertThat(assertions.function("element_at", "ARRAY[1.0E0, 2.5E0, 3.5E0]", "-1"))
                .isEqualTo(3.5);

        assertThat(assertions.function("element_at", "ARRAY[ARRAY[1, 2], ARRAY[3]]", "2"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(3));

        assertThat(assertions.function("element_at", "ARRAY[ARRAY[1, 2], ARRAY[3]]", "-1"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(3));

        assertThat(assertions.function("element_at", "ARRAY[ARRAY[1, 2], NULL, ARRAY[3]]", "2"))
                .isNull(new ArrayType(INTEGER));

        assertThat(assertions.function("element_at", "ARRAY[ARRAY[1, 2], NULL, ARRAY[3]]", "-2"))
                .isNull(new ArrayType(INTEGER));

        assertThat(assertions.function("element_at", "ELEMENT_AT(ARRAY[ARRAY[1, 2], ARRAY[3]], 2) ", "1"))
                .isEqualTo(3);

        assertThat(assertions.function("element_at", "ELEMENT_AT(ARRAY[ARRAY[1, 2], ARRAY[3]], -1) ", "1"))
                .isEqualTo(3);

        assertThat(assertions.function("element_at", "ELEMENT_AT(ARRAY[ARRAY[1, 2], ARRAY[3]], 2) ", "-1"))
                .isEqualTo(3);

        assertThat(assertions.function("element_at", "ELEMENT_AT(ARRAY[ARRAY[1, 2], ARRAY[3]], -1) ", "-1"))
                .isEqualTo(3);

        assertThat(assertions.function("element_at", "ARRAY['puppies', 'kittens']", "2"))
                .hasType(createVarcharType(7))
                .isEqualTo("kittens");

        assertThat(assertions.function("element_at", "ARRAY['crocodiles', 'kittens']", "2"))
                .hasType(createVarcharType(10))
                .isEqualTo("kittens");

        assertThat(assertions.function("element_at", "ARRAY['puppies', 'kittens']", "-1"))
                .hasType(createVarcharType(7))
                .isEqualTo("kittens");

        assertThat(assertions.function("element_at", "ARRAY['puppies', 'kittens', NULL]", "3"))
                .isNull(createVarcharType(7));

        assertThat(assertions.function("element_at", "ARRAY['puppies', 'kittens', NULL]", "-1"))
                .isNull(createVarcharType(7));

        assertThat(assertions.function("element_at", "ARRAY[TRUE, FALSE]", "2"))
                .isEqualTo(false);

        assertThat(assertions.function("element_at", "ARRAY[TRUE, FALSE]", "-1"))
                .isEqualTo(false);

        assertThat(assertions.function("element_at", "ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01']", "1"))
                .hasType(createTimestampType(0))
                .isEqualTo(sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0));

        assertThat(assertions.function("element_at", "ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01']", "-2"))
                .hasType(createTimestampType(0))
                .isEqualTo(sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0));

        assertThat(assertions.function("element_at", "ARRAY[infinity()]", "1"))
                .isEqualTo(POSITIVE_INFINITY);

        assertThat(assertions.function("element_at", "ARRAY[infinity()]", "-1"))
                .isEqualTo(POSITIVE_INFINITY);

        assertThat(assertions.function("element_at", "ARRAY[-infinity()]", "1"))
                .isEqualTo(NEGATIVE_INFINITY);

        assertThat(assertions.function("element_at", "ARRAY[-infinity()]", "-1"))
                .isEqualTo(NEGATIVE_INFINITY);

        assertThat(assertions.function("element_at", "ARRAY[sqrt(-1)]", "1"))
                .isEqualTo(NaN);

        assertThat(assertions.function("element_at", "ARRAY[sqrt(-1)]", "-1"))
                .isEqualTo(NaN);

        assertThat(assertions.function("element_at", "ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-05-10 12:34:56.123456789']", "2"))
                .matches("TIMESTAMP '1111-05-10 12:34:56.123456789'");

        assertThat(assertions.function("element_at", "ARRAY[2.1, 2.2, 2.3]", "3"))
                .isEqualTo(decimal("2.3", createDecimalType(2, 1)));

        assertThat(assertions.function("element_at", "ARRAY[2.111111222111111114111, 2.22222222222222222, 2.222222222222223]", "3"))
                .isEqualTo(decimal("2.222222222222223000000", createDecimalType(22, 21)));

        assertThat(assertions.function("element_at", "ARRAY[1.9, 2, 2.3]", "-1"))
                .isEqualTo(decimal("0000000002.3", createDecimalType(11, 1)));

        assertThat(assertions.function("element_at", "ARRAY[2.22222222222222222, 2.3]", "-2"))
                .isEqualTo(decimal("2.22222222222222222", createDecimalType(18, 17)));
    }

    @Test
    public void testSort()
    {
        assertThat(assertions.function("array_sort", "ARRAY[2, 3, 4, 1]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3, 4));

        assertThat(assertions.function("array_sort", "ARRAY[2, BIGINT '3', 4, 1]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(1L, 2L, 3L, 4L));

        assertThat(assertions.function("array_sort", "ARRAY[2.3, 2.1, 2.2]"))
                .matches("ARRAY[2.1, 2.2, 2.3]");

        assertThat(assertions.function("array_sort", "ARRAY[2, 1.900, 2.330]"))
                .matches("ARRAY[1.900, 2, 2.330]");

        assertThat(assertions.function("array_sort", "ARRAY['z', 'f', 's', 'd', 'g']"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("d", "f", "g", "s", "z"));

        assertThat(assertions.function("array_sort", "ARRAY[TRUE, FALSE]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(false, true));

        assertThat(assertions.function("array_sort", "ARRAY[22.1E0, 11.1E0, 1.1E0, 44.1E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.1, 11.1, 22.1, 44.1));

        assertThat(assertions.function("array_sort", "ARRAY[TIMESTAMP '1973-07-08 22:00:01', TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1989-02-06 12:00:00']"))
                .matches("ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01', TIMESTAMP '1989-02-06 12:00:00']");

        assertThat(assertions.function("array_sort", "ARRAY[ARRAY[1], ARRAY[2]]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2)));

        // with lambda function
        assertThat(assertions.expression("array_sort(a, (x, y) -> CASE " +
                                         "WHEN x IS NULL THEN -1 " +
                                         "WHEN y IS NULL THEN 1 " +
                                         "WHEN x < y THEN 1 " +
                                         "WHEN x = y THEN 0 " +
                                         "ELSE -1 END)")
                .binding("a", "ARRAY[2, 3, 2, null, null, 4, 1]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(null, null, 4, 3, 2, 2, 1));

        assertThat(assertions.expression("array_sort(a, (x, y) -> CASE " +
                                         "WHEN x IS NULL THEN 1 " +
                                         "WHEN y IS NULL THEN -1 " +
                                         "WHEN x < y THEN 1 " +
                                         "WHEN x = y THEN 0 " +
                                         "ELSE -1 END)")
                .binding("a", "ARRAY[2, 3, 2, null, null, 4, 1]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(4, 3, 2, 2, 1, null, null));

        assertThat(assertions.expression("array_sort(a, (x, y) -> CASE " +
                                         "WHEN x IS NULL THEN -1 " +
                                         "WHEN y IS NULL THEN 1 " +
                                         "WHEN x < y THEN 1 " +
                                         "WHEN x = y THEN 0 " +
                                         "ELSE -1 END)")
                .binding("a", "ARRAY[2, null, BIGINT '3', 4, null, 1]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(asList(null, null, 4L, 3L, 2L, 1L));

        assertThat(assertions.expression("array_sort(a, (x, y) -> CASE " +
                                         "WHEN x IS NULL THEN -1 " +
                                         "WHEN y IS NULL THEN 1 " +
                                         "WHEN x < y THEN 1 " +
                                         "WHEN x = y THEN 0 " +
                                         "ELSE -1 END)")
                .binding("a", "ARRAY['bc', null, 'ab', 'dc', null]"))
                .hasType(new ArrayType(createVarcharType(2)))
                .isEqualTo(asList(null, null, "dc", "bc", "ab"));

        assertThat(assertions.expression("array_sort(a, (x, y) -> CASE " +
                                         "WHEN x IS NULL THEN -1 " +
                                         "WHEN y IS NULL THEN 1 " +
                                         "WHEN length(x) < length(y) THEN 1 " +
                                         "WHEN length(x) = length(y) THEN 0 " +
                                         "ELSE -1 END)")
                .binding("a", "ARRAY['a', null, 'abcd', null, 'abc', 'zx']"))
                .hasType(new ArrayType(createVarcharType(4)))
                .isEqualTo(asList(null, null, "abcd", "abc", "zx", "a"));

        assertThat(assertions.expression("array_sort(a, (x, y) -> CASE " +
                                         "WHEN x IS NULL THEN -1 " +
                                         "WHEN y IS NULL THEN 1 " +
                                         "WHEN x = y THEN 0 " +
                                         "WHEN x THEN -1 " +
                                         "ELSE 1 END)")
                .binding("a", "ARRAY[TRUE, null, FALSE, TRUE, null, FALSE, TRUE]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(asList(null, null, true, true, true, false, false));

        assertThat(assertions.expression("array_sort(a, (x, y) -> CASE " +
                                         "WHEN x IS NULL THEN -1 " +
                                         "WHEN y IS NULL THEN 1 " +
                                         "WHEN x < y THEN 1 " +
                                         "WHEN x = y THEN 0 " +
                                         "ELSE -1 END)")
                .binding("a", "ARRAY[22.1E0, null, null, 11.1E0, 1.1E0, 44.1E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(asList(null, null, 44.1, 22.1, 11.1, 1.1));

        assertThat(assertions.expression("array_sort(a, (x, y) -> CASE " +
                                         "WHEN x IS NULL THEN -1 " +
                                         "WHEN y IS NULL THEN 1 " +
                                         "WHEN date_diff('millisecond', y, x) < 0 THEN 1 " +
                                         "WHEN date_diff('millisecond', y, x) = 0 THEN 0 " +
                                         "ELSE -1 END)")
                .binding("a", "ARRAY[TIMESTAMP '1973-07-08 22:00:01', NULL, TIMESTAMP '1970-01-01 00:00:01', NULL, TIMESTAMP '1989-02-06 12:00:00']"))
                .matches("ARRAY[null, null, TIMESTAMP '1989-02-06 12:00:00', TIMESTAMP '1973-07-08 22:00:01', TIMESTAMP '1970-01-01 00:00:01']");

        assertThat(assertions.expression("array_sort(a, (x, y) -> CASE " +
                                         "WHEN x IS NULL THEN -1 " +
                                         "WHEN y IS NULL THEN 1 " +
                                         "WHEN cardinality(x) < cardinality(y) THEN 1 " +
                                         "WHEN cardinality(x) = cardinality(y) THEN 0 " +
                                         "ELSE -1 END)")
                .binding("a", "ARRAY[ARRAY[2, 3, 1], null, ARRAY[4, null, 2, 1, 4], ARRAY[1, 2], null]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(asList(null, null, asList(4, null, 2, 1, 4), asList(2, 3, 1), asList(1, 2)));

        assertThat(assertions.expression("array_sort(a, (x, y) -> CASE " +
                                         "WHEN x IS NULL THEN -1 " +
                                         "WHEN y IS NULL THEN 1 " +
                                         "WHEN x < y THEN 1 " +
                                         "WHEN x = y THEN 0 " +
                                         "ELSE -1 END)")
                .binding("a", "ARRAY[2.3, null, 2.1, null, 2.2]"))
                .matches("ARRAY[null, null, 2.3, 2.2, 2.1]");

        assertThat(assertions.expression("array_sort(a, (x, y) -> CASE " +
                                         "WHEN month(x) > month(y) THEN 1 " +
                                         "ELSE -1 END)")
                .binding("a", "ARRAY[TIMESTAMP '1111-06-10 12:34:56.123456789', TIMESTAMP '2020-05-10 12:34:56.123456789']"))
                .matches("ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '1111-06-10 12:34:56.123456789']");

        // with null in the array, should be in nulls-last order
        assertThat(assertions.function("array_sort", "ARRAY[1, null, 0, null, -1]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(-1, 0, 1, null, null));

        assertThat(assertions.function("array_sort", "ARRAY[1, null, null, -1, 0]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(-1, 0, 1, null, null));

        // invalid functions
        assertTrinoExceptionThrownBy(() -> assertions.function("array_sort", "ARRAY[color('red'), color('blue')]").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);

        assertTrinoExceptionThrownBy(() -> assertions.expression("array_sort(a, (x, y) -> y - x)")
                .binding("a", "ARRAY[2, 1, 2, 4]").evaluate())
                .hasMessage("Lambda comparator must return either -1, 0, or 1");

        assertTrinoExceptionThrownBy(() -> assertions.expression("array_sort(a, (x, y) -> x / COALESCE(y, 0))")
                .binding("a", "ARRAY[1, 2]").evaluate())
                .hasMessage("Lambda comparator must return either -1, 0, or 1");

        assertTrinoExceptionThrownBy(() -> assertions.expression("array_sort(a, (x, y) -> IF(x > y, NULL, IF(x = y, 0, -1)))")
                .binding("a", "ARRAY[2, 3, 2, 4, 1]").evaluate())
                .hasMessage("Lambda comparator must return either -1, 0, or 1");

        assertTrinoExceptionThrownBy(() -> assertions.expression("array_sort(a, (x, y) -> x / COALESCE(y, 0))")
                .binding("a", "ARRAY[1, null]").evaluate())
                .hasMessage("Lambda comparator must return either -1, 0, or 1");
    }

    @Test
    public void testReverse()
    {
        assertThat(assertions.function("reverse", "ARRAY[1]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1));

        assertThat(assertions.function("reverse", "ARRAY[1, 2, 3, 4]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(4, 3, 2, 1));

        assertThat(assertions.function("reverse", "array_sort(ARRAY[2, 3, 4, 1])"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(4, 3, 2, 1));

        assertThat(assertions.function("reverse", "ARRAY[2, BIGINT '3', 4, 1]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(1L, 4L, 3L, 2L));

        assertThat(assertions.function("reverse", "ARRAY['a', 'b', 'c', 'd']"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("d", "c", "b", "a"));

        assertThat(assertions.function("reverse", "ARRAY[TRUE, FALSE]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(false, true));

        assertThat(assertions.function("reverse", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(4.4, 3.3, 2.2, 1.1));
    }

    @Test
    public void testDistinct()
    {
        assertThat(assertions.function("array_distinct", "ARRAY[]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(ImmutableList.of());

        // Order matters here. Result should be stable.
        assertThat(assertions.function("array_distinct", "ARRAY[0, NULL]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(0, null));

        assertThat(assertions.function("array_distinct", "ARRAY[0, NULL, 0, NULL]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(0, null));

        assertThat(assertions.function("array_distinct", "ARRAY[2, 3, 4, 3, 1, 2, 3]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(2, 3, 4, 1));

        assertThat(assertions.function("array_distinct", "ARRAY[0.0E0, NULL]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(asList(0.0, null));

        assertThat(assertions.function("array_distinct", "ARRAY[2.2E0, 3.3E0, 4.4E0, 3.3E0, 1, 2.2E0, 3.3E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(2.2, 3.3, 4.4, 1.0));

        assertThat(assertions.function("array_distinct", "ARRAY[FALSE, NULL]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(asList(false, null));

        assertThat(assertions.function("array_distinct", "ARRAY[FALSE, TRUE, NULL]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(asList(false, true, null));

        assertThat(assertions.function("array_distinct", "ARRAY[TRUE, TRUE, TRUE]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true));

        assertThat(assertions.function("array_distinct", "ARRAY[TRUE, FALSE, FALSE, TRUE]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));

        assertThat(assertions.function("array_distinct", "ARRAY[TIMESTAMP '1973-07-08 22:00:01', TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01']"))
                .hasType(new ArrayType(createTimestampType(0)))
                .isEqualTo(ImmutableList.of(
                        sqlTimestampOf(0, 1973, 7, 8, 22, 0, 1, 0),
                        sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0)));

        assertThat(assertions.function("array_distinct", "ARRAY['2', '3', '2']"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("2", "3"));

        assertThat(assertions.function("array_distinct", "ARRAY['BB', 'CCC', 'BB']"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("BB", "CCC"));

        assertThat(assertions.function("array_distinct", "ARRAY[ARRAY[1], ARRAY[1, 2], ARRAY[1, 2, 3], ARRAY[1, 2]]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1), ImmutableList.of(1, 2), ImmutableList.of(1, 2, 3)));

        assertThat(assertions.function("array_distinct", "ARRAY[NULL, 2.2E0, 3.3E0, 4.4E0, 3.3E0, 1, 2.2E0, 3.3E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(asList(null, 2.2, 3.3, 4.4, 1.0));

        assertThat(assertions.function("array_distinct", "ARRAY[2, 3, NULL, 4, 3, 1, 2, 3]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(2, 3, null, 4, 1));

        assertThat(assertions.function("array_distinct", "ARRAY['BB', 'CCC', 'BB', NULL]"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(asList("BB", "CCC", null));

        assertThat(assertions.function("array_distinct", "ARRAY[NULL]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(asList((Object) null));

        assertThat(assertions.function("array_distinct", "ARRAY[NULL, NULL]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(asList((Object) null));

        assertThat(assertions.function("array_distinct", "ARRAY[NULL, NULL, NULL]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(asList((Object) null));

        // Indeterminate values
        assertThat(assertions.function("array_distinct", "ARRAY[(123, 'abc'), (123, NULL)]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))))
                .isEqualTo(ImmutableList.of(asList(123, "abc"), asList(123, null)));

        assertThat(assertions.function("array_distinct", "ARRAY[(NULL, NULL), (42, 'def'), (NULL, 'abc'), (123, NULL), (42, 'def'), (NULL, NULL), (NULL, 'abc'), (123, NULL)]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))))
                .isEqualTo(ImmutableList.of(asList(null, null), asList(42, "def"), asList(null, "abc"), asList(123, null)));

        // Test for BIGINT-optimized implementation
        assertThat(assertions.function("array_distinct", "ARRAY[CAST(5 AS BIGINT), NULL, CAST(12 AS BIGINT), NULL]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(asList(5L, null, 12L));

        assertThat(assertions.function("array_distinct", "ARRAY[CAST(100 AS BIGINT), NULL, CAST(100 AS BIGINT), NULL, 0, -2, 0]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(asList(100L, null, 0L, -2L));

        assertThat(assertions.function("array_distinct", "ARRAY[2.3, 2.3, 2.2]"))
                .hasType(new ArrayType(createDecimalType(2, 1)))
                .isEqualTo(ImmutableList.of(
                        decimal("2.3", createDecimalType(2, 1)),
                        decimal("2.2", createDecimalType(2, 1))));

        assertThat(assertions.function("array_distinct", "ARRAY[2.330, 1.900, 2.330]"))
                .hasType(new ArrayType(createDecimalType(4, 3)))
                .isEqualTo(ImmutableList.of(
                        decimal("2.330", createDecimalType(4, 3)),
                        decimal("1.900", createDecimalType(4, 3))));
    }

    @Test
    public void testSlice()
    {
        assertThat(assertions.function("slice", "ARRAY[1, 2, 3, 4, 5]", "1", "4"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3, 4));

        assertThat(assertions.function("slice", "ARRAY[1, 2]", "1", "4"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));

        assertThat(assertions.function("slice", "ARRAY[1, 2, 3, 4, 5]", "3", "2"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(3, 4));

        assertThat(assertions.function("slice", "ARRAY['1', '2', '3', '4']", "2", "1"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("2"));

        assertThat(assertions.function("slice", "ARRAY[1, 2, 3, 4]", "3", "3"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(3, 4));

        assertThat(assertions.function("slice", "ARRAY[1, 2, 3, 4]", "-3", "3"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(2, 3, 4));

        assertThat(assertions.function("slice", "ARRAY[1, 2, 3, 4]", "-3", "5"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(2, 3, 4));

        assertThat(assertions.function("slice", "ARRAY[1, 2, 3, 4]", "1", "0"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("slice", "ARRAY[1, 2, 3, 4]", "-2", "0"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("slice", "ARRAY[1, 2, 3, 4]", "-5", "5"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("slice", "ARRAY[1, 2, 3, 4]", "-6", "5"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("slice", "ARRAY[ARRAY[1], ARRAY[2, 3], ARRAY[4, 5, 6]]", "1", "2"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2, 3)));

        assertThat(assertions.function("slice", "ARRAY[2.3, 2.3, 2.2]", "2", "3"))
                .matches("ARRAY[2.3, 2.2]");

        assertThat(assertions.function("slice", "ARRAY[2.330, 1.900, 2.330]", "1", "2"))
                .matches("ARRAY[2.330, 1.900]");

        assertTrinoExceptionThrownBy(() -> assertions.function("slice", "ARRAY[1, 2, 3, 4]", "1", "-1").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.function("slice", "ARRAY[1, 2, 3, 4]", "0", "1").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testArraysOverlap()
    {
        assertThat(assertions.function("arrays_overlap", "ARRAY[1, 2]", "ARRAY[2, 3]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[2, 1]", "ARRAY[2, 3]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[2, 1]", "ARRAY[3, 2]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[1, 2]", "ARRAY[3, 2]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[1, 3]", "ARRAY[2, 4]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[3, 1]", "ARRAY[2, 4]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[3, 1]", "ARRAY[4, 2]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[1, 3]", "ARRAY[4, 2]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[1, 3]", "ARRAY[2, 3, 4]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[3, 1]", "ARRAY[5, 4, 1]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(1 AS BIGINT), 2]", "ARRAY[CAST(2 AS BIGINT), 3]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(2 AS BIGINT), 1]", "ARRAY[CAST(2 AS BIGINT), 3]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(2 AS BIGINT), 1]", "ARRAY[CAST(3 AS BIGINT), 2]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(1 AS BIGINT), 2]", "ARRAY[CAST(3 AS BIGINT), 2]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(1 AS BIGINT), 3]", "ARRAY[CAST(2 AS BIGINT), 4]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(3 AS BIGINT), 1]", "ARRAY[CAST(2 AS BIGINT), 4]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(3 AS BIGINT), 1]", "ARRAY[CAST(4 AS BIGINT), 2]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(1 AS BIGINT), 3]", "ARRAY[CAST(4 AS BIGINT), 2]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY['dog', 'cat']", "ARRAY['monkey', 'dog']"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY['dog', 'cat']", "ARRAY['monkey', 'fox']"))
                .isEqualTo(false);

        // Test arrays with NULLs
        assertThat(assertions.function("arrays_overlap", "ARRAY[1, 2]", "ARRAY[NULL, 2]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[1, 2]", "ARRAY[2, NULL]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[2, 1]", "ARRAY[NULL, 3]"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY[2, 1]", "ARRAY[3, NULL]"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY[NULL, 2]", "ARRAY[1, 2]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[2, NULL]", "ARRAY[1, 2]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[NULL, 3]", "ARRAY[2, 1]"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY[3, NULL]", "ARRAY[2, 1]"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(1 AS BIGINT), 2]", "ARRAY[NULL, CAST(2 AS BIGINT)]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(1 AS BIGINT), 2]", "ARRAY[CAST(2 AS BIGINT), NULL]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(2 AS BIGINT), 1]", "ARRAY[CAST(3 AS BIGINT), NULL]"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(2 AS BIGINT), 1]", "ARRAY[NULL, CAST(3 AS BIGINT)]"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY[NULL, CAST(2 AS BIGINT)]", "ARRAY[CAST(1 AS BIGINT), 2]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(2 AS BIGINT), NULL]", "ARRAY[CAST(1 AS BIGINT), 2]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[CAST(3 AS BIGINT), NULL]", "ARRAY[CAST(2 AS BIGINT), 1]"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY[NULL, CAST(3 AS BIGINT)]", "ARRAY[CAST(2 AS BIGINT), 1]"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY['dog', 'cat']", "ARRAY[NULL, 'dog']"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY['dog', 'cat']", "ARRAY['monkey', NULL]"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY[NULL, 'dog']", "ARRAY['dog', 'cat']"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY['monkey', NULL]", "ARRAY['dog', 'cat']"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY[ARRAY[1, 2], ARRAY[3]]", "ARRAY[ARRAY[4], ARRAY[1, 2]]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[ARRAY[1, 2], ARRAY[3]]", "ARRAY[ARRAY[4], NULL]"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY[ARRAY[2], ARRAY[3]]", "ARRAY[ARRAY[4], ARRAY[1, 2]]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[]", "ARRAY[]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[]", "ARRAY[1, 2]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[]", "ARRAY[NULL]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[true]", "ARRAY[true, false]"))
                .isEqualTo(true);

        assertThat(assertions.function("arrays_overlap", "ARRAY[false]", "ARRAY[true, true]"))
                .isEqualTo(false);

        assertThat(assertions.function("arrays_overlap", "ARRAY[true, false]", "ARRAY[NULL]"))
                .isNull(BooleanType.BOOLEAN);

        assertThat(assertions.function("arrays_overlap", "ARRAY[false]", "ARRAY[true, NULL]"))
                .isNull(BooleanType.BOOLEAN);
    }

    @Test
    public void testArrayIntersect()
    {
        // test basic
        assertThat(assertions.function("array_intersect", "ARRAY[5]", "ARRAY[5]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(5));

        assertThat(assertions.function("array_intersect", "ARRAY[1, 2, 5, 5, 6]", "ARRAY[5, 5, 6, 6, 7, 8]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(5, 6));

        assertThat(assertions.function("array_intersect", "ARRAY[IF (RAND() < 1.0E0, 7, 1) , 2]", "ARRAY[7]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(7));

        assertThat(assertions.function("array_intersect", "ARRAY[CAST(5 AS BIGINT), CAST(5 AS BIGINT)]", "ARRAY[CAST(1 AS BIGINT), CAST(5 AS BIGINT)]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(5L));

        assertThat(assertions.function("array_intersect", "ARRAY[1, 5]", "ARRAY[1.0E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0));

        assertThat(assertions.function("array_intersect", "ARRAY[1.0E0, 5.0E0]", "ARRAY[5.0E0, 5.0E0, 6.0E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(5.0));

        assertThat(assertions.function("array_intersect", "ARRAY[8.3E0, 1.6E0, 4.1E0, 5.2E0]", "ARRAY[4.0E0, 5.2E0, 8.3E0, 9.7E0, 3.5E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(5.2, 8.3));

        assertThat(assertions.function("array_intersect", "ARRAY[5.1E0, 7, 3.0E0, 4.8E0, 10]", "ARRAY[6.5E0, 10.0E0, 1.9E0, 5.1E0, 3.9E0, 4.8E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(10.0, 5.1, 4.8));

        assertThat(assertions.function("array_intersect", "ARRAY[2.3, 2.3, 2.2]", "ARRAY[2.2, 2.3]"))
                .hasType(new ArrayType(createDecimalType(2, 1)))
                .isEqualTo(ImmutableList.of(
                        decimal("2.3", createDecimalType(2, 1)),
                        decimal("2.2", createDecimalType(2, 1))));

        assertThat(assertions.function("array_intersect", "ARRAY[2.330, 1.900, 2.330]", "ARRAY[2.3300, 1.9000]"))
                .hasType(new ArrayType(createDecimalType(5, 4)))
                .isEqualTo(ImmutableList.of(
                        decimal("2.3300", createDecimalType(5, 4)),
                        decimal("1.9000", createDecimalType(5, 4))));

        assertThat(assertions.function("array_intersect", "ARRAY[2, 3]", "ARRAY[2.0, 3.0]"))
                .hasType(new ArrayType(createDecimalType(11, 1)))
                .isEqualTo(ImmutableList.of(
                        decimal("00000000002.0", createDecimalType(11, 1)),
                        decimal("00000000003.0", createDecimalType(11, 1))));

        assertThat(assertions.function("array_intersect", "ARRAY[true]", "ARRAY[true]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true));

        assertThat(assertions.function("array_intersect", "ARRAY[true, false]", "ARRAY[true]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true));

        assertThat(assertions.function("array_intersect", "ARRAY[true, true]", "ARRAY[true, true]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true));

        assertThat(assertions.function("array_intersect", "ARRAY['abc']", "ARRAY['abc', 'bcd']"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("abc"));

        assertThat(assertions.function("array_intersect", "ARRAY['abc', 'abc']", "ARRAY['abc', 'abc']"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("abc"));

        assertThat(assertions.function("array_intersect", "ARRAY['foo', 'bar', 'baz']", "ARRAY['foo', 'test', 'bar']"))
                .hasType(new ArrayType(createVarcharType(4)))
                .isEqualTo(ImmutableList.of("foo", "bar"));

        // test empty results
        assertThat(assertions.function("array_intersect", "ARRAY[]", "ARRAY[5]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY[5, 6]", "ARRAY[]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY[1]", "ARRAY[5]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY[CAST(1 AS BIGINT)]", "ARRAY[CAST(5 AS BIGINT)]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY[true, true]", "ARRAY[false]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY[]", "ARRAY[false]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY[5]", "ARRAY[1.0E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY['abc']", "ARRAY[]"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY[]", "ARRAY['abc', 'bcd']"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY[]", "ARRAY[]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY[]", "ARRAY[NULL]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(ImmutableList.of());

        // test nulls
        assertThat(assertions.function("array_intersect", "ARRAY[NULL]", "ARRAY[NULL, NULL]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(asList((Object) null));

        assertThat(assertions.function("array_intersect", "ARRAY[0, 0, 1, NULL]", "ARRAY[0, 0, 1, NULL]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(0, 1, null));

        assertThat(assertions.function("array_intersect", "ARRAY[0, 0]", "ARRAY[0, 0, NULL]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(0));

        assertThat(assertions.function("array_intersect", "ARRAY[CAST(0 AS BIGINT), CAST(0 AS BIGINT)]", "ARRAY[CAST(0 AS BIGINT), NULL]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(0L));

        assertThat(assertions.function("array_intersect", "ARRAY[0.0E0]", "ARRAY[NULL]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY[0.0E0, NULL]", "ARRAY[0.0E0, NULL]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(asList(0.0, null));

        assertThat(assertions.function("array_intersect", "ARRAY[true, true, false, false, NULL]", "ARRAY[true, false, false, NULL]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(asList(true, false, null));

        assertThat(assertions.function("array_intersect", "ARRAY[false, false]", "ARRAY[false, false, NULL]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(false));

        assertThat(assertions.function("array_intersect", "ARRAY['abc']", "ARRAY[NULL]"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY['']", "ARRAY['', NULL]"))
                .hasType(new ArrayType(createVarcharType(0)))
                .isEqualTo(ImmutableList.of(""));

        assertThat(assertions.function("array_intersect", "ARRAY['', NULL]", "ARRAY['', NULL]"))
                .hasType(new ArrayType(createVarcharType(0)))
                .isEqualTo(asList("", null));

        assertThat(assertions.function("array_intersect", "ARRAY[NULL]", "ARRAY['abc', NULL]"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(singletonList(null));

        assertThat(assertions.function("array_intersect", "ARRAY['abc', NULL, 'xyz', NULL]", "ARRAY[NULL, 'abc', NULL, NULL]"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(asList("abc", null));

        assertThat(assertions.function("array_intersect", "ARRAY[]", "ARRAY[NULL]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_intersect", "ARRAY[NULL]", "ARRAY[NULL]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(singletonList(null));

        // test composite types
        assertThat(assertions.function("array_intersect", "ARRAY[(123, 456), (123, 789)]", "ARRAY[(123, 456), (123, 456), (123, 789)]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, INTEGER))))
                .isEqualTo(ImmutableList.of(asList(123, 456), asList(123, 789)));

        assertThat(assertions.function("array_intersect", "ARRAY[ARRAY[123, 456], ARRAY[123, 789]]", "ARRAY[ARRAY[123, 456], ARRAY[123, 456], ARRAY[123, 789]]"))
                .hasType(new ArrayType(new ArrayType((INTEGER))))
                .isEqualTo(ImmutableList.of(asList(123, 456), asList(123, 789)));

        assertThat(assertions.function("array_intersect", "ARRAY[(123, 'abc'), (123, 'cde')]", "ARRAY[(123, 'abc'), (123, 'cde')]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))))
                .isEqualTo(ImmutableList.of(asList(123, "abc"), asList(123, "cde")));

        assertThat(assertions.function("array_intersect", "ARRAY[(123, 'abc'), (123, 'cde'), NULL]", "ARRAY[(123, 'abc'), (123, 'cde')]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))))
                .isEqualTo(ImmutableList.of(asList(123, "abc"), asList(123, "cde")));

        assertThat(assertions.function("array_intersect", "ARRAY[(123, 'abc'), (123, 'cde'), NULL, NULL]", "ARRAY[(123, 'abc'), (123, 'cde'), NULL]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))))
                .isEqualTo(asList(asList(123, "abc"), asList(123, "cde"), null));

        assertThat(assertions.function("array_intersect", "ARRAY[(123, 'abc'), (123, 'abc')]", "ARRAY[(123, 'abc'), (123, NULL)]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))))
                .isEqualTo(ImmutableList.of(asList(123, "abc")));

        assertThat(assertions.function("array_intersect", "ARRAY[(123, 'abc')]", "ARRAY[(123, NULL)]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))))
                .isEqualTo(ImmutableList.of());

        // Indeterminate values
        assertThat(assertions.function("array_intersect", "ARRAY[(123, 'abc'), (123, NULL)]", "ARRAY[(123, 'abc'), (123, NULL)]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))))
                .isEqualTo(ImmutableList.of(asList(123, "abc"), asList(123, null)));

        assertThat(assertions.function("array_intersect", "ARRAY[(NULL, 'abc'), (123, 'abc')]", "ARRAY[(123, 'abc'),(NULL, 'abc')]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))))
                .isEqualTo(ImmutableList.of(asList(null, "abc"), asList(123, "abc")));

        assertThat(assertions.function("array_intersect", "ARRAY[(NULL, 'abc'), (123, 'abc'), (NULL, 'def'), (NULL, 'abc')]", "ARRAY[(123, 'abc'), (NULL, 'abc'), (123, 'def'), (123, 'abc'), (123, 'abc'), (123, 'abc'), (123, 'abc'), (NULL, 'abc'), (NULL, 'abc'), (NULL, 'abc')]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, createVarcharType(3)))))
                .isEqualTo(ImmutableList.of(asList(123, "abc"), asList(null, "abc")));

        assertThat(assertions.function("array_intersect", "ARRAY[(123, 456), (123, NULL), (42, 43)]", "ARRAY[(123, NULL), (123, 456), (42, NULL), (NULL, 43)]"))
                .hasType(new ArrayType(RowType.anonymous(ImmutableList.of(INTEGER, INTEGER))))
                .isEqualTo(ImmutableList.of(asList(123, null), asList(123, 456)));
    }

    @Test
    public void testArrayUnion()
    {
        assertThat(assertions.function("array_union", "ARRAY[CAST(10 as bigint), NULL, CAST(12 as bigint), NULL]", "ARRAY[NULL, CAST(10 as bigint), NULL, NULL]"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(asList(10L, null, 12L));

        assertThat(assertions.function("array_union", "ARRAY[12]", "ARRAY[10]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(12, 10));

        assertThat(assertions.function("array_union", "ARRAY['foo', 'bar', 'baz']", "ARRAY['foo', 'test', 'bar']"))
                .hasType(new ArrayType(createVarcharType(4)))
                .isEqualTo(ImmutableList.of("foo", "bar", "baz", "test"));

        assertThat(assertions.function("array_union", "ARRAY[NULL]", "ARRAY[NULL, NULL]"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(asList((Object) null));

        assertThat(assertions.function("array_union", "ARRAY['abc', NULL, 'xyz', NULL]", "ARRAY[NULL, 'abc', NULL, NULL]"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(asList("abc", null, "xyz"));

        assertThat(assertions.function("array_union", "ARRAY[1, 5]", "ARRAY[1]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 5));

        assertThat(assertions.function("array_union", "ARRAY[1, 1, 2, 4]", "ARRAY[1, 1, 4, 4]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 4));

        assertThat(assertions.function("array_union", "ARRAY[2, 8]", "ARRAY[8, 3]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(2, 8, 3));

        assertThat(assertions.function("array_union", "ARRAY[IF (RAND() < 1.0E0, 7, 1) , 2]", "ARRAY[7]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(7, 2));

        assertThat(assertions.function("array_union", "ARRAY[1, 5]", "ARRAY[1.0E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0, 5.0));

        assertThat(assertions.function("array_union", "ARRAY[8.3E0, 1.6E0, 4.1E0, 5.2E0]", "ARRAY[4.0E0, 5.2E0, 8.3E0, 9.7E0, 3.5E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(8.3, 1.6, 4.1, 5.2, 4.0, 9.7, 3.5));

        assertThat(assertions.function("array_union", "ARRAY[5.1E0, 7, 3.0E0, 4.8E0, 10]", "ARRAY[6.5E0, 10.0E0, 1.9E0, 5.1E0, 3.9E0, 4.8E0]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(5.1, 7.0, 3.0, 4.8, 10.0, 6.5, 1.9, 3.9));

        assertThat(assertions.function("array_union", "ARRAY[ARRAY[4, 5], ARRAY[6, 7]]", "ARRAY[ARRAY[4, 5], ARRAY[6, 8]]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(4, 5), ImmutableList.of(6, 7), ImmutableList.of(6, 8)));

        // results unique based on IS DISTINCT semantics
        assertThat(assertions.function("array_union", "ARRAY[NaN()]", "ARRAY[NaN()]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(NaN));

        assertThat(assertions.function("array_union", "ARRAY[1, NaN(), 3]", "ARRAY[1, NaN()]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.0, NaN, 3.0));
    }

    @Test
    public void testComparison()
    {
        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1, 2, 3]")
                .binding("b", "ARRAY[1, 2, 3]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1, 2, 3]")
                .binding("b", "ARRAY[1, 2, 3]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, FALSE]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, FALSE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY['puppies', 'kittens']")
                .binding("b", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY['puppies', 'kittens']")
                .binding("b", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[timestamp '2012-10-31 08:00 UTC']")
                .binding("b", "ARRAY[timestamp '2012-10-31 01:00 America/Los_Angeles']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[timestamp '2012-10-31 08:00 UTC']")
                .binding("b", "ARRAY[timestamp '2012-10-31 01:00 America/Los_Angeles']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1.0, 2.0, 3.0]")
                .binding("b", "ARRAY[1.0, 2.0, 3.0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1.0, 2.0, 3.0]")
                .binding("b", "ARRAY[1.0, 2.0, 3.1]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]")
                .binding("b", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]")
                .binding("b", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1.0, 2.0, 3.0]")
                .binding("b", "ARRAY[1.0, 2.0, 3.0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1.0, 2.0, 3.0]")
                .binding("b", "ARRAY[1.0, 2.0, 3.1]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]")
                .binding("b", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]")
                .binding("b", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1, 2, null]")
                .binding("b", "ARRAY[1, null]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY['1', '2', null]")
                .binding("b", "ARRAY['1', null]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1.0, 2.0, null]")
                .binding("b", "ARRAY[1.0, null]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1.0E0, 2.0E0, null]")
                .binding("b", "ARRAY[1.0E0, null]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1, 2, null]")
                .binding("b", "ARRAY[1, 2, null]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY['1', '2', null]")
                .binding("b", "ARRAY['1', '2', null]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1.0, 2.0, null]")
                .binding("b", "ARRAY[1.0, 2.0, null]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1.0E0, 2.0E0, null]")
                .binding("b", "ARRAY[1.0E0, 2.0E0, null]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1, 3, null]")
                .binding("b", "ARRAY[1, 2, null]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1E0, 3E0, null]")
                .binding("b", "ARRAY[1E0, 2E0, null]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY['1', '3', null]")
                .binding("b", "ARRAY['1', '2', null]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[ARRAY[1], ARRAY[null], ARRAY[2]]")
                .binding("b", "ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[ARRAY[1], ARRAY[null], ARRAY[3]]")
                .binding("b", "ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[5]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[5]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1, 2, 3]")
                .binding("b", "ARRAY[3, 2, 1]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1, 2, 3]")
                .binding("b", "ARRAY[3, 2, 1]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[TRUE, FALSE, TRUE]")
                .binding("b", "ARRAY[TRUE]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[TRUE, FALSE, TRUE]")
                .binding("b", "ARRAY[TRUE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[FALSE, FALSE]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[FALSE, FALSE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0]")
                .binding("b", "ARRAY[11.1E0, 22.1E0, 1.1E0, 44.1E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0]")
                .binding("b", "ARRAY[11.1E0, 22.1E0, 1.1E0, 44.1E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY['puppies', 'kittens']")
                .binding("b", "ARRAY['z', 'f', 's', 'd', 'g']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY['puppies', 'kittens']")
                .binding("b", "ARRAY['z', 'f', 's', 'd', 'g']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '04:05:06.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '04:05:06.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5, 6]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5, 6]]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2, 3], ARRAY[4, 5]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2, 3], ARRAY[4, 5]]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a = b")
                .binding("a", "ARRAY[1.0, 2.0, 3.0]")
                .binding("b", "ARRAY[1.0, 2.0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1.0, 2.0, 3.0]")
                .binding("b", "ARRAY[1.0, 2.0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1, 2, null]")
                .binding("b", "ARRAY[1, 2, null]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1, 2, null]")
                .binding("b", "ARRAY[1, null]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[1, 3, null]")
                .binding("b", "ARRAY[1, 2, null]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[ARRAY[1], ARRAY[null], ARRAY[2]]")
                .binding("b", "ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "ARRAY[ARRAY[1], ARRAY[null], ARRAY[3]]")
                .binding("b", "ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]]"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 20, 40, 50]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 20, 40, 50]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 40]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 40]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[10, 20]")
                .binding("b", "ARRAY[10, 20, 30]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[10, 20]")
                .binding("b", "ARRAY[10, 20, 30]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, TRUE, TRUE]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, TRUE, TRUE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[TRUE, FALSE, FALSE]")
                .binding("b", "ARRAY[TRUE, TRUE]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[TRUE, FALSE, FALSE]")
                .binding("b", "ARRAY[TRUE, TRUE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, FALSE, FALSE]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, FALSE, FALSE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 4.4E0, 4.4E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 4.4E0, 4.4E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 5.5E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 5.5E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[1.1E0, 2.2E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 5.5E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[1.1E0, 2.2E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 5.5E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'lizards', 'lizards']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'lizards', 'lizards']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'lizards']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'lizards']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY['puppies', 'kittens']")
                .binding("b", "ARRAY['puppies', 'kittens', 'lizards']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY['puppies', 'kittens']")
                .binding("b", "ARRAY['puppies', 'kittens', 'lizards']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '04:05:06.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '04:05:06.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '04:05:06.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '04:05:06.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5, 6]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5, 6]]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 5, 6]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 5, 6]]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[1.0, 2.0, 3.0]")
                .binding("b", "ARRAY[1.0, 2.0, 3.0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[1.0, 2.0, 3.0]")
                .binding("b", "ARRAY[1.0, 2.0, 3.0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[1.0, 2.0, 3.0]")
                .binding("b", "ARRAY[1.0, 2.0, 3.1]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[1.0, 2.0, 3.0]")
                .binding("b", "ARRAY[1.0, 2.0, 3.1]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]")
                .binding("b", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]")
                .binding("b", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]")
                .binding("b", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[1234567890.1234567890, 9876543210.9876543210]")
                .binding("b", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[1234567890.1234567890, 0]")
                .binding("b", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]")
                .binding("b", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 20, 20]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 20, 20]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 20]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 20]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[TRUE, TRUE, TRUE]")
                .binding("b", "ARRAY[TRUE, TRUE, FALSE]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[TRUE, TRUE, TRUE]")
                .binding("b", "ARRAY[TRUE, TRUE, FALSE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[TRUE, TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, TRUE]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[TRUE, TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, TRUE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 2.2E0, 4.4E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 2.2E0, 4.4E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 3.3E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 3.3E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'kittens', 'kittens']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'kittens', 'kittens']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:20.456-08:00']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:20.456-08:00']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4]]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 3, 4]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 3, 4]]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[50]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[50]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 20, 30]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 20, 30]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, FALSE, true]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, FALSE, true]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, FALSE]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[TRUE, FALSE]")
                .binding("b", "ARRAY[TRUE, FALSE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[2.2E0, 5.5E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[2.2E0, 5.5E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'lizards']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'lizards']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY['puppies', 'kittens']")
                .binding("b", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY['puppies', 'kittens']")
                .binding("b", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '04:05:06.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '04:05:06.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <= b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 5, 6]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 5, 6]]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 20, 30]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[10, 20, 30]")
                .binding("b", "ARRAY[10, 20, 30]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[TRUE, FALSE, TRUE]")
                .binding("b", "ARRAY[TRUE, FALSE, TRUE]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[TRUE, FALSE, TRUE]")
                .binding("b", "ARRAY[TRUE, FALSE, TRUE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[TRUE, FALSE, TRUE]")
                .binding("b", "ARRAY[TRUE]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[TRUE, FALSE, TRUE]")
                .binding("b", "ARRAY[TRUE]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]")
                .binding("b", "ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'kittens', 'kittens']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY['puppies', 'kittens', 'lizards']")
                .binding("b", "ARRAY['puppies', 'kittens', 'kittens']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY['puppies', 'kittens']")
                .binding("b", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY['puppies', 'kittens']")
                .binding("b", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00', TIME '10:20:30.456-08:00']")
                .binding("b", "ARRAY[TIME '01:02:03.456-08:00', TIME '10:20:30.456-08:00']"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4]]"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]"))
                .isEqualTo(true);

        assertThat(assertions.expression("a < b")
                .binding("a", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]")
                .binding("b", "ARRAY[ARRAY[1, 2], ARRAY[3, 4, 5]]"))
                .isEqualTo(false);
    }

    @Test
    public void testDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS ARRAY(UNKNOWN))", "CAST(NULL AS ARRAY(UNKNOWN))"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[NULL]", "ARRAY[NULL]"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "ARRAY[1, 2]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[1, 2]", "NULL"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[1, 2]", "ARRAY[1, 2]"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[1, 2, 3]", "ARRAY[1, 2]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[1, 2]", "ARRAY[1, NULL]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[1, 2]", "ARRAY[1, 3]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[1, NULL]", "ARRAY[1, NULL]"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[1, NULL]", "ARRAY[1, NULL]"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[1, 2, NULL]", "ARRAY[1, 2]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[TRUE, FALSE]", "ARRAY[TRUE, FALSE]"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[TRUE, NULL]", "ARRAY[TRUE, FALSE]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[FALSE, NULL]", "ARRAY[NULL, FALSE]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY['puppies', 'kittens']", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY['puppies', NULL]", "ARRAY['puppies', 'kittens']"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY['puppies', NULL]", "ARRAY[NULL, 'kittens']"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[ARRAY['puppies'], ARRAY['kittens']]", "ARRAY[ARRAY['puppies'], ARRAY['kittens']]"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[ARRAY['puppies'], NULL]", "ARRAY[ARRAY['puppies'], ARRAY['kittens']]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "ARRAY[ARRAY['puppies'], NULL]", "ARRAY[NULL, ARRAY['kittens']]"))
                .isEqualTo(true);
    }

    @Test
    public void testArrayRemove()
    {
        assertThat(assertions.function("array_remove", "ARRAY['foo', 'bar', 'baz']", "'foo'"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("bar", "baz"));

        assertThat(assertions.function("array_remove", "ARRAY['foo', 'bar', 'baz']", "'bar'"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("foo", "baz"));

        assertThat(assertions.function("array_remove", "ARRAY['foo', 'bar', 'baz']", "'baz'"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("foo", "bar"));

        assertThat(assertions.function("array_remove", "ARRAY['foo', 'bar', 'baz']", "'zzz'"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("foo", "bar", "baz"));

        assertThat(assertions.function("array_remove", "ARRAY['foo', 'foo', 'foo']", "'foo'"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_remove", "ARRAY[NULL, 'bar', 'baz']", "'foo'"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(asList(null, "bar", "baz"));

        assertThat(assertions.function("array_remove", "ARRAY['foo', 'bar', NULL]", "'foo'"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(asList("bar", null));

        assertThat(assertions.function("array_remove", "ARRAY[1, 2, 3]", "1"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(2, 3));

        assertThat(assertions.function("array_remove", "ARRAY[1, 2, 3]", "2"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 3));

        assertThat(assertions.function("array_remove", "ARRAY[1, 2, 3]", "3"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));

        assertThat(assertions.function("array_remove", "ARRAY[1, 2, 3]", "4"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3));

        assertThat(assertions.function("array_remove", "ARRAY[1, 1, 1]", "1"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("array_remove", "ARRAY[NULL, 2, 3]", "1"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(null, 2, 3));

        assertThat(assertions.function("array_remove", "ARRAY[1, NULL, 3]", "1"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(asList(null, 3));

        assertThat(assertions.function("array_remove", "ARRAY[-1.23E0, 3.14E0]", "3.14E0"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(-1.23));

        assertThat(assertions.function("array_remove", "ARRAY[3.14E0]", "0.0E0"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(3.14));

        assertThat(assertions.function("array_remove", "ARRAY[sqrt(-1), 3.14E0]", "3.14E0"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(NaN));

        assertThat(assertions.function("array_remove", "ARRAY[-1.23E0, sqrt(-1)]", "nan()"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(-1.23, NaN));

        assertThat(assertions.function("array_remove", "ARRAY[-1.23E0, nan()]", "nan()"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(-1.23, NaN));

        assertThat(assertions.function("array_remove", "ARRAY[-1.23E0, infinity()]", "-1.23E0"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(POSITIVE_INFINITY));

        assertThat(assertions.function("array_remove", "ARRAY[infinity(), 3.14E0]", "infinity()"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(3.14));

        assertThat(assertions.function("array_remove", "ARRAY[-1.23E0, NULL, 3.14E0]", "3.14E0"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(asList(-1.23, null));

        assertThat(assertions.function("array_remove", "ARRAY[TRUE, FALSE, TRUE]", "TRUE"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(false));

        assertThat(assertions.function("array_remove", "ARRAY[TRUE, FALSE, TRUE]", "FALSE"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, true));

        assertThat(assertions.function("array_remove", "ARRAY[NULL, FALSE, TRUE]", "TRUE"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(asList(null, false));

        assertThat(assertions.function("array_remove", "ARRAY[ARRAY['foo'], ARRAY['bar'], ARRAY['baz']]", "ARRAY['bar']"))
                .hasType(new ArrayType(new ArrayType(createVarcharType(3))))
                .isEqualTo(ImmutableList.of(ImmutableList.of("foo"), ImmutableList.of("baz")));

        assertThat(assertions.function("array_remove", "ARRAY[1.0, 2.0, 3.0]", "2.0"))
                .hasType(new ArrayType(createDecimalType(2, 1)))
                .isEqualTo(ImmutableList.of(
                        decimal("1.0", createDecimalType(2, 1)),
                        decimal("3.0", createDecimalType(2, 1))));

        assertThat(assertions.function("array_remove", "ARRAY[1.0, 2.0, 3.0]", "4.0"))
                .hasType(new ArrayType(createDecimalType(2, 1)))
                .isEqualTo(ImmutableList.of(
                        decimal("1.0", createDecimalType(2, 1)),
                        decimal("2.0", createDecimalType(2, 1)),
                        decimal("3.0", createDecimalType(2, 1))));

        assertThat(assertions.function("array_remove", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]", "1234567890.1234567890"))
                .hasType(new ArrayType(createDecimalType(22, 10)))
                .isEqualTo(ImmutableList.of(
                        decimal("9876543210.9876543210", createDecimalType(22, 10)),
                        decimal("123123123456.6549876543", createDecimalType(22, 10))));

        assertThat(assertions.function("array_remove", "ARRAY[1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]", "4.0"))
                .hasType(new ArrayType(createDecimalType(22, 10)))
                .isEqualTo(ImmutableList.of(
                        decimal("1234567890.1234567890", createDecimalType(22, 10)),
                        decimal("9876543210.9876543210", createDecimalType(22, 10)),
                        decimal("123123123456.6549876543", createDecimalType(22, 10))));

        assertTrinoExceptionThrownBy(() -> assertions.function("array_remove", "ARRAY[ARRAY[CAST(null AS BIGINT)]]", "ARRAY[CAST(1 AS BIGINT)]").evaluate())
                .hasErrorCode(NOT_SUPPORTED);

        assertTrinoExceptionThrownBy(() -> assertions.function("array_remove", "ARRAY[ARRAY[CAST(null AS BIGINT)]]", "ARRAY[CAST(null AS BIGINT)]").evaluate())
                .hasErrorCode(NOT_SUPPORTED);

        assertTrinoExceptionThrownBy(() -> assertions.function("array_remove", "ARRAY[ARRAY[CAST(1 AS BIGINT)]]", "ARRAY[CAST(null AS BIGINT)]").evaluate())
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testRepeat()
    {
        // concrete values
        assertThat(assertions.function("repeat", "1", "5"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 1, 1, 1, 1));

        assertThat(assertions.function("repeat", "'varchar'", "3"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("varchar", "varchar", "varchar"));

        assertThat(assertions.function("repeat", "true", "1"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true));

        assertThat(assertions.function("repeat", "0.5E0", "4"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(0.5, 0.5, 0.5, 0.5));

        assertThat(assertions.function("repeat", "array[1]", "4"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1), ImmutableList.of(1), ImmutableList.of(1), ImmutableList.of(1)));

        // null values
        assertThat(assertions.function("repeat", "null", "4"))
                .hasType(new ArrayType(UNKNOWN))
                .isEqualTo(asList(null, null, null, null));

        assertThat(assertions.function("repeat", "CAST(null as bigint)", "4"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(asList(null, null, null, null));

        assertThat(assertions.function("repeat", "CAST(null as double)", "4"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(asList(null, null, null, null));

        assertThat(assertions.function("repeat", "CAST(null as varchar)", "4"))
                .hasType(new ArrayType(VARCHAR))
                .isEqualTo(asList(null, null, null, null));

        assertThat(assertions.function("repeat", "CAST(null as boolean)", "4"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(asList(null, null, null, null));

        assertThat(assertions.function("repeat", "CAST(null as array(boolean))", "4"))
                .hasType(new ArrayType(new ArrayType(BOOLEAN)))
                .isEqualTo(asList(null, null, null, null));

        // 0 counts
        assertThat(assertions.function("repeat", "CAST(null as bigint)", "0"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("repeat", "1", "0"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("repeat", "'varchar'", "0"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("repeat", "true", "0"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("repeat", "0.5E0", "0"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of());

        assertThat(assertions.function("repeat", "array[1]", "0"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of());

        // illegal inputs
        assertTrinoExceptionThrownBy(() -> assertions.function("repeat", "2", "-1").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.function("repeat", "1", "1000000").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.function("repeat", "'loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongvarchar'", "9999").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.function("repeat", "array[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]", "9999").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(null as array(bigint))"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[1,2,3]"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "array[1,2,3,null]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array['test1', 'test2', 'test3', 'test4']"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "array['test1', 'test2', 'test3', null]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array['test1', null, 'test2', 'test3']"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[null, 'test1', 'test2', 'test3']"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[null, time '12:34:56', time '01:23:45']"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[null, timestamp '2016-01-02 12:34:56', timestamp '2016-12-23 01:23:45']"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[null]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[null, null, null]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[row(1), row(2), row(3)]"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "array[CAST(row(1) as row(a bigint)), CAST(null as row(a bigint)), CAST(row(3) as row(a bigint))]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[CAST(row(1) as row(a bigint)), CAST(row(null) as row(a bigint)), CAST(row(3) as row(a bigint))]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[map(array[2], array[-2]), map(array[1], array[-1])]"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "array[map(array[2], array[-2]), null]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[map(array[2], array[-2]), map(array[1], array[null])]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[array[1], array[2], array[3]]"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "array[array[1], array[null], array[3]]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[array[1], array[2], null]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[1E0, 2E0, null]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[1E0, 2E0, 3E0]"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "array[true, false, null]"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "array[true, false, true]"))
                .isEqualTo(false);
    }

    @Test
    public void testSequence()
    {
        // defaults to a step of 1
        assertThat(assertions.function("sequence", "1", "5"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(1L, 2L, 3L, 4L, 5L));

        assertThat(assertions.function("sequence", "-10", "-5"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(-10L, -9L, -8L, -7L, -6L, -5L));

        assertThat(assertions.function("sequence", "-5", "2"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(-5L, -4L, -3L, -2L, -1L, 0L, 1L, 2L));

        assertThat(assertions.function("sequence", "2", "2"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(2L));

        assertThat(assertions.function("sequence", "DATE '2016-04-12'", "DATE '2016-04-14'"))
                .matches("ARRAY[DATE '2016-04-12', DATE '2016-04-13', DATE '2016-04-14']");

        // defaults to a step of -1
        assertThat(assertions.function("sequence", "5", "1"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(5L, 4L, 3L, 2L, 1L));

        assertThat(assertions.function("sequence", "-5", "-10"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(-5L, -6L, -7L, -8L, -9L, -10L));

        assertThat(assertions.function("sequence", "2", "-5"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(2L, 1L, 0L, -1L, -2L, -3L, -4L, -5L));

        assertThat(assertions.function("sequence", "DATE '2016-04-14'", "DATE '2016-04-12'"))
                .matches("ARRAY[DATE '2016-04-14', DATE '2016-04-13', DATE '2016-04-12']");

        // with increment
        assertThat(assertions.function("sequence", "1", "9", "4"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(1L, 5L, 9L));

        assertThat(assertions.function("sequence", "-10", "-5", "2"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(-10L, -8L, -6L));

        assertThat(assertions.function("sequence", "-5", "2", "3"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(-5L, -2L, 1L));

        assertThat(assertions.function("sequence", "2", "2", "2"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(2L));

        assertThat(assertions.function("sequence", "5", "1", "-1"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(5L, 4L, 3L, 2L, 1L));

        assertThat(assertions.function("sequence", "10", "2", "-2"))
                .hasType(new ArrayType(BIGINT))
                .isEqualTo(ImmutableList.of(10L, 8L, 6L, 4L, 2L));

        // failure modes
        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "2", "-1", "1").evaluate())
                .hasMessage("sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "-1", "-10", "1").evaluate())
                .hasMessage("sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "1", "1000000").evaluate())
                .hasMessage("result of sequence function must not have more than 10000 entries");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "DATE '2000-04-14'", "DATE '2030-04-12'").evaluate())
                .hasMessage("result of sequence function must not have more than 10000 entries");
    }

    @Test
    public void testSequenceDateTimeDayToSecond()
    {
        assertThat(assertions.function("sequence", "DATE '2016-04-12'", "DATE '2016-04-14'", "interval '1' day"))
                .matches("ARRAY[DATE '2016-04-12', DATE '2016-04-13', DATE '2016-04-14']");

        assertThat(assertions.function("sequence", "DATE '2016-04-14'", "DATE '2016-04-12'", "interval '-1' day"))
                .matches("ARRAY[DATE '2016-04-14', DATE '2016-04-13', DATE '2016-04-12']");

        assertThat(assertions.function("sequence", "DATE '2016-04-12'", "DATE '2016-04-16'", "interval '2' day"))
                .matches("ARRAY[DATE '2016-04-12', DATE '2016-04-14', DATE '2016-04-16']");

        assertThat(assertions.function("sequence", "DATE '2016-04-16'", "DATE '2016-04-12'", "interval '-2' day"))
                .matches("ARRAY[DATE '2016-04-16', DATE '2016-04-14', DATE '2016-04-12']");

        assertThat(assertions.function("sequence", "timestamp '2016-04-16 01:00:10'", "timestamp '2016-04-16 01:07:00'", "interval '3' minute"))
                .matches("ARRAY[TIMESTAMP '2016-04-16 01:00:10', TIMESTAMP '2016-04-16 01:03:10', TIMESTAMP '2016-04-16 01:06:10']");

        assertThat(assertions.function("sequence", "timestamp '2016-04-16 01:10:10'", "timestamp '2016-04-16 01:03:00'", "interval '-3' minute"))
                .matches("ARRAY[TIMESTAMP '2016-04-16 01:10:10', TIMESTAMP '2016-04-16 01:07:10', TIMESTAMP '2016-04-16 01:04:10']");

        assertThat(assertions.function("sequence", "timestamp '2016-04-16 01:00:10'", "timestamp '2016-04-16 01:01:00'", "interval '20' second"))
                .matches("ARRAY[TIMESTAMP '2016-04-16 01:00:10',TIMESTAMP '2016-04-16 01:00:30',TIMESTAMP '2016-04-16 01:00:50']");

        assertThat(assertions.function("sequence", "timestamp '2016-04-16 01:01:10'", "timestamp '2016-04-16 01:00:20'", "interval '-20' second"))
                .matches("ARRAY[TIMESTAMP '2016-04-16 01:01:10', TIMESTAMP '2016-04-16 01:00:50', TIMESTAMP '2016-04-16 01:00:30']");

        assertThat(assertions.function("sequence", "timestamp '2016-04-16 01:00:10'", "timestamp '2016-04-18 01:01:00'", "interval '19' hour"))
                .matches("ARRAY[TIMESTAMP '2016-04-16 01:00:10',TIMESTAMP '2016-04-16 20:00:10',TIMESTAMP '2016-04-17 15:00:10']");

        assertThat(assertions.function("sequence", "timestamp '2016-04-16 01:00:10'", "timestamp '2016-04-14 01:00:20'", "interval '-19' hour"))
                .matches("ARRAY[TIMESTAMP '2016-04-16 01:00:10',TIMESTAMP '2016-04-15 06:00:10',TIMESTAMP '2016-04-14 11:00:10']");

        // failure modes
        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "DATE '2016-04-12'", "DATE '2016-04-14'", "interval '-1' day").evaluate())
                .hasMessage("sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "DATE '2016-04-14'", "DATE '2016-04-12'", "interval '1' day").evaluate())
                .hasMessage("sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "DATE '2000-04-14'", "DATE '2030-04-12'", "interval '1' day").evaluate())
                .hasMessage("result of sequence function must not have more than 10000 entries");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "DATE '2018-01-01'", "DATE '2018-01-04'", "interval '18' hour").evaluate())
                .hasMessage("sequence step must be a day interval if start and end values are dates");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "timestamp '2016-04-16 01:00:10'", "timestamp '2016-04-16 01:01:00'", "interval '-20' second").evaluate())
                .hasMessage("sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "timestamp '2016-04-16 01:10:10'", "timestamp '2016-04-16 01:01:00'", "interval '20' second").evaluate())
                .hasMessage("sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "timestamp '2016-04-16 01:00:10'", "timestamp '2016-04-16 09:01:00'", "interval '1' second").evaluate())
                .hasMessage("result of sequence function must not have more than 10000 entries");
    }

    @Test
    public void testSequenceDateTimeYearToMonth()
    {
        assertThat(assertions.function("sequence", "DATE '2016-04-12'", "DATE '2016-06-12'", "interval '1' month"))
                .matches("ARRAY[DATE '2016-04-12', DATE '2016-05-12', DATE '2016-06-12']");

        assertThat(assertions.function("sequence", "DATE '2016-06-12'", "DATE '2016-04-12'", "interval '-1' month"))
                .matches("ARRAY[DATE '2016-06-12', DATE '2016-05-12', DATE '2016-04-12']");

        assertThat(assertions.function("sequence", "DATE '2016-04-12'", "DATE '2016-08-12'", "interval '2' month"))
                .matches("ARRAY[DATE '2016-04-12', DATE '2016-06-12', DATE '2016-08-12']");

        assertThat(assertions.function("sequence", "DATE '2016-08-12'", "DATE '2016-04-12'", "interval '-2' month"))
                .matches("ARRAY[DATE '2016-08-12', DATE '2016-06-12', DATE '2016-04-12']");

        assertThat(assertions.function("sequence", "DATE '2016-04-12'", "DATE '2018-04-12'", "interval '1' year"))
                .matches("ARRAY[DATE '2016-04-12', DATE '2017-04-12', DATE '2018-04-12']");

        assertThat(assertions.function("sequence", "DATE '2018-04-12'", "DATE '2016-04-12'", "interval '-1' year"))
                .matches("ARRAY[DATE '2018-04-12', DATE '2017-04-12', DATE '2016-04-12']");

        assertThat(assertions.function("sequence", "timestamp '2016-04-16 01:00:10'", "timestamp '2016-09-16 01:10:00'", "interval '2' month"))
                .matches("ARRAY[TIMESTAMP '2016-04-16 01:00:10',TIMESTAMP '2016-06-16 01:00:10',TIMESTAMP '2016-08-16 01:00:10']");

        assertThat(assertions.function("sequence", "timestamp '2016-09-16 01:10:10'", "timestamp '2016-04-16 01:00:00'", "interval '-2' month"))
                .matches("ARRAY[TIMESTAMP '2016-09-16 01:10:10', TIMESTAMP '2016-07-16 01:10:10', TIMESTAMP '2016-05-16 01:10:10']");

        assertThat(assertions.function("sequence", "timestamp '2016-04-16 01:00:10'", "timestamp '2021-04-16 01:01:00'", "interval '2' year"))
                .matches("ARRAY[TIMESTAMP '2016-04-16 01:00:10', TIMESTAMP '2018-04-16 01:00:10', TIMESTAMP '2020-04-16 01:00:10']");

        assertThat(assertions.function("sequence", "timestamp '2016-04-16 01:01:10'", "timestamp '2011-04-16 01:00:00'", "interval '-2' year"))
                .matches("ARRAY[TIMESTAMP '2016-04-16 01:01:10', TIMESTAMP '2014-04-16 01:01:10', TIMESTAMP '2012-04-16 01:01:10']");

        // failure modes
        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "DATE '2016-06-12'", "DATE '2016-04-12'", "interval '1' month").evaluate())
                .hasMessage("sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "DATE '2016-04-12'", "DATE '2016-06-12'", "interval '-1' month").evaluate())
                .hasMessage("sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "DATE '2000-04-12'", "DATE '3000-06-12'", "interval '1' month").evaluate())
                .hasMessage("result of sequence function must not have more than 10000 entries");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "timestamp '2016-05-16 01:00:10'", "timestamp '2016-04-16 01:01:00'", "interval '1' month").evaluate())
                .hasMessage("sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "timestamp '2016-04-16 01:10:10'", "timestamp '2016-05-16 01:01:00'", "interval '-1' month").evaluate())
                .hasMessage("sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");

        assertTrinoExceptionThrownBy(() -> assertions.function("sequence", "timestamp '2016-04-16 01:00:10'", "timestamp '3000-04-16 09:01:00'", "interval '1' month").evaluate())
                .hasMessage("result of sequence function must not have more than 10000 entries");
    }

    @Test
    public void testFlatten()
    {
        // BOOLEAN Tests
        assertThat(assertions.function("flatten", "ARRAY[ARRAY[TRUE, FALSE], ARRAY[FALSE]]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false, false));

        assertThat(assertions.function("flatten", "ARRAY[ARRAY[TRUE, FALSE], NULL]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));

        assertThat(assertions.function("flatten", "ARRAY[ARRAY[TRUE, FALSE]]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));

        assertThat(assertions.function("flatten", "ARRAY[NULL, ARRAY[TRUE, FALSE]]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false));

        assertThat(assertions.function("flatten", "ARRAY[ARRAY[TRUE], ARRAY[FALSE], ARRAY[TRUE, FALSE]]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false, true, false));

        assertThat(assertions.function("flatten", "ARRAY[NULL, ARRAY[TRUE], NULL, ARRAY[FALSE], ARRAY[FALSE, TRUE]]"))
                .hasType(new ArrayType(BOOLEAN))
                .isEqualTo(ImmutableList.of(true, false, false, true));

        // VARCHAR Tests
        assertThat(assertions.function("flatten", "ARRAY[ARRAY['1', '2'], ARRAY['3']]"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("1", "2", "3"));

        assertThat(assertions.function("flatten", "ARRAY[ARRAY['1', '2'], NULL]"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("1", "2"));

        assertThat(assertions.function("flatten", "ARRAY[NULL, ARRAY['1', '2']]"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("1", "2"));

        assertThat(assertions.function("flatten", "ARRAY[ARRAY['0'], ARRAY['1'], ARRAY['2', '3']]"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("0", "1", "2", "3"));

        assertThat(assertions.function("flatten", "ARRAY[NULL, ARRAY['0'], NULL, ARRAY['1'], ARRAY['2', '3']]"))
                .hasType(new ArrayType(createVarcharType(1)))
                .isEqualTo(ImmutableList.of("0", "1", "2", "3"));

        // BIGINT Tests
        assertThat(assertions.function("flatten", "ARRAY[ARRAY[1, 2], ARRAY[3]]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2, 3));

        assertThat(assertions.function("flatten", "ARRAY[ARRAY[1, 2], NULL]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));

        assertThat(assertions.function("flatten", "ARRAY[NULL, ARRAY[1, 2]]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(1, 2));

        assertThat(assertions.function("flatten", "ARRAY[ARRAY[0], ARRAY[1], ARRAY[2, 3]]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(0, 1, 2, 3));

        assertThat(assertions.function("flatten", "ARRAY[NULL, ARRAY[0], NULL, ARRAY[1], ARRAY[2, 3]]"))
                .hasType(new ArrayType(INTEGER))
                .isEqualTo(ImmutableList.of(0, 1, 2, 3));

        // DOUBLE Tests
        assertThat(assertions.function("flatten", "ARRAY[ARRAY[1.2E0, 2.2E0], ARRAY[3.2E0]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.2, 2.2, 3.2));

        assertThat(assertions.function("flatten", "ARRAY[ARRAY[1.2E0, 2.2E0], NULL]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.2, 2.2));

        assertThat(assertions.function("flatten", "ARRAY[NULL, ARRAY[1.2E0, 2.2E0]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(1.2, 2.2));

        assertThat(assertions.function("flatten", "ARRAY[ARRAY[0.2E0], ARRAY[1.2E0], ARRAY[2.2E0, 3.2E0]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(0.2, 1.2, 2.2, 3.2));

        assertThat(assertions.function("flatten", "ARRAY[NULL, ARRAY[0.2E0], NULL, ARRAY[1.2E0], ARRAY[2.2E0, 3.2E0]]"))
                .hasType(new ArrayType(DOUBLE))
                .isEqualTo(ImmutableList.of(0.2, 1.2, 2.2, 3.2));

        // ARRAY(BIGINT) tests
        assertThat(assertions.function("flatten", "ARRAY[ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[ARRAY[5, 6], ARRAY[7, 8]]]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3, 4), ImmutableList.of(5, 6), ImmutableList.of(7, 8)));

        assertThat(assertions.function("flatten", "ARRAY[ARRAY[ARRAY[1, 2], ARRAY[3, 4]], NULL]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3, 4)));

        assertThat(assertions.function("flatten", "ARRAY[NULL, ARRAY[ARRAY[5, 6], ARRAY[7, 8]]]"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(5, 6), ImmutableList.of(7, 8)));

        // MAP<BIGINT, BIGINT> Tests
        assertThat(assertions.function("flatten", "ARRAY[ARRAY[MAP(ARRAY[1, 2], ARRAY[1, 2])], ARRAY[MAP(ARRAY[3, 4], ARRAY[3, 4])]]"))
                .hasType(new ArrayType(mapType(INTEGER, INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableMap.of(1, 1, 2, 2), ImmutableMap.of(3, 3, 4, 4)));

        assertThat(assertions.function("flatten", "ARRAY[ARRAY[MAP(ARRAY[1, 2], ARRAY[1, 2])], NULL]"))
                .hasType(new ArrayType(mapType(INTEGER, INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableMap.of(1, 1, 2, 2)));

        assertThat(assertions.function("flatten", "ARRAY[NULL, ARRAY[MAP(ARRAY[3, 4], ARRAY[3, 4])]]"))
                .hasType(new ArrayType(mapType(INTEGER, INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableMap.of(3, 3, 4, 4)));
    }
}
