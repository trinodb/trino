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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJsonOperators
{
    // Some of the tests in this class are expected to fail when coercion between primitive Trino types changes behavior

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    // todo add cases for decimal

    @Test
    public void testCastToBigint()
    {
        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON 'null'"))
                .isNull(BIGINT);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '128'"))
                .isEqualTo(128L);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '12345678901234567890'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '128.9'"))
                .isEqualTo(129L);

        // loss of precision
        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '1234567890123456789.0'"))
                .isEqualTo(1234567890123456768L);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '12345678901234567890.0'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '1e-324'"))
                .isEqualTo(0L);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '1e309'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON 'true'"))
                .isEqualTo(1L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON 'false'"))
                .isEqualTo(0L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '\"128\"'"))
                .isEqualTo(128L);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '\"12345678901234567890\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '\"128.9\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '\"true\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '\"false\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        // leading space
        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON ' 128'"))
                .isEqualTo(128L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "json_extract('{\"x\":999}', '$.x')"))
                .isEqualTo(999L);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BIGINT)")
                .binding("a", "JSON '{ \"x\" : 123}'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToInteger()
    {
        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON 'null'"))
                .isNull(INTEGER);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '128'"))
                .isEqualTo(128);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '12345678901'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '128.9'"))
                .isEqualTo(129);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '12345678901.0'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '1e-324'"))
                .isEqualTo(0);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '1e309'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON 'true'"))
                .isEqualTo(1);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON 'false'"))
                .isEqualTo(0);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '\"128\"'"))
                .isEqualTo(128);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '\"12345678901234567890\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '\"128.9\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '\"true\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '\"false\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        // leading space
        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON ' 128'"))
                .isEqualTo(128);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "json_extract('{\"x\":999}', '$.x')"))
                .isEqualTo(999);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as INTEGER)")
                .binding("a", "JSON '{ \"x\" : 123}'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToSmallint()
    {
        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON 'null'"))
                .isNull(SMALLINT);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '128'"))
                .isEqualTo((short) 128);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '123456'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '128.9'"))
                .isEqualTo((short) 129);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '123456.0'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '1e-324'"))
                .isEqualTo((short) 0);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '1e309'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON 'true'"))
                .isEqualTo((short) 1);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON 'false'"))
                .isEqualTo((short) 0);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '\"128\"'"))
                .isEqualTo((short) 128);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '\"123456\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '\"128.9\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '\"true\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '\"false\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        // leading space
        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON ' 128'"))
                .isEqualTo((short) 128);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "json_extract('{\"x\":999}', '$.x')"))
                .isEqualTo((short) 999);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as SMALLINT)")
                .binding("a", "JSON '{ \"x\" : 123}'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToTinyint()
    {
        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON 'null'"))
                .isNull(TINYINT);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '12'"))
                .isEqualTo((byte) 12);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '1234'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '12.9'"))
                .isEqualTo((byte) 13);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '1234.0'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '1e-324'"))
                .isEqualTo((byte) 0);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '1e309'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON 'true'"))
                .isEqualTo((byte) 1);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON 'false'"))
                .isEqualTo((byte) 0);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '\"12\"'"))
                .isEqualTo((byte) 12);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '\"1234\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '\"12.9\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '\"true\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '\"false\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        // leading space
        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON ' 12'"))
                .isEqualTo((byte) 12);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "json_extract('{\"x\":99}', '$.x')"))
                .isEqualTo((byte) 99);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as TINYINT)")
                .binding("a", "JSON '{ \"x\" : 123}'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testTypeConstructor()
    {
        assertThat(assertions.expression("JSON '123'"))
                .hasType(JSON)
                .isEqualTo("123");

        assertThat(assertions.expression("JSON '[4,5,6]'"))
                .hasType(JSON)
                .isEqualTo("[4,5,6]");

        assertThat(assertions.expression("JSON '{ \"a\": 789 }'"))
                .hasType(JSON)
                .isEqualTo("{\"a\":789}");

        assertThat(assertions.expression("JSON 'null'"))
                .hasType(JSON)
                .isEqualTo("null");

        assertThat(assertions.expression("JSON '[null]'"))
                .hasType(JSON)
                .isEqualTo("[null]");

        assertThat(assertions.expression("JSON '[13,null,42]'"))
                .hasType(JSON)
                .isEqualTo("[13,null,42]");

        assertThat(assertions.expression("JSON '{\"x\": null}'"))
                .hasType(JSON)
                .isEqualTo("{\"x\":null}");

        assertTrinoExceptionThrownBy(assertions.expression("JSON '{}{'")::evaluate)
                .hasErrorCode(INVALID_LITERAL);

        assertTrinoExceptionThrownBy(assertions.expression("JSON '{} \"a\"'")::evaluate)
                .hasErrorCode(INVALID_LITERAL);

        assertTrinoExceptionThrownBy(assertions.expression("JSON '{}{abc'")::evaluate)
                .hasErrorCode(INVALID_LITERAL);

        assertTrinoExceptionThrownBy(assertions.expression("JSON '{}abc'")::evaluate)
                .hasErrorCode(INVALID_LITERAL);

        assertTrinoExceptionThrownBy(assertions.expression("JSON ''")::evaluate)
                .hasErrorCode(INVALID_LITERAL);
    }

    @Test
    public void testCastFromIntegrals()
    {
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(null as integer)"))
                .isNull(JSON);

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(null as bigint)"))
                .isNull(JSON);

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(null as smallint)"))
                .isNull(JSON);

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(null as tinyint)"))
                .isNull(JSON);

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "128"))
                .hasType(JSON)
                .isEqualTo("128");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "BIGINT '128'"))
                .hasType(JSON)
                .isEqualTo("128");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "SMALLINT '128'"))
                .hasType(JSON)
                .isEqualTo("128");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "TINYINT '127'"))
                .hasType(JSON)
                .isEqualTo("127");
    }

    @Test
    public void testCastToDouble()
    {
        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON 'null'"))
                .isNull(DOUBLE);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '128'"))
                .isEqualTo(128.0);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '12345678901234567890'"))
                .isEqualTo(1.2345678901234567e19);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '128.9'"))
                .isEqualTo(128.9);

        // smaller than minimum subnormal positive
        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '1e-324'"))
                .isEqualTo(0.0);

        // overflow
        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '1e309'"))
                .isEqualTo(POSITIVE_INFINITY);

        // underflow
        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '-1e309'"))
                .isEqualTo(NEGATIVE_INFINITY);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON 'true'"))
                .isEqualTo(1.0);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON 'false'"))
                .isEqualTo(0.0);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '\"128\"'"))
                .isEqualTo(128.0);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '\"12345678901234567890\"'"))
                .isEqualTo(1.2345678901234567e19);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '\"128.9\"'"))
                .isEqualTo(128.9);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '\"NaN\"'"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '\"Infinity\"'"))
                .isEqualTo(POSITIVE_INFINITY);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '\"-Infinity\"'"))
                .isEqualTo(NEGATIVE_INFINITY);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '\"true\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        // leading space
        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON ' 128.9'"))
                .isEqualTo(128.9);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "json_extract('{\"x\":1.23}', '$.x')"))
                .isEqualTo(1.23);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DOUBLE)")
                .binding("a", "JSON '{ \"x\" : 123}'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastFromDouble()
    {
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(null as double)"))
                .isNull(JSON);

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "3.14E0"))
                .hasType(JSON)
                .isEqualTo("3.14");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "nan()"))
                .hasType(JSON)
                .isEqualTo("\"NaN\"");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "infinity()"))
                .hasType(JSON)
                .isEqualTo("\"Infinity\"");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "-infinity()"))
                .hasType(JSON)
                .isEqualTo("\"-Infinity\"");
    }

    @Test
    public void testCastFromReal()
    {
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(null as REAL)"))
                .isNull(JSON);

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "REAL '3.14'"))
                .hasType(JSON)
                .isEqualTo("3.14");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(nan() as REAL)"))
                .hasType(JSON)
                .isEqualTo("\"NaN\"");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(infinity() as REAL)"))
                .hasType(JSON)
                .isEqualTo("\"Infinity\"");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(-infinity() as REAL)"))
                .hasType(JSON)
                .isEqualTo("\"-Infinity\"");
    }

    @Test
    public void testCastToReal()
    {
        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON 'null'"))
                .isNull(REAL);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '-128'"))
                .isEqualTo(-128.0f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '128'"))
                .isEqualTo(128.0f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '12345678901234567890'"))
                .isEqualTo(1.2345679e19f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '128.9'"))
                .isEqualTo(128.9f);

        // smaller than minimum subnormal positive
        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '1e-46'"))
                .isEqualTo(0.0f);

        // overflow
        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '1e39'"))
                .isEqualTo(Float.POSITIVE_INFINITY);

        // underflow
        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '-1e39'"))
                .isEqualTo(Float.NEGATIVE_INFINITY);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON 'true'"))
                .isEqualTo(1.0f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON 'false'"))
                .isEqualTo(0.0f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '\"128\"'"))
                .isEqualTo(128.0f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '\"12345678901234567890\"'"))
                .isEqualTo(1.2345679e19f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '\"128.9\"'"))
                .isEqualTo(128.9f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '\"NaN\"'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '\"Infinity\"'"))
                .isEqualTo(Float.POSITIVE_INFINITY);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '\"-Infinity\"'"))
                .isEqualTo(Float.NEGATIVE_INFINITY);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '\"true\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        // leading space
        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "JSON ' 128.9'"))
                .isEqualTo(128.9f);

        assertThat(assertions.expression("cast(a as REAL)")
                .binding("a", "json_extract('{\"x\":1.23}', '$.x')"))
                .isEqualTo(1.23f);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as REAL)")
                .binding("a", "JSON '{ \"x\" : 123}'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToDecimal()
    {
        assertThat(assertions.expression("cast(a as DECIMAL(10,3))")
                .binding("a", "JSON 'null'"))
                .isNull(createDecimalType(10, 3));

        assertThat(assertions.expression("cast(a as DECIMAL(10,3))")
                .binding("a", "JSON '128'"))
                .hasType(createDecimalType(10, 3))
                .isEqualTo(decimal("128.000", createDecimalType(10, 3)));

        assertThat(assertions.expression("cast(a as DECIMAL(38,8))")
                .binding("a", "cast(DECIMAL '123456789012345678901234567890.12345678' as JSON)"))
                .hasType(createDecimalType(38, 8))
                .isEqualTo(decimal("123456789012345678901234567890.12345678", createDecimalType(38, 8)));

        assertThat(assertions.expression("cast(a as DECIMAL(10,5))")
                .binding("a", "JSON '123.456'"))
                .hasType(createDecimalType(10, 5))
                .isEqualTo(decimal("123.45600", createDecimalType(10, 5)));

        assertThat(assertions.expression("cast(a as DECIMAL(10,5))")
                .binding("a", "JSON 'true'"))
                .hasType(createDecimalType(10, 5))
                .isEqualTo(decimal("1.00000", createDecimalType(10, 5)));

        assertThat(assertions.expression("cast(a as DECIMAL(10,5))")
                .binding("a", "JSON 'false'"))
                .hasType(createDecimalType(10, 5))
                .isEqualTo(decimal("0.00000", createDecimalType(10, 5)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(10,3))")
                .binding("a", "JSON '1234567890123456'")
                .evaluate())
                .hasMessage("Cannot cast input json to DECIMAL(10,3)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(10,3))")
                .binding("a", "JSON '{ \"x\" : 123}'")
                .evaluate())
                .hasMessage("Cannot cast '{\"x\":123}' to DECIMAL(10,3)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as DECIMAL(10,3))")
                .binding("a", "JSON '\"abc\"'")
                .evaluate())
                .hasMessage("Cannot cast '\"abc\"' to DECIMAL(10,3)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastFromDecimal()
    {
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(null as decimal(5,2))"))
                .isNull(JSON);

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "DECIMAL '3.14'"))
                .hasType(JSON)
                .isEqualTo("3.14");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "DECIMAL '12345678901234567890.123456789012345678'"))
                .hasType(JSON)
                .isEqualTo("12345678901234567890.123456789012345678");
    }

    @Test
    public void testCastToBoolean()
    {
        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON 'null'"))
                .isNull(BOOLEAN);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '128'"))
                .isEqualTo(true);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '12345678901234567890'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '128.9'"))
                .isEqualTo(true);

        // smaller than minimum subnormal positive
        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '1e-324'"))
                .isEqualTo(false);

        // overflow
        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '1e309'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON 'true'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON 'false'"))
                .isEqualTo(false);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '\"True\"'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '\"true\"'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '\"false\"'"))
                .isEqualTo(false);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '\"128\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '\"\"'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        // leading space
        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON ' true'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "json_extract('{\"x\":true}', '$.x')"))
                .isEqualTo(true);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "JSON '{ \"x\" : 123}'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastFromBoolean()
    {
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(null as boolean)"))
                .isNull(JSON);

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "TRUE"))
                .hasType(JSON)
                .isEqualTo("true");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "FALSE"))
                .hasType(JSON)
                .isEqualTo("false");
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON 'null'"))
                .isNull(VARCHAR);

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON '128'"))
                .hasType(VARCHAR)
                .isEqualTo("128");

        // overflow, no loss of precision
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON '12345678901234567890'"))
                .hasType(VARCHAR)
                .isEqualTo("12345678901234567890");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON '128.9'"))
                .hasType(VARCHAR)
                .isEqualTo("1.289E2");

        // smaller than minimum subnormal positive
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON '1e-324'"))
                .hasType(VARCHAR)
                .isEqualTo("0E0");

        // overflow
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON '1e309'"))
                .hasType(VARCHAR)
                .isEqualTo("Infinity");

        // underflow
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON '-1e309'"))
                .hasType(VARCHAR)
                .isEqualTo("-Infinity");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON 'true'"))
                .hasType(VARCHAR)
                .isEqualTo("true");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON 'false'"))
                .hasType(VARCHAR)
                .isEqualTo("false");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON '\"test\"'"))
                .hasType(VARCHAR)
                .isEqualTo("test");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON '\"null\"'"))
                .hasType(VARCHAR)
                .isEqualTo("null");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON '\"\"'"))
                .hasType(VARCHAR)
                .isEqualTo("");

        // leading space
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON ' \"test\"'"))
                .hasType(VARCHAR)
                .isEqualTo("test");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "json_extract('{\"x\":\"y\"}', '$.x')"))
                .hasType(VARCHAR)
                .isEqualTo("y");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as VARCHAR)")
                .binding("a", "JSON '{ \"x\" : 123}'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testEquals()
    {
        assertThat(assertions.operator(EQUAL, "JSON '{\"a\": \"1.1\", \"b\": \"2.3\", \"c\": {\"d\": \"314E-2\" }}'", "JSON '{\"a\": \"1.1\", \"b\": \"2.3\", \"c\": {\"d\": \"314E-2\" }}'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "JSON '[1,2,3]'", "JSON '[1,2,3]'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "JSON '{\"a\":1, \"b\":2}'", "JSON '{\"b\":2, \"a\":1}'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "JSON '{\"a\":1, \"b\":2}'", "CAST(MAP(ARRAY['b','a'], ARRAY[2,1]) AS JSON)"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "JSON 'null'", "JSON 'null'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "JSON 'true'", "JSON 'true'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "JSON '{\"x\":\"y\"}'", "JSON '{\"x\":\"y\"}'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "JSON '[1,2,3]'", "JSON '[2,3,1]'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "JSON '{\"p_1\": 1, \"p_2\":\"v_2\", \"p_3\":null, \"p_4\":true, \"p_5\": {\"p_1\":1}}'", "JSON '{\"p_2\":\"v_2\", \"p_4\":true, \"p_1\": 1, \"p_3\":null, \"p_5\": {\"p_1\":1}}'"))
                .isEqualTo(true);
    }

    @Test
    public void testNotEquals()
    {
        assertThat(assertions.expression("a != b")
                .binding("a", "JSON '{\"a\": 1, \"b\": 2, \"c\": {\"d\": 3 }}'")
                .binding("b", "JSON '{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4 }}'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "JSON '[1,2,3]'")
                .binding("b", "JSON '[1,2,3]'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "JSON '{\"a\":1, \"b\":2}'")
                .binding("b", "JSON '{\"b\":2, \"a\":1}'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "JSON 'null'")
                .binding("b", "JSON 'null'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "JSON 'true'")
                .binding("b", "JSON 'true'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "JSON '{\"x\":\"y\"}'")
                .binding("b", "JSON '{\"x\":\"y\"}'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "JSON '[1,2,3]'")
                .binding("b", "JSON '[2,3,1]'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "JSON '{\"p_1\": 1, \"p_2\":\"v_2\", \"p_3\":null, \"p_4\":true, \"p_5\": {\"p_1\":1}}'")
                .binding("b", "JSON '{\"p_2\":\"v_2\", \"p_4\":true, \"p_1\": 1, \"p_3\":null, \"p_5\": {\"p_1\":1}}'"))
                .isEqualTo(false);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertThat(assertions.expression("a IS DISTINCT FROM b")
                .binding("a", "JSON 'null'")
                .binding("b", "JSON 'null'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a IS DISTINCT FROM b")
                .binding("a", "JSON '{ \"a\": 1 , \"b\": 2 , \"c\": { \"d\": 3 }}'")
                .binding("b", "JSON '{ \"a\": 1 , \"b\": 2 , \"c\" : { \"d\" : 4 }}'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a IS DISTINCT FROM b")
                .binding("a", "JSON '{ \"a\": 1 , \"b\": 2 , \"c\": { \"d\": 3 }}'")
                .binding("b", "JSON '{ \"b\": 2 , \"a\": 1 , \"c\": { \"d\": 3 }}'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a IS DISTINCT FROM b")
                .binding("a", "JSON '{ \"a\": 1 , \"b\": 2 , \"c\": { \"d\": 3 }}'")
                .binding("b", "JSON 'null'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a IS DISTINCT FROM b")
                .binding("a", "JSON 'null'")
                .binding("b", "JSON '{ \"a\": 1 , \"b\": 2 , \"c\" : { \"d\" : 4 }}'"))
                .isEqualTo(true);
    }

    @Test
    public void testCastFromVarchar()
    {
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(null as varchar)"))
                .isNull(JSON);

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "'abc'"))
                .hasType(JSON)
                .isEqualTo("\"abc\"");

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "'\"a\":2'"))
                .hasType(JSON)
                .isEqualTo("\"\\\"a\\\":2\"");
    }

    @Test
    public void testCastFromTimestamp()
    {
        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "cast(null as timestamp)"))
                .isNull(JSON);

        assertThat(assertions.expression("cast(a as JSON)")
                .binding("a", "TIMESTAMP '1970-01-01 00:00:01'"))
                .hasType(JSON)
                .isEqualTo(format("\"%s\"", sqlTimestampOf(0, 1970, 1, 1, 0, 0, 1, 0)));
    }

    @Test
    public void testCastWithJsonParse()
    {
        // the test is to make sure ExpressionOptimizer works with cast + json_parse
        assertThat(assertions.expression("CAST(json_parse(a) AS ARRAY(ARRAY(INTEGER)))")
                .binding("a", "'[[1,1], [2,2]]'"))
                .hasType(new ArrayType(new ArrayType(INTEGER)))
                .isEqualTo(ImmutableList.of(ImmutableList.of(1, 1), ImmutableList.of(2, 2)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(json_parse(a) AS ARRAY(INTEGER))")
                .binding("a", "'[1, \"abc\"]'")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT, INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Cannot cast to array(integer). Cannot cast 'abc' to INT\n[1, \"abc\"]");

        // Since we will not reformat the JSON string before parse and cast with the optimization,
        // these extra whitespaces in JSON string is to make sure the cast will work in such cases.
        assertThat(assertions.expression("CAST(json_parse(a) AS MAP(VARCHAR,INTEGER))")
                .binding("a", "'{\"a\"\n:1,  \"b\":\t2}'"))
                .hasType(mapType(VARCHAR, INTEGER))
                .isEqualTo(ImmutableMap.of("a", 1, "b", 2));

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(json_parse(a) AS MAP(ARRAY(INTEGER),ARRAY(INTEGER)))")
                .binding("a", "'{\"[1, 1]\":[2, 2]}'")
                .evaluate())
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:8: Cannot cast json to map(array(integer), array(integer))");

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(json_parse(a) AS MAP(BOOLEAN,BOOLEAN))")
                .binding("a", "'{true: false, false: false}'")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT, INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Cannot cast to map(boolean, boolean).\n{true: false, false: false}");

        assertThat(assertions.expression("CAST(json_parse(a) AS ROW(a INTEGER, b ARRAY(INTEGER)))")
                .binding("a", "'{\"a\":1, \"b\": [2, 3]}'"))
                .hasType(RowType.from(ImmutableList.of(
                        RowType.field("a", INTEGER),
                        RowType.field("b", new ArrayType(INTEGER)))))
                .isEqualTo(ImmutableList.of(1, ImmutableList.of(2, 3)));

        assertThat(assertions.expression("CAST(json_parse(a) AS ROW(INTEGER, ARRAY(INTEGER)))")
                .binding("a", "'[1, [2, 3]]'"))
                .hasType(RowType.anonymous(ImmutableList.of(INTEGER, new ArrayType(INTEGER))))
                .isEqualTo(ImmutableList.of(1, ImmutableList.of(2, 3)));

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(json_parse(a) AS ROW(a INTEGER, b ARRAY(INTEGER)))")
                .binding("a", "'{\"a\": 1, \"b\": {}}'")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT, INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Cannot cast to row(a integer, b array(integer)). Expected a json array, but got {\n{\"a\": 1, \"b\": {}}");

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(json_parse(a) AS ROW(INTEGER, ARRAY(INTEGER)))")
                .binding("a", "'[1, {}]'")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT, INVALID_FUNCTION_ARGUMENT)
                .hasMessage("Cannot cast to row(integer, array(integer)). Expected a json array, but got {\n[1, {}]");
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as JSON)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "JSON '128'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON 'true'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON 'false'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON '\"test\"'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON '\"null\"'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON '\"\"'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON 'true'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON 'false'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON '\"True\"'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON '\"true\"'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON '123.456'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON 'true'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON 'false'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON '\"NaN\"'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON '\"Infinity\"'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "JSON '\"-Infinity\"'"))
                .isEqualTo(false);
    }
}
