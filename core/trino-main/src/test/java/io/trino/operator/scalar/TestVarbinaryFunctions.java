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
package io.trino.operator.scalar;

import com.google.common.io.BaseEncoding;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Base64;

import static com.google.common.io.BaseEncoding.base16;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.SqlVarbinaryTestingUtil.sqlVarbinary;
import static io.trino.testing.SqlVarbinaryTestingUtil.sqlVarbinaryFromHex;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestVarbinaryFunctions
{
    private static final byte[] ALL_BYTES;

    static {
        ALL_BYTES = new byte[256];
        for (int i = 0; i < ALL_BYTES.length; i++) {
            ALL_BYTES[i] = (byte) i;
        }
    }

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

    @Test
    public void testBinaryLiteral()
    {
        assertThat(assertions.expression("X'58F7'"))
                .isEqualTo(sqlVarbinaryFromHex("58F7"));
    }

    @Test
    public void testLength()
    {
        assertThat(assertions.function("length", "CAST('' AS VARBINARY)"))
                .isEqualTo(0L);

        assertThat(assertions.function("length", "CAST('a' AS VARBINARY)"))
                .isEqualTo(1L);

        assertThat(assertions.function("length", "CAST('abc' AS VARBINARY)"))
                .isEqualTo(3L);
    }

    @Test
    public void testConcat()
    {
        assertTrinoExceptionThrownBy(assertions.function("CONCAT", "X''")::evaluate)
                .hasMessage("There must be two or more concatenation arguments");

        assertThat(assertions.expression("CAST('foo' AS VARBINARY) || CAST ('bar' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("foo" + "bar"));

        assertThat(assertions.expression("CAST('foo' AS VARBINARY) || CAST ('bar' AS VARBINARY) || CAST ('baz' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("foo" + "bar" + "baz"));

        assertThat(assertions.expression("CAST(' foo ' AS VARBINARY) || CAST ('  bar  ' AS VARBINARY) || CAST ('   baz   ' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary(" foo " + "  bar  " + "   baz   "));

        assertThat(assertions.expression("CAST('foo' AS VARBINARY) || CAST ('bar' AS VARBINARY) || CAST ('bazbaz' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("foo" + "bar" + "bazbaz"));

        assertThat(assertions.expression("X'000102' || X'AAABAC' || X'FDFEFF'"))
                .isEqualTo(sqlVarbinaryFromHex("000102" + "AAABAC" + "FDFEFF"));

        assertThat(assertions.expression("X'CAFFEE' || X'F7' || X'DE58'"))
                .isEqualTo(sqlVarbinaryFromHex("CAFFEE" + "F7" + "DE58"));

        assertThat(assertions.expression("X'58' || X'F7'"))
                .isEqualTo(sqlVarbinaryFromHex("58F7"));

        assertThat(assertions.expression("X'' || X'58' || X'F7'"))
                .isEqualTo(sqlVarbinaryFromHex("58F7"));

        assertThat(assertions.expression("X'58' || X'' || X'F7'"))
                .isEqualTo(sqlVarbinaryFromHex("58F7"));

        assertThat(assertions.expression("X'58' || X'F7' || X''"))
                .isEqualTo(sqlVarbinaryFromHex("58F7"));

        assertThat(assertions.expression("X'' || X'58' || X'' || X'F7' || X''"))
                .isEqualTo(sqlVarbinaryFromHex("58F7"));

        assertThat(assertions.expression("X'' || X'' || X'' || X'' || X'' || X''"))
                .isEqualTo(sqlVarbinaryFromHex(""));

        assertThat(assertions.function("CONCAT", "CAST('foo' AS VARBINARY)", "CAST ('bar' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("foo" + "bar"));

        assertThat(assertions.function("CONCAT", "CAST('foo' AS VARBINARY)", "CAST ('bar' AS VARBINARY)", "CAST ('baz' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("foo" + "bar" + "baz"));

        assertThat(assertions.function("CONCAT", "CAST('foo' AS VARBINARY)", "CAST ('bar' AS VARBINARY)", "CAST ('bazbaz' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("foo" + "bar" + "bazbaz"));

        assertThat(assertions.function("CONCAT", "X'000102'", "X'AAABAC'", "X'FDFEFF'"))
                .isEqualTo(sqlVarbinaryFromHex("000102" + "AAABAC" + "FDFEFF"));

        assertThat(assertions.function("CONCAT", "X'CAFFEE'", "X'F7'", "X'DE58'"))
                .isEqualTo(sqlVarbinaryFromHex("CAFFEE" + "F7" + "DE58"));

        assertThat(assertions.function("CONCAT", "X'58'", "X'F7'"))
                .isEqualTo(sqlVarbinaryFromHex("58F7"));

        assertThat(assertions.function("CONCAT", "X''", "X'58'", "X'F7'"))
                .isEqualTo(sqlVarbinaryFromHex("58F7"));

        assertThat(assertions.function("CONCAT", "X'58'", "X''", "X'F7'"))
                .isEqualTo(sqlVarbinaryFromHex("58F7"));

        assertThat(assertions.function("CONCAT", "X'58'", "X'F7'", "X''"))
                .isEqualTo(sqlVarbinaryFromHex("58F7"));

        assertThat(assertions.function("concat", "X''", "X'58'", "X''", "X'F7'", "X''"))
                .isEqualTo(sqlVarbinaryFromHex("58F7"));

        assertThat(assertions.function("concat", "X''", "X''", "X''", "X''", "X''", "X''"))
                .isEqualTo(sqlVarbinaryFromHex(""));
    }

    @Test
    public void testToBase64()
    {
        assertThat(assertions.function("to_base64", "CAST('' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase64(""));

        assertThat(assertions.function("to_base64", "CAST('a' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase64("a"));

        assertThat(assertions.function("to_base64", "CAST('abc' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase64("abc"));

        assertThat(assertions.function("to_base64", "CAST('hello world' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo("aGVsbG8gd29ybGQ=");
    }

    @Test
    public void testFromBase64()
    {
        assertThat(assertions.function("from_base64", "to_base64(CAST('' AS VARBINARY))"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("from_base64", "to_base64(CAST('a' AS VARBINARY))"))
                .isEqualTo(sqlVarbinary("a"));

        assertThat(assertions.function("from_base64", "to_base64(CAST('abc' AS VARBINARY))"))
                .isEqualTo(sqlVarbinary("abc"));

        assertThat(assertions.function("from_base64", "CAST(to_base64(CAST('' AS VARBINARY)) AS VARBINARY)"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("from_base64", "CAST(to_base64(CAST('a' AS VARBINARY)) AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("a"));

        assertThat(assertions.function("from_base64", "CAST(to_base64(CAST('abc' AS VARBINARY)) AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("abc"));

        assertThat(assertions.function("to_base64", "from_base64('%s')".formatted(encodeBase64(ALL_BYTES))))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase64(ALL_BYTES));
    }

    @Test
    public void testToBase64Url()
    {
        assertThat(assertions.function("to_base64url", "CAST('' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase64Url(""));

        assertThat(assertions.function("to_base64url", "CAST('a' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase64Url("a"));

        assertThat(assertions.function("to_base64url", "CAST('abc' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase64Url("abc"));

        assertThat(assertions.function("to_base64url", "CAST('hello world' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo("aGVsbG8gd29ybGQ=");
    }

    @Test
    public void testFromBase64Url()
    {
        assertThat(assertions.function("from_base64url", "to_base64url(CAST('' AS VARBINARY))"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("from_base64url", "to_base64url(CAST('a' AS VARBINARY))"))
                .isEqualTo(sqlVarbinary("a"));

        assertThat(assertions.function("from_base64url", "to_base64url(CAST('abc' AS VARBINARY))"))
                .isEqualTo(sqlVarbinary("abc"));

        assertThat(assertions.function("from_base64url", "CAST(to_base64url(CAST('' AS VARBINARY)) AS VARBINARY)"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("from_base64url", "CAST(to_base64url(CAST('a' AS VARBINARY)) AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("a"));

        assertThat(assertions.function("from_base64url", "CAST(to_base64url(CAST('abc' AS VARBINARY)) AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("abc"));

        assertThat(assertions.function("to_base64url", "from_base64url('%s')".formatted(encodeBase64Url(ALL_BYTES))))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase64Url(ALL_BYTES));
    }

    @Test
    public void testToBase32()
    {
        assertThat(assertions.function("to_base32", "CAST('' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase32(""));

        assertThat(assertions.function("to_base32", "CAST('a' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase32("a"));

        assertThat(assertions.function("to_base32", "CAST('abc' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase32("abc"));

        assertThat(assertions.function("to_base32", "CAST('hello world' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo("NBSWY3DPEB3W64TMMQ======");

        assertThat(assertions.function("to_base32", "NULL"))
                .isNull(VARCHAR);
    }

    @Test
    public void testFromBase32()
    {
        assertThat(assertions.function("from_base32", "''"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("from_base32", "'ME======'"))
                .isEqualTo(sqlVarbinary("a"));

        assertThat(assertions.function("from_base32", "'MFRGG==='"))
                .isEqualTo(sqlVarbinary("abc"));

        assertThat(assertions.function("from_base32", "'NBSWY3DPEB3W64TMMQ======'"))
                .isEqualTo(sqlVarbinary("hello world"));

        assertThat(assertions.function("from_base32", "to_base32(CAST('' AS VARBINARY))"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("from_base32", "to_base32(CAST('a' AS VARBINARY))"))
                .isEqualTo(sqlVarbinary("a"));

        assertThat(assertions.function("from_base32", "to_base32(CAST('abc' AS VARBINARY))"))
                .isEqualTo(sqlVarbinary("abc"));

        assertThat(assertions.function("from_base32", "to_base32(CAST('hello world' AS VARBINARY))"))
                .isEqualTo(sqlVarbinary("hello world"));

        assertThat(assertions.function("from_base32", "CAST(to_base32(CAST('' AS VARBINARY)) AS VARBINARY)"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("from_base32", "CAST(to_base32(CAST('a' AS VARBINARY)) AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("a"));

        assertThat(assertions.function("from_base32", "CAST(to_base32(CAST('abc' AS VARBINARY)) AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("abc"));

        assertThat(assertions.function("from_base32", "CAST(to_base32(CAST('hello world' AS VARBINARY)) AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("hello world"));

        assertThat(assertions.function("to_base32", "from_base32('%s')".formatted(encodeBase32(ALL_BYTES))))
                .hasType(VARCHAR)
                .isEqualTo(encodeBase32(ALL_BYTES));

        assertThat(assertions.function("from_base32", "CAST(NULL AS VARCHAR)"))
                .isNull(VARBINARY);

        assertThat(assertions.function("from_base32", "CAST(NULL AS VARBINARY)"))
                .isNull(VARBINARY);

        assertTrinoExceptionThrownBy(assertions.function("from_base32", "'1='")::evaluate)
                .hasMessage("Invalid input length 1");

        assertTrinoExceptionThrownBy(assertions.function("from_base32", "'M1======'")::evaluate)
                .hasMessage("Unrecognized character: 1");

        assertTrinoExceptionThrownBy(assertions.function("from_base32", "CAST('1=' AS VARBINARY)")::evaluate)
                .hasMessage("Invalid input length 1");

        assertTrinoExceptionThrownBy(assertions.function("from_base32", "CAST('M1======' AS VARBINARY)")::evaluate)
                .hasMessage("Unrecognized character: 1");
    }

    @Test
    public void testToHex()
    {
        assertThat(assertions.function("to_hex", "CAST('' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeHex(""));

        assertThat(assertions.function("to_hex", "CAST('a' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeHex("a"));

        assertThat(assertions.function("to_hex", "CAST('abc' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo(encodeHex("abc"));

        assertThat(assertions.function("to_hex", "CAST('hello world' AS VARBINARY)"))
                .hasType(VARCHAR)
                .isEqualTo("68656C6C6F20776F726C64");
    }

    @Test
    public void testFromHex()
    {
        assertThat(assertions.function("from_hex", "''"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("from_hex", "'61'"))
                .isEqualTo(sqlVarbinary("a"));

        assertThat(assertions.function("from_hex", "'617a6f'"))
                .isEqualTo(sqlVarbinary("azo"));

        assertThat(assertions.function("from_hex", "'617A6F'"))
                .isEqualTo(sqlVarbinary("azo"));

        assertThat(assertions.function("from_hex", "CAST('' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("from_hex", "CAST('61' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("a"));

        assertThat(assertions.function("from_hex", "CAST('617a6F' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("azo"));

        assertThat(assertions.function("to_hex", "from_hex('%s')".formatted(base16().encode(ALL_BYTES))))
                .hasType(VARCHAR)
                .isEqualTo(base16().encode(ALL_BYTES));

        // '0' - 1
        assertTrinoExceptionThrownBy(assertions.function("from_hex", "'f/'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        // '9' + 1
        assertTrinoExceptionThrownBy(assertions.function("from_hex", "'f:'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        // 'A' - 1
        assertTrinoExceptionThrownBy(assertions.function("from_hex", "'f@'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        // 'F' + 1
        assertTrinoExceptionThrownBy(assertions.function("from_hex", "'fG'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        // 'a' - 1
        assertTrinoExceptionThrownBy(assertions.function("from_hex", "'f`'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        // 'f' + 1
        assertTrinoExceptionThrownBy(assertions.function("from_hex", "'fg'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("from_hex", "'fff'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testToBigEndian64()
    {
        assertThat(assertions.function("to_big_endian_64", "0"))
                .isEqualTo(sqlVarbinaryFromHex("0000000000000000"));

        assertThat(assertions.function("to_big_endian_64", "1"))
                .isEqualTo(sqlVarbinaryFromHex("0000000000000001"));

        assertThat(assertions.function("to_big_endian_64", "9223372036854775807"))
                .isEqualTo(sqlVarbinaryFromHex("7FFFFFFFFFFFFFFF"));

        assertThat(assertions.function("to_big_endian_64", "-9223372036854775807"))
                .isEqualTo(sqlVarbinaryFromHex("8000000000000001"));
    }

    @Test
    public void testFromBigEndian64()
    {
        assertThat(assertions.function("from_big_endian_64", "from_hex('0000000000000000')"))
                .isEqualTo(0L);

        assertThat(assertions.function("from_big_endian_64", "from_hex('0000000000000001')"))
                .isEqualTo(1L);

        assertThat(assertions.function("from_big_endian_64", "from_hex('7FFFFFFFFFFFFFFF')"))
                .isEqualTo(9223372036854775807L);

        assertThat(assertions.function("from_big_endian_64", "from_hex('8000000000000001')"))
                .isEqualTo(-9223372036854775807L);

        assertTrinoExceptionThrownBy(assertions.function("from_big_endian_64", "from_hex('')")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("from_big_endian_64", "from_hex('1111')")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("from_big_endian_64", "from_hex('000000000000000011')")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testToBigEndian32()
    {
        assertThat(assertions.function("to_big_endian_32", "0"))
                .isEqualTo(sqlVarbinaryFromHex("00000000"));

        assertThat(assertions.function("to_big_endian_32", "1"))
                .isEqualTo(sqlVarbinaryFromHex("00000001"));

        assertThat(assertions.function("to_big_endian_32", "2147483647"))
                .isEqualTo(sqlVarbinaryFromHex("7FFFFFFF"));

        assertThat(assertions.function("to_big_endian_32", "-2147483647"))
                .isEqualTo(sqlVarbinaryFromHex("80000001"));
    }

    @Test
    public void testFromBigEndian32()
    {
        assertThat(assertions.function("from_big_endian_32", "from_hex('00000000')"))
                .hasType(INTEGER)
                .isEqualTo(0);

        assertThat(assertions.function("from_big_endian_32", "from_hex('00000001')"))
                .hasType(INTEGER)
                .isEqualTo(1);

        assertThat(assertions.function("from_big_endian_32", "from_hex('7FFFFFFF')"))
                .hasType(INTEGER)
                .isEqualTo(2147483647);

        assertThat(assertions.function("from_big_endian_32", "from_hex('80000001')"))
                .hasType(INTEGER)
                .isEqualTo(-2147483647);

        assertTrinoExceptionThrownBy(assertions.function("from_big_endian_32", "from_hex('')")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("from_big_endian_32", "from_hex('1111')")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("from_big_endian_32", "from_hex('000000000000000011')")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testToIEEE754Binary32()
    {
        assertThat(assertions.function("to_ieee754_32", "CAST(0.0 AS REAL)"))
                .isEqualTo(sqlVarbinaryFromHex("00000000"));

        assertThat(assertions.function("to_ieee754_32", "CAST(1.0 AS REAL)"))
                .isEqualTo(sqlVarbinaryFromHex("3F800000"));

        assertThat(assertions.function("to_ieee754_32", "CAST(3.14 AS REAL)"))
                .isEqualTo(sqlVarbinaryFromHex("4048F5C3"));

        assertThat(assertions.function("to_ieee754_32", "CAST(NAN() AS REAL)"))
                .isEqualTo(sqlVarbinaryFromHex("7FC00000"));

        assertThat(assertions.function("to_ieee754_32", "CAST(INFINITY() AS REAL)"))
                .isEqualTo(sqlVarbinaryFromHex("7F800000"));

        assertThat(assertions.function("to_ieee754_32", "CAST(-INFINITY() AS REAL)"))
                .isEqualTo(sqlVarbinaryFromHex("FF800000"));

        assertThat(assertions.function("to_ieee754_32", "CAST(3.4028235E38 AS REAL)"))
                .isEqualTo(sqlVarbinaryFromHex("7F7FFFFF"));

        assertThat(assertions.function("to_ieee754_32", "CAST(-3.4028235E38 AS REAL)"))
                .isEqualTo(sqlVarbinaryFromHex("FF7FFFFF"));

        assertThat(assertions.function("to_ieee754_32", "CAST(1.4E-45 AS REAL)"))
                .isEqualTo(sqlVarbinaryFromHex("00000001"));

        assertThat(assertions.function("to_ieee754_32", "CAST(-1.4E-45 AS REAL)"))
                .isEqualTo(sqlVarbinaryFromHex("80000001"));
    }

    @Test
    public void testFromIEEE754Binary32()
    {
        assertThat(assertions.function("from_ieee754_32", "from_hex('3F800000')"))
                .hasType(REAL)
                .isEqualTo(1.0f);

        assertThat(assertions.function("from_ieee754_32", "to_ieee754_32(CAST(1.0 AS REAL))"))
                .hasType(REAL)
                .isEqualTo(1.0f);

        assertThat(assertions.function("from_ieee754_32", "from_hex('4048F5C3')"))
                .hasType(REAL)
                .isEqualTo(3.14f);

        assertThat(assertions.function("from_ieee754_32", "to_ieee754_32(CAST(3.14 AS REAL))"))
                .hasType(REAL)
                .isEqualTo(3.14f);

        assertThat(assertions.function("from_ieee754_32", "to_ieee754_32(CAST(NAN() AS REAL))"))
                .hasType(REAL)
                .isEqualTo(Float.NaN);

        assertThat(assertions.function("from_ieee754_32", "to_ieee754_32(CAST(INFINITY() AS REAL))"))
                .hasType(REAL)
                .isEqualTo(Float.POSITIVE_INFINITY);

        assertThat(assertions.function("from_ieee754_32", "to_ieee754_32(CAST(-INFINITY() AS REAL))"))
                .hasType(REAL)
                .isEqualTo(Float.NEGATIVE_INFINITY);

        assertThat(assertions.function("from_ieee754_32", "to_ieee754_32(CAST(3.4028235E38 AS REAL))"))
                .hasType(REAL)
                .isEqualTo(3.4028235E38f);

        assertThat(assertions.function("from_ieee754_32", "to_ieee754_32(CAST(-3.4028235E38 AS REAL))"))
                .hasType(REAL)
                .isEqualTo(-3.4028235E38f);

        assertThat(assertions.function("from_ieee754_32", "to_ieee754_32(CAST(1.4E-45 AS REAL))"))
                .hasType(REAL)
                .isEqualTo(1.4E-45f);

        assertThat(assertions.function("from_ieee754_32", "to_ieee754_32(CAST(-1.4E-45 AS REAL))"))
                .hasType(REAL)
                .isEqualTo(-1.4E-45f);

        assertTrinoExceptionThrownBy(assertions.function("from_ieee754_32", "from_hex('0000')")::evaluate)
                .hasMessage("Input floating-point value must be exactly 4 bytes long");
    }

    @Test
    public void testToIEEE754Binary64()
    {
        assertThat(assertions.function("to_ieee754_64", "0.0"))
                .isEqualTo(sqlVarbinaryFromHex("0000000000000000"));

        assertThat(assertions.function("to_ieee754_64", "1.0"))
                .isEqualTo(sqlVarbinaryFromHex("3FF0000000000000"));

        assertThat(assertions.function("to_ieee754_64", "3.1415926"))
                .isEqualTo(sqlVarbinaryFromHex("400921FB4D12D84A"));

        assertThat(assertions.function("to_ieee754_64", "NAN()"))
                .isEqualTo(sqlVarbinaryFromHex("7FF8000000000000"));

        assertThat(assertions.function("to_ieee754_64", "INFINITY()"))
                .isEqualTo(sqlVarbinaryFromHex("7FF0000000000000"));

        assertThat(assertions.function("to_ieee754_64", "-INFINITY()"))
                .isEqualTo(sqlVarbinaryFromHex("FFF0000000000000"));

        assertThat(assertions.function("to_ieee754_64", "1.7976931348623157E308"))
                .isEqualTo(sqlVarbinaryFromHex("7FEFFFFFFFFFFFFF"));

        assertThat(assertions.function("to_ieee754_64", "-1.7976931348623157E308"))
                .isEqualTo(sqlVarbinaryFromHex("FFEFFFFFFFFFFFFF"));

        assertThat(assertions.function("to_ieee754_64", "4.9E-324"))
                .isEqualTo(sqlVarbinaryFromHex("0000000000000001"));

        assertThat(assertions.function("to_ieee754_64", "-4.9E-324"))
                .isEqualTo(sqlVarbinaryFromHex("8000000000000001"));
    }

    @Test
    public void testFromIEEE754Binary64()
    {
        assertThat(assertions.function("from_ieee754_64", "from_hex('0000000000000000')"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);

        assertThat(assertions.function("from_ieee754_64", "from_hex('3FF0000000000000')"))
                .hasType(DOUBLE)
                .isEqualTo(1.0);

        assertThat(assertions.function("from_ieee754_64", "to_ieee754_64(3.1415926)"))
                .hasType(DOUBLE)
                .isEqualTo(3.1415926);

        assertThat(assertions.function("from_ieee754_64", "to_ieee754_64(NAN())"))
                .hasType(DOUBLE)
                .isEqualTo(Double.NaN);

        assertThat(assertions.function("from_ieee754_64", "to_ieee754_64(INFINITY())"))
                .hasType(DOUBLE)
                .isEqualTo(Double.POSITIVE_INFINITY);

        assertThat(assertions.function("from_ieee754_64", "to_ieee754_64(-INFINITY())"))
                .hasType(DOUBLE)
                .isEqualTo(Double.NEGATIVE_INFINITY);

        assertThat(assertions.function("from_ieee754_64", "to_ieee754_64(1.7976931348623157E308)"))
                .hasType(DOUBLE)
                .isEqualTo(1.7976931348623157E308);

        assertThat(assertions.function("from_ieee754_64", "to_ieee754_64(-1.7976931348623157E308)"))
                .hasType(DOUBLE)
                .isEqualTo(-1.7976931348623157E308);

        assertThat(assertions.function("from_ieee754_64", "to_ieee754_64(4.9E-324)"))
                .hasType(DOUBLE)
                .isEqualTo(4.9E-324);

        assertThat(assertions.function("from_ieee754_64", "to_ieee754_64(-4.9E-324)"))
                .hasType(DOUBLE)
                .isEqualTo(-4.9E-324);

        assertTrinoExceptionThrownBy(assertions.function("from_ieee754_64", "from_hex('00000000')")::evaluate)
                .hasMessage("Input floating-point value must be exactly 8 bytes long");
    }

    @Test
    public void testLpad()
    {
        // TODO
        assertThat(assertions.function("lpad", "x'1234'", "7", "x'45'"))
                .isEqualTo(sqlVarbinaryFromHex("45454545451234"));

        assertThat(assertions.function("lpad", "x'1234'", "7", "x'4524'"))
                .isEqualTo(sqlVarbinaryFromHex("45244524451234"));

        assertThat(assertions.function("lpad", "x'1234'", "3", "x'4524'"))
                .isEqualTo(sqlVarbinaryFromHex("451234"));

        assertThat(assertions.function("lpad", "x'1234'", "0", "x'4524'"))
                .isEqualTo(sqlVarbinaryFromHex(""));

        assertThat(assertions.function("lpad", "x'1234'", "1", "x'4524'"))
                .isEqualTo(sqlVarbinaryFromHex("12"));

        assertTrinoExceptionThrownBy(assertions.function("lpad", "x'2312'", "-1", "x'4524'")::evaluate)
                .hasMessage("Target length must be in the range [0.." + Integer.MAX_VALUE + "]");

        assertTrinoExceptionThrownBy(assertions.function("lpad", "x'2312'", "1", "x''")::evaluate)
                .hasMessage("Padding bytes must not be empty");
    }

    @Test
    public void testRpad()
    {
        // TODO
        assertThat(assertions.function("rpad", "x'1234'", "7", "x'45'"))
                .isEqualTo(sqlVarbinaryFromHex("12344545454545"));

        assertThat(assertions.function("rpad", "x'1234'", "7", "x'4524'"))
                .isEqualTo(sqlVarbinaryFromHex("12344524452445"));

        assertThat(assertions.function("rpad", "x'1234'", "3", "x'4524'"))
                .isEqualTo(sqlVarbinaryFromHex("123445"));

        assertThat(assertions.function("rpad", "x'23'", "0", "x'4524'"))
                .isEqualTo(sqlVarbinaryFromHex(""));

        assertThat(assertions.function("rpad", "x'1234'", "1", "x'4524'"))
                .isEqualTo(sqlVarbinaryFromHex("12"));

        assertTrinoExceptionThrownBy(assertions.function("rpad", "x'1234'", "-1", "x'4524'")::evaluate)
                .hasMessage("Target length must be in the range [0.." + Integer.MAX_VALUE + "]");

        assertTrinoExceptionThrownBy(assertions.function("rpad", "x'1234'", "1", "x''")::evaluate)
                .hasMessage("Padding bytes must not be empty");
    }

    @Test
    public void testMd5()
    {
        assertThat(assertions.function("md5", "CAST('' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("D41D8CD98F00B204E9800998ECF8427E"));

        assertThat(assertions.function("md5", "CAST('hashme' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("533F6357E0210E67D91F651BC49E1278"));
    }

    @Test
    public void testSha1()
    {
        assertThat(assertions.function("sha1", "CAST('' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("DA39A3EE5E6B4B0D3255BFEF95601890AFD80709"));

        assertThat(assertions.function("sha1", "CAST('hashme' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("FB78992E561929A6967D5328F49413FA99048D06"));
    }

    @Test
    public void testSha256()
    {
        assertThat(assertions.function("sha256", "CAST('' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"));

        assertThat(assertions.function("sha256", "CAST('hashme' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("02208B9403A87DF9F4ED6B2EE2657EFAA589026B4CCE9ACCC8E8A5BF3D693C86"));
    }

    @Test
    public void testSha512()
    {
        assertThat(assertions.function("sha512", "CAST('' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("CF83E1357EEFB8BDF1542850D66D8007D620E4050B5715DC83F4A921D36CE9CE47D0D13C5D85F2B0FF8318D2877EEC2F63B931BD47417A81A538327AF927DA3E"));

        assertThat(assertions.function("sha512", "CAST('hashme' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("8A4B59FB9188D09B989FF596AC9CEFBF2ED91DED8DCD9498E8BF2236814A92B23BE6867E7FC340880E514F8FDF97E1F147EA4B0FD6C2DA3557D0CF1C0B58A204"));
    }

    @Test
    public void testMurmur3()
    {
        assertThat(assertions.function("murmur3", "CAST('' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("00000000000000000000000000000000"));

        assertThat(assertions.function("murmur3", "CAST('hashme' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("93192FE805BE23041C8318F67EC4F2BC"));
    }

    @Test
    public void testXxhash64()
    {
        assertThat(assertions.function("xxhash64", "CAST('' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("EF46DB3751D8E999"));

        assertThat(assertions.function("xxhash64", "CAST('hashme' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("F9D96E0E1165E892"));
    }

    @Test
    public void testSpookyHash()
    {
        assertThat(assertions.function("spooky_hash_v2_32", "CAST('' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("6BF50919"));

        assertThat(assertions.function("spooky_hash_v2_32", "CAST('hello' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("D382E6CA"));

        assertThat(assertions.function("spooky_hash_v2_64", "CAST('' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("232706FC6BF50919"));

        assertThat(assertions.function("spooky_hash_v2_64", "CAST('hello' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("3768826AD382E6CA"));
    }

    @Test
    public void testCrc32()
    {
        assertThat(assertions.function("crc32", "to_utf8('CRC me!')"))
                .isEqualTo(38028046L);

        assertThat(assertions.function("crc32", "to_utf8('1234567890')"))
                .isEqualTo(639479525L);

        assertThat(assertions.function("crc32", "to_utf8(CAST(1234567890 AS VARCHAR))"))
                .isEqualTo(639479525L);

        assertThat(assertions.function("crc32", "to_utf8('ABCDEFGHIJK')"))
                .isEqualTo(1129618807L);

        assertThat(assertions.function("crc32", "to_utf8('ABCDEFGHIJKLM')"))
                .isEqualTo(4223167559L);
    }

    @Test
    public void testVarbinarySubstring()
    {
        // TODO
        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "5"))
                .isEqualTo(sqlVarbinary("ratically"));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "50"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "-5"))
                .isEqualTo(sqlVarbinary("cally"));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "-50"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "0"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "5", "6"))
                .isEqualTo(sqlVarbinary("ratica"));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "5", "10"))
                .isEqualTo(sqlVarbinary("ratically"));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "5", "50"))
                .isEqualTo(sqlVarbinary("ratically"));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "50", "10"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "-5", "4"))
                .isEqualTo(sqlVarbinary("call"));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "-5", "40"))
                .isEqualTo(sqlVarbinary("cally"));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "-50", "4"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "0", "4"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("SUBSTR", "VARBINARY 'Quadratically'", "5", "0"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "VARBINARY 'Quadratically'")
                .binding("start", "5"))
                .isEqualTo(sqlVarbinary("ratically"));

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "VARBINARY 'Quadratically'")
                .binding("start", "50"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "VARBINARY 'Quadratically'")
                .binding("start", "-5"))
                .isEqualTo(sqlVarbinary("cally"));

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "VARBINARY 'Quadratically'")
                .binding("start", "-50"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "VARBINARY 'Quadratically'")
                .binding("start", "0"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "VARBINARY 'Quadratically'")
                .binding("start", "5")
                .binding("length", "6"))
                .isEqualTo(sqlVarbinary("ratica"));

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "VARBINARY 'Quadratically'")
                .binding("start", "5")
                .binding("length", "50"))
                .isEqualTo(sqlVarbinary("ratically"));

        // Test SUBSTRING for non-ASCII
        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "X'4FE15FF5'")
                .binding("start", "1")
                .binding("length", "1"))
                .isEqualTo(sqlVarbinary(0x4F));

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "X'4FE15FF5'")
                .binding("start", "2")
                .binding("length", "2"))
                .isEqualTo(sqlVarbinary(0xE1, 0x5F));

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "X'4FE15FF5'")
                .binding("start", "3"))
                .isEqualTo(sqlVarbinary(0x5F, 0xF5));

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "X'4FE15FF5'")
                .binding("start", "-2"))
                .isEqualTo(sqlVarbinary(0x5F, 0xF5));
    }

    @Test
    public void testHmacMd5()
    {
        assertThat(assertions.function("hmac_md5", "CAST('' AS VARBINARY)", "CAST('key' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("63530468A04E386459855DA0063B6596"));

        assertThat(assertions.function("hmac_md5", "CAST('hashme' AS VARBINARY)", "CAST('key' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("0A26EBEB0E7B65F528D96F7BC631BC8F"));
    }

    @Test
    public void testHmacSHA1()
    {
        assertThat(assertions.function("hmac_sha1", "CAST('' AS VARBINARY)", "CAST('key' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("F42BB0EEB018EBBD4597AE7213711EC60760843F"));

        assertThat(assertions.function("hmac_sha1", "CAST('hashme' AS VARBINARY)", "CAST('key' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("2E7C4C6AEFA7E69F106EEE3CE21944D0046D2F3D"));
    }

    @Test
    public void testHmacSHA256()
    {
        assertThat(assertions.function("hmac_sha256", "CAST('' AS VARBINARY)", "CAST('key' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("5D5D139563C95B5967B9BD9A8C9B233A9DEDB45072794CD232DC1B74832607D0"));

        assertThat(assertions.function("hmac_sha256", "CAST('hashme' AS VARBINARY)", "CAST('key' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("D3D72F9FACDE059DA3A4EB43A9ABDD4B35118E0FEF00E6D16FB04BB332AF0484"));
    }

    @Test
    public void testHmacSHA512()
    {
        assertThat(assertions.function("hmac_sha512", "CAST('' AS VARBINARY)", "CAST('key' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("84FA5AA0279BBC473267D05A53EA03310A987CECC4C1535FF29B6D76B8F1444A" +
                                               "728DF3AADB89D4A9A6709E1998F373566E8F824A8CA93B1821F0B69BC2A2F65E"));

        assertThat(assertions.function("hmac_sha512", "CAST('hashme' AS VARBINARY)", "CAST('key' AS VARBINARY)"))
                .isEqualTo(sqlVarbinaryFromHex("FEFA712B67DED871E1ED987F8B20D6A69EB9FCC87974218B9A1A6D5202B54C18" +
                                               "ECDA4839A979DED22F07E0881CF40B762691992D120408F49D6212E112509D72"));
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as varbinary)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "X'58'"))
                .isEqualTo(false);
    }

    @Test
    public void testReverse()
    {
        assertThat(assertions.function("reverse", "CAST('' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary(""));

        assertThat(assertions.function("reverse", "CAST('hello' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("olleh"));

        assertThat(assertions.function("reverse", "CAST('Quadratically' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("yllacitardauQ"));

        assertThat(assertions.function("reverse", "CAST('racecar' AS VARBINARY)"))
                .isEqualTo(sqlVarbinary("racecar"));
    }

    private static String encodeBase64(byte[] value)
    {
        return Base64.getEncoder().encodeToString(value);
    }

    private static String encodeBase64(String value)
    {
        return encodeBase64(value.getBytes(UTF_8));
    }

    private static String encodeBase64Url(byte[] value)
    {
        return Base64.getUrlEncoder().encodeToString(value);
    }

    private static String encodeBase64Url(String value)
    {
        return encodeBase64Url(value.getBytes(UTF_8));
    }

    private static String encodeBase32(String value)
    {
        return encodeBase32(value.getBytes(UTF_8));
    }

    private static String encodeBase32(byte[] value)
    {
        return BaseEncoding.base32().encode(value);
    }

    private static String encodeHex(String value)
    {
        return base16().encode(value.getBytes(UTF_8));
    }
}
