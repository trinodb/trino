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
import org.testng.annotations.Test;

import java.util.Base64;

import static com.google.common.io.BaseEncoding.base16;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.SqlVarbinaryTestingUtil.sqlVarbinary;
import static io.trino.testing.SqlVarbinaryTestingUtil.sqlVarbinaryFromHex;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestVarbinaryFunctions
        extends AbstractTestFunctions
{
    private static final byte[] ALL_BYTES;

    static {
        ALL_BYTES = new byte[256];
        for (int i = 0; i < ALL_BYTES.length; i++) {
            ALL_BYTES[i] = (byte) i;
        }
    }

    @Test
    public void testBinaryLiteral()
    {
        assertFunction("X'58F7'", VARBINARY, sqlVarbinaryFromHex("58F7"));
    }

    @Test
    public void testLength()
    {
        assertFunction("length(CAST('' AS VARBINARY))", BIGINT, 0L);
        assertFunction("length(CAST('a' AS VARBINARY))", BIGINT, 1L);
        assertFunction("length(CAST('abc' AS VARBINARY))", BIGINT, 3L);
    }

    @Test
    public void testConcat()
    {
        assertInvalidFunction("CONCAT(X'')", "There must be two or more concatenation arguments");

        assertFunction("CAST('foo' AS VARBINARY) || CAST ('bar' AS VARBINARY)", VARBINARY, sqlVarbinary("foo" + "bar"));
        assertFunction("CAST('foo' AS VARBINARY) || CAST ('bar' AS VARBINARY) || CAST ('baz' AS VARBINARY)", VARBINARY, sqlVarbinary("foo" + "bar" + "baz"));
        assertFunction("CAST(' foo ' AS VARBINARY) || CAST ('  bar  ' AS VARBINARY) || CAST ('   baz   ' AS VARBINARY)", VARBINARY, sqlVarbinary(" foo " + "  bar  " + "   baz   "));
        assertFunction("CAST('foo' AS VARBINARY) || CAST ('bar' AS VARBINARY) || CAST ('bazbaz' AS VARBINARY)", VARBINARY, sqlVarbinary("foo" + "bar" + "bazbaz"));

        assertFunction("X'000102' || X'AAABAC' || X'FDFEFF'", VARBINARY, sqlVarbinaryFromHex("000102" + "AAABAC" + "FDFEFF"));
        assertFunction("X'CAFFEE' || X'F7' || X'DE58'", VARBINARY, sqlVarbinaryFromHex("CAFFEE" + "F7" + "DE58"));

        assertFunction("X'58' || X'F7'", VARBINARY, sqlVarbinaryFromHex("58F7"));
        assertFunction("X'' || X'58' || X'F7'", VARBINARY, sqlVarbinaryFromHex("58F7"));
        assertFunction("X'58' || X'' || X'F7'", VARBINARY, sqlVarbinaryFromHex("58F7"));
        assertFunction("X'58' || X'F7' || X''", VARBINARY, sqlVarbinaryFromHex("58F7"));
        assertFunction("X'' || X'58' || X'' || X'F7' || X''", VARBINARY, sqlVarbinaryFromHex("58F7"));
        assertFunction("X'' || X'' || X'' || X'' || X'' || X''", VARBINARY, sqlVarbinaryFromHex(""));

        assertFunction("CONCAT(CAST('foo' AS VARBINARY), CAST ('bar' AS VARBINARY))", VARBINARY, sqlVarbinary("foo" + "bar"));
        assertFunction("CONCAT(CAST('foo' AS VARBINARY), CAST ('bar' AS VARBINARY), CAST ('baz' AS VARBINARY))", VARBINARY, sqlVarbinary("foo" + "bar" + "baz"));
        assertFunction("CONCAT(CAST('foo' AS VARBINARY), CAST ('bar' AS VARBINARY), CAST ('bazbaz' AS VARBINARY))", VARBINARY, sqlVarbinary("foo" + "bar" + "bazbaz"));

        assertFunction("CONCAT(X'000102', X'AAABAC', X'FDFEFF')", VARBINARY, sqlVarbinaryFromHex("000102" + "AAABAC" + "FDFEFF"));
        assertFunction("CONCAT(X'CAFFEE', X'F7', X'DE58')", VARBINARY, sqlVarbinaryFromHex("CAFFEE" + "F7" + "DE58"));

        assertFunction("CONCAT(X'58', X'F7')", VARBINARY, sqlVarbinaryFromHex("58F7"));
        assertFunction("CONCAT(X'', X'58', X'F7')", VARBINARY, sqlVarbinaryFromHex("58F7"));
        assertFunction("CONCAT(X'58', X'', X'F7')", VARBINARY, sqlVarbinaryFromHex("58F7"));
        assertFunction("CONCAT(X'58', X'F7', X'')", VARBINARY, sqlVarbinaryFromHex("58F7"));
        assertFunction("CONCAT(X'', X'58', X'', X'F7', X'')", VARBINARY, sqlVarbinaryFromHex("58F7"));
        assertFunction("CONCAT(X'', X'', X'', X'', X'', X'')", VARBINARY, sqlVarbinaryFromHex(""));
    }

    @Test
    public void testToBase64()
    {
        assertFunction("to_base64(CAST('' AS VARBINARY))", VARCHAR, encodeBase64(""));
        assertFunction("to_base64(CAST('a' AS VARBINARY))", VARCHAR, encodeBase64("a"));
        assertFunction("to_base64(CAST('abc' AS VARBINARY))", VARCHAR, encodeBase64("abc"));
        assertFunction("to_base64(CAST('hello world' AS VARBINARY))", VARCHAR, "aGVsbG8gd29ybGQ=");
    }

    @Test
    public void testFromBase64()
    {
        assertFunction("from_base64(to_base64(CAST('' AS VARBINARY)))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_base64(to_base64(CAST('a' AS VARBINARY)))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_base64(to_base64(CAST('abc' AS VARBINARY)))", VARBINARY, sqlVarbinary("abc"));
        assertFunction("from_base64(CAST(to_base64(CAST('' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_base64(CAST(to_base64(CAST('a' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_base64(CAST(to_base64(CAST('abc' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary("abc"));
        assertFunction(format("to_base64(from_base64('%s'))", encodeBase64(ALL_BYTES)), VARCHAR, encodeBase64(ALL_BYTES));
    }

    @Test
    public void testToBase64Url()
    {
        assertFunction("to_base64url(CAST('' AS VARBINARY))", VARCHAR, encodeBase64Url(""));
        assertFunction("to_base64url(CAST('a' AS VARBINARY))", VARCHAR, encodeBase64Url("a"));
        assertFunction("to_base64url(CAST('abc' AS VARBINARY))", VARCHAR, encodeBase64Url("abc"));
        assertFunction("to_base64url(CAST('hello world' AS VARBINARY))", VARCHAR, "aGVsbG8gd29ybGQ=");
    }

    @Test
    public void testFromBase64Url()
    {
        assertFunction("from_base64url(to_base64url(CAST('' AS VARBINARY)))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_base64url(to_base64url(CAST('a' AS VARBINARY)))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_base64url(to_base64url(CAST('abc' AS VARBINARY)))", VARBINARY, sqlVarbinary("abc"));
        assertFunction("from_base64url(CAST(to_base64url(CAST('' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_base64url(CAST(to_base64url(CAST('a' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_base64url(CAST(to_base64url(CAST('abc' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary("abc"));
        assertFunction(format("to_base64url(from_base64url('%s'))", encodeBase64Url(ALL_BYTES)), VARCHAR, encodeBase64Url(ALL_BYTES));
    }

    @Test
    public void testToBase32()
    {
        assertFunction("to_base32(CAST('' AS VARBINARY))", VARCHAR, encodeBase32(""));
        assertFunction("to_base32(CAST('a' AS VARBINARY))", VARCHAR, encodeBase32("a"));
        assertFunction("to_base32(CAST('abc' AS VARBINARY))", VARCHAR, encodeBase32("abc"));
        assertFunction("to_base32(CAST('hello world' AS VARBINARY))", VARCHAR, "NBSWY3DPEB3W64TMMQ======");
        assertFunction("to_base32(NULL)", VARCHAR, null);
    }

    @Test
    public void testFromBase32()
    {
        assertFunction("from_base32('')", VARBINARY, sqlVarbinary(""));
        assertFunction("from_base32('ME======')", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_base32('MFRGG===')", VARBINARY, sqlVarbinary("abc"));
        assertFunction("from_base32('NBSWY3DPEB3W64TMMQ======')", VARBINARY, sqlVarbinary("hello world"));

        assertFunction("from_base32(to_base32(CAST('' AS VARBINARY)))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_base32(to_base32(CAST('a' AS VARBINARY)))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_base32(to_base32(CAST('abc' AS VARBINARY)))", VARBINARY, sqlVarbinary("abc"));
        assertFunction("from_base32(to_base32(CAST('hello world' AS VARBINARY)))", VARBINARY, sqlVarbinary("hello world"));
        assertFunction("from_base32(CAST(to_base32(CAST('' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_base32(CAST(to_base32(CAST('a' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_base32(CAST(to_base32(CAST('abc' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary("abc"));
        assertFunction("from_base32(CAST(to_base32(CAST('hello world' AS VARBINARY)) AS VARBINARY))", VARBINARY, sqlVarbinary("hello world"));
        assertFunction(format("to_base32(from_base32('%s'))", encodeBase32(ALL_BYTES)), VARCHAR, encodeBase32(ALL_BYTES));

        assertFunction("from_base32(CAST(NULL AS VARCHAR))", VARBINARY, null);
        assertFunction("from_base32(CAST(NULL AS VARBINARY))", VARBINARY, null);

        assertInvalidFunction("from_base32('1=')", "Invalid input length 1");
        assertInvalidFunction("from_base32('M1======')", "Unrecognized character: 1");
        assertInvalidFunction("from_base32(CAST('1=' AS VARBINARY))", "Invalid input length 1");
        assertInvalidFunction("from_base32(CAST('M1======' AS VARBINARY))", "Unrecognized character: 1");
    }

    @Test
    public void testToHex()
    {
        assertFunction("to_hex(CAST('' AS VARBINARY))", VARCHAR, encodeHex(""));
        assertFunction("to_hex(CAST('a' AS VARBINARY))", VARCHAR, encodeHex("a"));
        assertFunction("to_hex(CAST('abc' AS VARBINARY))", VARCHAR, encodeHex("abc"));
        assertFunction("to_hex(CAST('hello world' AS VARBINARY))", VARCHAR, "68656C6C6F20776F726C64");
    }

    @Test
    public void testFromHex()
    {
        assertFunction("from_hex('')", VARBINARY, sqlVarbinary(""));
        assertFunction("from_hex('61')", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_hex('617a6f')", VARBINARY, sqlVarbinary("azo"));
        assertFunction("from_hex('617A6F')", VARBINARY, sqlVarbinary("azo"));
        assertFunction("from_hex(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinary(""));
        assertFunction("from_hex(CAST('61' AS VARBINARY))", VARBINARY, sqlVarbinary("a"));
        assertFunction("from_hex(CAST('617a6F' AS VARBINARY))", VARBINARY, sqlVarbinary("azo"));
        assertFunction(format("to_hex(from_hex('%s'))", base16().encode(ALL_BYTES)), VARCHAR, base16().encode(ALL_BYTES));
        assertInvalidFunction("from_hex('f/')", INVALID_FUNCTION_ARGUMENT); // '0' - 1
        assertInvalidFunction("from_hex('f:')", INVALID_FUNCTION_ARGUMENT); // '9' + 1
        assertInvalidFunction("from_hex('f@')", INVALID_FUNCTION_ARGUMENT); // 'A' - 1
        assertInvalidFunction("from_hex('fG')", INVALID_FUNCTION_ARGUMENT); // 'F' + 1
        assertInvalidFunction("from_hex('f`')", INVALID_FUNCTION_ARGUMENT); // 'a' - 1
        assertInvalidFunction("from_hex('fg')", INVALID_FUNCTION_ARGUMENT); // 'f' + 1
        assertInvalidFunction("from_hex('fff')", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testToBigEndian64()
    {
        assertFunction("to_big_endian_64(0)", VARBINARY, sqlVarbinaryFromHex("0000000000000000"));
        assertFunction("to_big_endian_64(1)", VARBINARY, sqlVarbinaryFromHex("0000000000000001"));
        assertFunction("to_big_endian_64(9223372036854775807)", VARBINARY, sqlVarbinaryFromHex("7FFFFFFFFFFFFFFF"));
        assertFunction("to_big_endian_64(-9223372036854775807)", VARBINARY, sqlVarbinaryFromHex("8000000000000001"));
    }

    @Test
    public void testFromBigEndian64()
    {
        assertFunction("from_big_endian_64(from_hex('0000000000000000'))", BIGINT, 0L);
        assertFunction("from_big_endian_64(from_hex('0000000000000001'))", BIGINT, 1L);
        assertFunction("from_big_endian_64(from_hex('7FFFFFFFFFFFFFFF'))", BIGINT, 9223372036854775807L);
        assertFunction("from_big_endian_64(from_hex('8000000000000001'))", BIGINT, -9223372036854775807L);
        assertInvalidFunction("from_big_endian_64(from_hex(''))", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("from_big_endian_64(from_hex('1111'))", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("from_big_endian_64(from_hex('000000000000000011'))", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testToBigEndian32()
    {
        assertFunction("to_big_endian_32(0)", VARBINARY, sqlVarbinaryFromHex("00000000"));
        assertFunction("to_big_endian_32(1)", VARBINARY, sqlVarbinaryFromHex("00000001"));
        assertFunction("to_big_endian_32(2147483647)", VARBINARY, sqlVarbinaryFromHex("7FFFFFFF"));
        assertFunction("to_big_endian_32(-2147483647)", VARBINARY, sqlVarbinaryFromHex("80000001"));
    }

    @Test
    public void testFromBigEndian32()
    {
        assertFunction("from_big_endian_32(from_hex('00000000'))", INTEGER, 0);
        assertFunction("from_big_endian_32(from_hex('00000001'))", INTEGER, 1);
        assertFunction("from_big_endian_32(from_hex('7FFFFFFF'))", INTEGER, 2147483647);
        assertFunction("from_big_endian_32(from_hex('80000001'))", INTEGER, -2147483647);
        assertInvalidFunction("from_big_endian_32(from_hex(''))", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("from_big_endian_32(from_hex('1111'))", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("from_big_endian_32(from_hex('000000000000000011'))", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testToIEEE754Binary32()
    {
        assertFunction("to_ieee754_32(CAST(0.0 AS REAL))", VARBINARY, sqlVarbinaryFromHex("00000000"));
        assertFunction("to_ieee754_32(CAST(1.0 AS REAL))", VARBINARY, sqlVarbinaryFromHex("3F800000"));
        assertFunction("to_ieee754_32(CAST(3.14 AS REAL))", VARBINARY, sqlVarbinaryFromHex("4048F5C3"));
        assertFunction("to_ieee754_32(CAST(NAN() AS REAL))", VARBINARY, sqlVarbinaryFromHex("7FC00000"));
        assertFunction("to_ieee754_32(CAST(INFINITY() AS REAL))", VARBINARY, sqlVarbinaryFromHex("7F800000"));
        assertFunction("to_ieee754_32(CAST(-INFINITY() AS REAL))", VARBINARY, sqlVarbinaryFromHex("FF800000"));
        assertFunction("to_ieee754_32(CAST(3.4028235E38 AS REAL))", VARBINARY, sqlVarbinaryFromHex("7F7FFFFF"));
        assertFunction("to_ieee754_32(CAST(-3.4028235E38 AS REAL))", VARBINARY, sqlVarbinaryFromHex("FF7FFFFF"));
        assertFunction("to_ieee754_32(CAST(1.4E-45 AS REAL))", VARBINARY, sqlVarbinaryFromHex("00000001"));
        assertFunction("to_ieee754_32(CAST(-1.4E-45 AS REAL))", VARBINARY, sqlVarbinaryFromHex("80000001"));
    }

    @Test
    public void testFromIEEE754Binary32()
    {
        assertFunction("from_ieee754_32(from_hex('3F800000'))", REAL, 1.0f);
        assertFunction("from_ieee754_32(to_ieee754_32(CAST(1.0 AS REAL)))", REAL, 1.0f);
        assertFunction("from_ieee754_32(from_hex('4048F5C3'))", REAL, 3.14f);
        assertFunction("from_ieee754_32(to_ieee754_32(CAST(3.14 AS REAL)))", REAL, 3.14f);
        assertFunction("from_ieee754_32(to_ieee754_32(CAST(NAN() AS REAL)))", REAL, Float.NaN);
        assertFunction("from_ieee754_32(to_ieee754_32(CAST(INFINITY() AS REAL)))", REAL, Float.POSITIVE_INFINITY);
        assertFunction("from_ieee754_32(to_ieee754_32(CAST(-INFINITY() AS REAL)))", REAL, Float.NEGATIVE_INFINITY);
        assertFunction("from_ieee754_32(to_ieee754_32(CAST(3.4028235E38 AS REAL)))", REAL, 3.4028235E38f);
        assertFunction("from_ieee754_32(to_ieee754_32(CAST(-3.4028235E38 AS REAL)))", REAL, -3.4028235E38f);
        assertFunction("from_ieee754_32(to_ieee754_32(CAST(1.4E-45 AS REAL)))", REAL, 1.4E-45f);
        assertFunction("from_ieee754_32(to_ieee754_32(CAST(-1.4E-45 AS REAL)))", REAL, -1.4E-45f);
        assertInvalidFunction("from_ieee754_32(from_hex('0000'))", "Input floating-point value must be exactly 4 bytes long");
    }

    @Test
    public void testToIEEE754Binary64()
    {
        assertFunction("to_ieee754_64(0.0)", VARBINARY, sqlVarbinaryFromHex("0000000000000000"));
        assertFunction("to_ieee754_64(1.0)", VARBINARY, sqlVarbinaryFromHex("3FF0000000000000"));
        assertFunction("to_ieee754_64(3.1415926)", VARBINARY, sqlVarbinaryFromHex("400921FB4D12D84A"));
        assertFunction("to_ieee754_64(NAN())", VARBINARY, sqlVarbinaryFromHex("7FF8000000000000"));
        assertFunction("to_ieee754_64(INFINITY())", VARBINARY, sqlVarbinaryFromHex("7FF0000000000000"));
        assertFunction("to_ieee754_64(-INFINITY())", VARBINARY, sqlVarbinaryFromHex("FFF0000000000000"));
        assertFunction("to_ieee754_64(1.7976931348623157E308)", VARBINARY, sqlVarbinaryFromHex("7FEFFFFFFFFFFFFF"));
        assertFunction("to_ieee754_64(-1.7976931348623157E308)", VARBINARY, sqlVarbinaryFromHex("FFEFFFFFFFFFFFFF"));
        assertFunction("to_ieee754_64(4.9E-324)", VARBINARY, sqlVarbinaryFromHex("0000000000000001"));
        assertFunction("to_ieee754_64(-4.9E-324)", VARBINARY, sqlVarbinaryFromHex("8000000000000001"));
    }

    @Test
    public void testFromIEEE754Binary64()
    {
        assertFunction("from_ieee754_64(from_hex('0000000000000000'))", DOUBLE, 0.0);
        assertFunction("from_ieee754_64(from_hex('3FF0000000000000'))", DOUBLE, 1.0);
        assertFunction("from_ieee754_64(to_ieee754_64(3.1415926))", DOUBLE, 3.1415926);
        assertFunction("from_ieee754_64(to_ieee754_64(NAN()))", DOUBLE, Double.NaN);
        assertFunction("from_ieee754_64(to_ieee754_64(INFINITY()))", DOUBLE, Double.POSITIVE_INFINITY);
        assertFunction("from_ieee754_64(to_ieee754_64(-INFINITY()))", DOUBLE, Double.NEGATIVE_INFINITY);
        assertFunction("from_ieee754_64(to_ieee754_64(1.7976931348623157E308))", DOUBLE, 1.7976931348623157E308);
        assertFunction("from_ieee754_64(to_ieee754_64(-1.7976931348623157E308))", DOUBLE, -1.7976931348623157E308);
        assertFunction("from_ieee754_64(to_ieee754_64(4.9E-324))", DOUBLE, 4.9E-324);
        assertFunction("from_ieee754_64(to_ieee754_64(-4.9E-324))", DOUBLE, -4.9E-324);
        assertInvalidFunction("from_ieee754_64(from_hex('00000000'))", "Input floating-point value must be exactly 8 bytes long");
    }

    @Test
    public void testLpad()
    {
        assertFunction("lpad(x'1234',7,x'45')", VARBINARY, sqlVarbinaryFromHex("45454545451234"));
        assertFunction("lpad(x'1234',7,x'4524')", VARBINARY, sqlVarbinaryFromHex("45244524451234"));
        assertFunction("lpad(x'1234',3,x'4524')", VARBINARY, sqlVarbinaryFromHex("451234"));
        assertFunction("lpad(x'1234',0,x'4524')", VARBINARY, sqlVarbinaryFromHex(""));
        assertFunction("lpad(x'1234',1,x'4524')", VARBINARY, sqlVarbinaryFromHex("12"));
        assertInvalidFunction("lpad(x'2312',-1,x'4524')", "Target length must be in the range [0.." + Integer.MAX_VALUE + "]");
        assertInvalidFunction("lpad(x'2312',1,x'')", "Padding bytes must not be empty");
    }

    @Test
    public void testRpad()
    {
        assertFunction("rpad(x'1234',7,x'45')", VARBINARY, sqlVarbinaryFromHex("12344545454545"));
        assertFunction("rpad(x'1234',7,x'4524')", VARBINARY, sqlVarbinaryFromHex("12344524452445"));
        assertFunction("rpad(x'1234',3,x'4524')", VARBINARY, sqlVarbinaryFromHex("123445"));
        assertFunction("rpad(x'23',0,x'4524')", VARBINARY, sqlVarbinaryFromHex(""));
        assertFunction("rpad(x'1234',1,x'4524')", VARBINARY, sqlVarbinaryFromHex("12"));
        assertInvalidFunction("rpad(x'1234',-1,x'4524')", "Target length must be in the range [0.." + Integer.MAX_VALUE + "]");
        assertInvalidFunction("rpad(x'1234',1,x'')", "Padding bytes must not be empty");
    }

    @Test
    public void testMd5()
    {
        assertFunction("md5(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("D41D8CD98F00B204E9800998ECF8427E"));
        assertFunction("md5(CAST('hashme' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("533F6357E0210E67D91F651BC49E1278"));
    }

    @Test
    public void testSha1()
    {
        assertFunction("sha1(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("DA39A3EE5E6B4B0D3255BFEF95601890AFD80709"));
        assertFunction("sha1(CAST('hashme' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("FB78992E561929A6967D5328F49413FA99048D06"));
    }

    @Test
    public void testSha256()
    {
        assertFunction("sha256(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"));
        assertFunction("sha256(CAST('hashme' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("02208B9403A87DF9F4ED6B2EE2657EFAA589026B4CCE9ACCC8E8A5BF3D693C86"));
    }

    @Test
    public void testSha512()
    {
        assertFunction("sha512(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("CF83E1357EEFB8BDF1542850D66D8007D620E4050B5715DC83F4A921D36CE9CE47D0D13C5D85F2B0FF8318D2877EEC2F63B931BD47417A81A538327AF927DA3E"));
        assertFunction("sha512(CAST('hashme' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("8A4B59FB9188D09B989FF596AC9CEFBF2ED91DED8DCD9498E8BF2236814A92B23BE6867E7FC340880E514F8FDF97E1F147EA4B0FD6C2DA3557D0CF1C0B58A204"));
    }

    @Test
    public void testMurmur3()
    {
        assertFunction("murmur3(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("00000000000000000000000000000000"));
        assertFunction("murmur3(CAST('hashme' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("93192FE805BE23041C8318F67EC4F2BC"));
    }

    @Test
    public void testXxhash64()
    {
        assertFunction("xxhash64(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("EF46DB3751D8E999"));
        assertFunction("xxhash64(CAST('hashme' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("F9D96E0E1165E892"));
    }

    @Test
    public void testSpookyHash()
    {
        assertFunction("spooky_hash_v2_32(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("6BF50919"));
        assertFunction("spooky_hash_v2_32(CAST('hello' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("D382E6CA"));
        assertFunction("spooky_hash_v2_64(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("232706FC6BF50919"));
        assertFunction("spooky_hash_v2_64(CAST('hello' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("3768826AD382E6CA"));
    }

    @Test
    public void testCrc32()
    {
        assertFunction("crc32(to_utf8('CRC me!'))", BIGINT, 38028046L);
        assertFunction("crc32(to_utf8('1234567890'))", BIGINT, 639479525L);
        assertFunction("crc32(to_utf8(CAST(1234567890 AS VARCHAR)))", BIGINT, 639479525L);
        assertFunction("crc32(to_utf8('ABCDEFGHIJK'))", BIGINT, 1129618807L);
        assertFunction("crc32(to_utf8('ABCDEFGHIJKLM'))", BIGINT, 4223167559L);
    }

    @Test
    public void testVarbinarySubstring()
    {
        assertFunction("SUBSTR(VARBINARY 'Quadratically', 5)", VARBINARY, sqlVarbinary("ratically"));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', 50)", VARBINARY, sqlVarbinary(""));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', -5)", VARBINARY, sqlVarbinary("cally"));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', -50)", VARBINARY, sqlVarbinary(""));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', 0)", VARBINARY, sqlVarbinary(""));

        assertFunction("SUBSTR(VARBINARY 'Quadratically', 5, 6)", VARBINARY, sqlVarbinary("ratica"));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', 5, 10)", VARBINARY, sqlVarbinary("ratically"));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', 5, 50)", VARBINARY, sqlVarbinary("ratically"));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', 50, 10)", VARBINARY, sqlVarbinary(""));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', -5, 4)", VARBINARY, sqlVarbinary("call"));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', -5, 40)", VARBINARY, sqlVarbinary("cally"));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', -50, 4)", VARBINARY, sqlVarbinary(""));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', 0, 4)", VARBINARY, sqlVarbinary(""));
        assertFunction("SUBSTR(VARBINARY 'Quadratically', 5, 0)", VARBINARY, sqlVarbinary(""));

        assertFunction("SUBSTRING(VARBINARY 'Quadratically' FROM 5)", VARBINARY, sqlVarbinary("ratically"));
        assertFunction("SUBSTRING(VARBINARY 'Quadratically' FROM 50)", VARBINARY, sqlVarbinary(""));
        assertFunction("SUBSTRING(VARBINARY 'Quadratically' FROM -5)", VARBINARY, sqlVarbinary("cally"));
        assertFunction("SUBSTRING(VARBINARY 'Quadratically' FROM -50)", VARBINARY, sqlVarbinary(""));
        assertFunction("SUBSTRING(VARBINARY 'Quadratically' FROM 0)", VARBINARY, sqlVarbinary(""));

        assertFunction("SUBSTRING(VARBINARY 'Quadratically' FROM 5 FOR 6)", VARBINARY, sqlVarbinary("ratica"));
        assertFunction("SUBSTRING(VARBINARY 'Quadratically' FROM 5 FOR 50)", VARBINARY, sqlVarbinary("ratically"));

        // Test SUBSTRING for non-ASCII
        assertFunction("SUBSTRING(X'4FE15FF5' FROM 1 FOR 1)", VARBINARY, sqlVarbinary(0x4F));
        assertFunction("SUBSTRING(X'4FE15FF5' FROM 2 FOR 2)", VARBINARY, sqlVarbinary(0xE1, 0x5F));
        assertFunction("SUBSTRING(X'4FE15FF5' FROM 3)", VARBINARY, sqlVarbinary(0x5F, 0xF5));
        assertFunction("SUBSTRING(X'4FE15FF5' FROM -2)", VARBINARY, sqlVarbinary(0x5F, 0xF5));
    }

    @Test
    public void testHmacMd5()
    {
        assertFunction("hmac_md5(CAST('' AS VARBINARY), CAST('key' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("63530468A04E386459855DA0063B6596"));
        assertFunction("hmac_md5(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("0A26EBEB0E7B65F528D96F7BC631BC8F"));
    }

    @Test
    public void testHmacSHA1()
    {
        assertFunction("hmac_sha1(CAST('' AS VARBINARY), CAST('key' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("F42BB0EEB018EBBD4597AE7213711EC60760843F"));
        assertFunction("hmac_sha1(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY))", VARBINARY, sqlVarbinaryFromHex("2E7C4C6AEFA7E69F106EEE3CE21944D0046D2F3D"));
    }

    @Test
    public void testHmacSHA256()
    {
        assertFunction("hmac_sha256(CAST('' AS VARBINARY), CAST('key' AS VARBINARY))",
                VARBINARY, sqlVarbinaryFromHex("5D5D139563C95B5967B9BD9A8C9B233A9DEDB45072794CD232DC1B74832607D0"));
        assertFunction("hmac_sha256(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY))",
                VARBINARY, sqlVarbinaryFromHex("D3D72F9FACDE059DA3A4EB43A9ABDD4B35118E0FEF00E6D16FB04BB332AF0484"));
    }

    @Test
    public void testHmacSHA512()
    {
        assertFunction("hmac_sha512(CAST('' AS VARBINARY), CAST('key' AS VARBINARY))",
                VARBINARY, sqlVarbinaryFromHex("84FA5AA0279BBC473267D05A53EA03310A987CECC4C1535FF29B6D76B8F1444A" +
                        "728DF3AADB89D4A9A6709E1998F373566E8F824A8CA93B1821F0B69BC2A2F65E"));
        assertFunction("hmac_sha512(CAST('hashme' AS VARBINARY), CAST('key' AS VARBINARY))",
                VARBINARY, sqlVarbinaryFromHex("FEFA712B67DED871E1ED987F8B20D6A69EB9FCC87974218B9A1A6D5202B54C18" +
                        "ECDA4839A979DED22F07E0881CF40B762691992D120408F49D6212E112509D72"));
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as varbinary)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "X'58'", BOOLEAN, false);
    }

    @Test
    public void testReverse()
    {
        assertFunction("REVERSE(CAST('' AS VARBINARY))", VARBINARY, sqlVarbinary(""));
        assertFunction("REVERSE(CAST('hello' AS VARBINARY))", VARBINARY, sqlVarbinary("olleh"));
        assertFunction("REVERSE(CAST('Quadratically' AS VARBINARY))", VARBINARY, sqlVarbinary("yllacitardauQ"));
        assertFunction("REVERSE(CAST('racecar' AS VARBINARY))", VARBINARY, sqlVarbinary("racecar"));
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
