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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.TOO_MANY_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.util.StructuralTestUtil.mapType;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestStringFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(getClass()) // To use utf8 function
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Description("Varchar length")
    @ScalarFunction(value = "vl", deterministic = true)
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long varcharLength(@LiteralParameter("x") Long param, @SqlType("varchar(x)") Slice slice)
    {
        return param;
    }

    @ScalarFunction(value = "utf8", deterministic = false)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice convertBinaryToVarchar(@SqlType(StandardTypes.VARBINARY) Slice binary)
    {
        return binary;
    }

    public static String padRight(String s, int n)
    {
        return s + " ".repeat(n - s.codePointCount(0, s.length()));
    }

    @Test
    public void testChr()
    {
        assertThat(assertions.function("chr", "65"))
                .hasType(createVarcharType(1))
                .isEqualTo("A");

        assertThat(assertions.function("chr", "9731"))
                .hasType(createVarcharType(1))
                .isEqualTo("\u2603");

        assertThat(assertions.function("chr", "131210"))
                .hasType(createVarcharType(1))
                .isEqualTo(new String(Character.toChars(131210)));

        assertThat(assertions.function("chr", "0"))
                .hasType(createVarcharType(1))
                .isEqualTo("\0");

        assertTrinoExceptionThrownBy(() -> assertions.function("chr", "-1").evaluate())
                .hasMessage("Not a valid Unicode code point: -1");

        assertTrinoExceptionThrownBy(() -> assertions.function("chr", "1234567").evaluate())
                .hasMessage("Not a valid Unicode code point: 1234567");

        assertTrinoExceptionThrownBy(() -> assertions.function("chr", "8589934592").evaluate())
                .hasMessage("Not a valid Unicode code point: 8589934592");
    }

    @Test
    public void testCodepoint()
    {
        assertThat(assertions.function("codepoint", "'x'"))
                .hasType(INTEGER)
                .isEqualTo(0x78);

        assertThat(assertions.function("codepoint", "'\u840C'"))
                .hasType(INTEGER)
                .isEqualTo(0x840C);

        assertThat(assertions.function("codepoint", "CHR(128077)"))
                .hasType(INTEGER)
                .isEqualTo(128077);

        assertThat(assertions.function("codepoint", "CHR(33804)"))
                .hasType(INTEGER)
                .isEqualTo(33804);

        assertTrinoExceptionThrownBy(() -> assertions.function("codepoint", "'hello'").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);

        assertTrinoExceptionThrownBy(() -> assertions.function("codepoint", "'\u666E\u5217\u65AF\u6258'").evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);

        assertTrinoExceptionThrownBy(() -> assertions.function("codepoint", "''").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testConcat()
    {
        assertTrinoExceptionThrownBy(() -> assertions.function("concat", "''").evaluate())
                .hasMessage("There must be two or more concatenation arguments");

        assertThat(assertions.function("concat", "'hello'", "' world'"))
                .hasType(VARCHAR)
                .isEqualTo("hello world");

        assertThat(assertions.function("concat", "''", "''"))
                .hasType(VARCHAR)
                .isEqualTo("");

        assertThat(assertions.function("concat", "'what'", "''"))
                .hasType(VARCHAR)
                .isEqualTo("what");

        assertThat(assertions.function("concat", "''", "'what'"))
                .hasType(VARCHAR)
                .isEqualTo("what");

        assertThat(assertions.function("concat", "'this'", "' is'", "' cool'"))
                .hasType(VARCHAR)
                .isEqualTo("this is cool");

        assertThat(assertions.function("concat", "'this'", "' is'", "' cool'"))
                .hasType(VARCHAR)
                .isEqualTo("this is cool");

        // Test concat for non-ASCII
        assertThat(assertions.function("concat", "'hello na\u00EFve'", "' world'"))
                .hasType(VARCHAR)
                .isEqualTo("hello na\u00EFve world");

        assertThat(assertions.function("concat", "'\uD801\uDC2D'", "'end'"))
                .hasType(VARCHAR)
                .isEqualTo("\uD801\uDC2Dend");

        assertThat(assertions.function("concat", "'\uD801\uDC2D'", "'end'", "'\uD801\uDC2D'"))
                .hasType(VARCHAR)
                .isEqualTo("\uD801\uDC2Dend\uD801\uDC2D");

        assertThat(assertions.function("concat", "'\u4FE1\u5FF5'", "',\u7231'", "',\u5E0C\u671B'"))
                .hasType(VARCHAR)
                .isEqualTo("\u4FE1\u5FF5,\u7231,\u5E0C\u671B");

        // Test argument count limit
        assertThat(assertions.expression("CONCAT(" + Joiner.on(", ").join(nCopies(127, "'x'")) + ")"))
                .hasType(VARCHAR)
                .isEqualTo(Joiner.on("").join(nCopies(127, "x")));

        assertTrinoExceptionThrownBy(() -> assertions.expression("CONCAT(" + Joiner.on(", ").join(nCopies(128, "'x'")) + ")").evaluate())
                .hasErrorCode(TOO_MANY_ARGUMENTS)
                .hasMessage("line 1:12: Too many arguments for function call concat()");
    }

    @Test
    public void testLength()
    {
        assertThat(assertions.function("length", "''"))
                .isEqualTo(0L);

        assertThat(assertions.function("length", "'hello'"))
                .isEqualTo(5L);

        assertThat(assertions.function("length", "'Quadratically'"))
                .isEqualTo(13L);

        // Test length for non-ASCII
        assertThat(assertions.function("length", "'hello na\u00EFve world'"))
                .isEqualTo(17L);

        assertThat(assertions.function("length", "'\uD801\uDC2Dend'"))
                .isEqualTo(4L);

        assertThat(assertions.function("length", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'"))
                .isEqualTo(7L);
    }

    @Test
    public void testCharLength()
    {
        assertThat(assertions.function("length", "CAST('hello' AS CHAR(5))"))
                .isEqualTo(5L);

        assertThat(assertions.function("length", "CAST('Quadratically' AS CHAR(13))"))
                .isEqualTo(13L);

        assertThat(assertions.function("length", "CAST('' AS CHAR(20))"))
                .isEqualTo(20L);

        assertThat(assertions.function("length", "CAST('hello' AS CHAR(20))"))
                .isEqualTo(20L);

        assertThat(assertions.function("length", "CAST('Quadratically' AS CHAR(20))"))
                .isEqualTo(20L);

        // Test length for non-ASCII
        assertThat(assertions.function("length", "CAST('hello na\u00EFve world' AS CHAR(17))"))
                .isEqualTo(17L);

        assertThat(assertions.function("length", "CAST('\uD801\uDC2Dend' AS CHAR(4))"))
                .isEqualTo(4L);

        assertThat(assertions.function("length", "CAST('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' AS CHAR(7))"))
                .isEqualTo(7L);

        assertThat(assertions.function("length", "CAST('hello na\u00EFve world' AS CHAR(20))"))
                .isEqualTo(20L);

        assertThat(assertions.function("length", "CAST('\uD801\uDC2Dend' AS CHAR(20))"))
                .isEqualTo(20L);

        assertThat(assertions.function("length", "CAST('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' AS CHAR(20))"))
                .isEqualTo(20L);
    }

    @Test
    public void testLevenshteinDistance()
    {
        assertThat(assertions.function("levenshtein_distance", "''", "''"))
                .isEqualTo(0L);

        assertThat(assertions.function("levenshtein_distance", "''", "'hello'"))
                .isEqualTo(5L);

        assertThat(assertions.function("levenshtein_distance", "'hello'", "''"))
                .isEqualTo(5L);

        assertThat(assertions.function("levenshtein_distance", "'hello'", "'hello'"))
                .isEqualTo(0L);

        assertThat(assertions.function("levenshtein_distance", "'hello'", "'hello world'"))
                .isEqualTo(6L);

        assertThat(assertions.function("levenshtein_distance", "'hello world'", "'hel wold'"))
                .isEqualTo(3L);

        assertThat(assertions.function("levenshtein_distance", "'hello world'", "'hellq wodld'"))
                .isEqualTo(2L);

        assertThat(assertions.function("levenshtein_distance", "'helo word'", "'hello world'"))
                .isEqualTo(2L);

        assertThat(assertions.function("levenshtein_distance", "'hello word'", "'dello world'"))
                .isEqualTo(2L);

        // Test for non-ASCII
        assertThat(assertions.function("levenshtein_distance", "'hello na\u00EFve world'", "'hello naive world'"))
                .isEqualTo(1L);

        assertThat(assertions.function("levenshtein_distance", "'hello na\u00EFve world'", "'hello na:ive world'"))
                .isEqualTo(2L);

        assertThat(assertions.function("levenshtein_distance", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'\u4FE1\u4EF0,\u7231,\u5E0C\u671B'"))
                .isEqualTo(1L);

        assertThat(assertions.function("levenshtein_distance", "'\u4F11\u5FF5,\u7231,\u5E0C\u671B'", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'"))
                .isEqualTo(1L);

        assertThat(assertions.function("levenshtein_distance", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'\u4FE1\u5FF5\u5E0C\u671B'"))
                .isEqualTo(3L);

        assertThat(assertions.function("levenshtein_distance", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'\u4FE1\u5FF5,love,\u5E0C\u671B'"))
                .isEqualTo(4L);

        // Test for invalid utf-8 characters
        assertTrinoExceptionThrownBy(() -> assertions.function("levenshtein_distance", "'hello world'", "utf8(from_hex('81'))").evaluate())
                .hasMessage("Invalid UTF-8 encoding in characters: �");

        assertTrinoExceptionThrownBy(() -> assertions.function("levenshtein_distance", "'hello wolrd'", "utf8(from_hex('3281'))").evaluate())
                .hasMessage("Invalid UTF-8 encoding in characters: 2�");

        // Test for maximum length
        assertThat(assertions.function("levenshtein_distance", "'hello'", "'%s'".formatted("e".repeat(100_000))))
                .isEqualTo(99999L);

        assertThat(assertions.function("levenshtein_distance", "'%s'".formatted("l".repeat(100_000)), "'hello'"))
                .isEqualTo(99998L);

        assertTrinoExceptionThrownBy(() -> assertions.function("levenshtein_distance", "'%s'".formatted("x".repeat(1001)), "'%s'".formatted("x".repeat(1001))).evaluate())
                .hasMessage("The combined inputs for Levenshtein distance are too large");

        assertTrinoExceptionThrownBy(() -> assertions.function("levenshtein_distance", "'hello'", "'%s'".formatted("x".repeat(500_000))).evaluate())
                .hasMessage("The combined inputs for Levenshtein distance are too large");

        assertTrinoExceptionThrownBy(() -> assertions.function("levenshtein_distance", "'%s'".formatted("x".repeat(500_000)), "'hello'").evaluate())
                .hasMessage("The combined inputs for Levenshtein distance are too large");
    }

    @Test
    public void testHammingDistance()
    {
        assertThat(assertions.function("hamming_distance", "''", "''"))
                .isEqualTo(0L);

        assertThat(assertions.function("hamming_distance", "'hello'", "'hello'"))
                .isEqualTo(0L);

        assertThat(assertions.function("hamming_distance", "'hello'", "'jello'"))
                .isEqualTo(1L);

        assertThat(assertions.function("hamming_distance", "'like'", "'hate'"))
                .isEqualTo(3L);

        assertThat(assertions.function("hamming_distance", "'hello'", "'world'"))
                .isEqualTo(4L);

        assertThat(assertions.function("hamming_distance", "NULL", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.function("hamming_distance", "'hello'", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.function("hamming_distance", "NULL", "'world'"))
                .isNull(BIGINT);

        // Test for unicode
        assertThat(assertions.function("hamming_distance", "'hello na\u00EFve world'", "'hello naive world'"))
                .isEqualTo(1L);

        assertThat(assertions.function("hamming_distance", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'\u4FE1\u4EF0,\u7231,\u5E0C\u671B'"))
                .isEqualTo(1L);

        assertThat(assertions.function("hamming_distance", "'\u4F11\u5FF5,\u7231,\u5E0C\u671B'", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'"))
                .isEqualTo(1L);

        // Test for invalid arguments
        assertTrinoExceptionThrownBy(() -> assertions.function("hamming_distance", "'hello'", "''").evaluate())
                .hasMessage("The input strings to hamming_distance function must have the same length");

        assertTrinoExceptionThrownBy(() -> assertions.function("hamming_distance", "''", "'hello'").evaluate())
                .hasMessage("The input strings to hamming_distance function must have the same length");

        assertTrinoExceptionThrownBy(() -> assertions.function("hamming_distance", "'hello'", "'o'").evaluate())
                .hasMessage("The input strings to hamming_distance function must have the same length");

        assertTrinoExceptionThrownBy(() -> assertions.function("hamming_distance", "'h'", "'hello'").evaluate())
                .hasMessage("The input strings to hamming_distance function must have the same length");

        assertTrinoExceptionThrownBy(() -> assertions.function("hamming_distance", "'hello na\u00EFve world'", "'hello na:ive world'").evaluate())
                .hasMessage("The input strings to hamming_distance function must have the same length");

        assertTrinoExceptionThrownBy(() -> assertions.function("hamming_distance", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'\u4FE1\u5FF5\u5E0C\u671B'").evaluate())
                .hasMessage("The input strings to hamming_distance function must have the same length");
    }

    @Test
    public void testReplace()
    {
        assertThat(assertions.function("replace", "'aaa'", "'a'", "'aa'"))
                .hasType(createVarcharType(11))
                .isEqualTo("aaaaaa");

        assertThat(assertions.function("replace", "'abcdefabcdef'", "'cd'", "'XX'"))
                .hasType(createVarcharType(38))
                .isEqualTo("abXXefabXXef");

        assertThat(assertions.function("replace", "'abcdefabcdef'", "'cd'"))
                .hasType(createVarcharType(12))
                .isEqualTo("abefabef");

        assertThat(assertions.function("replace", "'123123tech'", "'123'"))
                .hasType(createVarcharType(10))
                .isEqualTo("tech");

        assertThat(assertions.function("replace", "'123tech123'", "'123'"))
                .hasType(createVarcharType(10))
                .isEqualTo("tech");

        assertThat(assertions.function("replace", "'222tech'", "'2'", "'3'"))
                .hasType(createVarcharType(15))
                .isEqualTo("333tech");

        assertThat(assertions.function("replace", "'0000123'", "'0'"))
                .hasType(createVarcharType(7))
                .isEqualTo("123");

        assertThat(assertions.function("replace", "'0000123'", "'0'", "' '"))
                .hasType(createVarcharType(15))
                .isEqualTo("    123");

        assertThat(assertions.function("replace", "'foo'", "''"))
                .hasType(createVarcharType(3))
                .isEqualTo("foo");

        assertThat(assertions.function("replace", "'foo'", "''", "''"))
                .hasType(createVarcharType(3))
                .isEqualTo("foo");

        assertThat(assertions.function("replace", "'foo'", "'foo'", "''"))
                .hasType(createVarcharType(3))
                .isEqualTo("");

        assertThat(assertions.function("replace", "'abc'", "''", "'xx'"))
                .hasType(createVarcharType(11))
                .isEqualTo("xxaxxbxxcxx");

        assertThat(assertions.function("replace", "''", "''", "'xx'"))
                .hasType(createVarcharType(2))
                .isEqualTo("xx");

        assertThat(assertions.function("replace", "''", "''"))
                .hasType(createVarcharType(0))
                .isEqualTo("");

        assertThat(assertions.function("replace", "''", "''", "''"))
                .hasType(createVarcharType(0))
                .isEqualTo("");

        assertThat(assertions.function("replace", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "','", "'\u2014'"))
                .hasType(createVarcharType(15))
                .isEqualTo("\u4FE1\u5FF5\u2014\u7231\u2014\u5E0C\u671B");

        //\uD801\uDC2D is one character
        assertThat(assertions.function("replace", "'::\uD801\uDC2D::'", "':'", "''"))
                .hasType(createVarcharType(5))
                .isEqualTo("\uD801\uDC2D");

        assertThat(assertions.function("replace", "'\u00D6sterreich'", "'\u00D6'", "'Oe'"))
                .hasType(createVarcharType(32))
                .isEqualTo("Oesterreich");
    }

    @Test
    public void testReverse()
    {
        assertThat(assertions.function("reverse", "''"))
                .hasType(createVarcharType(0))
                .isEqualTo("");

        assertThat(assertions.function("reverse", "'hello'"))
                .hasType(createVarcharType(5))
                .isEqualTo("olleh");

        assertThat(assertions.function("reverse", "'Quadratically'"))
                .hasType(createVarcharType(13))
                .isEqualTo("yllacitardauQ");

        assertThat(assertions.function("reverse", "'racecar'"))
                .hasType(createVarcharType(7))
                .isEqualTo("racecar");

        // Test REVERSE for non-ASCII
        assertThat(assertions.function("reverse", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'"))
                .hasType(createVarcharType(7))
                .isEqualTo("\u671B\u5E0C,\u7231,\u5FF5\u4FE1");

        assertThat(assertions.function("reverse", "'\u00D6sterreich'"))
                .hasType(createVarcharType(10))
                .isEqualTo("hcierrets\u00D6");

        assertThat(assertions.function("reverse", "'na\u00EFve'"))
                .hasType(createVarcharType(5))
                .isEqualTo("ev\u00EFan");

        assertThat(assertions.function("reverse", "'\uD801\uDC2Dend'"))
                .hasType(createVarcharType(4))
                .isEqualTo("dne\uD801\uDC2D");
    }

    @Test
    public void testStringPosition()
    {
        assertThat(assertions.function("strpos", "'high'", "'ig'"))
                .isEqualTo(2L);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "'ig'")
                .binding("b", "'high'"))
                .isEqualTo(2L);

        assertThat(assertions.function("strpos", "'high'", "'igx'"))
                .isEqualTo(0L);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "'igx'")
                .binding("b", "'high'"))
                .isEqualTo(0L);

        assertThat(assertions.function("strpos", "'Quadratically'", "'a'"))
                .isEqualTo(3L);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "'a'")
                .binding("b", "'Quadratically'"))
                .isEqualTo(3L);

        assertThat(assertions.function("strpos", "'foobar'", "'foobar'"))
                .isEqualTo(1L);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "'foobar'")
                .binding("b", "'foobar'"))
                .isEqualTo(1L);

        assertThat(assertions.function("strpos", "'foobar'", "'obar'"))
                .isEqualTo(3L);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "'obar'")
                .binding("b", "'foobar'"))
                .isEqualTo(3L);

        assertThat(assertions.function("strpos", "'zoo!'", "'!'"))
                .isEqualTo(4L);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "'!'")
                .binding("b", "'zoo!'"))
                .isEqualTo(4L);

        assertThat(assertions.function("strpos", "'x'", "''"))
                .isEqualTo(1L);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "''")
                .binding("b", "'x'"))
                .isEqualTo(1L);

        assertThat(assertions.function("strpos", "''", "''"))
                .isEqualTo(1L);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "''")
                .binding("b", "''"))
                .isEqualTo(1L);

        assertThat(assertions.function("strpos", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'\u7231'"))
                .isEqualTo(4L);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "'\u7231'")
                .binding("b", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'"))
                .isEqualTo(4L);

        assertThat(assertions.function("strpos", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'\u5E0C\u671B'"))
                .isEqualTo(6L);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "'\u5E0C\u671B'")
                .binding("b", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'"))
                .isEqualTo(6L);

        assertThat(assertions.function("strpos", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'nice'"))
                .isEqualTo(0L);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "'nice'")
                .binding("b", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'"))
                .isEqualTo(0L);

        assertThat(assertions.function("strpos", "NULL", "''"))
                .isNull(BIGINT);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "''")
                .binding("b", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.function("strpos", "''", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "NULL")
                .binding("b", "''"))
                .isNull(BIGINT);

        assertThat(assertions.function("strpos", "NULL", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.expression("POSITION(a IN b)")
                .binding("a", "NULL")
                .binding("b", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.function("starts_with", "'foo'", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.function("starts_with", "'foo'", "'bar'"))
                .isEqualTo(false);

        assertThat(assertions.function("starts_with", "'foo'", "''"))
                .isEqualTo(true);

        assertThat(assertions.function("starts_with", "''", "'foo'"))
                .isEqualTo(false);

        assertThat(assertions.function("starts_with", "''", "''"))
                .isEqualTo(true);

        assertThat(assertions.function("starts_with", "'foo_bar_baz'", "'foo'"))
                .isEqualTo(true);

        assertThat(assertions.function("starts_with", "'foo_bar_baz'", "'bar'"))
                .isEqualTo(false);

        assertThat(assertions.function("starts_with", "'foo'", "'foo_bar_baz'"))
                .isEqualTo(false);

        assertThat(assertions.function("starts_with", "'信念 爱 希望'", "'信念'"))
                .isEqualTo(true);

        assertThat(assertions.function("starts_with", "'信念 爱 希望'", "'爱'"))
                .isEqualTo(false);

        assertThat(assertions.function("strpos", "NULL", "''"))
                .isNull(BIGINT);

        assertThat(assertions.function("strpos", "''", "NULL"))
                .isNull(BIGINT);

        assertThat(assertions.function("strpos", "NULL", "NULL"))
                .isNull(BIGINT);

        assertTrinoExceptionThrownBy(() -> assertions.function("strpos", "'abc/xyz/foo/bar'", "'/'", "0").evaluate())
                .hasMessage("'instance' must be a positive or negative number.");

        assertTrinoExceptionThrownBy(() -> assertions.function("strpos", "''", "''", "0").evaluate())
                .hasMessage("'instance' must be a positive or negative number.");

        assertThat(assertions.function("strpos", "'abc/xyz/foo/bar'", "'/'"))
                .isEqualTo(4L);

        assertThat(assertions.function("strpos", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'\u7231'"))
                .isEqualTo(4L);

        assertThat(assertions.function("strpos", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'\u5E0C\u671B'"))
                .isEqualTo(6L);

        assertThat(assertions.function("strpos", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'nice'"))
                .isEqualTo(0L);

        assertThat(assertions.function("strpos", "'high'", "'ig'"))
                .isEqualTo(2L);

        assertThat(assertions.function("strpos", "'high'", "'igx'"))
                .isEqualTo(0L);

        assertThat(assertions.function("strpos", "'Quadratically'", "'a'"))
                .isEqualTo(3L);

        assertThat(assertions.function("strpos", "'foobar'", "'foobar'"))
                .isEqualTo(1L);

        assertThat(assertions.function("strpos", "'foobar'", "'obar'"))
                .isEqualTo(3L);

        assertThat(assertions.function("strpos", "'zoo!'", "'!'"))
                .isEqualTo(4L);

        assertThat(assertions.function("strpos", "'x'", "''"))
                .isEqualTo(1L);

        assertThat(assertions.function("strpos", "''", "''"))
                .isEqualTo(1L);

        assertThat(assertions.function("strpos", "'abc abc abc'", "'abc'", "1"))
                .isEqualTo(1L);

        assertThat(assertions.function("strpos", "'abc/xyz/foo/bar'", "'/'", "1"))
                .isEqualTo(4L);

        assertThat(assertions.function("strpos", "'abc/xyz/foo/bar'", "'/'", "2"))
                .isEqualTo(8L);

        assertThat(assertions.function("strpos", "'abc/xyz/foo/bar'", "'/'", "3"))
                .isEqualTo(12L);

        assertThat(assertions.function("strpos", "'abc/xyz/foo/bar'", "'/'", "4"))
                .isEqualTo(0L);

        assertThat(assertions.function("strpos", "'highhigh'", "'ig'", "1"))
                .isEqualTo(2L);

        assertThat(assertions.function("strpos", "'foobarfoo'", "'fb'", "1"))
                .isEqualTo(0L);

        assertThat(assertions.function("strpos", "'foobarfoo'", "'oo'", "1"))
                .isEqualTo(2L);

        assertThat(assertions.function("strpos", "'abc abc abc'", "'abc'", "-1"))
                .isEqualTo(9L);

        assertThat(assertions.function("strpos", "'abc/xyz/foo/bar'", "'/'", "-1"))
                .isEqualTo(12L);

        assertThat(assertions.function("strpos", "'abc/xyz/foo/bar'", "'/'", "-2"))
                .isEqualTo(8L);

        assertThat(assertions.function("strpos", "'abc/xyz/foo/bar'", "'/'", "-3"))
                .isEqualTo(4L);

        assertThat(assertions.function("strpos", "'abc/xyz/foo/bar'", "'/'", "-4"))
                .isEqualTo(0L);

        assertThat(assertions.function("strpos", "'highhigh'", "'ig'", "-1"))
                .isEqualTo(6L);

        assertThat(assertions.function("strpos", "'highhigh'", "'ig'", "-2"))
                .isEqualTo(2L);

        assertThat(assertions.function("strpos", "'foobarfoo'", "'fb'", "-1"))
                .isEqualTo(0L);

        assertThat(assertions.function("strpos", "'foobarfoo'", "'oo'", "-1"))
                .isEqualTo(8L);

        assertThat(assertions.function("strpos", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'\u7231'", "-1"))
                .isEqualTo(4L);

        assertThat(assertions.function("strpos", "'\u4FE1\u5FF5,\u7231,\u5E0C\u7231\u671B'", "'\u7231'", "-1"))
                .isEqualTo(7L);

        assertThat(assertions.function("strpos", "'\u4FE1\u5FF5,\u7231,\u5E0C\u7231\u671B'", "'\u7231'", "-2"))
                .isEqualTo(4L);

        assertThat(assertions.function("strpos", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'\u5E0C\u671B'", "-1"))
                .isEqualTo(6L);

        assertThat(assertions.function("strpos", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "'nice'", "-1"))
                .isEqualTo(0L);
    }

    @Test
    public void testSubstring()
    {
        assertThat(assertions.function("substr", "'Quadratically'", "5"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratically");

        assertThat(assertions.function("substr", "'Quadratically'", "50"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "'Quadratically'", "-5"))
                .hasType(createVarcharType(13))
                .isEqualTo("cally");

        assertThat(assertions.function("substr", "'Quadratically'", "-50"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "'Quadratically'", "0"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "'Quadratically'", "5", "6"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratica");

        assertThat(assertions.function("substr", "'Quadratically'", "5", "10"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratically");

        assertThat(assertions.function("substr", "'Quadratically'", "5", "50"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratically");

        assertThat(assertions.function("substr", "'Quadratically'", "50", "10"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "'Quadratically'", "-5", "4"))
                .hasType(createVarcharType(13))
                .isEqualTo("call");

        assertThat(assertions.function("substr", "'Quadratically'", "-5", "40"))
                .hasType(createVarcharType(13))
                .isEqualTo("cally");

        assertThat(assertions.function("substr", "'Quadratically'", "-50", "4"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "'Quadratically'", "0", "4"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "'Quadratically'", "5", "0"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "'Quadratically'")
                .binding("start", "5"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratically");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "'Quadratically'")
                .binding("start", "50"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "'Quadratically'")
                .binding("start", "-5"))
                .hasType(createVarcharType(13))
                .isEqualTo("cally");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "'Quadratically'")
                .binding("start", "-50"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "'Quadratically'")
                .binding("start", "0"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "'Quadratically'")
                .binding("start", "5")
                .binding("length", "6"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratica");

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "'Quadratically'")
                .binding("start", "5")
                .binding("length", "50"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratically");

        // Test SUBSTRING for non-ASCII
        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'")
                .binding("start", "1")
                .binding("length", "1"))
                .hasType(createVarcharType(7))
                .isEqualTo("\u4FE1");

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'")
                .binding("start", "3")
                .binding("length", "5"))
                .hasType(createVarcharType(7))
                .isEqualTo(",\u7231,\u5E0C\u671B");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'")
                .binding("start", "4"))
                .hasType(createVarcharType(7))
                .isEqualTo("\u7231,\u5E0C\u671B");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'")
                .binding("start", "-2"))
                .hasType(createVarcharType(7))
                .isEqualTo("\u5E0C\u671B");

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "'\uD801\uDC2Dend'")
                .binding("start", "1")
                .binding("length", "1"))
                .hasType(createVarcharType(4))
                .isEqualTo("\uD801\uDC2D");

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "'\uD801\uDC2Dend'")
                .binding("start", "2")
                .binding("length", "3"))
                .hasType(createVarcharType(4))
                .isEqualTo("end");
    }

    @Test
    public void testCharSubstring()
    {
        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "5"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratically");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "50"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "-5"))
                .hasType(createVarcharType(13))
                .isEqualTo("cally");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "-50"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "0"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "5", "6"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratica");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "5", "10"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratically");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "5", "50"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratically");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "50", "10"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "-5", "4"))
                .hasType(createVarcharType(13))
                .isEqualTo("call");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "-5", "40"))
                .hasType(createVarcharType(13))
                .isEqualTo("cally");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "-50", "4"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "0", "4"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "CAST('Quadratically' AS CHAR(13))", "5", "0"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substr", "CAST('abc def' AS CHAR(7))", "1", "4"))
                .hasType(createVarcharType(7))
                .isEqualTo("abc ");

        assertThat(assertions.function("substr", "CAST('keep trailing' AS CHAR(14))", "1"))
                .hasType(createVarcharType(14))
                .isEqualTo("keep trailing ");

        assertThat(assertions.function("substr", "CAST('keep trailing' AS CHAR(14))", "1", "14"))
                .hasType(createVarcharType(14))
                .isEqualTo("keep trailing ");

        assertThat(assertions.function("substring", "CAST('Quadratically' AS CHAR(13))", "5"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratically");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "CAST('Quadratically' AS CHAR(13))")
                .binding("start", "5"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratically");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "CAST('Quadratically' AS CHAR(13))")
                .binding("start", "50"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "CAST('Quadratically' AS CHAR(13))")
                .binding("start", "-5"))
                .hasType(createVarcharType(13))
                .isEqualTo("cally");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "CAST('Quadratically' AS CHAR(13))")
                .binding("start", "-50"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "CAST('Quadratically' AS CHAR(13))")
                .binding("start", "0"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("substring", "CAST('Quadratically' AS CHAR(13))", "5", "6"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratica");

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "CAST('Quadratically' AS CHAR(13))")
                .binding("start", "5")
                .binding("length", "6"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratica");

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "CAST('Quadratically' AS CHAR(13))")
                .binding("start", "5")
                .binding("length", "50"))
                .hasType(createVarcharType(13))
                .isEqualTo("ratically");

        // Test SUBSTRING for non-ASCII
        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "CAST('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' AS CHAR(7))")
                .binding("start", "1")
                .binding("length", "1"))
                .hasType(createVarcharType(7))
                .isEqualTo("\u4FE1");

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "CAST('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' AS CHAR(7))")
                .binding("start", "3")
                .binding("length", "5"))
                .hasType(createVarcharType(7))
                .isEqualTo(",\u7231,\u5E0C\u671B");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "CAST('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' AS CHAR(7))")
                .binding("start", "4"))
                .hasType(createVarcharType(7))
                .isEqualTo("\u7231,\u5E0C\u671B");

        assertThat(assertions.expression("SUBSTRING(value FROM start)")
                .binding("value", "CAST('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' AS CHAR(7))")
                .binding("start", "-2"))
                .hasType(createVarcharType(7))
                .isEqualTo("\u5E0C\u671B");

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "CAST('\uD801\uDC2Dend' AS CHAR(4))")
                .binding("start", "1")
                .binding("length", "1"))
                .hasType(createVarcharType(4))
                .isEqualTo("\uD801\uDC2D");

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "CAST('\uD801\uDC2Dend' AS CHAR(4))")
                .binding("start", "2")
                .binding("length", "3"))
                .hasType(createVarcharType(4))
                .isEqualTo("end");

        assertThat(assertions.expression("SUBSTRING(value FROM start FOR length)")
                .binding("value", "CAST('\uD801\uDC2Dend' AS CHAR(40))")
                .binding("start", "2")
                .binding("length", "3"))
                .hasType(createVarcharType(40))
                .isEqualTo("end");
    }

    @Test
    public void testSplit()
    {
        assertThat(assertions.function("split", "'a.b.c'", "'.'"))
                .hasType(new ArrayType(createVarcharType(5)))
                .isEqualTo(ImmutableList.of("a", "b", "c"));

        assertThat(assertions.function("split", "'ab'", "'.'", "1"))
                .hasType(new ArrayType(createVarcharType(2)))
                .isEqualTo(ImmutableList.of("ab"));

        assertThat(assertions.function("split", "'a.b'", "'.'", "1"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("a.b"));

        assertThat(assertions.function("split", "'a.b.c'", "'.'"))
                .hasType(new ArrayType(createVarcharType(5)))
                .isEqualTo(ImmutableList.of("a", "b", "c"));

        assertThat(assertions.function("split", "'a..b..c'", "'..'"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("a", "b", "c"));

        assertThat(assertions.function("split", "'a.b.c'", "'.'", "2"))
                .hasType(new ArrayType(createVarcharType(5)))
                .isEqualTo(ImmutableList.of("a", "b.c"));

        assertThat(assertions.function("split", "'a.b.c'", "'.'", "3"))
                .hasType(new ArrayType(createVarcharType(5)))
                .isEqualTo(ImmutableList.of("a", "b", "c"));

        assertThat(assertions.function("split", "'a.b.c'", "'.'", "4"))
                .hasType(new ArrayType(createVarcharType(5)))
                .isEqualTo(ImmutableList.of("a", "b", "c"));

        assertThat(assertions.function("split", "'a.b.c.'", "'.'", "4"))
                .hasType(new ArrayType(createVarcharType(6)))
                .isEqualTo(ImmutableList.of("a", "b", "c", ""));

        assertThat(assertions.function("split", "'a.b.c.'", "'.'", "3"))
                .hasType(new ArrayType(createVarcharType(6)))
                .isEqualTo(ImmutableList.of("a", "b", "c."));

        assertThat(assertions.function("split", "'...'", "'.'"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("", "", "", ""));

        assertThat(assertions.function("split", "'..a...a..'", "'.'"))
                .hasType(new ArrayType(createVarcharType(9)))
                .isEqualTo(ImmutableList.of("", "", "a", "", "", "a", "", ""));

        // Test SPLIT for non-ASCII
        assertThat(assertions.function("split", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "','", "3"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("\u4FE1\u5FF5", "\u7231", "\u5E0C\u671B"));

        assertThat(assertions.function("split", "'\u8B49\u8BC1\u8A3C'", "'\u8BC1'", "2"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("\u8B49", "\u8A3C"));

        assertThat(assertions.function("split", "'.a.b.c'", "'.'", "4"))
                .hasType(new ArrayType(createVarcharType(6)))
                .isEqualTo(ImmutableList.of("", "a", "b", "c"));

        assertThat(assertions.function("split", "'.a.b.c'", "'.'", "3"))
                .hasType(new ArrayType(createVarcharType(6)))
                .isEqualTo(ImmutableList.of("", "a", "b.c"));

        assertThat(assertions.function("split", "'.a.b.c'", "'.'", "2"))
                .hasType(new ArrayType(createVarcharType(6)))
                .isEqualTo(ImmutableList.of("", "a.b.c"));

        assertThat(assertions.function("split", "'a..b..c'", "'.'", "3"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("a", "", "b..c"));

        assertThat(assertions.function("split", "'a.b..'", "'.'", "3"))
                .hasType(new ArrayType(createVarcharType(5)))
                .isEqualTo(ImmutableList.of("a", "b", "."));

        assertTrinoExceptionThrownBy(() -> assertions.function("split", "'a.b.c'", "''", "1").evaluate())
                .hasMessage("The delimiter may not be the empty string");

        assertTrinoExceptionThrownBy(() -> assertions.function("split", "'a.b.c'", "'.'", "0").evaluate())
                .hasMessage("Limit must be positive");

        assertTrinoExceptionThrownBy(() -> assertions.function("split", "'a.b.c'", "'.'", "-1").evaluate())
                .hasMessage("Limit must be positive");

        assertTrinoExceptionThrownBy(() -> assertions.function("split", "'a.b.c'", "'.'", "2147483648").evaluate())
                .hasMessage("Limit is too large");
    }

    @Test
    public void testSplitToMap()
    {
        assertThat(assertions.function("split_to_map", "''", "','", "'='"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.function("split_to_map", "'a=123,b=.4,c=,=d'", "','", "'='"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("a", "123", "b", ".4", "c", "", "", "d"));

        assertThat(assertions.function("split_to_map", "'='", "','", "'='"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("", ""));

        assertThat(assertions.function("split_to_map", "'key=>value'", "','", "'=>'"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("key", "value"));

        assertThat(assertions.function("split_to_map", "'key => value'", "','", "'=>'"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("key ", " value"));

        // Test SPLIT_TO_MAP for non-ASCII
        assertThat(assertions.function("split_to_map", "'\u4EA0\u4EFF\u4EA1'", "'\u4E00'", "'\u4EFF'"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("\u4EA0", "\u4EA1"));

        // If corresponding value is not found, then ""(empty string) is its value
        assertThat(assertions.function("split_to_map", "'\u4EC0\u4EFF'", "'\u4E00'", "'\u4EFF'"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("\u4EC0", ""));

        // If corresponding key is not found, then ""(empty string) is its key
        assertThat(assertions.function("split_to_map", "'\u4EFF\u4EC1'", "'\u4E00'", "'\u4EFF'"))
                .hasType(mapType(VARCHAR, VARCHAR))
                .isEqualTo(ImmutableMap.of("", "\u4EC1"));

        // Entry delimiter and key-value delimiter must not be the same.
        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_map", "''", "'\u4EFF'", "'\u4EFF'").evaluate())
                .hasMessage("entryDelimiter and keyValueDelimiter must not be the same");

        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_map", "'a=123,b=.4,c='", "'='", "'='").evaluate())
                .hasMessage("entryDelimiter and keyValueDelimiter must not be the same");

        // Duplicate keys are not allowed to exist.
        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_map", "'a=123,a=.4'", "','", "'='").evaluate())
                .hasMessage("Duplicate keys (a) are not allowed");

        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_map", "'\u4EA0\u4EFF\u4EA1\u4E00\u4EA0\u4EFF\u4EB1'", "'\u4E00'", "'\u4EFF'").evaluate())
                .hasMessage("Duplicate keys (\u4EA0) are not allowed");

        // Key-value delimiter must appear exactly once in each entry.
        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_map", "'key'", "','", "'='").evaluate())
                .hasMessage("Key-value delimiter must appear exactly once in each entry. Bad input: 'key'");

        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_map", "'key==value'", "','", "'='").evaluate())
                .hasMessage("Key-value delimiter must appear exactly once in each entry. Bad input: 'key==value'");

        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_map", "'key=va=lue'", "','", "'='").evaluate())
                .hasMessage("Key-value delimiter must appear exactly once in each entry. Bad input: 'key=va=lue'");
    }

    @Test
    public void testSplitToMultimap()
    {
        assertThat(assertions.function("split_to_multimap", "''", "','", "'='"))
                .hasType(mapType(VARCHAR, new ArrayType(VARCHAR)))
                .isEqualTo(ImmutableMap.of());

        assertThat(assertions.function("split_to_multimap", "'a=123,b=.4,c=,=d'", "','", "'='"))
                .hasType(mapType(VARCHAR, new ArrayType(VARCHAR)))
                .isEqualTo(ImmutableMap.of(
                        "a", ImmutableList.of("123"),
                        "b", ImmutableList.of(".4"),
                        "c", ImmutableList.of(""),
                        "", ImmutableList.of("d")));

        assertThat(assertions.function("split_to_multimap", "'='", "','", "'='"))
                .hasType(mapType(VARCHAR, new ArrayType(VARCHAR)))
                .isEqualTo(ImmutableMap.of("", ImmutableList.of("")));

        // Test multiple values of the same key preserve the order as they appear in input
        assertThat(assertions.function("split_to_multimap", "'a=123,a=.4,a=5.67'", "','", "'='"))
                .hasType(mapType(VARCHAR, new ArrayType(VARCHAR)))
                .isEqualTo(ImmutableMap.of("a", ImmutableList.of("123", ".4", "5.67")));

        // Test multi-character delimiters
        assertThat(assertions.function("split_to_multimap", "'key=>value,key=>value'", "','", "'=>'"))
                .hasType(mapType(VARCHAR, new ArrayType(VARCHAR)))
                .isEqualTo(ImmutableMap.of("key", ImmutableList.of("value", "value")));

        assertThat(assertions.function("split_to_multimap", "'key => value, key => value'", "','", "'=>'"))
                .hasType(mapType(VARCHAR, new ArrayType(VARCHAR)))
                .isEqualTo(ImmutableMap.of(
                        "key ", ImmutableList.of(" value"),
                        " key ", ImmutableList.of(" value")));

        assertThat(assertions.function("split_to_multimap", "'key => value, key => value'", "', '", "'=>'"))
                .hasType(mapType(VARCHAR, new ArrayType(VARCHAR)))
                .isEqualTo(ImmutableMap.of(
                        "key ", ImmutableList.of(" value", " value")));

        // Test non-ASCII
        assertThat(assertions.function("split_to_multimap", "'\u4EA0\u4EFF\u4EA1'", "'\u4E00'", "'\u4EFF'"))
                .hasType(mapType(VARCHAR, new ArrayType(VARCHAR)))
                .isEqualTo(ImmutableMap.of("\u4EA0", ImmutableList.of("\u4EA1")));

        assertThat(assertions.function("split_to_multimap", "'\u4EA0\u4EFF\u4EA1\u4E00\u4EA0\u4EFF\u4EB1'", "'\u4E00'", "'\u4EFF'"))
                .hasType(mapType(VARCHAR, new ArrayType(VARCHAR)))
                .isEqualTo(ImmutableMap.of("\u4EA0", ImmutableList.of("\u4EA1", "\u4EB1")));

        // Entry delimiter and key-value delimiter must not be the same.
        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_multimap", "''", "'\u4EFF'", "'\u4EFF'").evaluate())
                .hasMessage("entryDelimiter and keyValueDelimiter must not be the same");

        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_multimap", "'a=123,b=.4,c='", "'='", "'='").evaluate())
                .hasMessage("entryDelimiter and keyValueDelimiter must not be the same");

        // Key-value delimiter must appear exactly once in each entry.
        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_multimap", "'key'", "','", "'='").evaluate())
                .hasMessage("Key-value delimiter must appear exactly once in each entry. Bad input: key");

        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_multimap", "'key==value'", "','", "'='").evaluate())
                .hasMessage("Key-value delimiter must appear exactly once in each entry. Bad input: key==value");

        assertTrinoExceptionThrownBy(() -> assertions.function("split_to_multimap", "'key=va=lue'", "','", "'='").evaluate())
                .hasMessage("Key-value delimiter must appear exactly once in each entry. Bad input: key=va=lue");
    }

    @Test
    public void testSplitPart()
    {
        assertThat(assertions.function("split_part", "'abc-@-def-@-ghi'", "'-@-'", "1"))
                .hasType(createVarcharType(15))
                .isEqualTo("abc");

        assertThat(assertions.function("split_part", "'abc-@-def-@-ghi'", "'-@-'", "2"))
                .hasType(createVarcharType(15))
                .isEqualTo("def");

        assertThat(assertions.function("split_part", "'abc-@-def-@-ghi'", "'-@-'", "3"))
                .hasType(createVarcharType(15))
                .isEqualTo("ghi");

        assertThat(assertions.function("split_part", "'abc-@-def-@-ghi'", "'-@-'", "4"))
                .isNull(createVarcharType(15));

        assertThat(assertions.function("split_part", "'abc-@-def-@-ghi'", "'-@-'", "99"))
                .isNull(createVarcharType(15));

        assertThat(assertions.function("split_part", "'abc'", "'abc'", "1"))
                .hasType(createVarcharType(3))
                .isEqualTo("");

        assertThat(assertions.function("split_part", "'abc'", "'abc'", "2"))
                .hasType(createVarcharType(3))
                .isEqualTo("");

        assertThat(assertions.function("split_part", "'abc'", "'abc'", "3"))
                .isNull(createVarcharType(3));

        assertThat(assertions.function("split_part", "'abc'", "'-@-'", "1"))
                .hasType(createVarcharType(3))
                .isEqualTo("abc");

        assertThat(assertions.function("split_part", "'abc'", "'-@-'", "2"))
                .isNull(createVarcharType(3));

        assertThat(assertions.function("split_part", "''", "'abc'", "1"))
                .hasType(createVarcharType(0))
                .isEqualTo("");

        assertThat(assertions.function("split_part", "''", "''", "1"))
                .isNull(createVarcharType(0));

        assertThat(assertions.function("split_part", "'abc'", "''", "1"))
                .hasType(createVarcharType(3))
                .isEqualTo("a");

        assertThat(assertions.function("split_part", "'abc'", "''", "2"))
                .hasType(createVarcharType(3))
                .isEqualTo("b");

        assertThat(assertions.function("split_part", "'abc'", "''", "3"))
                .hasType(createVarcharType(3))
                .isEqualTo("c");

        assertThat(assertions.function("split_part", "'abc'", "''", "4"))
                .isNull(createVarcharType(3));

        assertThat(assertions.function("split_part", "'abc'", "''", "99"))
                .isNull(createVarcharType(3));

        assertThat(assertions.function("split_part", "'abc'", "'abcd'", "1"))
                .hasType(createVarcharType(3))
                .isEqualTo("abc");

        assertThat(assertions.function("split_part", "'abc'", "'abcd'", "2"))
                .isNull(createVarcharType(3));

        assertThat(assertions.function("split_part", "'abc--@--def'", "'-@-'", "1"))
                .hasType(createVarcharType(11))
                .isEqualTo("abc-");

        assertThat(assertions.function("split_part", "'abc--@--def'", "'-@-'", "2"))
                .hasType(createVarcharType(11))
                .isEqualTo("-def");

        assertThat(assertions.function("split_part", "'abc-@-@-@-def'", "'-@-'", "1"))
                .hasType(createVarcharType(13))
                .isEqualTo("abc");

        assertThat(assertions.function("split_part", "'abc-@-@-@-def'", "'-@-'", "2"))
                .hasType(createVarcharType(13))
                .isEqualTo("@");

        assertThat(assertions.function("split_part", "'abc-@-@-@-def'", "'-@-'", "3"))
                .hasType(createVarcharType(13))
                .isEqualTo("def");

        assertThat(assertions.function("split_part", "' '", "' '", "1"))
                .hasType(createVarcharType(1))
                .isEqualTo("");

        assertThat(assertions.function("split_part", "'abcdddddef'", "'dd'", "1"))
                .hasType(createVarcharType(10))
                .isEqualTo("abc");

        assertThat(assertions.function("split_part", "'abcdddddef'", "'dd'", "2"))
                .hasType(createVarcharType(10))
                .isEqualTo("");

        assertThat(assertions.function("split_part", "'abcdddddef'", "'dd'", "3"))
                .hasType(createVarcharType(10))
                .isEqualTo("def");

        assertThat(assertions.function("split_part", "'a/b/c'", "'/'", "4"))
                .isNull(createVarcharType(5));

        assertThat(assertions.function("split_part", "'a/b/c/'", "'/'", "4"))
                .hasType(createVarcharType(6))
                .isEqualTo("");

        // Test SPLIT_PART for non-ASCII
        assertThat(assertions.function("split_part", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "','", "1"))
                .hasType(createVarcharType(7))
                .isEqualTo("\u4FE1\u5FF5");

        assertThat(assertions.function("split_part", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "','", "2"))
                .hasType(createVarcharType(7))
                .isEqualTo("\u7231");

        assertThat(assertions.function("split_part", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "','", "3"))
                .hasType(createVarcharType(7))
                .isEqualTo("\u5E0C\u671B");

        assertThat(assertions.function("split_part", "'\u4FE1\u5FF5,\u7231,\u5E0C\u671B'", "','", "4"))
                .isNull(createVarcharType(7));

        assertThat(assertions.function("split_part", "'\u8B49\u8BC1\u8A3C'", "'\u8BC1'", "1"))
                .hasType(createVarcharType(3))
                .isEqualTo("\u8B49");

        assertThat(assertions.function("split_part", "'\u8B49\u8BC1\u8A3C'", "'\u8BC1'", "2"))
                .hasType(createVarcharType(3))
                .isEqualTo("\u8A3C");

        assertThat(assertions.function("split_part", "'\u8B49\u8BC1\u8A3C'", "'\u8BC1'", "3"))
                .isNull(createVarcharType(3));

        assertTrinoExceptionThrownBy(() -> assertions.function("split_part", "'abc'", "''", "0").evaluate())
                .hasMessage("Index must be greater than zero");

        assertTrinoExceptionThrownBy(() -> assertions.function("split_part", "'abc'", "''", "-1").evaluate())
                .hasMessage("Index must be greater than zero");

        assertTrinoExceptionThrownBy(() -> assertions.function("split_part", "utf8(from_hex('CE'))", "''", "1").evaluate())
                .hasMessage("Invalid UTF-8 encoding");
    }

    @Test
    public void testSplitPartInvalid()
    {
        assertTrinoExceptionThrownBy(() -> assertions.function("split_part", "'abc-@-def-@-ghi'", "'-@-'", "0").evaluate())
                .hasMessage("Index must be greater than zero");
    }

    @Test
    public void testLeftTrim()
    {
        assertThat(assertions.function("ltrim", "''"))
                .hasType(createVarcharType(0))
                .isEqualTo("");

        assertThat(assertions.function("ltrim", "'   '"))
                .hasType(createVarcharType(3))
                .isEqualTo("");

        assertThat(assertions.function("ltrim", "'  hello  '"))
                .hasType(createVarcharType(9))
                .isEqualTo("hello  ");

        assertThat(assertions.function("ltrim", "'  hello'"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello");

        assertThat(assertions.function("ltrim", "'hello  '"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello  ");

        assertThat(assertions.function("ltrim", "' hello world '"))
                .hasType(createVarcharType(13))
                .isEqualTo("hello world ");

        assertThat(assertions.function("ltrim", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '"))
                .hasType(createVarcharType(9))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");

        assertThat(assertions.function("ltrim", "' \u4FE1\u5FF5 \u7231 \u5E0C\u671B '"))
                .hasType(createVarcharType(9))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B ");

        assertThat(assertions.function("ltrim", "'  \u4FE1\u5FF5 \u7231 \u5E0C\u671B'"))
                .hasType(createVarcharType(9))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B");

        assertThat(assertions.function("ltrim", "' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B'"))
                .hasType(createVarcharType(10))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testCharLeftTrim()
    {
        assertThat(assertions.function("ltrim", "CAST('' AS CHAR(20))"))
                .hasType(createVarcharType(20))
                .isEqualTo("");

        assertThat(assertions.function("ltrim", "CAST('  hello  ' AS CHAR(9))"))
                .hasType(createVarcharType(9))
                .isEqualTo("hello");

        assertThat(assertions.function("ltrim", "CAST('  hello' AS CHAR(7))"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello");

        assertThat(assertions.function("ltrim", "CAST('hello  ' AS CHAR(7))"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello");

        assertThat(assertions.function("ltrim", "CAST(' hello world ' AS CHAR(13))"))
                .hasType(createVarcharType(13))
                .isEqualTo("hello world");

        assertThat(assertions.function("ltrim", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ' AS CHAR(9))"))
                .hasType(createVarcharType(9))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B");

        assertThat(assertions.function("ltrim", "CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ' AS CHAR(9))"))
                .hasType(createVarcharType(9))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B");

        assertThat(assertions.function("ltrim", "CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(9))"))
                .hasType(createVarcharType(9))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B");

        assertThat(assertions.function("ltrim", "CAST(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(10))"))
                .hasType(createVarcharType(10))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testRightTrim()
    {
        assertThat(assertions.function("rtrim", "''"))
                .hasType(createVarcharType(0))
                .isEqualTo("");

        assertThat(assertions.function("rtrim", "'   '"))
                .hasType(createVarcharType(3))
                .isEqualTo("");

        assertThat(assertions.function("rtrim", "'  hello  '"))
                .hasType(createVarcharType(9))
                .isEqualTo("  hello");

        assertThat(assertions.function("rtrim", "'  hello'"))
                .hasType(createVarcharType(7))
                .isEqualTo("  hello");

        assertThat(assertions.function("rtrim", "'hello  '"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello");

        assertThat(assertions.function("rtrim", "' hello world '"))
                .hasType(createVarcharType(13))
                .isEqualTo(" hello world");

        assertThat(assertions.function("rtrim", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 '"))
                .hasType(createVarcharType(10))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B");

        assertThat(assertions.function("rtrim", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '"))
                .hasType(createVarcharType(9))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B");

        assertThat(assertions.function("rtrim", "' \u4FE1\u5FF5 \u7231 \u5E0C\u671B '"))
                .hasType(createVarcharType(9))
                .isEqualTo(" \u4FE1\u5FF5 \u7231 \u5E0C\u671B");

        assertThat(assertions.function("rtrim", "'  \u4FE1\u5FF5 \u7231 \u5E0C\u671B'"))
                .hasType(createVarcharType(9))
                .isEqualTo("  \u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testCharRightTrim()
    {
        assertThat(assertions.function("rtrim", "CAST('' AS CHAR(20))"))
                .hasType(createVarcharType(20))
                .isEqualTo("");

        assertThat(assertions.function("rtrim", "CAST('  hello  ' AS CHAR(9))"))
                .hasType(createVarcharType(9))
                .isEqualTo("  hello");

        assertThat(assertions.function("rtrim", "CAST('  hello' AS CHAR(7))"))
                .hasType(createVarcharType(7))
                .isEqualTo("  hello");

        assertThat(assertions.function("rtrim", "CAST('hello  ' AS CHAR(7))"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello");

        assertThat(assertions.function("rtrim", "CAST(' hello world ' AS CHAR(13))"))
                .hasType(createVarcharType(13))
                .isEqualTo(" hello world");

        assertThat(assertions.function("rtrim", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ' AS CHAR(10))"))
                .hasType(createVarcharType(10))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B");

        assertThat(assertions.function("rtrim", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ' AS CHAR(9))"))
                .hasType(createVarcharType(9))
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B");

        assertThat(assertions.function("rtrim", "CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ' AS CHAR(9))"))
                .hasType(createVarcharType(9))
                .isEqualTo(" \u4FE1\u5FF5 \u7231 \u5E0C\u671B");

        assertThat(assertions.function("rtrim", "CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(9))"))
                .hasType(createVarcharType(9))
                .isEqualTo("  \u4FE1\u5FF5 \u7231 \u5E0C\u671B");
    }

    @Test
    public void testLeftTrimParametrized()
    {
        assertThat(assertions.function("ltrim", "''", "''"))
                .hasType(createVarcharType(0))
                .isEqualTo("");

        assertThat(assertions.function("ltrim", "'   '", "''"))
                .hasType(createVarcharType(3))
                .isEqualTo("   ");

        assertThat(assertions.function("ltrim", "'  hello  '", "''"))
                .hasType(createVarcharType(9))
                .isEqualTo("  hello  ");

        assertThat(assertions.function("ltrim", "'  hello  '", "' '"))
                .hasType(createVarcharType(9))
                .isEqualTo("hello  ");

        assertThat(assertions.function("ltrim", "'  hello  '", "CHAR ' '"))
                .hasType(createVarcharType(9))
                .isEqualTo("hello  ");

        assertThat(assertions.function("ltrim", "'  hello  '", "'he '"))
                .hasType(createVarcharType(9))
                .isEqualTo("llo  ");

        assertThat(assertions.function("ltrim", "'  hello'", "' '"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello");

        assertThat(assertions.function("ltrim", "'  hello'", "'e h'"))
                .hasType(createVarcharType(7))
                .isEqualTo("llo");

        assertThat(assertions.function("ltrim", "'hello  '", "'l'"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello  ");

        assertThat(assertions.function("ltrim", "' hello world '", "' '"))
                .hasType(createVarcharType(13))
                .isEqualTo("hello world ");

        assertThat(assertions.function("ltrim", "' hello world '", "' eh'"))
                .hasType(createVarcharType(13))
                .isEqualTo("llo world ");

        assertThat(assertions.function("ltrim", "' hello world '", "' ehlowrd'"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("ltrim", "' hello world '", "' x'"))
                .hasType(createVarcharType(13))
                .isEqualTo("hello world ");

        // non latin characters
        assertThat(assertions.function("ltrim", "'\u017a\u00f3\u0142\u0107'", "'\u00f3\u017a'"))
                .hasType(createVarcharType(4))
                .isEqualTo("\u0142\u0107");

        // invalid utf-8 characters
        assertThat(assertions.expression("CAST(LTRIM(utf8(from_hex('81')), ' ') AS VARBINARY)"))
                .isEqualTo(varbinary(0x81));

        assertThat(assertions.expression("CAST(LTRIM(CONCAT(utf8(from_hex('81')), ' '), ' ') AS VARBINARY)"))
                .isEqualTo(varbinary(0x81, ' '));

        assertThat(assertions.expression("CAST(LTRIM(CONCAT(' ', utf8(from_hex('81'))), ' ') AS VARBINARY)"))
                .isEqualTo(varbinary(0x81));

        assertThat(assertions.expression("CAST(LTRIM(CONCAT(' ', utf8(from_hex('81')), ' '), ' ') AS VARBINARY)"))
                .isEqualTo(varbinary(0x81, ' '));

        assertTrinoExceptionThrownBy(() -> assertions.function("ltrim", "'hello world'", "utf8(from_hex('81'))").evaluate())
                .hasMessage("Invalid UTF-8 encoding in characters: �");

        assertTrinoExceptionThrownBy(() -> assertions.function("ltrim", "'hello wolrd'", "utf8(from_hex('3281'))").evaluate())
                .hasMessage("Invalid UTF-8 encoding in characters: 2�");
    }

    @Test
    public void testCharLeftTrimParametrized()
    {
        assertThat(assertions.function("ltrim", "CAST('' AS CHAR(1))", "''"))
                .hasType(createVarcharType(1))
                .isEqualTo("");

        assertThat(assertions.function("ltrim", "CAST('   ' AS CHAR(3))", "''"))
                .hasType(createVarcharType(3))
                .isEqualTo("");

        assertThat(assertions.function("ltrim", "CAST('  hello  ' AS CHAR(9))", "''"))
                .hasType(createVarcharType(9))
                .isEqualTo("  hello");

        assertThat(assertions.function("ltrim", "CAST('  hello  ' AS CHAR(9))", "' '"))
                .hasType(createVarcharType(9))
                .isEqualTo("hello");

        assertThat(assertions.function("ltrim", "CAST('  hello  ' AS CHAR(9))", "'he '"))
                .hasType(createVarcharType(9))
                .isEqualTo("llo");

        assertThat(assertions.function("ltrim", "CAST('  hello' AS CHAR(7))", "' '"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello");

        assertThat(assertions.function("ltrim", "CAST('  hello' AS CHAR(7))", "'e h'"))
                .hasType(createVarcharType(7))
                .isEqualTo("llo");

        assertThat(assertions.function("ltrim", "CAST('hello  ' AS CHAR(7))", "'l'"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello");

        assertThat(assertions.function("ltrim", "CAST(' hello world ' AS CHAR(13))", "' '"))
                .hasType(createVarcharType(13))
                .isEqualTo("hello world");

        assertThat(assertions.function("ltrim", "CAST(' hello world ' AS CHAR(13))", "' eh'"))
                .hasType(createVarcharType(13))
                .isEqualTo("llo world");

        assertThat(assertions.function("ltrim", "CAST(' hello world ' AS CHAR(13))", "' ehlowrd'"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("ltrim", "CAST(' hello world ' AS CHAR(13))", "' x'"))
                .hasType(createVarcharType(13))
                .isEqualTo("hello world");

        // non latin characters
        assertThat(assertions.function("ltrim", "CAST('\u017a\u00f3\u0142\u0107' AS CHAR(4))", "'\u00f3\u017a'"))
                .hasType(createVarcharType(4))
                .isEqualTo("\u0142\u0107");
    }

    private static SqlVarbinary varbinary(int... bytesAsInts)
    {
        byte[] bytes = new byte[bytesAsInts.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) bytesAsInts[i];
        }
        return new SqlVarbinary(bytes);
    }

    @Test
    public void testRightTrimParametrized()
    {
        assertThat(assertions.function("rtrim", "''", "''"))
                .hasType(createVarcharType(0))
                .isEqualTo("");

        assertThat(assertions.function("rtrim", "'   '", "''"))
                .hasType(createVarcharType(3))
                .isEqualTo("   ");

        assertThat(assertions.function("rtrim", "'  hello  '", "''"))
                .hasType(createVarcharType(9))
                .isEqualTo("  hello  ");

        assertThat(assertions.function("rtrim", "'  hello  '", "' '"))
                .hasType(createVarcharType(9))
                .isEqualTo("  hello");

        assertThat(assertions.function("rtrim", "'  hello  '", "'lo '"))
                .hasType(createVarcharType(9))
                .isEqualTo("  he");

        assertThat(assertions.function("rtrim", "'hello  '", "' '"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello");

        assertThat(assertions.function("rtrim", "'hello  '", "'l o'"))
                .hasType(createVarcharType(7))
                .isEqualTo("he");

        assertThat(assertions.function("rtrim", "'hello  '", "'l'"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello  ");

        assertThat(assertions.function("rtrim", "' hello world '", "' '"))
                .hasType(createVarcharType(13))
                .isEqualTo(" hello world");

        assertThat(assertions.function("rtrim", "' hello world '", "' ld'"))
                .hasType(createVarcharType(13))
                .isEqualTo(" hello wor");

        assertThat(assertions.function("rtrim", "' hello world '", "' ehlowrd'"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("rtrim", "' hello world '", "' x'"))
                .hasType(createVarcharType(13))
                .isEqualTo(" hello world");

        assertThat(assertions.function("rtrim", "CAST('abc def' AS CHAR(7))", "'def'"))
                .hasType(createVarcharType(7))
                .isEqualTo("abc");

        // non latin characters
        assertThat(assertions.function("rtrim", "'\u017a\u00f3\u0142\u0107'", "'\u0107\u0142'"))
                .hasType(createVarcharType(4))
                .isEqualTo("\u017a\u00f3");

        // invalid utf-8 characters
        // TODO
        assertThat(assertions.expression("CAST(RTRIM(utf8(from_hex('81')), ' ') AS VARBINARY)"))
                .isEqualTo(varbinary(0x81));

        assertThat(assertions.expression("CAST(RTRIM(CONCAT(utf8(from_hex('81')), ' '), ' ') AS VARBINARY)"))
                .isEqualTo(varbinary(0x81));

        assertThat(assertions.expression("CAST(RTRIM(CONCAT(' ', utf8(from_hex('81'))), ' ') AS VARBINARY)"))
                .isEqualTo(varbinary(' ', 0x81));

        assertThat(assertions.expression("CAST(RTRIM(CONCAT(' ', utf8(from_hex('81')), ' '), ' ') AS VARBINARY)"))
                .isEqualTo(varbinary(' ', 0x81));

        assertTrinoExceptionThrownBy(() -> assertions.function("rtrim", "'hello world'", "utf8(from_hex('81'))").evaluate())
                .hasMessage("Invalid UTF-8 encoding in characters: �");

        assertTrinoExceptionThrownBy(() -> assertions.function("rtrim", "'hello world'", "utf8(from_hex('3281'))").evaluate())
                .hasMessage("Invalid UTF-8 encoding in characters: 2�");
    }

    @Test
    public void testCharRightTrimParametrized()
    {
        assertThat(assertions.function("rtrim", "CAST('' AS CHAR(1))", "''"))
                .hasType(createVarcharType(1))
                .isEqualTo("");

        assertThat(assertions.function("rtrim", "CAST('   ' AS CHAR(3))", "''"))
                .hasType(createVarcharType(3))
                .isEqualTo("");

        assertThat(assertions.function("rtrim", "CAST('  hello  ' AS CHAR(9))", "''"))
                .hasType(createVarcharType(9))
                .isEqualTo("  hello");

        assertThat(assertions.function("rtrim", "CAST('  hello  ' AS CHAR(9))", "' '"))
                .hasType(createVarcharType(9))
                .isEqualTo("  hello");

        assertThat(assertions.function("rtrim", "CAST('  hello  ' AS CHAR(9))", "'he '"))
                .hasType(createVarcharType(9))
                .isEqualTo("  hello");

        assertThat(assertions.function("rtrim", "CAST('  hello' AS CHAR(7))", "' '"))
                .hasType(createVarcharType(7))
                .isEqualTo("  hello");

        assertThat(assertions.function("rtrim", "CAST('  hello' AS CHAR(7))", "'e h'"))
                .hasType(createVarcharType(7))
                .isEqualTo("  hello");

        assertThat(assertions.function("rtrim", "CAST('hello  ' AS CHAR(7))", "'l'"))
                .hasType(createVarcharType(7))
                .isEqualTo("hello");

        assertThat(assertions.function("rtrim", "CAST(' hello world ' AS CHAR(13))", "' '"))
                .hasType(createVarcharType(13))
                .isEqualTo(" hello world");

        assertThat(assertions.function("rtrim", "CAST(' hello world ' AS CHAR(13))", "' eh'"))
                .hasType(createVarcharType(13))
                .isEqualTo(" hello world");

        assertThat(assertions.function("rtrim", "CAST(' hello world ' AS CHAR(13))", "' ehlowrd'"))
                .hasType(createVarcharType(13))
                .isEqualTo("");

        assertThat(assertions.function("rtrim", "CAST(' hello world ' AS CHAR(13))", "' x'"))
                .hasType(createVarcharType(13))
                .isEqualTo(" hello world");

        // non latin characters
        assertThat(assertions.function("rtrim", "CAST('\u017a\u00f3\u0142\u0107' AS CHAR(4))", "'\u0107\u0142'"))
                .hasType(createVarcharType(4))
                .isEqualTo("\u017a\u00f3");
    }

    @Test
    public void testVarcharToVarcharX()
    {
        assertThat(assertions.function("lower", "VARCHAR 'HELLO'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("hello");
    }

    @Test
    public void testLower()
    {
        assertThat(assertions.function("lower", "''"))
                .hasType(createVarcharType(0))
                .isEqualTo("");

        assertThat(assertions.function("lower", "'Hello World'"))
                .hasType(createVarcharType(11))
                .isEqualTo("hello world");

        assertThat(assertions.function("lower", "'WHAT!!'"))
                .hasType(createVarcharType(6))
                .isEqualTo("what!!");

        assertThat(assertions.function("lower", "'\u00D6STERREICH'"))
                .hasType(createVarcharType(10))
                .isEqualTo(lowerByCodePoint("\u00D6sterreich"));

        assertThat(assertions.function("lower", "'From\uD801\uDC2DTo'"))
                .hasType(createVarcharType(7))
                .isEqualTo(lowerByCodePoint("from\uD801\uDC2Dto"));
    }

    @Test
    public void testCharLower()
    {
        assertThat(assertions.function("lower", "CAST('' AS CHAR(10))"))
                .hasType(createCharType(10))
                .isEqualTo(padRight("", 10));

        assertThat(assertions.function("lower", "CAST('Hello World' AS CHAR(11))"))
                .hasType(createCharType(11))
                .isEqualTo(padRight("hello world", 11));

        assertThat(assertions.function("lower", "CAST('WHAT!!' AS CHAR(6))"))
                .hasType(createCharType(6))
                .isEqualTo(padRight("what!!", 6));

        assertThat(assertions.function("lower", "CAST('\u00D6STERREICH' AS CHAR(10))"))
                .hasType(createCharType(10))
                .isEqualTo(padRight(lowerByCodePoint("\u00D6sterreich"), 10));

        assertThat(assertions.function("lower", "CAST('From\uD801\uDC2DTo' AS CHAR(7))"))
                .hasType(createCharType(7))
                .isEqualTo(padRight(lowerByCodePoint("from\uD801\uDC2Dto"), 7));
    }

    @Test
    public void testUpper()
    {
        assertThat(assertions.function("upper", "''"))
                .hasType(createVarcharType(0))
                .isEqualTo("");

        assertThat(assertions.function("upper", "'Hello World'"))
                .hasType(createVarcharType(11))
                .isEqualTo("HELLO WORLD");

        assertThat(assertions.function("upper", "'what!!'"))
                .hasType(createVarcharType(6))
                .isEqualTo("WHAT!!");

        assertThat(assertions.function("upper", "'\u00D6sterreich'"))
                .hasType(createVarcharType(10))
                .isEqualTo(upperByCodePoint("\u00D6") + "STERREICH");

        assertThat(assertions.function("upper", "'From\uD801\uDC2DTo'"))
                .hasType(createVarcharType(7))
                .isEqualTo("FROM" + upperByCodePoint("\uD801\uDC2D") + "TO");
    }

    @Test
    public void testCharUpper()
    {
        assertThat(assertions.function("upper", "CAST('' AS CHAR(10))"))
                .hasType(createCharType(10))
                .isEqualTo(padRight("", 10));

        assertThat(assertions.function("upper", "CAST('Hello World' AS CHAR(11))"))
                .hasType(createCharType(11))
                .isEqualTo(padRight("HELLO WORLD", 11));

        assertThat(assertions.function("upper", "CAST('what!!' AS CHAR(6))"))
                .hasType(createCharType(6))
                .isEqualTo(padRight("WHAT!!", 6));

        assertThat(assertions.function("upper", "CAST('\u00D6sterreich' AS CHAR(10))"))
                .hasType(createCharType(10))
                .isEqualTo(padRight(upperByCodePoint("\u00D6") + "STERREICH", 10));

        assertThat(assertions.function("upper", "CAST('From\uD801\uDC2DTo' AS CHAR(7))"))
                .hasType(createCharType(7))
                .isEqualTo(padRight("FROM" + upperByCodePoint("\uD801\uDC2D") + "TO", 7));
    }

    @Test
    public void testLeftPad()
    {
        assertThat(assertions.function("lpad", "'text'", "5", "'x'"))
                .hasType(VARCHAR)
                .isEqualTo("xtext");

        assertThat(assertions.function("lpad", "'text'", "4", "'x'"))
                .hasType(VARCHAR)
                .isEqualTo("text");

        assertThat(assertions.function("lpad", "'text'", "6", "'xy'"))
                .hasType(VARCHAR)
                .isEqualTo("xytext");

        assertThat(assertions.function("lpad", "'text'", "7", "'xy'"))
                .hasType(VARCHAR)
                .isEqualTo("xyxtext");

        assertThat(assertions.function("lpad", "'text'", "9", "'xyz'"))
                .hasType(VARCHAR)
                .isEqualTo("xyzxytext");

        assertThat(assertions.function("lpad", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '", "10", "'\u671B'"))
                .hasType(VARCHAR)
                .isEqualTo("\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");

        assertThat(assertions.function("lpad", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '", "11", "'\u671B'"))
                .hasType(VARCHAR)
                .isEqualTo("\u671B\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");

        assertThat(assertions.function("lpad", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '", "12", "'\u5E0C\u671B'"))
                .hasType(VARCHAR)
                .isEqualTo("\u5E0C\u671B\u5E0C\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");

        assertThat(assertions.function("lpad", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '", "13", "'\u5E0C\u671B'"))
                .hasType(VARCHAR)
                .isEqualTo("\u5E0C\u671B\u5E0C\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ");

        assertThat(assertions.function("lpad", "''", "3", "'a'"))
                .hasType(VARCHAR)
                .isEqualTo("aaa");

        assertThat(assertions.function("lpad", "'abc'", "0", "'e'"))
                .hasType(VARCHAR)
                .isEqualTo("");

        // truncation
        assertThat(assertions.function("lpad", "'text'", "3", "'xy'"))
                .hasType(VARCHAR)
                .isEqualTo("tex");

        assertThat(assertions.function("lpad", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '", "5", "'\u671B'"))
                .hasType(VARCHAR)
                .isEqualTo("\u4FE1\u5FF5 \u7231 ");

        // failure modes
        assertTrinoExceptionThrownBy(() -> assertions.function("lpad", "'abc'", "3", "''").evaluate())
                .hasMessage("Padding string must not be empty");

        // invalid target lengths
        long maxSize = Integer.MAX_VALUE;
        assertTrinoExceptionThrownBy(() -> assertions.function("lpad", "'abc'", "-1", "'foo'").evaluate())
                .hasMessage("Target length must be in the range [0.." + maxSize + "]");

        assertTrinoExceptionThrownBy(() -> assertions.function("lpad", "'abc'", Long.toString(maxSize + 1), "''").evaluate())
                .hasMessage("Target length must be in the range [0.." + maxSize + "]");
    }

    @Test
    public void testRightPad()
    {
        assertThat(assertions.function("rpad", "'text'", "5", "'x'"))
                .hasType(VARCHAR)
                .isEqualTo("textx");

        assertThat(assertions.function("rpad", "'text'", "4", "'x'"))
                .hasType(VARCHAR)
                .isEqualTo("text");

        assertThat(assertions.function("rpad", "'text'", "6", "'xy'"))
                .hasType(VARCHAR)
                .isEqualTo("textxy");

        assertThat(assertions.function("rpad", "'text'", "7", "'xy'"))
                .hasType(VARCHAR)
                .isEqualTo("textxyx");

        assertThat(assertions.function("rpad", "'text'", "9", "'xyz'"))
                .hasType(VARCHAR)
                .isEqualTo("textxyzxy");

        assertThat(assertions.function("rpad", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '", "10", "'\u671B'"))
                .hasType(VARCHAR)
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u671B");

        assertThat(assertions.function("rpad", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '", "11", "'\u671B'"))
                .hasType(VARCHAR)
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u671B\u671B");

        assertThat(assertions.function("rpad", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '", "12", "'\u5E0C\u671B'"))
                .hasType(VARCHAR)
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u5E0C\u671B\u5E0C");

        assertThat(assertions.function("rpad", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '", "13", "'\u5E0C\u671B'"))
                .hasType(VARCHAR)
                .isEqualTo("\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u5E0C\u671B\u5E0C\u671B");

        assertThat(assertions.function("rpad", "''", "3", "'a'"))
                .hasType(VARCHAR)
                .isEqualTo("aaa");

        assertThat(assertions.function("rpad", "'abc'", "0", "'e'"))
                .hasType(VARCHAR)
                .isEqualTo("");

        // truncation
        assertThat(assertions.function("rpad", "'text'", "3", "'xy'"))
                .hasType(VARCHAR)
                .isEqualTo("tex");

        assertThat(assertions.function("rpad", "'\u4FE1\u5FF5 \u7231 \u5E0C\u671B  '", "5", "'\u671B'"))
                .hasType(VARCHAR)
                .isEqualTo("\u4FE1\u5FF5 \u7231 ");

        // failure modes
        assertTrinoExceptionThrownBy(() -> assertions.function("rpad", "'abc'", "3", "''").evaluate())
                .hasMessage("Padding string must not be empty");

        // invalid target lengths
        long maxSize = Integer.MAX_VALUE;
        assertTrinoExceptionThrownBy(() -> assertions.function("rpad", "'abc'", "-1", "'foo'").evaluate())
                .hasMessage("Target length must be in the range [0.." + maxSize + "]");

        assertTrinoExceptionThrownBy(() -> assertions.function("rpad", "'abc'", Long.toString(maxSize + 1), "''").evaluate())
                .hasMessage("Target length must be in the range [0.." + maxSize + "]");
    }

    @Test
    public void testNormalize()
    {
        assertThat(assertions.expression("normalize(value, NFD)")
                .binding("value", "'sch\u00f6n'"))
                .hasType(VARCHAR)
                .isEqualTo("scho\u0308n");

        assertThat(assertions.expression("normalize('sch\u00f6n')"))
                .hasType(VARCHAR)
                .isEqualTo("sch\u00f6n");

        assertThat(assertions.expression("normalize(value, NFC)")
                .binding("value", "'sch\u00f6n'"))
                .hasType(VARCHAR)
                .isEqualTo("sch\u00f6n");

        assertThat(assertions.expression("normalize(value, NFKD)")
                .binding("value", "'sch\u00f6n'"))
                .hasType(VARCHAR)
                .isEqualTo("scho\u0308n");

        assertThat(assertions.expression("normalize(value, NFKC)")
                .binding("value", "'sch\u00f6n'"))
                .hasType(VARCHAR)
                .isEqualTo("sch\u00f6n");

        assertThat(assertions.expression("normalize(value, NFKC)")
                .binding("value", "'\u3231\u3327\u3326\u2162'"))
                .hasType(VARCHAR)
                .isEqualTo("(\u682a)\u30c8\u30f3\u30c9\u30ebIII");

        assertThat(assertions.expression("normalize(value, NFKC)")
                .binding("value", "'\uff8a\uff9d\uff76\uff78\uff76\uff85'"))
                .hasType(VARCHAR)
                .isEqualTo("\u30cf\u30f3\u30ab\u30af\u30ab\u30ca");
    }

    @Test
    public void testFromLiteralParameter()
    {
        assertThat(assertions.function("vl", "cast('aaa' as varchar(3))"))
                .isEqualTo(3L);

        assertThat(assertions.function("vl", "cast('aaa' as varchar(7))"))
                .isEqualTo(7L);

        assertThat(assertions.function("vl", "'aaaa'"))
                .isEqualTo(4L);
    }

    // We do not use String toLowerCase or toUpperCase here because they can do multi character transforms
    // and we want to verify our implementation spec which states we perform case transform code point by
    // code point
    private static String lowerByCodePoint(String string)
    {
        int[] upperCodePoints = string.codePoints().map(Character::toLowerCase).toArray();
        return new String(upperCodePoints, 0, upperCodePoints.length);
    }

    private static String upperByCodePoint(String string)
    {
        int[] upperCodePoints = string.codePoints().map(Character::toUpperCase).toArray();
        return new String(upperCodePoints, 0, upperCodePoints.length);
    }

    @Test
    public void testFromUtf8()
    {
        assertThat(assertions.function("from_utf8", "to_utf8('hello')"))
                .hasType(VARCHAR)
                .isEqualTo("hello");

        assertThat(assertions.function("from_utf8", "from_hex('58BF')"))
                .hasType(VARCHAR)
                .isEqualTo("X\uFFFD");

        assertThat(assertions.function("from_utf8", "from_hex('58DF')"))
                .hasType(VARCHAR)
                .isEqualTo("X\uFFFD");

        assertThat(assertions.function("from_utf8", "from_hex('58F7')"))
                .hasType(VARCHAR)
                .isEqualTo("X\uFFFD");

        assertThat(assertions.function("from_utf8", "from_hex('58BF')", "'#'"))
                .hasType(VARCHAR)
                .isEqualTo("X#");

        assertThat(assertions.function("from_utf8", "from_hex('58DF')", "35"))
                .hasType(VARCHAR)
                .isEqualTo("X#");

        assertThat(assertions.function("from_utf8", "from_hex('58BF')", "''"))
                .hasType(VARCHAR)
                .isEqualTo("X");

        assertTrinoExceptionThrownBy(() -> assertions.function("from_utf8", "to_utf8('hello')", "'foo'").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.function("from_utf8", "to_utf8('hello')", "1114112").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testCharConcat()
    {
        assertThat(assertions.function("concat", "'ab '", "cast(' ' as char(1))"))
                .hasType(createCharType(4))
                .isEqualTo("ab  ");

        assertThat(assertions.function("concat", "'ab '", "cast(' ' as char(1))"))
                .hasType(createCharType(4))
                .isEqualTo("ab  ");

        assertThat(assertions.function("concat", "'ab '", "cast('a' as char(2))"))
                .hasType(createCharType(5))
                .isEqualTo("ab a ");

        assertThat(assertions.function("concat", "'ab '", "cast('a' as char(2))"))
                .hasType(createCharType(5))
                .isEqualTo("ab a ");

        assertThat(assertions.function("concat", "'ab '", "cast('' as char(0))"))
                .hasType(createCharType(3))
                .isEqualTo("ab ");

        assertThat(assertions.function("concat", "'ab '", "cast('' as char(0))"))
                .hasType(createCharType(3))
                .isEqualTo("ab ");

        assertThat(assertions.function("concat", "'hello na\u00EFve'", "cast(' world' as char(6))"))
                .hasType(createCharType(17))
                .isEqualTo("hello na\u00EFve world");

        assertTrinoExceptionThrownBy(() -> assertions.function("concat", "cast('ab ' as char(40000))", "cast('' as char(40000))").evaluate())
                .hasErrorCode(TYPE_NOT_FOUND)
                .hasMessage("line 1:8: Unknown type: char(80000)");

        assertThat(assertions.function("concat", "cast(null as char(1))", "cast(' ' as char(1))"))
                .isNull(createCharType(2));
    }

    @Test
    public void testTranslate()
    {
        assertThat(assertions.function("translate", "'abcd'", "''", "''"))
                .hasType(VARCHAR)
                .isEqualTo("abcd");

        assertThat(assertions.function("translate", "'abcd'", "'a'", "'z'"))
                .hasType(VARCHAR)
                .isEqualTo("zbcd");

        assertThat(assertions.function("translate", "'abcda'", "'a'", "'z'"))
                .hasType(VARCHAR)
                .isEqualTo("zbcdz");

        assertThat(assertions.function("translate", "'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ'", "'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ'", "'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'"))
                .hasType(VARCHAR)
                .isEqualTo("aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU");

        assertThat(assertions.function("translate", "'Goiânia'", "'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ'", "'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'"))
                .hasType(VARCHAR)
                .isEqualTo("Goiania");

        assertThat(assertions.function("translate", "'São Paulo'", "'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ'", "'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'"))
                .hasType(VARCHAR)
                .isEqualTo("Sao Paulo");

        assertThat(assertions.function("translate", "'Palhoça'", "'ç'", "'c'"))
                .hasType(VARCHAR)
                .isEqualTo("Palhoca");

        assertThat(assertions.function("translate", "'Várzea Paulista'", "'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ'", "'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'"))
                .hasType(VARCHAR)
                .isEqualTo("Varzea Paulista");

        // Test chars that don't fit in 16 bits - - U+20000 is written as "\uD840\uDC00"

        assertThat(assertions.function("translate", "'\uD840\uDC00bcd'", "'\uD840\uDC00'", "'z'"))
                .hasType(VARCHAR)
                .isEqualTo("zbcd");

        assertThat(assertions.function("translate", "'\uD840\uDC00bcd\uD840\uDC00'", "'\uD840\uDC00'", "'z'"))
                .hasType(VARCHAR)
                .isEqualTo("zbcdz");

        assertThat(assertions.function("translate", "'abcd'", "'b'", "'\uD840\uDC00'"))
                .hasType(VARCHAR)
                .isEqualTo("a\uD840\uDC00cd");

        // Test that the to string can be shorter than the from string, and that we choose the first duplicate in the from string

        assertThat(assertions.function("translate", "'abcd'", "'a'", "''"))
                .hasType(VARCHAR)
                .isEqualTo("bcd");

        assertThat(assertions.function("translate", "'abcd'", "'a'", "'zy'"))
                .hasType(VARCHAR)
                .isEqualTo("zbcd");

        assertThat(assertions.function("translate", "'abcd'", "'ac'", "'z'"))
                .hasType(VARCHAR)
                .isEqualTo("zbd");

        assertThat(assertions.function("translate", "'abcd'", "'aac'", "'zq'"))
                .hasType(VARCHAR)
                .isEqualTo("zbd");
    }

    @Test
    public void testSoundex()
    {
        assertThat(assertions.function("soundex", "'jim'"))
                .hasType(createVarcharType(4))
                .isEqualTo("J500");

        assertThat(assertions.function("soundex", "'jIM'"))
                .hasType(createVarcharType(4))
                .isEqualTo("J500");

        assertThat(assertions.function("soundex", "'JIM'"))
                .hasType(createVarcharType(4))
                .isEqualTo("J500");

        assertThat(assertions.function("soundex", "'Jim'"))
                .hasType(createVarcharType(4))
                .isEqualTo("J500");

        assertThat(assertions.function("soundex", "'John'"))
                .hasType(createVarcharType(4))
                .isEqualTo("J500");

        assertThat(assertions.function("soundex", "'johannes'"))
                .hasType(createVarcharType(4))
                .isEqualTo("J520");

        assertThat(assertions.function("soundex", "'Sarah'"))
                .hasType(createVarcharType(4))
                .isEqualTo("S600");

        assertThat(assertions.function("soundex", "null"))
                .isNull(createVarcharType(4));

        assertThat(assertions.function("soundex", "''"))
                .hasType(createVarcharType(4))
                .isEqualTo("");

        assertThat(assertions.function("soundex", "'123'"))
                .hasType(createVarcharType(4))
                .isEqualTo("");

        assertThat(assertions.function("soundex", "'\uD83D\uDE80'"))
                .hasType(createVarcharType(4))
                .isEqualTo("");

        assertThat(assertions.function("soundex", "'j~im'"))
                .hasType(createVarcharType(4))
                .isEqualTo("J500");

        assertTrinoExceptionThrownBy(() -> assertions.function("soundex", "'jąmes'").evaluate())
                .hasMessage("The character is not mapped: Ą (index=195)");

        assertThat(assertions.function("soundex", "'x123'"))
                .hasType(createVarcharType(4))
                .isEqualTo("X000");
    }
}
