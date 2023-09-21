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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.FeaturesConfig;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.analyzer.RegexLibrary;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.LocalQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collections;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class AbstractTestRegexpFunctions
{
    private final RegexLibrary regexLibrary;
    private QueryAssertions assertions;

    protected AbstractTestRegexpFunctions(RegexLibrary regexLibrary)
    {
        this.regexLibrary = requireNonNull(regexLibrary, "regexLibrary is null");
    }

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions(
                LocalQueryRunner.builder(testSessionBuilder().build())
                        .withFeaturesConfig(new FeaturesConfig().setRegexLibrary(regexLibrary))
                        .build());

        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(AbstractTestRegexpFunctions.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @ScalarFunction(deterministic = false) // if not non-deterministic, constant folding code accidentally fix invalid characters
    @SqlType(StandardTypes.VARCHAR)
    public static Slice invalidUtf8()
    {
        return Slices.wrappedBuffer(new byte[] {
                // AAA\uD800AAAA\uDFFFAAA, D800 and DFFF are valid unicode code points, but not valid UTF8
                (byte) 0x41, 0x41, (byte) 0xed, (byte) 0xa0, (byte) 0x80, 0x41, 0x41,
                0x41, 0x41, (byte) 0xed, (byte) 0xbf, (byte) 0xbf, 0x41, 0x41, 0x41,
        });
    }

    @Test
    public void testRegexpLike()
    {
        // Tests that REGEXP_LIKE doesn't loop infinitely on invalid UTF-8 input. Return value is irrelevant.
        assertions.function("regexp_like", "invalid_utf8()", "invalid_utf8()").evaluate();

        assertThat(assertions.function("regexp_like", "'Stephen'", "'Ste(v|ph)en'"))
                .isEqualTo(true);

        assertThat(assertions.function("regexp_like", "'Stevens'", "'Ste(v|ph)en'"))
                .isEqualTo(true);

        assertThat(assertions.function("regexp_like", "'Stephen'", "'^Ste(v|ph)en$'"))
                .isEqualTo(true);

        assertThat(assertions.function("regexp_like", "'Stevens'", "'^Ste(v|ph)en$'"))
                .isEqualTo(false);

        assertThat(assertions.function("regexp_like", "'hello world'", "'[a-z]'"))
                .isEqualTo(true);

        assertThat(assertions.function("regexp_like", "'hello\nworld'", "'.*hello\nworld.*'"))
                .isEqualTo(true);

        assertThat(assertions.function("regexp_like", "'Hello'", "'^[a-z]+$'"))
                .isEqualTo(false);

        assertThat(assertions.function("regexp_like", "'Hello'", "'^(?i)[a-z]+$'"))
                .isEqualTo(true);

        assertThat(assertions.function("regexp_like", "'Hello'", "'^[a-zA-Z]+$'"))
                .isEqualTo(true);

        // verify word boundaries at end of pattern (https://github.com/airlift/joni/pull/11)
        assertThat(assertions.function("regexp_like", "'test'", "'test\\b'"))
                .isEqualTo(true);
    }

    @Test
    public void testRegexCharLike()
    {
        assertThat(assertions.function("regexp_like", "'ala'", "CHAR 'ala  '"))
                .isEqualTo(false);

        assertThat(assertions.function("regexp_like", "'ala  '", "CHAR 'ala  '"))
                .isEqualTo(true);
    }

    @Test
    public void testRegexpReplace()
    {
        assertThat(assertions.function("regexp_replace", "'abc有朋$%X自9远方来'", "''", "'Y'"))
                .hasType(createVarcharType(97))
                .isEqualTo("YaYbYcY有Y朋Y$Y%YXY自Y9Y远Y方Y来Y");

        assertThat(assertions.function("regexp_replace", "'a有朋💰'", "'.'", "'Y'"))
                .hasType(createVarcharType(14))
                .isEqualTo("YYYY");

        assertThat(assertions.function("regexp_replace", "'a有朋💰'", "'.'", "'1$02'"))
                .hasType(createVarcharType(44))
                .isEqualTo("1a21有21朋21💰2");

        assertThat(assertions.function("regexp_replace", "''", "''", "'Y'"))
                .hasType(createVarcharType(1))
                .isEqualTo("Y");

        assertThat(assertions.function("regexp_replace", "'fun stuff.'", "'[a-z]'"))
                .hasType(createVarcharType(10))
                .isEqualTo(" .");

        assertThat(assertions.function("regexp_replace", "'fun stuff.'", "'[a-z]'", "'*'"))
                .hasType(createVarcharType(65))
                .isEqualTo("*** *****.");

        assertThat(assertions.function("regexp_replace", "'call 555.123.4444 now'", "'(\\d{3})\\.(\\d{3}).(\\d{4})'"))
                .hasType(createVarcharType(21))
                .isEqualTo("call  now");

        assertThat(assertions.function("regexp_replace", "'call 555.123.4444 now'", "'(\\d{3})\\.(\\d{3}).(\\d{4})'", "'($1) $2-$3'"))
                .hasType(createVarcharType(2331))
                .isEqualTo("call (555) 123-4444 now");

        assertThat(assertions.function("regexp_replace", "'xxx xxx xxx'", "'x'", "'x'"))
                .hasType(createVarcharType(71))
                .isEqualTo("xxx xxx xxx");

        assertThat(assertions.function("regexp_replace", "'xxx xxx xxx'", "'x'", "'\\x'"))
                .hasType(createVarcharType(143))
                .isEqualTo("xxx xxx xxx");

        assertThat(assertions.function("regexp_replace", "'xxx'", "''", "'y'"))
                .hasType(createVarcharType(7))
                .isEqualTo("yxyxyxy");

        assertTrinoExceptionThrownBy(assertions.function("regexp_replace", "'xxx'", "'x'", "'\\'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertThat(assertions.function("regexp_replace", "'xxx xxx xxx'", "'x'", "'$0'"))
                .hasType(createVarcharType(143))
                .isEqualTo("xxx xxx xxx");

        assertThat(assertions.function("regexp_replace", "'xxx'", "'(x)'", "'$01'"))
                .hasType(createVarcharType(19))
                .isEqualTo("xxx");

        assertThat(assertions.function("regexp_replace", "'xxx'", "'x'", "'$05'"))
                .hasType(createVarcharType(19))
                .isEqualTo("x5x5x5");

        assertThat(assertions.function("regexp_replace", "'123456789'", "'(1)(2)(3)(4)(5)(6)(7)(8)(9)'", "'$10'"))
                .hasType(createVarcharType(139))
                .isEqualTo("10");

        assertThat(assertions.function("regexp_replace", "'1234567890'", "'(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)'", "'$10'"))
                .hasType(createVarcharType(175))
                .isEqualTo("0");

        assertThat(assertions.function("regexp_replace", "'1234567890'", "'(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)'", "'$11'"))
                .hasType(createVarcharType(175))
                .isEqualTo("11");

        assertThat(assertions.function("regexp_replace", "'1234567890'", "'(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)'", "'$1a'"))
                .hasType(createVarcharType(175))
                .isEqualTo("1a");

        assertTrinoExceptionThrownBy(assertions.function("regexp_replace", "'xxx'", "'x'", "'$1'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("regexp_replace", "'xxx'", "'x'", "'$a'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("regexp_replace", "'xxx'", "'x'", "'$'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertThat(assertions.function("regexp_replace", "'wxyz'", "'(?<xyz>[xyz])'", "'${xyz}${xyz}'"))
                .hasType(createVarcharType(124))
                .isEqualTo("wxxyyzz");

        assertThat(assertions.function("regexp_replace", "'wxyz'", "'(?<w>w)|(?<xyz>[xyz])'", "'[${w}](${xyz})'"))
                .hasType(createVarcharType(144))
                .isEqualTo("[w]()[](x)[](y)[](z)");

        assertThat(assertions.function("regexp_replace", "'xyz'", "'(?<xyz>[xyz])+'", "'${xyz}'"))
                .hasType(createVarcharType(39))
                .isEqualTo("z");

        assertThat(assertions.function("regexp_replace", "'xyz'", "'(?<xyz>[xyz]+)'", "'${xyz}'"))
                .hasType(createVarcharType(39))
                .isEqualTo("xyz");

        assertTrinoExceptionThrownBy(assertions.function("regexp_replace", "'xxx'", "'(?<name>x)'", "'${}'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("regexp_replace", "'xxx'", "'(?<name>x)'", "'${0}'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("regexp_replace", "'xxx'", "'(?<name>x)'", "'${nam}'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertThat(assertions.function("regexp_replace", "VARCHAR 'x'", "'.*'", "'xxxxx'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("xxxxxxxxxx");
    }

    @Test
    public void testRegexpReplaceLambda()
    {
        assertThat(assertions.expression("regexp_replace(a, b, x -> 'Y')")
                .binding("a", "'abc有朋$%X自9远方来'")
                .binding("b", "''"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("YaYbYcY有Y朋Y$Y%YXY自Y9Y远Y方Y来Y");

        assertThat(assertions.expression("regexp_replace(a, b, x -> 'Y')")
                .binding("a", "'a有朋💰'")
                .binding("b", "'.'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("YYYY");

        assertThat(assertions.expression("regexp_replace(a, b, x -> '1' || x[1] || '2')")
                .binding("a", "'a有朋💰'")
                .binding("b", "'(.)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("1a21有21朋21💰2");

        assertThat(assertions.expression("regexp_replace(a, b, x -> 'Y')")
                .binding("a", "''")
                .binding("b", "''"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("Y");

        // One or more matches with non-empty, not null capturing groups
        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[1]))")
                .binding("a", "'x'")
                .binding("b", "'(x)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("X");

        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[1]))")
                .binding("a", "'xxx xxx xxx'")
                .binding("b", "'(x)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("XXX XXX XXX");

        assertThat(assertions.expression("regexp_replace(a, b, x -> x[1])")
                .binding("a", "'new'")
                .binding("b", "'(\\w)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("new");

        assertThat(assertions.expression("regexp_replace(a, b, x -> x[1] || upper(x[1]))")
                .binding("a", "'new'")
                .binding("b", "'(\\w)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("nNeEwW");

        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[1]) || lower(x[2]))")
                .binding("a", "'new york'")
                .binding("b", "'(\\w)(\\w*)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("New York");

        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[2]) || lower(x[3]))")
                .binding("a", "'new york'")
                .binding("b", "'((\\w)(\\w*))'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("New York");

        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[1]))")
                .binding("a", "'new york'")
                .binding("b", "'(n\\w*)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("NEW york");

        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[1]))")
                .binding("a", "'new york'")
                .binding("b", "'(y\\w*)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("new YORK");

        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[1]))")
                .binding("a", "'new york city'")
                .binding("b", "'(yo\\w*)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("new YORK city");

        assertThat(assertions.expression("regexp_replace(a, b, x -> 'm')")
                .binding("a", "'abc abc'")
                .binding("b", "'(abc)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("m m");

        assertThat(assertions.expression("regexp_replace(a, b, x -> x[1])")
                .binding("a", "'123 456'")
                .binding("b", "'([0-9]*)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("123 456");

        assertThat(assertions.expression("regexp_replace(a, b, x -> x[2] || x[3])")
                .binding("a", "'123 456'")
                .binding("b", "'(([0-9]*) ([0-9]*))'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("123456");

        assertThat(assertions.expression("regexp_replace(a, b, x -> 'm')")
                .binding("a", "'abbabba'")
                .binding("b", "'(abba)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("mbba");

        assertThat(assertions.expression("regexp_replace(a, b, x -> 'm' || x[1])")
                .binding("a", "'abbabba'")
                .binding("b", "'(abba)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("mabbabba");

        assertThat(assertions.expression("regexp_replace(a, b, x -> CASE WHEN x[1] IS NULL THEN 'foo' ELSE 'bar' END)")
                .binding("a", "'abcde'")
                .binding("b", "'ab(c)?de'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("bar");

        assertThat(assertions.expression("regexp_replace(a, b, x -> 'm')")
                .binding("a", "'abc'")
                .binding("b", "'(.)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("mmm");

        // Matches that contains empty capturing groups
        // Empty block passed to lambda
        assertThat(assertions.expression("regexp_replace(a, b, x -> 'm')")
                .binding("a", "'abc'")
                .binding("b", "'.'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("mmm");

        // Empty block passed to lambda
        assertThat(assertions.expression("regexp_replace(a, b, x -> 'm')")
                .binding("a", "'abbabba'")
                .binding("b", "'abba'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("mbba");

        // Empty block passed to lambda
        assertThat(assertions.expression("regexp_replace(a, b, x -> 'm')")
                .binding("a", "'abc abc'")
                .binding("b", "'abc'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("m m");

        // Empty block passed to lambda
        assertThat(assertions.expression("regexp_replace(a, b, x -> 'OK')")
                .binding("a", "'abc'")
                .binding("b", "''"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("OKaOKbOKcOK");

        // Passed a block containing multiple empty capturing groups to lambda
        assertThat(assertions.expression("regexp_replace(a, b, x -> x[1])")
                .binding("a", "'abc'")
                .binding("b", "'()'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("abc");

        // Passed a block containing multiple empty capturing groups to lambda
        assertThat(assertions.expression("regexp_replace(a, b, x -> 'OK')")
                .binding("a", "'abc'")
                .binding("b", "'()'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("OKaOKbOKcOK");

        // Two matches: ["new"] and [""]
        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[1]))")
                .binding("a", "'new'")
                .binding("b", "'(\\w*)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("NEW");

        // Two matches: ["new"] and [""]
        assertThat(assertions.expression("regexp_replace(a, b, x -> x[1] || upper(x[1]))")
                .binding("a", "'new'")
                .binding("b", "'(\\w*)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("newNEW");

        // Two matches: ["new"] and [""]
        assertThat(assertions.expression("regexp_replace(a, b, x -> CAST(length(x[1]) AS VARCHAR))")
                .binding("a", "'new'")
                .binding("b", "'(\\w*)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("30");

        // Four matches: ["new"], [""], ["york"], [""]
        assertThat(assertions.expression("regexp_replace(a, b, x -> '<' || x[1] || '>')")
                .binding("a", "'new york'")
                .binding("b", "'(\\w*)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("<new><> <york><>");

        // Matches that contains null capturing groups
        assertThat(assertions.expression("regexp_replace(a, b, x -> x[1] )")
                .binding("a", "'aaa'")
                .binding("b", "'(b)?'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo((Object) null);

        // Matched once with two matching groups[(0,3), null].
        assertThat(assertions.expression("regexp_replace(a, b, x -> x[1])")
                .binding("a", "'abde'")
                .binding("b", "'ab(c)?de'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo((Object) null);

        // Matched once with two matching groups[(0,3), null]. Passed null to lambda and returns OK, so whole string replace with OK
        assertThat(assertions.expression("regexp_replace(a, b, x -> 'OK')")
                .binding("a", "'abde'")
                .binding("b", "'ab(c)?de'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("OK");

        // Passed null to lambda and returns null.
        assertThat(assertions.expression("regexp_replace(a, b, x -> x[1] || 'OK')")
                .binding("a", "'abde'")
                .binding("b", "'ab(c)?de'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo((Object) null);

        // Passed null to lambda and returns null.
        assertThat(assertions.expression("regexp_replace(a, b, x -> 'OK' || x[1])")
                .binding("a", "'abde'")
                .binding("b", "'ab(c)?de'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo((Object) null);

        assertThat(assertions.expression("regexp_replace(a, b, x -> CASE WHEN x[1] IS NULL THEN 'foo' ELSE 'bar' END)")
                .binding("a", "'abde'")
                .binding("b", "'ab(c)?de'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("foo");

        assertThat(assertions.expression("regexp_replace(a, b, x -> CASE WHEN (x[1] IS NOT NULL) AND (x[2] IS NOT NULL) THEN 'foo' ELSE NULL END)")
                .binding("a", "'ab'")
                .binding("b", "'(a)?(b)?'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo((Object) null);

        // Matches that contains non-empty and not null capturing groups but lambda returns null
        assertThat(assertions.expression("regexp_replace(a, b, x -> CAST(NULL AS VARCHAR))")
                .binding("a", "'aaa'")
                .binding("b", "'(a)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo((Object) null);

        assertThat(assertions.expression("regexp_replace(a, b, x -> CASE WHEN (x[1] IS NOT NULL) AND (x[2] IS NULL) OR (x[1] IS NULL) AND (x[2] IS NOT NULL) THEN 'foo' ELSE NULL END)")
                .binding("a", "'ab'")
                .binding("b", "'(a)?(b)?'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo((Object) null);

        assertThat(assertions.expression("regexp_replace(a, b, x -> CASE WHEN (x[1] IS NOT NULL) AND (x[2] IS NULL) OR (x[1] IS NULL) AND (x[2] IS NOT NULL) THEN 'foo' ELSE NULL END)")
                .binding("a", "'abacdb'")
                .binding("b", "'(a)?(b)?'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo((Object) null);

        // No matches
        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[1]))")
                .binding("a", "'new york'")
                .binding("b", "'(a)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("new york");

        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[1]))")
                .binding("a", "''")
                .binding("b", "'(a)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("");

        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[1]))")
                .binding("a", "null")
                .binding("b", "'(a)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo((Object) null);

        assertThat(assertions.expression("regexp_replace(a, b, x -> upper(x[1]))")
                .binding("a", "'new'")
                .binding("b", "null"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo((Object) null);

        assertThat(assertions.expression("regexp_replace(a, b, x -> x[1])")
                .binding("a", "'abde'")
                .binding("b", "'(c)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("abde");

        assertThat(assertions.expression("regexp_replace(a, b, x -> 'm')")
                .binding("a", "'abde'")
                .binding("b", "'(c)'"))
                .hasType(createUnboundedVarcharType())
                .isEqualTo("abde");

        // Invalid array indexes
        assertTrinoExceptionThrownBy(() -> assertions.expression("regexp_replace(a, b, x -> upper(x[2]))")
                .binding("a", "'new'")
                .binding("b", "'(\\w)'").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("regexp_replace(a, b, x -> upper(x[0]))")
                .binding("a", "'new'")
                .binding("b", "'(\\w)'").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("regexp_replace(a, b, x -> x[1])")
                .binding("a", "'abc'")
                .binding("b", "''").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        // Empty block passed to lambda but referencing an element out of bound
        assertTrinoExceptionThrownBy(() -> assertions.expression("regexp_replace(a, b, x -> upper(x[1]))")
                .binding("a", "'x'")
                .binding("b", "'x'").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        // Empty block passed to lambda but referencing an element out of bound
        assertTrinoExceptionThrownBy(() -> assertions.expression("regexp_replace(a, b, x -> 'm' || x[1])")
                .binding("a", "'abbabba'")
                .binding("b", "'abba'").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testRegexpExtract()
    {
        assertThat(assertions.function("regexp_extract", "'Hello world bye'", "'\\b[a-z]([a-z]*)'"))
                .hasType(createVarcharType(15))
                .isEqualTo("world");

        assertThat(assertions.function("regexp_extract", "'Hello world bye'", "'\\b[a-z]([a-z]*)'", "1"))
                .hasType(createVarcharType(15))
                .isEqualTo("orld");

        assertThat(assertions.function("regexp_extract", "'rat cat\nbat dog'", "'ra(.)|blah(.)(.)'", "2"))
                .hasType(createVarcharType(15))
                .isEqualTo((Object) null);

        assertThat(assertions.function("regexp_extract", "'12345'", "'x'"))
                .hasType(createVarcharType(5))
                .isEqualTo((Object) null);

        assertThat(assertions.function("regexp_extract", "'Baby X'", "'by ([A-Z].*)\\b[a-z]'"))
                .hasType(createVarcharType(6))
                .isEqualTo((Object) null);

        assertTrinoExceptionThrownBy(assertions.function("regexp_extract", "'Hello world bye'", "'\\b[a-z]([a-z]*)'", "-1")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("regexp_extract", "'Hello world bye'", "'\\b[a-z]([a-z]*)'", "2")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testRegexpExtractAll()
    {
        assertThat(assertions.function("regexp_extract_all", "'abc有朋$%X自9远方来💰'", "''"))
                .hasType(new ArrayType(createVarcharType(14)))
                .isEqualTo(ImmutableList.of("", "", "", "", "", "", "", "", "", "", "", "", "", "", ""));

        assertThat(assertions.function("regexp_extract_all", "'a有朋💰'", "'.'"))
                .hasType(new ArrayType(createVarcharType(4)))
                .isEqualTo(ImmutableList.of("a", "有", "朋", "💰"));

        assertThat(assertions.function("regexp_extract_all", "''", "''"))
                .hasType(new ArrayType(createVarcharType(0)))
                .isEqualTo(ImmutableList.of(""));

        assertThat(assertions.function("regexp_extract_all", "'rat cat\nbat dog'", "'.at'"))
                .hasType(new ArrayType(createVarcharType(15)))
                .isEqualTo(ImmutableList.of("rat", "cat", "bat"));

        assertThat(assertions.function("regexp_extract_all", "'rat cat\nbat dog'", "'(.)at'", "1"))
                .hasType(new ArrayType(createVarcharType(15)))
                .isEqualTo(ImmutableList.of("r", "c", "b"));

        assertThat(assertions.function("regexp_extract_all", "'rat cat\nbat dog'", "'ra(.)|blah(.)(.)'", "2"))
                .hasType(new ArrayType(createVarcharType(15)))
                .isEqualTo(Collections.<String>singletonList(null));

        assertTrinoExceptionThrownBy(assertions.function("regexp_extract_all", "'hello'", "'(.)'", "2")::evaluate)
                .hasMessage("Pattern has 1 groups. Cannot access group 2");

        assertThat(assertions.function("regexp_extract_all", "'12345'", "''"))
                .hasType(new ArrayType(createVarcharType(5)))
                .isEqualTo(ImmutableList.of("", "", "", "", "", ""));

        assertTrinoExceptionThrownBy(assertions.function("regexp_extract_all", "'12345'", "'('")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testRegexpSplit()
    {
        assertThat(assertions.function("regexp_split", "'abc有朋$%X自9远方来💰'", "''"))
                .hasType(new ArrayType(createVarcharType(14)))
                .isEqualTo(ImmutableList.of("", "a", "b", "c", "有", "朋", "$", "%", "X", "自", "9", "远", "方", "来", "💰", ""));

        assertThat(assertions.function("regexp_split", "'a有朋💰'", "'.'"))
                .hasType(new ArrayType(createVarcharType(4)))
                .isEqualTo(ImmutableList.of("", "", "", "", ""));

        assertThat(assertions.function("regexp_split", "''", "''"))
                .hasType(new ArrayType(createVarcharType(0)))
                .isEqualTo(ImmutableList.of("", ""));

        assertThat(assertions.function("regexp_split", "'abc'", "'a'"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("", "bc"));

        assertThat(assertions.function("regexp_split", "'a.b:c;d'", "'[\\.:;]'"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("a", "b", "c", "d"));

        assertThat(assertions.function("regexp_split", "'a.b:c;d'", "'\\.'"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("a", "b:c;d"));

        assertThat(assertions.function("regexp_split", "'a.b:c;d'", "':'"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("a.b", "c;d"));

        assertThat(assertions.function("regexp_split", "'a,b,c'", "','"))
                .hasType(new ArrayType(createVarcharType(5)))
                .isEqualTo(ImmutableList.of("a", "b", "c"));

        assertThat(assertions.function("regexp_split", "'a1b2c3d'", "'\\d'"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("a", "b", "c", "d"));

        assertThat(assertions.function("regexp_split", "'a1b2346c3d'", "'\\d+'"))
                .hasType(new ArrayType(createVarcharType(10)))
                .isEqualTo(ImmutableList.of("a", "b", "c", "d"));

        assertThat(assertions.function("regexp_split", "'abcd'", "'x'"))
                .hasType(new ArrayType(createVarcharType(4)))
                .isEqualTo(ImmutableList.of("abcd"));

        assertThat(assertions.function("regexp_split", "'abcd'", "''"))
                .hasType(new ArrayType(createVarcharType(4)))
                .isEqualTo(ImmutableList.of("", "a", "b", "c", "d", ""));

        assertThat(assertions.function("regexp_split", "''", "'x'"))
                .hasType(new ArrayType(createVarcharType(0)))
                .isEqualTo(ImmutableList.of(""));

        // test empty splits, leading & trailing empty splits, consecutive empty splits
        assertThat(assertions.function("regexp_split", "'a,b,c,d'", "','"))
                .hasType(new ArrayType(createVarcharType(7)))
                .isEqualTo(ImmutableList.of("a", "b", "c", "d"));

        assertThat(assertions.function("regexp_split", "',,a,,,b,c,d,,'", "','"))
                .hasType(new ArrayType(createVarcharType(13)))
                .isEqualTo(ImmutableList.of("", "", "a", "", "", "b", "c", "d", "", ""));

        assertThat(assertions.function("regexp_split", "',,,'", "','"))
                .hasType(new ArrayType(createVarcharType(3)))
                .isEqualTo(ImmutableList.of("", "", "", ""));
    }

    @Test
    public void testRegexpCount()
    {
        assertThat(assertions.function("regexp_count", "'a.b:c;d'", "'[\\.:;]'"))
                .isEqualTo(3L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('a.b:c;d', '[\\.:;]')"))
                .isEqualTo(3L);

        assertThat(assertions.function("regexp_count", "'a.b:c;d'", "'\\.'"))
                .isEqualTo(1L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('a.b:c;d', '\\.')"))
                .isEqualTo(1L);

        assertThat(assertions.function("regexp_count", "'a.b:c;d'", "':'"))
                .isEqualTo(1L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('a.b:c;d', ':')"))
                .isEqualTo(1L);

        assertThat(assertions.function("regexp_count", "'a,b,c'", "','"))
                .isEqualTo(2L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('a,b,c', ',')"))
                .isEqualTo(2L);

        assertThat(assertions.function("regexp_count", "'a1b2c3d'", "'\\d'"))
                .isEqualTo(3L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('a1b2c3d', '\\d')"))
                .isEqualTo(3L);

        assertThat(assertions.function("regexp_count", "'a1b2346c3d'", "'\\d+'"))
                .isEqualTo(3L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('a1b2346c3d', '\\d+')"))
                .isEqualTo(3L);

        assertThat(assertions.function("regexp_count", "'abcd'", "'x'"))
                .isEqualTo(0L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('abcd', 'x')"))
                .isEqualTo(0L);

        assertThat(assertions.function("regexp_count", "'Hello world bye'", "'\\b[a-z]([a-z]*)'"))
                .isEqualTo(2L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('Hello world bye', '\\b[a-z]([a-z]*)')"))
                .isEqualTo(2L);

        assertThat(assertions.function("regexp_count", "'rat cat\nbat dog'", "'ra(.)|blah(.)(.)'"))
                .isEqualTo(1L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('rat cat\nbat dog', 'ra(.)|blah(.)(.)')"))
                .isEqualTo(1L);

        assertThat(assertions.function("regexp_count", "'Baby X'", "'by ([A-Z].*)\\b[a-z]'"))
                .isEqualTo(0L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('Baby X', 'by ([A-Z].*)\\b[a-z]')"))
                .isEqualTo(0L);

        assertThat(assertions.function("regexp_count", "'rat cat bat dog'", "'.at'"))
                .isEqualTo(3L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('rat cat bat dog', '.at')"))
                .isEqualTo(3L);

        assertThat(assertions.function("regexp_count", "''", "'x'"))
                .isEqualTo(0L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('', 'x')"))
                .isEqualTo(0L);

        assertThat(assertions.function("regexp_count", "''", "''"))
                .isEqualTo(1L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('', '')"))
                .isEqualTo(1L);

        // Non-unicode string
        assertThat(assertions.function("regexp_count", "'君子矜而不争，党而不群'", "'不'"))
                .isEqualTo(2L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('君子矜而不争，党而不群', '不')"))
                .isEqualTo(2L);

        // empty pattern
        assertThat(assertions.function("regexp_count", "'abcd'", "''"))
                .isEqualTo(5L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('abcd', '')"))
                .isEqualTo(5L);

        // sql in document
        assertThat(assertions.function("regexp_count", "'1a 2b 14m'", "'\\s*[a-z]+\\s*'"))
                .isEqualTo(3L);

        assertThat(assertions.function("cardinality", "regexp_extract_all('1a 2b 14m', '\\s*[a-z]+\\s*')"))
                .isEqualTo(3L);
    }

    @Test
    public void testRegexpPosition()
    {
        assertThat(assertions.function("regexp_position", "'a.b:c;d'", "'[\\.:;]'"))
                .isEqualTo(2);

        assertThat(assertions.function("regexp_position", "'a.b:c;d'", "'\\.'"))
                .isEqualTo(2);

        assertThat(assertions.function("regexp_position", "'a.b:c;d'", "':'"))
                .isEqualTo(4);

        assertThat(assertions.function("regexp_position", "'a,b,c'", "','"))
                .isEqualTo(2);

        assertThat(assertions.function("regexp_position", "'a1b2c3d'", "'\\d'"))
                .isEqualTo(2);

        // match from specified position
        assertThat(assertions.function("regexp_position", "'a,b,c'", "','", "3"))
                .isEqualTo(4);

        assertThat(assertions.function("regexp_position", "'a1b2c3d'", "'\\d'", "5"))
                .isEqualTo(6);

        // match the n-th occurrence from from specified position
        assertThat(assertions.function("regexp_position", "'a1b2c3d4e'", "'\\d'", "4", "2"))
                .isEqualTo(6);

        assertThat(assertions.function("regexp_position", "'a1b2c3d'", "'\\d'", "4", "3"))
                .isEqualTo(-1);

        // empty pattern
        assertThat(assertions.function("regexp_position", "'a1b2c3d'", "''"))
                .isEqualTo(1);

        assertThat(assertions.function("regexp_position", "'a1b2c3d'", "''", "2"))
                .isEqualTo(2);

        assertThat(assertions.function("regexp_position", "'a1b2c3d'", "''", "2", "2"))
                .isEqualTo(3);

        assertThat(assertions.function("regexp_position", "'a1b2c3d'", "''", "2", "6"))
                .isEqualTo(7);

        assertThat(assertions.function("regexp_position", "'a1b2c3d'", "''", "2", "7"))
                .isEqualTo(8);

        assertThat(assertions.function("regexp_position", "'a1b2c3d'", "''", "2", "8"))
                .isEqualTo(-1);

        // Non-unicode string
        assertThat(assertions.function("regexp_position", "'行成于思str而毁123于随'", "'于'", "3", "2"))
                .isEqualTo(13);

        assertThat(assertions.function("regexp_position", "'行成于思str而毁123于随'", "''", "3", "2"))
                .isEqualTo(4);

        assertThat(assertions.function("regexp_position", "'行成于思str而毁123于随'", "''", "3", "1"))
                .isEqualTo(3);

        // empty source
        assertThat(assertions.function("regexp_position", "''", "', '"))
                .isEqualTo(-1);

        assertThat(assertions.function("regexp_position", "''", "', '", "4"))
                .isEqualTo(-1);

        assertThat(assertions.function("regexp_position", "''", "', '", "4", "2"))
                .isEqualTo(-1);

        // boundary test
        assertThat(assertions.function("regexp_position", "'a,b,c'", "','", "2"))
                .isEqualTo(2);

        assertThat(assertions.function("regexp_position", "'a1b2c3d'", "'\\d'", "4"))
                .isEqualTo(4);

        assertThat(assertions.function("regexp_position", "'有朋$%X自9远方来'", "'\\d'", "7"))
                .isEqualTo(7);

        assertThat(assertions.function("regexp_position", "'有朋$%X自9远方9来'", "'\\d'", "10", "1"))
                .isEqualTo(10);

        assertThat(assertions.function("regexp_position", "'有朋$%X自9远方9来'", "'\\d'", "10", "2"))
                .isEqualTo(-1);

        assertThat(assertions.function("regexp_position", "'a,b,c'", "', '", "1000"))
                .isEqualTo(-1);

        assertThat(assertions.function("regexp_position", "'a,b,c'", "', '", "8"))
                .isEqualTo(-1);

        assertThat(assertions.function("regexp_position", "'有朋$%X自9远方9来'", "'来'", "999"))
                .isEqualTo(-1);

        assertTrinoExceptionThrownBy(assertions.function("regexp_position", "'有朋$%X自9远方9来'", "'来'", "-1", "0")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("regexp_position", "'有朋$%X自9远方9来'", "'来'", "1", "0")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("regexp_position", "'有朋$%X自9远方9来'", "'来'", "1", "-1")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        // sql in document
        assertThat(assertions.function("regexp_position", "'9102, say good bye'", "'\\s*[a-z]+\\s*'"))
                .isEqualTo(6);

        assertThat(assertions.function("regexp_position", "'natasha, 9102, miss you'", "'\\s*[a-z]+\\s*'", "10"))
                .isEqualTo(15);

        assertThat(assertions.function("regexp_position", "'natasha, 9102, miss you'", "'\\s'", "10", "2"))
                .isEqualTo(20);
    }
}
