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

import io.trino.sql.query.QueryAssertions;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestMultiMatchAnyFunction
{
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
    public void testNullAndEmpty()
    {
        assertThat(assertions.function("multi_match_any", "NULL", "ARRAY['x']")).isNull(BOOLEAN);
        assertThat(assertions.function("multi_match_any", "'x'", "NULL")).isNull(BOOLEAN);
        assertThat(assertions.function("multi_match_any", "NULL", "ARRAY[NULL]")).isNull(BOOLEAN);
        assertThat(assertions.function("multi_match_any", "NULL", "ARRAY[]")).isNull(BOOLEAN);
        assertThat(assertions.function("multi_match_any", "'x'", "ARRAY[]")).isEqualTo(false);
        assertThat(assertions.function("multi_match_any", "''", "ARRAY[]")).isEqualTo(false);
        assertThat(assertions.function("multi_match_any", "''", "ARRAY['']")).isEqualTo(true);
        assertInvalid("'x'", "ARRAY[NULL]", "pattern at array index 1 is null");
        assertInvalid("'x'", "ARRAY['x', NULL]", "pattern at array index 2 is null");
    }

    @Test
    public void testSubstringAndAnchors()
    {
        assertThat(assertions.function("multi_match_any", "'WARN: request timed out'", "ARRAY['ERROR:', 'timed out']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'INFO: request complete'", "ARRAY['ERROR:', 'timed out']")).isEqualTo(false);
        assertThat(assertions.function("multi_match_any", "'beta suffix'", "ARRAY['^(alpha|beta)$', '^gamma$']")).isEqualTo(false);
        assertThat(assertions.function("multi_match_any", "'beta'", "ARRAY['^(alpha|beta)$', '^gamma$']")).isEqualTo(true);
    }

    @Test
    public void testFlagsAreLocalToEachPattern()
    {
        assertThat(assertions.function("multi_match_any", "'BETA'", "ARRAY['(?i)^alpha$', '^beta$']")).isEqualTo(false);
        assertThat(assertions.function("multi_match_any", "'ALPHA'", "ARRAY['(?i)^alpha$', '^beta$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'a' || chr(10) || 'b'", "ARRAY['(?-s)^a.b$', '^absent$']")).isEqualTo(false);
        assertThat(assertions.function("multi_match_any", "'a' || chr(10) || 'b'", "ARRAY['(?-s)^absent$', '^a.b$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'a' || chr(10) || 'b'", "ARRAY['(?m)^absent$', '^b$']")).isEqualTo(false);
        assertThat(assertions.function("multi_match_any", "'a' || chr(10) || 'b'", "ARRAY['^absent$', '(?m)^b$']")).isEqualTo(true);
    }

    @Test
    public void testQuotedPatterns()
    {
        assertThat(assertions.function("multi_match_any", "'a|b'", "ARRAY['\\Qa|b', '^other$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'other'", "ARRAY['\\Qa|b', '^other$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'a'", "ARRAY['\\Qa|b', '^other$']")).isEqualTo(false);
        assertThat(assertions.function("multi_match_any", "'a\\'", "ARRAY['\\Qa\\', '^other$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'\\Q'", "ARRAY['^\\\\Q$', '^other$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'abab'", "ARRAY['^\\Qab\\E+$', '^other$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'abb'", "ARRAY['^\\Qab\\E+$', '^other$']")).isEqualTo(false);
        assertThat(assertions.function("multi_match_any", "'ab|c'", "ARRAY['^\\Qa\\E\\Qb|c', '^other$']")).isEqualTo(true);
    }

    @Test
    public void testNamedCapturesAreIndependent()
    {
        assertThat(assertions.function("multi_match_any", "'alpha'", "ARRAY['^(?P<word>alpha)$', '^(?P<word>beta)$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'beta'", "ARRAY['^(?P<word>alpha)$', '^(?P<word>beta)$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'gamma'", "ARRAY['^(?P<word>alpha)$', '^(?P<word>beta)$']")).isEqualTo(false);
        assertThat(assertions.function("multi_match_any", "'A]x'", "ARRAY['^[A-[:alpha:]](?P<word>x)$', '^(?P<word>y)$']")).isEqualTo(true);
    }

    @Test
    public void testUtf8AndNul()
    {
        assertThat(assertions.function("multi_match_any", "'😀'", "ARRAY['^.$', '^absent$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'é'", "ARRAY['^\\w+$', '^absent$']")).isEqualTo(false);
        assertThat(assertions.function("multi_match_any", "'éaé'", "ARRAY['\\ba\\b', '^absent$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'a' || chr(0) || 'b'", "ARRAY['^a' || chr(0) || 'b$', '^absent$']")).isEqualTo(true);
        assertThat(assertions.function("multi_match_any", "'ab'", "ARRAY[chr(0)]")).isEqualTo(false);
    }

    @Test
    public void testAllPatternsMustBeValid()
    {
        assertInvalid("'match'", "ARRAY['match', '[']", "invalid regular expression at array index 2");
        assertInvalid("''", "ARRAY['', '[']", "invalid regular expression at array index 2");
        assertInvalid("'aa'", "ARRAY['a', '(a)\\1']", "invalid regular expression at array index 2");
        assertInvalid("'ab'", "ARRAY['(?<=a)b']", "invalid regular expression at array index 1");
        assertInvalid("'match'", "ARRAY['match', ')|match|(']", "invalid regular expression at array index 2");
    }

    @Test
    public void testDynamicArrays()
    {
        assertThat(assertions.query(
                """
                SELECT x, multi_match_any(
                    'item-' || CAST(x AS varchar),
                    CASE x % 3
                        WHEN 1 THEN ARRAY['^item-' || CAST(x AS varchar) || '$', 'absent']
                        WHEN 2 THEN ARRAY['^item-4$', 'missing']
                        ELSE ARRAY[]
                    END)
                FROM UNNEST(sequence(1, 6)) AS t(x)
                """))
                .matches("VALUES (BIGINT '1', true), (2, false), (3, false), (4, true), (5, false), (6, false)");
    }

    @Test
    public void testIndependentOfRegexLibrary()
    {
        try (QueryAssertions re2j = new QueryAssertions(new StandaloneQueryRunner(TEST_SESSION, builder -> builder.addProperty("deprecated.regex-library", "RE2J")))) {
            assertThat(re2j.function("multi_match_any", "'a' || chr(10) || 'b'", "ARRAY['a.b']")).isEqualTo(true);
            assertThat(re2j.function("regexp_like", "'a' || chr(10) || 'b'", "'a.b'")).isEqualTo(false);
        }
        assertThat(assertions.function("multi_match_any", "'a' || chr(10) || 'b'", "ARRAY['a.b']")).isEqualTo(true);
        assertThat(assertions.function("regexp_like", "'aa'", "'(a)\\1'")).isEqualTo(true);
    }

    private void assertInvalid(String source, String patterns, String message)
    {
        assertTrinoExceptionThrownBy(assertions.function("multi_match_any", source, patterns)::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining(message);
    }
}
