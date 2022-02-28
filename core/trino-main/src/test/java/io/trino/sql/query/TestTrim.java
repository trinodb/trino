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
package io.trino.sql.query;

import io.trino.metadata.InternalFunctionBundle;
import io.trino.operator.scalar.TestStringFunctions;
import io.trino.spi.TrinoException;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTrim
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestStringFunctions.class) // To use utf8 function
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testLeftTrim()
    {
        assertFunction("TRIM(LEADING FROM '')", "CAST('' AS VARCHAR(0))");
        assertFunction("TRIM(LEADING FROM '   ')", "CAST('' AS VARCHAR(3))");
        assertFunction("TRIM(LEADING FROM '  hello  ')", "CAST('hello  ' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING FROM '  hello')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(LEADING FROM 'hello  ')", "CAST('hello  ' AS VARCHAR(7))");
        assertFunction("TRIM(LEADING FROM ' hello world ')", "CAST('hello world ' AS VARCHAR(13))");

        assertFunction("TRIM(LEADING FROM '\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING FROM ' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B ' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING FROM '  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING FROM ' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
    }

    @Test
    public void testCharLeftTrim()
    {
        assertFunction("TRIM(LEADING FROM CAST('' AS CHAR(20)))", "CAST('' AS VARCHAR(20))");
        assertFunction("TRIM(LEADING FROM CAST('' AS CHAR(20)))", "CAST('' AS VARCHAR(20))");
        assertFunction("TRIM(LEADING FROM CAST('  hello  ' AS CHAR(9)))", "CAST('hello' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING FROM CAST('  hello' AS CHAR(7)))", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(LEADING FROM CAST('hello  ' AS CHAR(7)))", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(LEADING FROM CAST(' hello world ' AS CHAR(13)))", "CAST('hello world' AS VARCHAR(13))");

        assertFunction("TRIM(LEADING FROM CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ' AS CHAR(9)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING FROM CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ' AS CHAR(9)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING FROM CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(9)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING FROM CAST(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(10)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
    }

    @Test
    public void testRightTrim()
    {
        assertFunction("TRIM(TRAILING FROM '')", "CAST('' AS VARCHAR(0))");
        assertFunction("TRIM(TRAILING FROM '   ')", "CAST('' AS VARCHAR(3))");
        assertFunction("TRIM(TRAILING FROM '  hello  ')", "CAST('  hello' AS VARCHAR(9))");
        assertFunction("TRIM(TRAILING FROM '  hello')", "CAST('  hello' AS VARCHAR(7))");
        assertFunction("TRIM(TRAILING FROM 'hello  ')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(TRAILING FROM ' hello world ')", "CAST(' hello world' AS VARCHAR(13))");

        assertFunction("TRIM(TRAILING FROM '\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
        assertFunction("TRIM(TRAILING FROM '\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(TRAILING FROM ' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", "CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(TRAILING FROM '  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", "CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
    }

    @Test
    public void testCharRightTrim()
    {
        assertFunction("TRIM(TRAILING FROM CAST('' AS CHAR(20)))", "CAST('' AS VARCHAR(20))");
        assertFunction("TRIM(TRAILING FROM CAST('  hello  ' AS CHAR(9)))", "CAST('  hello' AS VARCHAR(9))");
        assertFunction("TRIM(TRAILING FROM CAST('  hello' AS CHAR(7)))", "CAST('  hello' AS VARCHAR(7))");
        assertFunction("TRIM(TRAILING FROM CAST('hello  ' AS CHAR(7)))", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(TRAILING FROM CAST(' hello world ' AS CHAR(13)))", "CAST(' hello world' AS VARCHAR(13))");

        assertFunction("TRIM(TRAILING FROM CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ' AS CHAR(10)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
        assertFunction("TRIM(TRAILING FROM CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ' AS CHAR(9)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(TRAILING FROM CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ' AS CHAR(9)))", "CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(TRAILING FROM CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(9)))", "CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
    }

    @Test
    public void testLeftTrimParametrized()
    {
        assertFunction("TRIM(LEADING '' FROM '')", "CAST('' AS VARCHAR(0))");
        assertFunction("TRIM(LEADING '' FROM '   ')", "CAST('   ' AS VARCHAR(3))");
        assertFunction("TRIM(LEADING '' FROM '  hello  ')", "CAST('  hello  ' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING ' ' FROM '  hello  ')", "CAST('hello  ' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING CHAR ' ' FROM '  hello  ')", "CAST('hello  ' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING 'he ' FROM '  hello  ')", "CAST('llo  ' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING ' ' FROM '  hello')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(LEADING 'e h' FROM '  hello')", "CAST('llo' AS VARCHAR(7))");
        assertFunction("TRIM(LEADING 'l' FROM 'hello  ')", "CAST('hello  ' AS VARCHAR(7))");
        assertFunction("TRIM(LEADING ' ' FROM ' hello world ')", "CAST('hello world ' AS VARCHAR(13))");
        assertFunction("TRIM(LEADING ' eh' FROM ' hello world ')", "CAST('llo world ' AS VARCHAR(13))");
        assertFunction("TRIM(LEADING ' ehlowrd' FROM ' hello world ')", "CAST('' AS VARCHAR(13))");
        assertFunction("TRIM(LEADING ' x' FROM ' hello world ')", "CAST('hello world ' AS VARCHAR(13))");

        // non latin characters
        assertFunction("TRIM(LEADING '\u00f3\u017a' FROM '\u017a\u00f3\u0142\u0107')", "CAST('\u0142\u0107' AS VARCHAR(4))");

        // invalid utf-8 characters
        assertInvalidFunction("TRIM(LEADING utf8(from_hex('3281')) FROM 'hello wolrd')", "Invalid UTF-8 encoding in characters: 2�");
    }

    @Test
    public void testCharLeftTrimParametrized()
    {
        assertFunction("TRIM(LEADING '' FROM CAST('' AS CHAR(1)))", "CAST('' AS VARCHAR(1))");
        assertFunction("TRIM(LEADING '' FROM CAST('   ' AS CHAR(3)))", "CAST('' AS VARCHAR(3))");
        assertFunction("TRIM(LEADING '' FROM CAST('  hello  ' AS CHAR(9)))", "CAST('  hello' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING ' ' FROM CAST('  hello  ' AS CHAR(9)))", "CAST('hello' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING 'he ' FROM CAST('  hello  ' AS CHAR(9)))", "CAST('llo' AS VARCHAR(9))");
        assertFunction("TRIM(LEADING ' ' FROM CAST('  hello' AS CHAR(7)))", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(LEADING 'e h' FROM CAST('  hello' AS CHAR(7)))", "CAST('llo' AS VARCHAR(7))");
        assertFunction("TRIM(LEADING 'l' FROM CAST('hello  ' AS CHAR(7)))", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(LEADING ' ' FROM CAST(' hello world ' AS CHAR(13)))", "CAST('hello world' AS VARCHAR(13))");
        assertFunction("TRIM(LEADING ' eh' FROM CAST(' hello world ' AS CHAR(13)))", "CAST('llo world' AS VARCHAR(13))");
        assertFunction("TRIM(LEADING ' ehlowrd' FROM CAST(' hello world ' AS CHAR(13)))", "CAST('' AS VARCHAR(13))");
        assertFunction("TRIM(LEADING ' x' FROM CAST(' hello world ' AS CHAR(13)))", "CAST('hello world' AS VARCHAR(13))");

        // non latin characters
        assertFunction("TRIM(LEADING '\u00f3\u017a' FROM CAST('\u017a\u00f3\u0142\u0107' AS CHAR(4)))", "CAST('\u0142\u0107' AS VARCHAR(4))");
    }

    @Test
    public void testRightTrimParametrized()
    {
        assertFunction("TRIM(TRAILING '' FROM '')", "CAST('' AS VARCHAR(0))");
        assertFunction("TRIM(TRAILING '' FROM '   ')", "CAST('   ' AS VARCHAR(3))");
        assertFunction("TRIM(TRAILING '' FROM '  hello  ')", "CAST('  hello  ' AS VARCHAR(9))");
        assertFunction("TRIM(TRAILING ' ' FROM '  hello  ')", "CAST('  hello' AS VARCHAR(9))");
        assertFunction("TRIM(TRAILING 'lo ' FROM '  hello  ')", "CAST('  he' AS VARCHAR(9))");
        assertFunction("TRIM(TRAILING ' ' FROM 'hello  ')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(TRAILING 'l o' FROM 'hello  ')", "CAST('he' AS VARCHAR(7))");
        assertFunction("TRIM(TRAILING 'l' FROM 'hello  ')", "CAST('hello  ' AS VARCHAR(7))");
        assertFunction("TRIM(TRAILING ' ' FROM ' hello world ')", "CAST(' hello world' AS VARCHAR(13))");
        assertFunction("TRIM(TRAILING ' ld' FROM ' hello world ')", "CAST(' hello wor' AS VARCHAR(13))");
        assertFunction("TRIM(TRAILING ' ehlowrd' FROM ' hello world ')", "CAST('' AS VARCHAR(13))");
        assertFunction("TRIM(TRAILING ' x' FROM ' hello world ')", "CAST(' hello world' AS VARCHAR(13))");
        assertFunction("TRIM(TRAILING 'def' FROM CAST('abc def' AS CHAR(7)))", "CAST('abc' AS VARCHAR(7))");

        // non latin characters
        assertFunction("TRIM(TRAILING '\u0107\u0142' FROM '\u017a\u00f3\u0142\u0107')", "CAST('\u017a\u00f3' AS VARCHAR(4))");

        // invalid utf-8 characters
        assertInvalidFunction("TRIM(TRAILING utf8(from_hex('81')) FROM 'hello world')", "Invalid UTF-8 encoding in characters: �");
        assertInvalidFunction("TRIM(TRAILING utf8(from_hex('3281')) FROM 'hello world')", "Invalid UTF-8 encoding in characters: 2�");
    }

    @Test
    public void testTrim()
    {
        assertFunction("TRIM('')", "CAST('' AS VARCHAR(0))");
        assertFunction("TRIM('   ')", "CAST('' AS VARCHAR(3))");
        assertFunction("TRIM('  hello  ')", "CAST('hello' AS VARCHAR(9))");
        assertFunction("TRIM('  hello')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM('hello  ')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(' hello world ')", "CAST('hello world' AS VARCHAR(13))");
        assertFunction("TRIM(BOTH FROM '')", "CAST('' AS VARCHAR(0))");
        assertFunction("TRIM(BOTH FROM '   ')", "CAST('' AS VARCHAR(3))");
        assertFunction("TRIM(BOTH FROM '  hello  ')", "CAST('hello' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH FROM '  hello')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH FROM 'hello  ')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH FROM ' hello world ')", "CAST('hello world' AS VARCHAR(13))");
        assertFunction("TRIM(' ' FROM '')", "CAST('' AS VARCHAR(0))");
        assertFunction("TRIM(' ' FROM '   ')", "CAST('' AS VARCHAR(3))");
        assertFunction("TRIM(' ' FROM '  hello  ')", "CAST('hello' AS VARCHAR(9))");
        assertFunction("TRIM(' ' FROM '  hello')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(' ' FROM 'hello  ')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(' ' FROM ' hello world ')", "CAST('hello world' AS VARCHAR(13))");

        assertFunction("TRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
        assertFunction("TRIM('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
        assertFunction("TRIM(BOTH FROM '\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
        assertFunction("TRIM(BOTH FROM '\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH FROM ' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH FROM '  \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH FROM ' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B')", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
    }

    @Test
    public void testCharTrim()
    {
        assertFunction("TRIM(CAST('' AS CHAR(20)))", "CAST('' AS VARCHAR(20))");
        assertFunction("TRIM(CAST('  hello  ' AS CHAR(9)))", "CAST('hello' AS VARCHAR(9))");
        assertFunction("TRIM(CAST('  hello' AS CHAR(7)))", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(CAST('hello  ' AS CHAR(7)))", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(CAST(' hello world ' AS CHAR(13)))", "CAST('hello world' AS VARCHAR(13))");
        assertFunction("TRIM(BOTH FROM CAST('' AS CHAR(20)))", "CAST('' AS VARCHAR(20))");
        assertFunction("TRIM(BOTH FROM CAST('  hello  ' AS CHAR(9)))", "CAST('hello' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH FROM CAST('  hello' AS CHAR(7)))", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH FROM CAST('hello  ' AS CHAR(7)))", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH FROM CAST(' hello world ' AS CHAR(13)))", "CAST('hello world' AS VARCHAR(13))");

        assertFunction("TRIM(CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ' AS CHAR(10)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
        assertFunction("TRIM(CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ' AS CHAR(9)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ' AS CHAR(9)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(9)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(CAST(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(10)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
        assertFunction("TRIM(BOTH FROM CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ' AS CHAR(10)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
        assertFunction("TRIM(BOTH FROM CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ' AS CHAR(9)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH FROM CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ' AS CHAR(9)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH FROM CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(9)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH FROM CAST(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(10)))", "CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS VARCHAR(10))");
    }

    @Test
    public void testCharTrimParametrized()
    {
        assertFunction("TRIM(CAST('' AS CHAR(1)), '')", "CAST('' AS VARCHAR(1))");
        assertFunction("TRIM(CAST('   ' AS CHAR(3)), '')", "CAST('' AS VARCHAR(3))");
        assertFunction("TRIM(CAST('  hello  ' AS CHAR(9)), '')", "CAST('  hello' AS VARCHAR(9))");
        assertFunction("TRIM(CAST('  hello  ' AS CHAR(9)), ' ')", "CAST('hello' AS VARCHAR(9))");
        assertFunction("TRIM(CAST('  hello  ' AS CHAR(9)), 'he ')", "CAST('llo' AS VARCHAR(9))");
        assertFunction("TRIM(CAST('  hello' AS CHAR(7)), ' ')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(CAST('  hello' AS CHAR(7)), 'e h')", "CAST('llo' AS VARCHAR(7))");
        assertFunction("TRIM(CAST('hello  ' AS CHAR(7)), 'l')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(CAST(' hello world ' AS CHAR(13)), ' ')", "CAST('hello world' AS VARCHAR(13))");
        assertFunction("TRIM(CAST(' hello world ' AS CHAR(13)), ' eh')", "CAST('llo world' AS VARCHAR(13))");
        assertFunction("TRIM(CAST(' hello world ' AS CHAR(13)), ' ehlowrd')", "CAST('' AS VARCHAR(13))");
        assertFunction("TRIM(CAST(' hello world ' AS CHAR(13)), ' x')", "CAST('hello world' AS VARCHAR(13))");
        assertFunction("TRIM(CAST('abc def' AS CHAR(7)), 'def')", "CAST('abc' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH '' FROM CAST('' AS CHAR(1)))", "CAST('' AS VARCHAR(1))");
        assertFunction("TRIM(BOTH '' FROM CAST('   ' AS CHAR(3)))", "CAST('' AS VARCHAR(3))");
        assertFunction("TRIM(BOTH '' FROM CAST('  hello  ' AS CHAR(9)))", "CAST('  hello' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH ' ' FROM CAST('  hello  ' AS CHAR(9)))", "CAST('hello' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH 'he ' FROM CAST('  hello  ' AS CHAR(9)))", "CAST('llo' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH ' ' FROM CAST('  hello' AS CHAR(7)))", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH 'e h' FROM CAST('  hello' AS CHAR(7)))", "CAST('llo' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH 'l' FROM CAST('hello  ' AS CHAR(7)))", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH ' ' FROM CAST(' hello world ' AS CHAR(13)))", "CAST('hello world' AS VARCHAR(13))");
        assertFunction("TRIM(BOTH ' eh' FROM CAST(' hello world ' AS CHAR(13)))", "CAST('llo world' AS VARCHAR(13))");
        assertFunction("TRIM(BOTH ' ehlowrd' FROM CAST(' hello world ' AS CHAR(13)))", "CAST('' AS VARCHAR(13))");
        assertFunction("TRIM(BOTH ' x' FROM CAST(' hello world ' AS CHAR(13)))", "CAST('hello world' AS VARCHAR(13))");
        assertFunction("TRIM(BOTH 'def' FROM CAST('abc def' AS CHAR(7)))", "CAST('abc' AS VARCHAR(7))");

        // non latin characters
        assertFunction("TRIM(CAST('\u017a\u00f3\u0142\u0107' AS CHAR(4)), '\u017a\u0107\u0142')", "CAST('\u00f3' AS VARCHAR(4))");
        assertFunction("TRIM(BOTH '\u017a\u0107\u0142' FROM CAST('\u017a\u00f3\u0142\u0107' AS CHAR(4)))", "CAST('\u00f3' AS VARCHAR(4))");
    }

    @Test
    public void testTrimParametrized()
    {
        assertFunction("TRIM('', '')", "CAST('' AS VARCHAR(0))");
        assertFunction("TRIM('   ', '')", "CAST('   ' AS VARCHAR(3))");
        assertFunction("TRIM('  hello  ', '')", "CAST('  hello  ' AS VARCHAR(9))");
        assertFunction("TRIM('  hello  ', ' ')", "CAST('hello' AS VARCHAR(9))");
        assertFunction("TRIM('  hello  ', 'he ')", "CAST('llo' AS VARCHAR(9))");
        assertFunction("TRIM('  hello  ', 'lo ')", "CAST('he' AS VARCHAR(9))");
        assertFunction("TRIM('  hello', ' ')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM('hello  ', ' ')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM('hello  ', 'l o')", "CAST('he' AS VARCHAR(7))");
        assertFunction("TRIM('hello  ', 'l')", "CAST('hello  ' AS VARCHAR(7))");
        assertFunction("TRIM(' hello world ', ' ')", "CAST('hello world' AS VARCHAR(13))");
        assertFunction("TRIM(' hello world ', ' ld')", "CAST('hello wor' AS VARCHAR(13))");
        assertFunction("TRIM(' hello world ', ' eh')", "CAST('llo world' AS VARCHAR(13))");
        assertFunction("TRIM(' hello world ', ' ehlowrd')", "CAST('' AS VARCHAR(13))");
        assertFunction("TRIM(' hello world ', ' x')", "CAST('hello world' AS VARCHAR(13))");
        assertFunction("TRIM(BOTH '' FROM '')", "CAST('' AS VARCHAR(0))");
        assertFunction("TRIM(BOTH '' FROM '   ')", "CAST('   ' AS VARCHAR(3))");
        assertFunction("TRIM(BOTH '' FROM '  hello  ')", "CAST('  hello  ' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH ' ' FROM '  hello  ')", "CAST('hello' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH 'he ' FROM '  hello  ')", "CAST('llo' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH 'lo ' FROM '  hello  ')", "CAST('he' AS VARCHAR(9))");
        assertFunction("TRIM(BOTH ' ' FROM '  hello')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH ' ' FROM 'hello  ')", "CAST('hello' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH 'l o' FROM 'hello  ')", "CAST('he' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH 'l' FROM 'hello  ')", "CAST('hello  ' AS VARCHAR(7))");
        assertFunction("TRIM(BOTH ' ' FROM ' hello world ')", "CAST('hello world' AS VARCHAR(13))");
        assertFunction("TRIM(BOTH ' ld' FROM ' hello world ')", "CAST('hello wor' AS VARCHAR(13))");
        assertFunction("TRIM(BOTH ' eh' FROM ' hello world ')", "CAST('llo world' AS VARCHAR(13))");
        assertFunction("TRIM(BOTH ' ehlowrd' FROM ' hello world ')", "CAST('' AS VARCHAR(13))");
        assertFunction("TRIM(BOTH ' x' FROM ' hello world ')", "CAST('hello world' AS VARCHAR(13))");

        // non latin characters
        assertFunction("TRIM('\u017a\u00f3\u0142\u0107', '\u0107\u017a')", "CAST('\u00f3\u0142' AS VARCHAR(4))");
        assertFunction("TRIM(BOTH '\u0107\u017a' FROM '\u017a\u00f3\u0142\u0107')", "CAST('\u00f3\u0142' AS VARCHAR(4))");

        // invalid utf-8 characters
        assertFunction("CAST(TRIM(utf8(from_hex('81')), ' ') AS VARBINARY)", "x'81'");
        assertFunction("CAST(TRIM(CONCAT(utf8(from_hex('81')), ' '), ' ') AS VARBINARY)", "x'81'");
        assertFunction("CAST(TRIM(CONCAT(' ', utf8(from_hex('81'))), ' ') AS VARBINARY)", "x'81'");
        assertFunction("CAST(TRIM(CONCAT(' ', utf8(from_hex('81')), ' '), ' ') AS VARBINARY)", "x'81'");
        assertInvalidFunction("TRIM('hello world', utf8(from_hex('81')))", "Invalid UTF-8 encoding in characters: �");
        assertInvalidFunction("TRIM('hello world', utf8(from_hex('3281')))", "Invalid UTF-8 encoding in characters: 2�");
        assertInvalidFunction("TRIM(BOTH utf8(from_hex('3281')) FROM 'hello world')", "Invalid UTF-8 encoding in characters: 2�");
    }

    private void assertFunction(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertThat(assertions.query("SELECT " + actual)).matches("SELECT " + expected);
    }

    private void assertInvalidFunction(@Language("SQL") String actual, String message)
    {
        assertThatThrownBy(() -> assertions.query("SELECT " + actual))
                .isInstanceOf(TrinoException.class)
                .hasMessage(message)
                .extracting(e -> ((TrinoException) e).getErrorCode().getCode())
                .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode().getCode());
    }
}
