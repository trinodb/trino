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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestVarcharMethods
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
    public void testLength()
    {
        assertThat(assertions.expression("'foo'.length()"))
                .matches("length('foo')");
    }

    @Test
    public void testReplace()
    {
        assertThat(assertions.expression("'abcba'.replace('b')"))
                .matches("replace('abcba', 'b')");
        assertThat(assertions.expression("'abc'.replace('b', 'xx')"))
                .matches("replace('abc', 'b', 'xx')");
    }

    @Test
    public void testReverse()
    {
        assertThat(assertions.expression("'abc'.reverse()"))
                .matches("reverse('abc')");
    }

    @Test
    public void testStrpos()
    {
        assertThat(assertions.expression("'hello'.strpos('l')"))
                .matches("strpos('hello', 'l')");
        assertThat(assertions.expression("'hello'.strpos('l', 2)"))
                .matches("strpos('hello', 'l', 2)");
    }

    @Test
    public void testSubstring()
    {
        assertThat(assertions.expression("'hello'.substring(2)"))
                .matches("substring('hello', 2)");
        assertThat(assertions.expression("'hello'.substring(2, 3)"))
                .matches("substring('hello', 2, 3)");
    }

    @Test
    public void testSplit()
    {
        assertThat(assertions.expression("'a,b,c'.split(',')"))
                .matches("split('a,b,c', ',')");
        assertThat(assertions.expression("'a,b,c'.split(',', 2)"))
                .matches("split('a,b,c', ',', 2)");
    }

    @Test
    public void testTrims()
    {
        assertThat(assertions.expression("'  x '.trim()"))
                .matches("trim('  x ')");
        assertThat(assertions.expression("'xxhixx'.trim('x')"))
                .matches("trim('xxhixx', 'x')");
        assertThat(assertions.expression("'  x '.ltrim()"))
                .matches("ltrim('  x ')");
        assertThat(assertions.expression("'  x '.rtrim()"))
                .matches("rtrim('  x ')");
        assertThat(assertions.expression("'xxhixx'.ltrim('x')"))
                .matches("ltrim('xxhixx', 'x')");
        assertThat(assertions.expression("'xxhixx'.rtrim('x')"))
                .matches("rtrim('xxhixx', 'x')");
    }

    @Test
    public void testLowerUpper()
    {
        assertThat(assertions.expression("'AbC'.lower()"))
                .matches("lower('AbC')");
        assertThat(assertions.expression("'AbC'.upper()"))
                .matches("upper('AbC')");
    }

    @Test
    public void testPad()
    {
        assertThat(assertions.expression("'hi'.lpad(5, '*')"))
                .matches("lpad('hi', 5, '*')");
        assertThat(assertions.expression("'hi'.rpad(5, '*')"))
                .matches("rpad('hi', 5, '*')");
    }

    @Test
    public void testToUtf8()
    {
        assertThat(assertions.expression("'abc'.to_utf8()"))
                .matches("to_utf8('abc')");
    }

    @Test
    public void testStartsWith()
    {
        assertThat(assertions.expression("'hello'.starts_with('he')"))
                .matches("starts_with('hello', 'he')");
    }

    @Test
    public void testEndsWith()
    {
        assertThat(assertions.expression("'hello'.ends_with('lo')"))
                .matches("ends_with('hello', 'lo')");
    }

    @Test
    public void testTranslate()
    {
        assertThat(assertions.expression("'abcd'.translate('ac', 'xy')"))
                .matches("translate('abcd', 'ac', 'xy')");
    }

    @Test
    public void testVarcharStatics()
    {
        assertThat(assertions.expression("varchar::chr(65)"))
                .matches("chr(65)");
        assertThat(assertions.expression("varchar::from_utf8(to_utf8('hello'))"))
                .matches("from_utf8(to_utf8('hello'))");
        assertThat(assertions.expression("varchar::from_utf8(X'58BF', '#')"))
                .matches("from_utf8(X'58BF', '#')");
        assertThat(assertions.expression("varchar::from_utf8(X'58BF', 35)"))
                .matches("from_utf8(X'58BF', 35)");
    }
}
