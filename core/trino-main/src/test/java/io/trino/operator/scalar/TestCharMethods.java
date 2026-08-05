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
public class TestCharMethods
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
        assertThat(assertions.expression("CAST('ab' AS char(4)).length()"))
                .matches("length(CAST('ab' AS char(4)))");
    }

    @Test
    public void testReverse()
    {
        assertThat(assertions.expression("CAST('abc' AS char(3)).reverse()"))
                .matches("reverse(CAST('abc' AS char(3)))");
    }

    @Test
    public void testSubstring()
    {
        assertThat(assertions.expression("CAST('hello' AS char(5)).substring(2)"))
                .matches("substring(CAST('hello' AS char(5)), 2)");
        assertThat(assertions.expression("CAST('hello' AS char(5)).substring(2, 3)"))
                .matches("substring(CAST('hello' AS char(5)), 2, 3)");
    }

    @Test
    public void testLowerUpper()
    {
        assertThat(assertions.expression("CAST('AbC' AS char(3)).lower()"))
                .matches("lower(CAST('AbC' AS char(3)))");
        assertThat(assertions.expression("CAST('AbC' AS char(3)).upper()"))
                .matches("upper(CAST('AbC' AS char(3)))");
    }

    @Test
    public void testPad()
    {
        assertThat(assertions.expression("CAST('hi' AS char(2)).lpad(5, '*')"))
                .matches("lpad(CAST('hi' AS char(2)), 5, '*')");
    }

    @Test
    public void testToUtf8()
    {
        assertThat(assertions.expression("CAST('abc' AS char(3)).to_utf8()"))
                .matches("to_utf8(CAST('abc' AS char(3)))");
    }
}
