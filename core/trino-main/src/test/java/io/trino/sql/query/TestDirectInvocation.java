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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestDirectInvocation
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    void teardown()
    {
        assertions.close();
    }

    @Test
    void testDirectInvocation()
    {
        assertThat(assertions.query("SELECT ('hello').upper().concat(' world!')"))
                .matches("SELECT VARCHAR 'HELLO world!'");

        assertThat(assertions.query("SELECT (-123).abs()"))
                .matches("SELECT 123");
    }

    @Test
    void testQualifiedFunctionName()
    {
        assertThat(assertions.query("SELECT (-123).system.builtin.abs()"))
                .matches("SELECT 123");
    }

    @Test
    void testInvalidType()
    {
        assertThat(assertions.query("SELECT ('hello').abs()")).failure()
                .hasMessage("line 1:8: Unexpected parameters (varchar(5)) for function abs. " +
                        "Expected: abs(bigint), abs(decimal(p,s)), abs(double), abs(integer), abs(real), abs(smallint), abs(tinyint)");
    }

    @Test
    void testInvalidParameter()
    {
        assertThat(assertions.query("SELECT (-123).e()")).failure()
                .hasMessage("line 1:8: Unexpected parameters (integer) for function e. Expected: e()");
    }
}
