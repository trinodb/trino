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

import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestExecuteImmediate
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testNoParameters()
    {
        assertThat(assertions.query("EXECUTE IMMEDIATE 'SELECT * FROM (VALUES 1, 2, 3)'"))
                .matches("VALUES 1,2,3");
    }

    @Test
    public void testParameterInLambda()
    {
        assertThat(assertions.query("EXECUTE IMMEDIATE 'SELECT * FROM (VALUES ARRAY[1,2,3], ARRAY[4,5,6]) t(a) WHERE any_match(t.a, v -> v = ?)' USING 1"))
                .matches("VALUES ARRAY[1,2,3]");
    }

    @Test
    public void testQuotesInStatement()
    {
        assertThat(assertions.query("EXECUTE IMMEDIATE 'SELECT ''foo'''"))
                .matches("VALUES 'foo'");
    }

    @Test
    public void testSyntaxError()
    {
        assertThat(assertions.query("EXECUTE IMMEDIATE 'SELECT ''foo'"))
                .failure()
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessageMatching("line 1:27: mismatched input '''. Expecting: .*");
        assertThat(assertions.query("EXECUTE IMMEDIATE\n'SELECT ''foo'"))
                .failure()
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessageMatching("line 2:8: mismatched input '''. Expecting: .*");
    }

    @Test
    public void testSemanticError()
    {
        assertThat(assertions.query("EXECUTE IMMEDIATE 'SELECT * FROM tiny.tpch.orders'"))
                .failure()
                .hasMessageMatching("line 1:34: Catalog 'tiny' not found");
        assertThat(assertions.query("EXECUTE IMMEDIATE\n'SELECT *\nFROM tiny.tpch.orders'"))
                .failure()
                .hasMessageMatching("line 3:6: Catalog 'tiny' not found");
    }
}
