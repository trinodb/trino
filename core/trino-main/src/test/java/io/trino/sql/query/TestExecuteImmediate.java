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

import io.trino.sql.parser.ParsingException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestExecuteImmediate
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
        assertThatThrownBy(() -> assertions.query("EXECUTE IMMEDIATE 'SELECT ''foo'"))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 1:27: mismatched input '''. Expecting: .*");
        assertThatThrownBy(() -> assertions.query("EXECUTE IMMEDIATE\n'SELECT ''foo'"))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 2:8: mismatched input '''. Expecting: .*");
    }

    @Test
    public void testSemanticError()
    {
        assertTrinoExceptionThrownBy(() -> assertions.query("EXECUTE IMMEDIATE 'SELECT * FROM tiny.tpch.orders'"))
                .hasMessageMatching("line 1:34: Catalog 'tiny' does not exist");
        assertTrinoExceptionThrownBy(() -> assertions.query("EXECUTE IMMEDIATE\n'SELECT *\nFROM tiny.tpch.orders'"))
                .hasMessageMatching("line 3:6: Catalog 'tiny' does not exist");
    }
}
