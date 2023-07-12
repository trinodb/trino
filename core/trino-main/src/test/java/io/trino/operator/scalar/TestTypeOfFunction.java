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

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTypeOfFunction
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
    public void testSimpleType()
    {
        assertThat(assertions.function("typeof", "CAST(1 AS BIGINT)"))
                .isEqualTo("bigint");
        assertThat(assertions.function("typeof", "CAST(1 AS INTEGER)"))
                .isEqualTo("integer");
        assertThat(assertions.function("typeof", "CAST(1 AS VARCHAR)"))
                .isEqualTo("varchar");
        assertThat(assertions.function("typeof", "CAST(1 AS DOUBLE)"))
                .isEqualTo("double");
        assertThat(assertions.function("typeof", "123"))
                .isEqualTo("integer");
        assertThat(assertions.function("typeof", "'cat'"))
                .isEqualTo("varchar(3)");
        assertThat(assertions.function("typeof", "NULL"))
                .isEqualTo("unknown");
    }

    @Test
    public void testParametricType()
    {
        assertThat(assertions.function("typeof", "CAST(NULL AS VARCHAR(10))"))
                .isEqualTo("varchar(10)");
        assertThat(assertions.function("typeof", "CAST(NULL AS DECIMAL(5,1))"))
                .isEqualTo("decimal(5,1)");
        assertThat(assertions.function("typeof", "CAST(NULL AS DECIMAL(1))"))
                .isEqualTo("decimal(1,0)");
        assertThat(assertions.function("typeof", "CAST(NULL AS DECIMAL)"))
                .isEqualTo("decimal(38,0)");
        assertThat(assertions.function("typeof", "CAST(NULL AS ARRAY(INTEGER))"))
                .isEqualTo("array(integer)");
        assertThat(assertions.function("typeof", "CAST(NULL AS ARRAY(DECIMAL(5,1)))"))
                .isEqualTo("array(decimal(5,1))");
    }

    @Test
    public void testNestedType()
    {
        assertThat(assertions.function("typeof", "CAST(NULL AS ARRAY(ARRAY(ARRAY(INTEGER))))"))
                .isEqualTo("array(array(array(integer)))");
        assertThat(assertions.function("typeof", "CAST(NULL AS ARRAY(ARRAY(ARRAY(DECIMAL(5,1)))))"))
                .isEqualTo("array(array(array(decimal(5,1))))");
    }

    @Test
    public void testComplex()
    {
        assertThat(assertions.function("typeof", "CONCAT('ala','ma','kota')"))
                .isEqualTo("varchar");
        assertThat(assertions.function("typeof", "CONCAT(CONCAT('ala','ma','kota'), 'baz')"))
                .isEqualTo("varchar");
        assertThat(assertions.function("typeof", "ARRAY [CAST(1 AS INTEGER),CAST(2 AS INTEGER),CAST(3 AS INTEGER)]"))
                .isEqualTo("array(integer)");
        assertThat(assertions.function("typeof", "sin(2)"))
                .isEqualTo("double");
        assertThat(assertions.function("typeof", "2+sin(2)+2.3"))
                .isEqualTo("double");
    }

    @Test
    public void testLambda()
    {
        assertTrinoExceptionThrownBy(assertions.expression("typeof(x -> x)")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessage("line 1:12: Unexpected parameters (<function>) for function typeof. Expected: typeof(t) T");
    }
}
