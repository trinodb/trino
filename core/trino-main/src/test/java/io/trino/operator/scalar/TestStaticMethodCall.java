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

import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestStaticMethodCall
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
    void testPostgreSqlStyleCast()
    {
        assertThat(assertions.expression("1::double"))
                .hasType(DoubleType.DOUBLE)
                .isEqualTo(1.0);

        assertThat(assertions.expression("1::varchar"))
                .hasType(VarcharType.VARCHAR)
                .isEqualTo("1");

        assertThatThrownBy(() -> assertions.expression("1::varchar(100)").evaluate())
                .hasMessage("line 1:13: Static method calls are not supported");

        assertThat(assertions.expression("(a + b)::double")
                .binding("a", "1")
                .binding("b", "2"))
                .hasType(DoubleType.DOUBLE)
                .isEqualTo(3.0);

        assertThatThrownBy(() -> assertions.expression("1::decimal(3, 2)").evaluate())
                .hasMessage("line 1:13: Static method calls are not supported");
    }

    @Test
    void testCall()
    {
        assertThatThrownBy(() -> assertions.expression("1::double(2)").evaluate())
                .hasMessage("line 1:13: Static method calls are not supported");

        assertThatThrownBy(() -> assertions.expression("1::foo").evaluate())
                .hasMessage("line 1:13: Static method calls are not supported");

        assertThatThrownBy(() -> assertions.expression("integer::foo").evaluate())
                .hasMessage("line 1:19: Static method calls are not supported");

        assertThatThrownBy(() -> assertions.expression("integer::foo(1, 2)").evaluate())
                .hasMessage("line 1:19: Static method calls are not supported");

        assertThat(assertions.query("SELECT bigint::real FROM (VALUES 1) AS t(bigint)"))
                .failure()
                .hasMessage("line 1:14: Static method calls are not supported");
    }
}
