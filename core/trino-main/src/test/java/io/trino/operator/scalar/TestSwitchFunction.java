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

import io.trino.metadata.InternalFunctionBundle;
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
public class TestSwitchFunction
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder().build());
    }

    @AfterAll
    public void teardown()
    {
        if (assertions != null) {
            assertions.close();
        }
        assertions = null;
    }

    @Test
    public void testVarchar()
    {
        assertThat(assertions.expression("switch('test1', v -> if(v = 'test1', 'foo'), v -> if(v = 'test2', 'bar', 'def'))"))
                .isEqualTo("foo");

        assertThat(assertions.expression("switch('test2', v -> if(v = 'test1', 'foo'), v -> if(v = 'test2', 'bar', 'def'))"))
                .isEqualTo("bar");

        assertThat(assertions.expression("switch('test3', v -> if(v = 'test1', 'foo'), v -> if(v = 'test2', 'bar', 'def'))"))
                .isEqualTo("def");
    }

    @Test
    public void testInteger()
    {
        assertThat(assertions.expression("switch('test1', v -> if(v = 'test1', 1), v -> if(v = 'test2', 2, -1))"))
                .isEqualTo(1);

        assertThat(assertions.expression("switch('test2', v -> if(v = 'test1', 1), v -> if(v = 'test2', 2, -1))"))
                .isEqualTo(2);

        assertThat(assertions.expression("switch('test3', v -> if(v = 'test1', 1), v -> if(v = 'test2', 2, -1))"))
                .isEqualTo(-1);
    }
}
