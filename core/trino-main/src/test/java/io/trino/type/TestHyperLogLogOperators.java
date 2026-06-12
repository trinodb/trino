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
package io.trino.type;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestHyperLogLogOperators
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testCastFromVarbinaryRejectsInvalid()
    {
        assertTrinoExceptionThrownBy(assertions.expression("CAST(X'00' AS HyperLogLog)")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Cannot cast to HyperLogLog: Sparse v1 encoding no longer supported");

        assertTrinoExceptionThrownBy(assertions.expression("CAST(X'deadbeef' AS HyperLogLog)")::evaluate)
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessageStartingWith("Cannot cast to HyperLogLog: ");
    }
}
