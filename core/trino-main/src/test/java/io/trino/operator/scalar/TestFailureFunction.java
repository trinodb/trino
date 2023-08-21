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

import io.airlift.json.JsonCodec;
import io.trino.client.FailureInfo;
import io.trino.sql.query.QueryAssertions;
import io.trino.util.Failures;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestFailureFunction
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
    public void testFailure()
    {
        String failure = JsonCodec.jsonCodec(FailureInfo.class).toJson(Failures.toFailure(new RuntimeException("fail me")).toFailureInfo());
        assertTrinoExceptionThrownBy(assertions.function("fail", "json_parse('" + failure + "')")::evaluate)
                .hasErrorCode(GENERIC_USER_ERROR)
                .hasMessage("fail me");
    }
}
