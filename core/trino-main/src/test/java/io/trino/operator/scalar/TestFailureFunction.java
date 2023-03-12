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
import io.trino.spi.TrinoException;
import io.trino.testing.LocalQueryRunner;
import io.trino.util.Failures;
import org.testng.annotations.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFailureFunction
        extends AbstractTestFunctions
{
    private static final String FAILURE_INFO = JsonCodec.jsonCodec(FailureInfo.class).toJson(Failures.toFailure(new RuntimeException("fail me")).toFailureInfo());

    @Test
    public void testFailure()
    {
        assertInvalidFunction("fail(json_parse('" + FAILURE_INFO + "'))", GENERIC_USER_ERROR, "fail me");
    }

    @Test
    public void testQuery()
    {
        // The other test does not exercise this function during execution (i.e. inside a page processor).
        // It only verifies constant folding works.
        try (LocalQueryRunner runner = LocalQueryRunner.create(TEST_SESSION)) {
            assertThatThrownBy(() -> runner.execute("select if(x, 78, 0/0) from (values rand() >= 0, rand() < 0) t(x)"))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageMatching("Division by zero");
        }
    }
}
