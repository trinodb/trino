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

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLuhnCheckFunction
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
    public void testLuhnCheck()
    {
        assertThat(assertions.function("luhn_check", "'4242424242424242'"))
                .isEqualTo(true);

        assertThat(assertions.function("luhn_check", "'1234567891234567'"))
                .isEqualTo(false);

        assertThat(assertions.function("luhn_check", "''"))
                .isEqualTo(false);

        assertThat(assertions.function("luhn_check", "NULL"))
                .isNull(BOOLEAN);

        assertTrinoExceptionThrownBy(assertions.function("luhn_check", "'abcd424242424242'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertThat(assertions.function("luhn_check", "'123456789'"))
                .isEqualTo(false);

        assertTrinoExceptionThrownBy(assertions.function("luhn_check", "'\u4EA0\u4EFF\u4EA112345'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.function("luhn_check", "'4242\u4FE124242424242'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }
}
