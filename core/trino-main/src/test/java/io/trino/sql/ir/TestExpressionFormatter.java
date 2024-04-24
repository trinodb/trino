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
package io.trino.sql.ir;

import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionFormatter
{
    @Test
    public void testReference()
    {
        assertFormattedExpression(
                new Reference(BIGINT, "abc"),
                "abc");
        assertFormattedExpression(
                new Reference(BIGINT, "with a space"),
                "\"with a space\"");
        assertFormattedExpression(
                new Reference(BIGINT, "with \" quote, $ dollar and ' apostrophe"),
                "\"with \"\" quote, $ dollar and ' apostrophe\"");
    }

    private void assertFormattedExpression(Expression expression, String expected)
    {
        assertThat(ExpressionFormatter.formatExpression(expression)).as("formatted expression")
                .isEqualTo(expected);
    }
}
