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
package io.trino.sql;

import io.trino.sql.tree.Expression;
import io.trino.sql.tree.IntervalLiteral;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.parser.ParserAssert.expression;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.HOUR;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.SECOND;
import static io.trino.sql.tree.IntervalLiteral.Sign.NEGATIVE;
import static io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionFormatter
{
    @Test
    public void testIntervalLiteral()
    {
        // positive
        assertFormattedExpression(
                new IntervalLiteral("2", POSITIVE, HOUR),
                "INTERVAL '2' HOUR");
        // negative
        assertFormattedExpression(
                new IntervalLiteral("2", NEGATIVE, HOUR),
                "INTERVAL -'2' HOUR");

        // from .. to
        assertFormattedExpression(
                new IntervalLiteral("2", POSITIVE, HOUR, Optional.of(SECOND)),
                "INTERVAL '2' HOUR TO SECOND");

        // negative from .. to
        assertFormattedExpression(
                new IntervalLiteral("2", NEGATIVE, HOUR, Optional.of(SECOND)),
                "INTERVAL -'2' HOUR TO SECOND");
    }

    private void assertFormattedExpression(Expression expression, String expected)
    {
        assertThat(ExpressionFormatter.formatExpression(expression)).as("formatted expression")
                .isEqualTo(expected);

        // validate that the expected parses back
        assertThat(expression(expected))
                .ignoringLocation()
                .isEqualTo(expression);
    }
}
