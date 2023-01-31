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
package io.trino.plugin.jdbc.expression;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionIndex;
import io.trino.spi.expression.ConnectorExpression;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expressionTypes;
import static java.util.Objects.requireNonNull;

public class ExpressionListCapture
        extends ExpressionListPattern
{
    private final String name;

    private final Capture<List<? extends ConnectorExpression>> capture = newCapture();
    private final Pattern<List<? extends ConnectorExpression>> pattern;

    public ExpressionListCapture(String name, Predicate<ConnectorExpressionIndex> predicate)
    {
        this.name = requireNonNull(name, "name is null");
        this.pattern = Pattern.typeOfList(ConnectorExpression.class)
                .matching(expressionTypes(predicate))
                .capturedAs(capture);
    }

    @Override
    public Pattern<List<? extends ConnectorExpression>> getPattern()
    {
        return pattern;
    }

    @Override
    public void resolve(Captures captures, MatchContext matchContext)
    {
        matchContext.record(name, captures.get(capture));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExpressionListCapture that = (ExpressionListCapture) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(capture, that.capture) &&
                Objects.equals(pattern, that.pattern);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, capture, pattern);
    }

    @Override
    public String toString()
    {
        return name;
    }
}
