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
import io.trino.spi.expression.ConnectorExpression;

import java.util.Objects;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExpressionCapture
        extends ExpressionPattern
{
    private final String name;
    private final TypePattern type;

    private final Capture<ConnectorExpression> capture = newCapture();
    private final Pattern<ConnectorExpression> pattern;

    public ExpressionCapture(String name, TypePattern type)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.pattern = Pattern.typeOf(ConnectorExpression.class).capturedAs(capture)
                .with(type().matching(type.getPattern()));
    }

    @Override
    public Pattern<ConnectorExpression> getPattern()
    {
        return pattern;
    }

    @Override
    public void resolve(Captures captures, MatchContext matchContext)
    {
        matchContext.record(name, captures.get(capture));
        type.resolve(captures, matchContext);
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
        ExpressionCapture that = (ExpressionCapture) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public String toString()
    {
        return format("%s: %s", name, type);
    }
}
