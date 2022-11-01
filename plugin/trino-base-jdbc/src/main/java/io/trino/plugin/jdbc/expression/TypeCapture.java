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
import io.trino.spi.type.Type;

import java.util.Objects;

import static io.trino.matching.Capture.newCapture;
import static java.lang.String.format;

public class TypeCapture
        implements TypePattern
{
    private final TypePattern typePattern;
    private final String name;

    private final Pattern<Type> pattern;
    private final Capture<Type> capture = newCapture();

    public TypeCapture(TypePattern typePattern, String name)
    {
        this.typePattern = typePattern;
        this.name = name;
        this.pattern = typePattern.getPattern().capturedAs(capture);
    }

    @Override
    public Pattern<Type> getPattern()
    {
        return this.pattern;
    }

    @Override
    public void resolve(Captures captures, MatchContext matchContext)
    {
        matchContext.record(name, captures.get(capture));
        typePattern.resolve(captures, matchContext);
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
        TypeCapture that = (TypeCapture) o;
        return Objects.equals(typePattern, that.typePattern) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typePattern, name);
    }

    @Override
    public String toString()
    {
        return format("%s as %s", typePattern, name);
    }
}
