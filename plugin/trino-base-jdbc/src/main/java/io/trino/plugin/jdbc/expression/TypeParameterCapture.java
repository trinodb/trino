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
import io.trino.matching.Property;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.Objects;

import static io.trino.matching.Capture.newCapture;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class TypeParameterCapture
        extends TypeParameterPattern
{
    private final String name;

    private final Capture<TypeSignatureParameter> capture = newCapture();
    private final Pattern<TypeSignatureParameter> pattern;

    public TypeParameterCapture(String name)
    {
        this.name = requireNonNull(name, "name is null");
        this.pattern = Pattern.typeOf(TypeSignatureParameter.class).with(self().capturedAs(capture));
    }

    @Override
    public Pattern<? extends TypeSignatureParameter> getPattern()
    {
        return pattern;
    }

    @Override
    public void resolve(Captures captures, MatchContext matchContext)
    {
        TypeSignatureParameter parameter = captures.get(capture);
        switch (parameter.getKind()) {
            case TYPE:
                matchContext.record(name, parameter.getTypeSignature());
                break;
            case LONG:
                matchContext.record(name, parameter.getLongLiteral());
                break;
            default:
                throw new UnsupportedOperationException("Unsupported parameter: " + parameter);
        }
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
        TypeParameterCapture that = (TypeParameterCapture) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public String toString()
    {
        return name;
    }

    public static Property<TypeSignatureParameter, ?, TypeSignatureParameter> self()
    {
        return Property.property("self", identity());
    }
}
