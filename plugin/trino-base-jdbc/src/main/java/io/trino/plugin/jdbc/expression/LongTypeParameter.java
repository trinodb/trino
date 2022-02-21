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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.matching.Property;
import io.trino.spi.type.ParameterKind;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.Optional;

public class LongTypeParameter
        extends TypeParameterPattern
{
    private final long value;
    private final Pattern<TypeSignatureParameter> pattern;

    public LongTypeParameter(long value)
    {
        this.value = value;
        this.pattern = Pattern.typeOf(TypeSignatureParameter.class).with(value().equalTo(value));
    }

    @Override
    public Pattern<? extends TypeSignatureParameter> getPattern()
    {
        return pattern;
    }

    @Override
    public void resolve(Captures captures, MatchContext matchContext) {}

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LongTypeParameter that = (LongTypeParameter) o;
        return value == that.value;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(value);
    }

    @Override
    public String toString()
    {
        return Long.toString(value);
    }

    public static Property<TypeSignatureParameter, ?, Long> value()
    {
        return Property.optionalProperty("value", parameter -> {
            if (parameter.getKind() != ParameterKind.LONG) {
                return Optional.empty();
            }
            return Optional.of(parameter.getLongLiteral());
        });
    }
}
