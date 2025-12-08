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
package io.trino.spi.type;

import com.google.errorprone.annotations.Immutable;

import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Immutable
public final class TypeParameter
{
    private final Optional<String> name;
    private final ParameterKind kind;
    private final Object value;

    public static TypeParameter typeParameter(TypeSignature typeSignature)
    {
        return new TypeParameter(Optional.empty(), ParameterKind.TYPE, typeSignature);
    }

    public static TypeParameter typeParameter(Optional<String> name, TypeSignature typeSignature)
    {
        return new TypeParameter(name, ParameterKind.TYPE, typeSignature);
    }

    public static TypeParameter numericParameter(long longLiteral)
    {
        return new TypeParameter(Optional.empty(), ParameterKind.LONG, longLiteral);
    }

    public static TypeParameter namedField(String name, TypeSignature type)
    {
        return new TypeParameter(Optional.of(name), ParameterKind.TYPE, type);
    }

    public static TypeParameter anonymousField(TypeSignature type)
    {
        return new TypeParameter(Optional.empty(), ParameterKind.TYPE, type);
    }

    public static TypeParameter typeVariable(String variable)
    {
        return new TypeParameter(Optional.empty(), ParameterKind.VARIABLE, variable);
    }

    private TypeParameter(Optional<String> name, ParameterKind kind, Object value)
    {
        this.name = requireNonNull(name, "name is null");
        this.kind = requireNonNull(kind, "kind is null");
        this.value = requireNonNull(value, "value is null");
    }

    public Optional<String> name()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return switch (kind) {
            case VARIABLE, LONG -> value.toString();
            case TYPE -> {
                if (name.isPresent()) {
                    yield format("\"%s\" %s", name.get().replace("\"", "\"\""), value.toString());
                }
                yield value.toString();
            }
        };
    }

    public String jsonValue()
    {
        String prefix = "";

        if (name.isPresent()) {
            prefix = format("\"%s\" ", name.get().replace("\"", "\"\""));
        }

        if (kind == ParameterKind.VARIABLE) {
            prefix += "@";
        }

        String valueJson;
        if (value instanceof TypeSignature typeSignature) {
            valueJson = typeSignature.jsonValue();
        }
        else {
            valueJson = value.toString();
        }
        return prefix + valueJson;
    }

    public ParameterKind getKind()
    {
        return kind;
    }

    public boolean isTypeSignature()
    {
        return kind == ParameterKind.TYPE;
    }

    public boolean isLongLiteral()
    {
        return kind == ParameterKind.LONG;
    }

    public boolean isVariable()
    {
        return kind == ParameterKind.VARIABLE;
    }

    private <A> A getValue(ParameterKind expectedParameterKind, Class<A> target)
    {
        if (kind != expectedParameterKind) {
            throw new IllegalArgumentException(format("ParameterKind is [%s] but expected [%s]", kind, expectedParameterKind));
        }
        return target.cast(value);
    }

    public TypeSignature getTypeSignature()
    {
        return getValue(ParameterKind.TYPE, TypeSignature.class);
    }

    public Long getLongLiteral()
    {
        return getValue(ParameterKind.LONG, Long.class);
    }

    public String getVariable()
    {
        return getValue(ParameterKind.VARIABLE, String.class);
    }

    public boolean isCalculated()
    {
        return switch (kind) {
            case TYPE -> getTypeSignature().isCalculated();
            case LONG -> false;
            case VARIABLE -> true;
        };
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof TypeParameter that)) {
            return false;
        }
        return Objects.equals(name, that.name) && kind == that.kind && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, kind, value);
    }
}
