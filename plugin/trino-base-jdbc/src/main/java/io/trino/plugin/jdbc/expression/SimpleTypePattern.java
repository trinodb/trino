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

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.matching.Property;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class SimpleTypePattern
        implements TypePattern
{
    private final String baseName;
    private final List<TypeParameterPattern> parameters;
    private final Pattern<Type> pattern;

    public SimpleTypePattern(String baseName, List<TypeParameterPattern> parameters)
    {
        this.baseName = requireNonNull(baseName, "baseName is null");
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        Pattern<Type> pattern = Pattern.typeOf(Type.class).with(baseName().equalTo(baseName));
        for (int i = 0; i < parameters.size(); i++) {
            pattern = pattern.with(parameter(i).matching(parameters.get(i).getPattern()));
        }
        this.pattern = pattern;
    }

    @Override
    public Pattern<Type> getPattern()
    {
        return pattern;
    }

    @Override
    public void resolve(Captures captures, MatchContext matchContext)
    {
        for (TypeParameterPattern parameter : parameters) {
            parameter.resolve(captures, matchContext);
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
        SimpleTypePattern that = (SimpleTypePattern) o;
        return Objects.equals(baseName, that.baseName) &&
                Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(baseName, parameters);
    }

    @Override
    public String toString()
    {
        if (parameters.isEmpty()) {
            return baseName;
        }
        return format(
                "%s(%s)",
                baseName,
                parameters.stream()
                        .map(Object::toString)
                        .collect(joining(", ")));
    }

    public static Property<Type, ?, String> baseName()
    {
        return Property.property("baseName", Type::getBaseName);
    }

    public static Property<Type, ?, TypeSignatureParameter> parameter(int i)
    {
        return Property.property(format("parameter(%s)", i), type -> type.getTypeSignature().getParameters().get(i));
    }
}
