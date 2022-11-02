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

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.matching.Property;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TypeClassPattern
        implements TypePattern
{
    private final String typeClassName;
    private final Set<String> typeNames;
    private final Pattern<Type> pattern;

    public TypeClassPattern(String typeClassName, Set<String> typeNames)
    {
        this.typeClassName = requireNonNull(typeClassName, "typeClassName is null");
        this.typeNames = ImmutableSet.copyOf(requireNonNull(typeNames, "typeNames is null"));
        this.pattern = Pattern.typeOf(Type.class).with(baseName().matching(this.typeNames::contains));
    }

    @Override
    public Pattern<Type> getPattern()
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
        TypeClassPattern that = (TypeClassPattern) o;
        return Objects.equals(typeClassName, that.typeClassName) &&
                Objects.equals(typeNames, that.typeNames);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeClassName, typeNames);
    }

    @Override
    public String toString()
    {
        return typeClassName;
    }

    private static Property<Type, ?, String> baseName()
    {
        return Property.property("baseName", Type::getBaseName);
    }
}
