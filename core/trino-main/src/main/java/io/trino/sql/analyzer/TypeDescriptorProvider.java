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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;

import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class TypeDescriptorProvider
{
    // hasDependency field exists primarily to make manipulating types without dependencies easy,
    // and to make toString more friendly.
    private final boolean hasDependency;
    private final Function<List<Type>, TypeDescriptor> typeDescriptorResolver;

    public TypeDescriptorProvider(TypeDescriptor typeDescriptor)
    {
        this.hasDependency = false;
        requireNonNull(typeDescriptor, "typeDescriptor is null");
        this.typeDescriptorResolver = _ -> typeDescriptor;
    }

    public TypeDescriptorProvider(Function<List<Type>, TypeDescriptor> typeDescriptorResolver)
    {
        this.hasDependency = true;
        this.typeDescriptorResolver = requireNonNull(typeDescriptorResolver, "typeDescriptorResolver is null");
    }

    public boolean hasDependency()
    {
        return hasDependency;
    }

    public TypeDescriptor getTypeDescriptor()
    {
        checkState(!hasDependency);
        return typeDescriptorResolver.apply(ImmutableList.of());
    }

    public TypeDescriptor getTypeDescriptor(List<Type> boundTypeParameters)
    {
        checkState(hasDependency);
        return typeDescriptorResolver.apply(boundTypeParameters);
    }

    public static List<TypeDescriptorProvider> fromTypes(Type... types)
    {
        return fromTypes(ImmutableList.copyOf(types));
    }

    public static List<TypeDescriptorProvider> fromTypes(List<? extends Type> types)
    {
        return types.stream()
                .map(Type::getTypeDescriptor)
                .map(TypeDescriptorProvider::new)
                .collect(toImmutableList());
    }

    public static List<TypeDescriptorProvider> fromTypeDescriptors(TypeDescriptor... typeDescriptors)
    {
        return fromTypeDescriptors(ImmutableList.copyOf(typeDescriptors));
    }

    public static List<TypeDescriptorProvider> fromTypeDescriptors(List<? extends TypeDescriptor> typeDescriptors)
    {
        return typeDescriptors.stream()
                .map(TypeDescriptorProvider::new)
                .collect(toImmutableList());
    }

    @Override
    public String toString()
    {
        if (hasDependency) {
            return "<function>";
        }
        return getTypeDescriptor().toString();
    }
}
