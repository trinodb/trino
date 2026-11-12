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
package io.trino.type.collation;

import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;

public final class Collations
{
    private Collations() {}

    /// Whether a type contains a character string with a non-default collation.
    ///
    /// Equality under such a collation does not identify the string: `'Alice'` and `'ALICE'` are
    /// equal under a case-insensitive collation and remain distinguishable. Rewrites that replace
    /// a value by an equal one must not apply to these types, and their values cannot be put in
    /// domains, whose type operators have no collation comparisons.
    public static boolean hasNonDefaultCollation(Type type)
    {
        if (type instanceof VarcharType varcharType) {
            return !varcharType.getCollation().isDefault();
        }
        for (Type parameter : type.getTypeParameters()) {
            if (hasNonDefaultCollation(parameter)) {
                return true;
            }
        }
        return false;
    }

    /// The type with the default collation in every character string it contains, including the
    /// elements, fields, and values of nested types.
    public static Type withoutCollations(Type type, Function<TypeDescriptor, Type> lookupType)
    {
        if (!hasNonDefaultCollation(type)) {
            return type;
        }
        return lookupType.apply(withoutCollations(type.getTypeDescriptor()));
    }

    /// The descriptor with the collation parameter removed from every character string type it
    /// contains.
    public static TypeDescriptor withoutCollations(TypeDescriptor descriptor)
    {
        List<TypeParameter> parameters = descriptor.getParameters().stream()
                .filter(parameter -> !(isCharacterString(descriptor) && parameter instanceof TypeParameter.Text))
                .map(parameter -> parameter instanceof TypeParameter.Type(var name, TypeDescriptor nested)
                        ? new TypeParameter.Type(name, withoutCollations(nested))
                        : parameter)
                .collect(toImmutableList());
        return new TypeDescriptor(descriptor.getBase(), parameters);
    }

    private static boolean isCharacterString(TypeDescriptor descriptor)
    {
        return descriptor.getBase().equals(StandardTypes.VARCHAR) || descriptor.getBase().equals(StandardTypes.CHAR);
    }
}
