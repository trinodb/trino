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

import java.util.Optional;

import static java.lang.String.format;

@Immutable
public sealed interface TypeParameter
        permits TypeParameter.Numeric,
                TypeParameter.Type
{
    static TypeParameter typeParameter(TypeDescriptor typeDescriptor)
    {
        return new Type(Optional.empty(), typeDescriptor);
    }

    static TypeParameter typeParameter(Optional<String> name, TypeDescriptor typeDescriptor)
    {
        return new Type(name, typeDescriptor);
    }

    static TypeParameter numericParameter(long longLiteral)
    {
        return new Numeric(longLiteral);
    }

    static TypeParameter namedField(String name, TypeDescriptor type)
    {
        return new Type(Optional.of(name), type);
    }

    static TypeParameter anonymousField(TypeDescriptor type)
    {
        return new Type(Optional.empty(), type);
    }

    String jsonValue();

    boolean isCalculated();

    record Type(Optional<String> name, TypeDescriptor type)
            implements TypeParameter
    {
        @Override
        public String toString()
        {
            if (name.isPresent()) {
                return format("\"%s\" %s", name.get().replace("\"", "\"\""), type.toString());
            }
            return type.toString();
        }

        @Override
        public String jsonValue()
        {
            String prefix = "";

            if (name.isPresent()) {
                prefix = format("\"%s\" ", name.get().replace("\"", "\"\""));
            }

            return prefix + type.jsonValue();
        }

        @Override
        public boolean isCalculated()
        {
            return type.isCalculated();
        }
    }

    record Numeric(long value)
            implements TypeParameter
    {
        @Override
        public String toString()
        {
            return Long.toString(value);
        }

        @Override
        public String jsonValue()
        {
            return toString();
        }

        @Override
        public boolean isCalculated()
        {
            return false;
        }
    }
}
