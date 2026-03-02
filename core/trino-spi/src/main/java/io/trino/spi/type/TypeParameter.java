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
        permits TypeParameter.Type, TypeParameter.Numeric, TypeParameter.Variable
{
    static TypeParameter typeParameter(TypeSignature typeSignature)
    {
        return new Type(Optional.empty(), typeSignature);
    }

    static TypeParameter typeParameter(Optional<String> name, TypeSignature typeSignature)
    {
        return new Type(name, typeSignature);
    }

    static TypeParameter numericParameter(long longLiteral)
    {
        return new Numeric(longLiteral);
    }

    static TypeParameter namedField(String name, TypeSignature type)
    {
        return new Type(Optional.of(name), type);
    }

    static TypeParameter anonymousField(TypeSignature type)
    {
        return new Type(Optional.empty(), type);
    }

    static TypeParameter typeVariable(String variable)
    {
        return new Variable(variable);
    }

    String jsonValue();

    boolean isCalculated();

    record Type(Optional<String> name, TypeSignature type)
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

    record Variable(String name)
            implements TypeParameter
    {
        @Override
        public String toString()
        {
            return name;
        }

        @Override
        public String jsonValue()
        {
            return "@" + name;
        }

        @Override
        public boolean isCalculated()
        {
            return true;
        }
    }
}
