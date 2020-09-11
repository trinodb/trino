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
package io.prestosql.spi.type;

import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.OperatorType;

import java.lang.invoke.MethodHandle;
import java.util.List;

public interface TypeManager
{
    /**
     * Gets the type with the specified signature.
     *
     * @throws TypeNotFoundException if not found
     */
    Type getType(TypeSignature signature);

    /**
     * Gets a type given it's SQL representation
     */
    Type fromSqlType(String type);

    /**
     * Gets the type with the give (opaque) id
     */
    Type getType(TypeId id);

    /**
     * Gets the type with the specified base type and the given parameters.
     *
     * @throws TypeNotFoundException if not found
     */
    default Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        return getType(new TypeSignature(baseTypeName, typeParameters));
    }

    MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes, InvocationConvention invocationConvention);
}
