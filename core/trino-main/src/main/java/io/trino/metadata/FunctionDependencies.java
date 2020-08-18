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
package io.trino.metadata;

import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.util.List;

public interface FunctionDependencies
{
    Type getType(TypeSignature typeSignature);

    FunctionNullability getFunctionNullability(QualifiedFunctionName name, List<Type> parameterTypes);

    FunctionNullability getOperatorNullability(OperatorType operatorType, List<Type> parameterTypes);

    FunctionNullability getCastNullability(Type fromType, Type toType);

    FunctionInvoker getFunctionInvoker(QualifiedFunctionName name, List<Type> parameterTypes, InvocationConvention invocationConvention);

    FunctionInvoker getFunctionSignatureInvoker(QualifiedFunctionName name, List<TypeSignature> parameterTypes, InvocationConvention invocationConvention);

    FunctionInvoker getOperatorInvoker(OperatorType operatorType, List<Type> parameterTypes, InvocationConvention invocationConvention);

    FunctionInvoker getOperatorSignatureInvoker(OperatorType operatorType, List<TypeSignature> parameterTypes, InvocationConvention invocationConvention);

    FunctionInvoker getCastInvoker(Type fromType, Type toType, InvocationConvention invocationConvention);

    FunctionInvoker getCastSignatureInvoker(TypeSignature fromType, TypeSignature toType, InvocationConvention invocationConvention);
}
