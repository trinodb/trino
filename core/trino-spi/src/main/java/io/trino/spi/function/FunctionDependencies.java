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
package io.trino.spi.function;

import io.trino.spi.Experimental;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.util.List;

@Experimental(eta = "2022-10-31")
public interface FunctionDependencies
{
    Type getType(TypeSignature typeSignature);

    FunctionNullability getFunctionNullability(QualifiedFunctionName name, List<Type> parameterTypes);

    FunctionNullability getOperatorNullability(OperatorType operatorType, List<Type> parameterTypes);

    FunctionNullability getCastNullability(Type fromType, Type toType);

    ScalarFunctionImplementation getScalarFunctionImplementation(QualifiedFunctionName name, List<Type> parameterTypes, InvocationConvention invocationConvention);

    ScalarFunctionImplementation getScalarFunctionImplementationSignature(QualifiedFunctionName name, List<TypeSignature> parameterTypes, InvocationConvention invocationConvention);

    ScalarFunctionImplementation getOperatorImplementation(OperatorType operatorType, List<Type> parameterTypes, InvocationConvention invocationConvention);

    ScalarFunctionImplementation getOperatorImplementationSignature(OperatorType operatorType, List<TypeSignature> parameterTypes, InvocationConvention invocationConvention);

    ScalarFunctionImplementation getCastImplementation(Type fromType, Type toType, InvocationConvention invocationConvention);

    ScalarFunctionImplementation getCastImplementationSignature(TypeSignature fromType, TypeSignature toType, InvocationConvention invocationConvention);
}
