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
package io.trino.hive.functions;

import io.trino.spi.function.*;

import javax.inject.Inject;
import java.util.Map;
import java.util.Optional;

public class HiveFunctionProvider implements FunctionProvider {
    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        Optional<Signature> signature = FunctionRegistry.getSignature(boundSignature.getName());
        return getHiveScalarFunctionImplementation(signature);
    }

    private ScalarFunctionImplementation getHiveScalarFunctionImplementation(Signature signature)
    {
        // construct a SqlScalarFunction instance and call `specialize`
        ScalarFunctionImplementation.builder().methodHandle()
        return null;
    }

    public static HiveScalarFunction createHiveScalarFunction(Class<?> cls, QualifiedObjectName name, List<TypeSignature> argumentTypes, TypeManager typeManager)
    {
        HiveScalarFunctionInvoker invoker = createFunctionInvoker(cls, name, argumentTypes, typeManager);
        MethodHandle methodHandle = ScalarMethodHandles.generateUnbound(invoker.getSignature(), typeManager).bindTo(invoker);
        Signature signature = invoker.getSignature();
        FunctionMetadata functionMetadata = new FunctionMetadata(name,
                signature.getArgumentTypes(),
                signature.getReturnType(),
                SCALAR,
                FunctionImplementationType.JAVA,
                true,
                true);
        InvocationConvention invocationConvention = new InvocationConvention(
                signature.getArgumentTypes().stream().map(t -> BOXED_NULLABLE).collect(toImmutableList()),
                InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN,
                false);
        JavaScalarFunctionImplementation implementation = new HiveScalarFunctionImplementation(methodHandle, invocationConvention);
        return new HiveScalarFunction(functionMetadata, signature, name.getObjectName(), implementation);
    }
}
