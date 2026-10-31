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

package io.trino.plugin.faker;

import com.google.inject.Inject;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionBundle;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;

import java.lang.invoke.MethodHandle;
import java.util.Locale;

import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

public class FakerFunctionProvider
        implements FunctionProvider
{
    private static final MethodHandle FAKER_FUNCTIONS_CONSTRUCTOR;

    static {
        try {
            FAKER_FUNCTIONS_CONSTRUCTOR = lookup().findConstructor(FakerFunctions.class, methodType(void.class, Locale.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final FunctionProvider delegate;
    private final MethodHandle instanceFactory;

    @Inject
    public FakerFunctionProvider(FunctionBundle functionBundle, FakerConfig config)
    {
        delegate = functionBundle.toProvider();
        MethodHandle constructor = insertArguments(FAKER_FUNCTIONS_CONSTRUCTOR, 0, config.getLocale());
        instanceFactory = constructor.asType(constructor.type().changeReturnType(Object.class));
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        ScalarFunctionImplementation implementation = delegate.getScalarFunctionImplementation(functionId, boundSignature, functionDependencies, invocationConvention);
        // Function bundles create instances using a constructor without arguments, so replace it to pass the configured locale
        return ScalarFunctionImplementation.builder()
                .methodHandle(implementation.getMethodHandle())
                .instanceFactory(implementation.getInstanceFactory().map(_ -> instanceFactory))
                .lambdaInterfaces(implementation.getLambdaInterfaces())
                .build();
    }
}
