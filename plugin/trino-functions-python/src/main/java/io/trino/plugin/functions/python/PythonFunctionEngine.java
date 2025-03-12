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
package io.trino.plugin.functions.python;

import io.trino.spi.TrinoException;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.LanguageFunctionEngine;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.trino.plugin.functions.python.TrinoTypes.validateReturnType;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_PROPERTY;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;

final class PythonFunctionEngine
        implements LanguageFunctionEngine
{
    private static final MethodHandle FACTORY_METHOD;
    private static final MethodHandle EXECUTE_METHOD;

    static {
        try {
            FACTORY_METHOD = lookup().findVirtual(Supplier.class, "get", methodType(Object.class));
            EXECUTE_METHOD = lookup().findVirtual(PythonEngine.class, "execute", methodType(Object.class, Object[].class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getLanguage()
    {
        return "PYTHON";
    }

    @Override
    public List<PropertyMetadata<?>> getFunctionProperties()
    {
        return List.of(stringProperty("handler", "Name of the Python method to call", "", false));
    }

    @Override
    public void validateScalarFunction(Type returnType, List<Type> argumentTypes, String definition, Map<String, Object> properties)
    {
        validateReturnType(returnType);

        String code = definition.stripIndent();

        String handler = (String) properties.get("handler");
        if (handler.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_PROPERTY, "Property 'handler' is required");
        }

        try (PythonEngine engine = new PythonEngine(code)) {
            engine.setup(returnType, argumentTypes, handler);
        }
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            Type returnType,
            List<Type> argumentTypes,
            String definition,
            Map<String, Object> properties,
            InvocationConvention invocationConvention)
    {
        String code = definition.stripIndent();
        String handler = (String) properties.get("handler");

        Supplier<Object> factory = () -> createEngine(returnType, argumentTypes, code, handler);

        List<Class<?>> types = new ArrayList<>();
        types.add(Object.class);
        for (Type type : argumentTypes) {
            types.add(type.getJavaType());
        }
        MethodType methodType = methodType(returnType.getJavaType(), types).wrap();

        MethodHandle target = EXECUTE_METHOD
                .asCollector(1, Object[].class, argumentTypes.size())
                .asType(methodType);

        InvocationConvention callingConvention = new InvocationConvention(
                nCopies(argumentTypes.size(), BOXED_NULLABLE),
                NULLABLE_RETURN,
                false,
                true);

        MethodHandle adapted = ScalarFunctionAdapter.adapt(
                target,
                returnType,
                argumentTypes,
                callingConvention,
                invocationConvention);

        return ScalarFunctionImplementation.builder()
                .methodHandle(adapted)
                .instanceFactory(FACTORY_METHOD.bindTo(factory))
                .build();
    }

    private static PythonEngine createEngine(Type returnType, List<Type> argumentTypes, String code, String handler)
    {
        PythonEngine engine = new PythonEngine(code);
        engine.setup(returnType, argumentTypes, handler);
        return engine;
    }
}
