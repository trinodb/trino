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
package io.prestosql.operator.window;

import com.google.common.collect.Lists;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ValueWindowFunction;
import io.prestosql.spi.function.WindowFunction;
import io.prestosql.spi.type.Type;
import org.apache.bval.jsr.xml.ConstructorType;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.metadata.FunctionKind.WINDOW;
import static java.util.Objects.requireNonNull;

public class ReflectionWindowFunctionSupplier<T extends WindowFunction>
        extends AbstractWindowFunctionSupplier
{
    @SuppressWarnings("unchecked")
    private enum ConstructorType
    {
        NO_INPUTS(0),
        INPUTS(1),
        INPUTS_IGNORE_NULLS(2);

        private int value;
        private static Map map = new HashMap<>();

        ConstructorType(int value)
        {
            this.value = value;
        }

        static {
            for (ConstructorType constructorType : ConstructorType.values()) {
                map.put(constructorType.value, constructorType);
            }
        }

        public static ConstructorType valueOf(int nParameters)
        {
            return (ConstructorType) map.get(nParameters);
        }
    }

    private final Constructor<T> constructor;
    private final ConstructorType constructorType;

    public ReflectionWindowFunctionSupplier(String name, Type returnType, List<? extends Type> argumentTypes, Class<T> type)
    {
        this(new Signature(name, WINDOW, returnType.getTypeSignature(), Lists.transform(argumentTypes, Type::getTypeSignature)), type);
    }

    public ReflectionWindowFunctionSupplier(Signature signature, Class<T> type)
    {
        super(signature, getDescription(requireNonNull(type, "type is null")));
        try {
            Constructor<T> tmpConstructor;

            if (signature.getArgumentTypes().isEmpty()) {
                tmpConstructor = type.getConstructor();
            }
            else if (ValueWindowFunction.class.isAssignableFrom(type)) {
                try {
                    tmpConstructor = type.getConstructor(List.class, boolean.class);
                }
                catch (NoSuchMethodException e) {
                    // Fallback to default constructor.
                    tmpConstructor = type.getConstructor(List.class);
                }
            }
            else {
                tmpConstructor = type.getConstructor(List.class);
            }

            constructor = tmpConstructor;
            constructorType = ConstructorType.valueOf(constructor.getParameterCount());
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected T newWindowFunction(List<Integer> inputs, boolean ignoreNulls)
    {
        try {
            switch (constructorType) {
                case NO_INPUTS:
                    return constructor.newInstance();
                case INPUTS:
                    return constructor.newInstance(inputs);
                case INPUTS_IGNORE_NULLS:
                    return constructor.newInstance(inputs, ignoreNulls);
                default:
                    // This should never happen.
                    throw new RuntimeException("Invalid constructor type.");
            }
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getDescription(AnnotatedElement annotatedElement)
    {
        Description description = annotatedElement.getAnnotation(Description.class);
        return (description == null) ? null : description.value();
    }
}
