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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.aggregation.LambdaProvider;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ValueWindowFunction;
import io.prestosql.spi.function.WindowFunction;
import io.prestosql.spi.type.Type;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ReflectionWindowFunctionSupplier<T extends WindowFunction>
        extends AbstractWindowFunctionSupplier
{
    private enum ConstructorType
    {
        NO_INPUTS,
        INPUTS,
        INPUTS_IGNORE_NULLS;
    }

    private final Constructor<T> constructor;
    private final ConstructorType constructorType;

    public ReflectionWindowFunctionSupplier(String name, Type returnType, List<? extends Type> argumentTypes, Class<T> type)
    {
        this(new Signature(name, returnType.getTypeSignature(), Lists.transform(argumentTypes, Type::getTypeSignature)), type);
    }

    public ReflectionWindowFunctionSupplier(Signature signature, Class<T> type)
    {
        super(signature, getDescription(requireNonNull(type, "type is null")), ImmutableList.of());
        try {
            Constructor<T> constructor;
            ConstructorType constructorType;

            if (signature.getArgumentTypes().isEmpty()) {
                constructor = type.getConstructor();
                constructorType = ConstructorType.NO_INPUTS;
            }
            else if (ValueWindowFunction.class.isAssignableFrom(type)) {
                try {
                    constructor = type.getConstructor(List.class, boolean.class);
                    constructorType = ConstructorType.INPUTS_IGNORE_NULLS;
                }
                catch (NoSuchMethodException e) {
                    // Fallback to default constructor.
                    constructor = type.getConstructor(List.class);
                    constructorType = ConstructorType.INPUTS;
                }
            }
            else {
                constructor = type.getConstructor(List.class);
                constructorType = ConstructorType.INPUTS;
            }

            this.constructor = constructor;
            this.constructorType = constructorType;
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected T newWindowFunction(List<Integer> inputs, boolean ignoreNulls, List<LambdaProvider> lambdaProviders)
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
                    throw new VerifyException("Unhandled constructor type: " + constructorType);
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
