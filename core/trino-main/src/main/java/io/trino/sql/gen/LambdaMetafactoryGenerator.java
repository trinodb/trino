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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.Access;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.expression.BytecodeExpression;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static org.objectweb.asm.Type.getMethodType;
import static org.objectweb.asm.Type.getType;

public final class LambdaMetafactoryGenerator
{
    private static final Method METAFACTORY;

    static {
        try {
            METAFACTORY = LambdaMetafactory.class.getMethod("metafactory", MethodHandles.Lookup.class, String.class, MethodType.class, MethodType.class, MethodHandle.class, MethodType.class);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private LambdaMetafactoryGenerator() {}

    public static <T> BytecodeExpression generateMetafactory(Class<T> interfaceType, MethodDefinition targetMethod, List<BytecodeExpression> additionalArguments)
    {
        Method interfaceMethod = getSingleAbstractMethod(interfaceType);

        // verify target method has signature of additionalArguments + interfaceMethod
        List<ParameterizedType> expectedTypes = new ArrayList<>();
        if (targetMethod.getAccess().contains(Access.STATIC)) {
            additionalArguments.forEach(argument -> expectedTypes.add(argument.getType()));
        }
        else {
            checkArgument(!additionalArguments.isEmpty() && additionalArguments.get(0).getType().equals(targetMethod.getDeclaringClass().getType()),
                    "Expected first additional argument to be 'this' for non-static method");
            additionalArguments
                    .subList(1, additionalArguments.size())
                    .forEach(argument -> expectedTypes.add(argument.getType()));
        }
        Arrays.stream(interfaceMethod.getParameterTypes()).forEach(type -> expectedTypes.add(type(type)));
        checkArgument(expectedTypes.equals(targetMethod.getParameterTypes()),
                "Expected target method to have parameter types %s, but has %s", expectedTypes, targetMethod.getParameterTypes());

        Type interfaceMethodType = toMethodType(interfaceMethod);
        return invokeDynamic(
                METAFACTORY,
                ImmutableList.of(
                        interfaceMethodType,
                        new Handle(
                                targetMethod.getAccess().contains(Access.STATIC) ? Opcodes.H_INVOKESTATIC : Opcodes.H_INVOKEVIRTUAL,
                                targetMethod.getDeclaringClass().getName(),
                                targetMethod.getName(),
                                targetMethod.getMethodDescriptor(),
                                false),
                        interfaceMethodType),
                "build",
                type(interfaceType),
                additionalArguments);
    }

    private static Type toMethodType(Method interfaceMethod)
    {
        return getMethodType(
                getType(interfaceMethod.getReturnType()),
                Arrays.stream(interfaceMethod.getParameterTypes()).map(Type::getType).toArray(Type[]::new));
    }

    private static <T> Method getSingleAbstractMethod(Class<T> interfaceType)
    {
        List<Method> interfaceMethods = Arrays.stream(interfaceType.getMethods())
                .filter(m -> Modifier.isAbstract(m.getModifiers()))
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .filter(LambdaMetafactoryGenerator::notJavaObjectMethod)
                .collect(toImmutableList());
        if (interfaceMethods.size() != 1) {
            throw new IllegalArgumentException(interfaceType.getSimpleName() + "  does not have a single abstract method");
        }
        return interfaceMethods.get(0);
    }

    private static boolean notJavaObjectMethod(Method method)
    {
        return !methodMatches(method, "toString", String.class) &&
                !methodMatches(method, "hashCode", int.class) &&
                !methodMatches(method, "equals", boolean.class, Object.class);
    }

    private static boolean methodMatches(Method method, String name, Class<?> returnType, Class<?>... parameterTypes)
    {
        return method.getParameterCount() == parameterTypes.length &&
                method.getReturnType() == returnType &&
                name.equals(method.getName()) &&
                Arrays.equals(method.getParameterTypes(), parameterTypes);
    }
}
