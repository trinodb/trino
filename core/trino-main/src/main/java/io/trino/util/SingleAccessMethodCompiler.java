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
package io.trino.util;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.SYNTHETIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.trino.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.lang.invoke.MethodType.methodType;

public final class SingleAccessMethodCompiler
{
    private SingleAccessMethodCompiler() {}

    // Note: this currently only handles interfaces, and has no mechanism to declare generic types.
    public static <T> T compileSingleAccessMethod(String suggestedClassName, Class<T> interfaceType, MethodHandle methodHandle)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL, SYNTHETIC),
                makeClassName(suggestedClassName),
                type(Object.class),
                type(interfaceType));

        classDefinition.declareDefaultConstructor(a(PUBLIC));

        Method method = getSingleAbstractMethod(interfaceType);
        Class<?>[] parameterTypes = method.getParameterTypes();
        MethodHandle adaptedMethodHandle = methodHandle.asType(methodType(method.getReturnType(), parameterTypes));

        List<io.airlift.bytecode.Parameter> parameters = new ArrayList<>();
        for (int i = 0; i < parameterTypes.length; i++) {
            parameters.add(arg("arg" + i, parameterTypes[i]));
        }

        MethodDefinition methodDefinition = classDefinition.declareMethod(
                a(PUBLIC),
                method.getName(),
                type(method.getReturnType()),
                parameters);

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        BytecodeExpression invocation = invokeDynamic(
                BOOTSTRAP_METHOD,
                ImmutableList.of(callSiteBinder.bind(adaptedMethodHandle).getBindingId()),
                method.getName(),
                method.getReturnType(),
                parameters);
        if (method.getReturnType() != void.class) {
            invocation = invocation.ret();
        }
        methodDefinition.getBody().append(invocation);
        // note this will not work if interface class is not visible from this class loader,
        // but we must use this class loader to ensure the bootstrap method is visible
        ClassLoader classLoader = SingleAccessMethodCompiler.class.getClassLoader();
        Class<? extends T> newClass = defineClass(classDefinition, interfaceType, callSiteBinder.getBindings(), classLoader);
        try {
            return newClass
                    .getDeclaredConstructor()
                    .newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> Method getSingleAbstractMethod(Class<T> interfaceType)
    {
        List<Method> interfaceMethods = Arrays.stream(interfaceType.getMethods())
                .filter(m -> Modifier.isAbstract(m.getModifiers()))
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .filter(SingleAccessMethodCompiler::notJavaObjectMethod)
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
