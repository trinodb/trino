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

import com.google.inject.Inject;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.trino.spi.NodeVersion;
import io.trino.spi.VersionEmbedder;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.trino.util.CompilerUtils.defineNamedClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Reflection.constructorMethodHandle;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class EmbedVersion
        implements VersionEmbedder
{
    private final MethodHandle runnableConstructor;
    private final MethodHandle callableConstructor;

    @Inject
    public EmbedVersion(NodeVersion version)
    {
        this(version.version());
    }

    // the version classes are named, and a name can only be defined once in the shared
    // generated class loader, so instances with the same version share the classes
    private static final ConcurrentMap<String, VersionClasses> CLASSES = new ConcurrentHashMap<>();

    private record VersionClasses(Class<?> runnableClass, Class<?> callableClass) {}

    public EmbedVersion(String version)
    {
        VersionClasses classes = CLASSES.computeIfAbsent(version, EmbedVersion::createClasses);
        this.runnableConstructor = constructorMethodHandle(classes.runnableClass(), Runnable.class);
        this.callableConstructor = constructorMethodHandle(classes.callableClass(), Callable.class);
    }

    private static VersionClasses createClasses(String version)
    {
        return new VersionClasses(
                createWrapperClass(format("Trino_%s___Runnable", version), Runnable.class, "run", void.class),
                createWrapperClass(format("Trino_%s___Callable", version), Callable.class, "call", Object.class));
    }

    private static Class<?> createWrapperClass(String baseClassName, Class<?> interfaceType, String methodName, Class<?> returnType)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(baseClassName),
                type(Object.class),
                type(interfaceType));

        FieldDefinition field = classDefinition.declareField(a(PRIVATE, FINAL), "delegate", interfaceType);

        Parameter parameter = arg("delegate", type(interfaceType));
        MethodDefinition constructor = classDefinition.declareConstructor(a(PUBLIC), parameter);
        constructor.getBody()
                .comment("super();")
                .append(constructor.getThis())
                .invokeConstructor(Object.class)
                .append(constructor.getThis())
                .append(parameter)
                .putField(field)
                .ret();

        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), methodName, type(returnType));
        method.getBody()
                .comment("delegate.%s();", methodName)
                .append(method.getThis())
                .getField(field)
                .invokeInterface(interfaceType, methodName, returnType)
                .ret(returnType);

        // defined as a named class: the version bearing name must stay visible in stack
        // traces, which hidden class frames are not
        return defineNamedClass(classDefinition, interfaceType);
    }

    @Override
    public Runnable embedVersion(Runnable runnable)
    {
        requireNonNull(runnable, "runnable is null");
        try {
            return (Runnable) runnableConstructor.invoke(runnable);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    @Override
    public <T> Callable<T> embedVersion(Callable<T> callable)
    {
        requireNonNull(callable, "callable is null");
        try {
            @SuppressWarnings("unchecked")
            Callable<T> wrapped = (Callable<T>) callableConstructor.invoke(callable);
            return wrapped;
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public static EmbedVersion testingVersionEmbedder()
    {
        return new EmbedVersion("testversion");
    }
}
