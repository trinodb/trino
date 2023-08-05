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
package io.trino.version;

import com.google.inject.Inject;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.trino.client.NodeVersion;
import io.trino.spi.VersionEmbedder;
import io.trino.sql.gen.ClassBuilder;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.Callable;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.trino.util.Reflection.constructorMethodHandle;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class EmbedVersion
        implements VersionEmbedder
{
    private final MethodHandle runnableConstructor;
    private final MethodHandle callableConstructor;

    @Inject
    public EmbedVersion(NodeVersion version)
    {
        this(version.getVersion());
    }

    public EmbedVersion(String version)
    {
        Class<?> generatedClass = createClass(format("Trino_%s___", version));
        this.runnableConstructor = constructorMethodHandle(generatedClass, Runnable.class);
        this.callableConstructor = constructorMethodHandle(generatedClass, Callable.class);
    }

    private static Class<?> createClass(String baseClassName)
    {
        ClassBuilder classBuilder = ClassBuilder.createStandardClass(
                lookup(),
                a(PUBLIC, FINAL),
                baseClassName,
                type(Object.class),
                type(Runnable.class),
                type(Callable.class));

        implementRunnable(classBuilder);
        implementCallable(classBuilder);

        return classBuilder.defineClass(Runnable.class);
    }

    private static void implementRunnable(ClassBuilder classBuilder)
    {
        FieldDefinition field = classBuilder.declareField(a(PRIVATE), "runnable", Runnable.class);

        Parameter parameter = arg("runnable", type(Runnable.class));
        MethodDefinition constructor = classBuilder.declareConstructor(a(PUBLIC), parameter);
        constructor.getBody()
                .comment("super();")
                .append(constructor.getThis())
                .invokeConstructor(Object.class)
                .append(constructor.getThis())
                .append(parameter)
                .putField(field)
                .ret();

        MethodDefinition run = classBuilder.declareMethod(a(PUBLIC), "run", type(void.class));
        run.getBody()
                .comment("runnable.run();")
                .append(run.getThis())
                .getField(field)
                .invokeInterface(Runnable.class, "run", void.class)
                .ret();
    }

    private static void implementCallable(ClassBuilder classBuilder)
    {
        FieldDefinition field = classBuilder.declareField(a(PRIVATE), "callable", Callable.class);

        Parameter parameter = arg("callable", type(Callable.class));
        MethodDefinition constructor = classBuilder.declareConstructor(a(PUBLIC), parameter);
        constructor.getBody()
                .comment("super();")
                .append(constructor.getThis())
                .invokeConstructor(Object.class)
                .append(constructor.getThis())
                .append(parameter)
                .putField(field)
                .ret();

        MethodDefinition run = classBuilder.declareMethod(a(PUBLIC), "call", type(Object.class));
        run.getBody()
                .comment("callable.call();")
                .append(run.getThis())
                .getField(field)
                .invokeInterface(Callable.class, "call", Object.class)
                .ret(Object.class);
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
