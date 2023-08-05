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
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.Access;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.log.Logger;
import io.trino.annotation.UsedByGeneratedCode;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.bytecode.BytecodeUtils.toJavaIdentifierString;
import static io.airlift.bytecode.ClassGenerator.classGenerator;
import static io.airlift.bytecode.HiddenClassGenerator.hiddenClassGenerator;
import static io.airlift.bytecode.ParameterizedType.typeFromJavaClassName;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantDynamic;
import static java.time.Instant.now;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public final class ClassBuilder
{
    private static final Logger log = Logger.get(ClassBuilder.class);
    private static final Method BOOTSTRAP_METHOD;
    private static final AtomicLong CLASS_ID = new AtomicLong();
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private final boolean hiddenClass;

    static {
        try {
            BOOTSTRAP_METHOD = ClassBuilder.class.getMethod("bootstrap", Lookup.class, String.class, Class.class, int.class);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private final Lookup lookup;
    private final ClassDefinition definition;
    private int nextId;

    private final List<Object> bindings = new ArrayList<>();

    public static ClassBuilder createStandardClass(
            Lookup lookup,
            EnumSet<Access> access,
            String name,
            ParameterizedType superClass,
            ParameterizedType... interfaces)
    {
        String generatedClassName = name + "_" + now().atZone(UTC).format(TIMESTAMP_FORMAT) + "_" + CLASS_ID.incrementAndGet();
        return new ClassBuilder(
                lookup,
                new ClassDefinition(
                        access,
                        typeFromJavaClassName("io.trino.$gen." + toJavaIdentifierString(generatedClassName)),
                        superClass,
                        interfaces),
                false);
    }

    public static ClassBuilder createHiddenClass(
            Lookup lookup,
            EnumSet<Access> access,
            String name,
            ParameterizedType superClass,
            ParameterizedType... interfaces)
    {
        String generatedClassName = toJavaIdentifierString(name + "_" + now().atZone(UTC).format(TIMESTAMP_FORMAT) + "_" + CLASS_ID.incrementAndGet());
        String packageName = lookup.lookupClass().getPackage().getName();
        return new ClassBuilder(
                lookup,
                new ClassDefinition(
                        access,
                        typeFromJavaClassName(packageName + "." + toJavaIdentifierString(generatedClassName)),
                        superClass,
                        interfaces),
                true);
    }

    private ClassBuilder(Lookup lookup, ClassDefinition definition, boolean hiddenClass)
    {
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.definition = requireNonNull(definition, "definition is null");
        this.hiddenClass = hiddenClass;
    }

    public ParameterizedType getType()
    {
        return definition.getType();
    }

    public FieldDefinition declareField(EnumSet<Access> access, String name, Class<?> type)
    {
        return definition.declareField(access, name, type);
    }

    public FieldDefinition declareField(EnumSet<Access> access, String name, ParameterizedType type)
    {
        return definition.declareField(access, name, type);
    }

    public MethodDefinition getClassInitializer()
    {
        return definition.getClassInitializer();
    }

    public MethodDefinition declareConstructor(EnumSet<Access> access, Parameter... parameters)
    {
        return definition.declareConstructor(access, parameters);
    }

    public MethodDefinition declareConstructor(EnumSet<Access> access, Iterable<Parameter> parameters)
    {
        return definition.declareConstructor(access, parameters);
    }

    public ClassDefinition declareDefaultConstructor(EnumSet<Access> access)
    {
        return definition.declareDefaultConstructor(access);
    }

    public MethodDefinition declareMethod(EnumSet<Access> access, String name, ParameterizedType returnType, Parameter... parameters)
    {
        return definition.declareMethod(access, name, returnType, parameters);
    }

    public MethodDefinition declareMethod(EnumSet<Access> access, String name, ParameterizedType returnType, Iterable<Parameter> parameters)
    {
        return definition.declareMethod(access, name, returnType, parameters);
    }

    public BytecodeExpression loadConstant(Object constant, Class<?> type)
    {
        int binding = bind(constant);
        return constantDynamic(
                "_",
                type,
                BOOTSTRAP_METHOD,
                ImmutableList.of(binding));
    }

    public BytecodeExpression invoke(MethodHandle method, String name, BytecodeExpression... parameters)
    {
        return invoke(method, name, ImmutableList.copyOf(parameters));
    }

    public BytecodeExpression invoke(MethodHandle method, String name, List<? extends BytecodeExpression> parameters)
    {
        checkArgument(
                method.type().parameterCount() == parameters.size(),
                "Method requires %s parameters, but only %s supplied",
                method.type().parameterCount(),
                parameters.size());

        return loadConstant(method, MethodHandle.class).invoke("invoke", method.type().returnType(), parameters);
    }

    private int bind(Object constant)
    {
        requireNonNull(constant, "constant is null");

        int bindingId = nextId++;

        verify(bindingId == bindings.size());
        bindings.add(constant);

        return bindingId;
    }

    public Class<?> defineClass()
    {
        return defineClass(Object.class);
    }

    public <T> Class<? extends T> defineClass(Class<T> superType)
    {
        log.debug("Defining class: %s", definition.getName());
        Class<?> clazz;
        if (hiddenClass) {
            clazz = hiddenClassGenerator(lookup).defineHiddenClass(definition, Object.class, Optional.of(bindings));
        }
        else {
            ImmutableMap.Builder<Long, MethodHandle> bindingsMap = ImmutableMap.builder();
            for (int i = 0; i < bindings.size(); i++) {
                // DynamicClassLoader only supports MethodHandles, so wrapper constants in a method handle
                bindingsMap.put((long) i, MethodHandles.constant(Object.class, bindings.get(i)));
            }
            DynamicClassLoader classLoader = new DynamicClassLoader(lookup.lookupClass().getClassLoader(), bindingsMap.buildOrThrow());
            clazz = classGenerator(classLoader).defineClass(definition, Object.class);
        }
        return clazz.asSubclass(superType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nextId", nextId)
                .add("bindings", bindings)
                .toString();
    }

    @UsedByGeneratedCode
    public static Object bootstrap(Lookup callerLookup, String name, Class<?> type, int bindingId)
    {
        try {
            Object data = MethodHandles.classDataAt(callerLookup, name, type, bindingId);
            if (data != null) {
                return data;
            }
        }
        catch (IllegalAccessException ignored) {
        }

        ClassLoader classLoader = callerLookup.lookupClass().getClassLoader();
        checkArgument(classLoader instanceof DynamicClassLoader, "Expected %s's classloader to be of type %s", callerLookup.lookupClass().getName(), DynamicClassLoader.class.getName());

        DynamicClassLoader dynamicClassLoader = (DynamicClassLoader) classLoader;
        MethodHandle target = dynamicClassLoader.getCallSiteBindings().get((long) bindingId);
        checkArgument(target != null, "Binding %s for constant %s with type %s not found", bindingId, name, type);

        try {
            return target.invoke();
        }
        catch (Throwable e) {
            throw new RuntimeException("Error loading value for constant %s with type %s not found".formatted(name, type));
        }
    }
}
