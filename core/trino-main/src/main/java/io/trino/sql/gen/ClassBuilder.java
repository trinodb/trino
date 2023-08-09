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
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.instruction.BootstrapMethod;
import io.airlift.log.Logger;

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
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.ParameterizedType.typeFromJavaClassName;
import static io.airlift.bytecode.SingleClassGenerator.declareStandardClassDataAtBootstrapMethod;
import static io.airlift.bytecode.SingleClassGenerator.singleClassGenerator;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantDynamic;
import static java.time.Instant.now;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public final class ClassBuilder
{
    private static final Logger log = Logger.get(ClassBuilder.class);
    private static final Method HIDDEN_CLASS_BOOTSTRAP_METHOD;
    private static final AtomicLong CLASS_ID = new AtomicLong();
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    static {
        try {
            HIDDEN_CLASS_BOOTSTRAP_METHOD = MethodHandles.class.getMethod("classDataAt", Lookup.class, String.class, Class.class, int.class);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private final Lookup lookup;
    private final ClassDefinition definition;
    private final boolean hiddenClass;

    private BootstrapMethod bootstrapMethod;
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

        if (hiddenClass) {
            bootstrapMethod = BootstrapMethod.from(HIDDEN_CLASS_BOOTSTRAP_METHOD);
        }
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
        // bootstrap method is only added if necessary
        if (bootstrapMethod == null) {
            this.bootstrapMethod = declareStandardClassDataAtBootstrapMethod(definition);
        }

        int binding = bind(constant);
        return constantDynamic(
                "_",
                type(type),
                bootstrapMethod,
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
        if (hiddenClass) {
            return singleClassGenerator(lookup).defineHiddenClass(definition, superType, Optional.of(bindings));
        }
        return singleClassGenerator(lookup).defineStandardClass(definition, superType, Optional.of(bindings));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nextId", nextId)
                .add("bindings", bindings)
                .toString();
    }
}
