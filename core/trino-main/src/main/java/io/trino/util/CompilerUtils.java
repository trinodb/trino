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
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.log.Logger;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.bytecode.BytecodeUtils.toJavaIdentifierString;
import static io.airlift.bytecode.ClassGenerator.classGenerator;
import static io.airlift.bytecode.HiddenClassGenerator.hiddenClassGenerator;
import static io.airlift.bytecode.ParameterizedType.typeFromJavaClassName;
import static java.time.ZoneOffset.UTC;

public final class CompilerUtils
{
    private static final Logger log = Logger.get(CompilerUtils.class);

    private static final AtomicLong CLASS_ID = new AtomicLong();
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private CompilerUtils() {}

    public static ParameterizedType makeHiddenClassName(Lookup lookup, String baseName)
    {
        return makeHiddenClassName(lookup, baseName, Optional.empty());
    }

    public static ParameterizedType makeHiddenClassName(Lookup lookup, String baseName, Optional<String> suffix)
    {
        String className = baseName + "_" + suffix.orElseGet(() -> Instant.now().atZone(UTC).format(TIMESTAMP_FORMAT));
        String packageName = lookup.lookupClass().getPackage().getName();
        return typeFromJavaClassName(packageName + "." + toJavaIdentifierString(className));
    }

    public static ParameterizedType makeClassName(String baseName)
    {
        String className = baseName
                + "_" + Optional.<String>empty().orElseGet(() -> Instant.now().atZone(UTC).format(TIMESTAMP_FORMAT))
                + "_" + CLASS_ID.incrementAndGet();
        return typeFromJavaClassName("io.trino.$gen." + toJavaIdentifierString(className));
    }

    public static <T> Class<? extends T> defineClass(Lookup lookup, ClassDefinition classDefinition, Class<T> superType)
    {
        return defineClass(lookup, classDefinition, superType, ImmutableList.of());
    }

    public static <T> Class<? extends T> defineClass(Lookup lookup, ClassDefinition classDefinition, Class<T> superType, List<Object> constants)
    {
        log.debug("Defining class: %s", classDefinition.getName());
        ImmutableMap.Builder<Long, MethodHandle> bindings = ImmutableMap.builder();
        for (int i = 0; i < constants.size(); i++) {
            // DynamicClassLoader only supports MethodHandles, so wrapper constants in a method handle
            bindings.put((long) i, MethodHandles.constant(Object.class, constants.get(i)));
        }
        DynamicClassLoader classLoader = new DynamicClassLoader(lookup.lookupClass().getClassLoader(), bindings.buildOrThrow());
        return classGenerator(classLoader).defineClass(classDefinition, superType);
    }

    public static <T> Class<? extends T> defineHiddenClass(Lookup lookup, ClassDefinition classDefinition, Class<T> superType)
    {
        log.debug("Defining hidden class: %s", classDefinition.getName());
        return hiddenClassGenerator(lookup).defineHiddenClass(classDefinition, superType, Optional.empty());
    }

    public static <T> Class<? extends T> defineHiddenClass(Lookup lookup, ClassDefinition classDefinition, Class<T> superType, List<Object> constants)
    {
        log.debug("Defining hidden class: %s", classDefinition.getName());
        return hiddenClassGenerator(lookup).defineHiddenClass(classDefinition, superType, Optional.of(constants));
    }
}
