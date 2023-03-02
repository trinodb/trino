/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.airlift.bytecode.DynamicClassLoader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

/**
 * Based on io.trino.sql.gen.IsolatedClass
 */
public final class IsolatedClass
{
    private IsolatedClass() {}

    public static <T> Class<? extends T> isolateClass(
            DynamicClassLoader dynamicClassLoader,
            Class<T> publicBaseClass,
            Class<? extends T> implementationClass,
            Class<?>... additionalClasses)
    {
        ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
        builder.put(implementationClass.getName(), getBytecode(implementationClass));
        for (Class<?> additionalClass : additionalClasses) {
            builder.put(additionalClass.getName(), getBytecode(additionalClass));
        }

        // load classes into a private class loader
        Map<String, Class<?>> isolatedClasses = dynamicClassLoader.defineClasses(builder.buildOrThrow());
        Class<?> isolatedClass = isolatedClasses.get(implementationClass.getName());

        // verify the isolated class
        checkArgument(isolatedClass != null, "Could load class %s", implementationClass.getName());
        checkArgument(publicBaseClass.isAssignableFrom(isolatedClass),
                "Error isolating class %s, newly loaded class is not a sub type of %s",
                implementationClass.getName(),
                publicBaseClass.getName());
        checkState(isolatedClass != implementationClass, "Isolation failed");

        return isolatedClass.asSubclass(publicBaseClass);
    }

    private static byte[] getBytecode(Class<?> clazz)
    {
        try (InputStream stream = clazz.getClassLoader().getResourceAsStream(clazz.getName().replace('.', '/') + ".class")) {
            checkArgument(stream != null, "Could not obtain byte code for class %s", clazz.getName());
            return ByteStreams.toByteArray(stream);
        }
        catch (IOException e) {
            throw new RuntimeException(format("Could not obtain byte code for class %s", clazz.getName()), e);
        }
    }
}
