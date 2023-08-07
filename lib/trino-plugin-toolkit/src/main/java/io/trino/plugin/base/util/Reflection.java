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
package io.trino.plugin.base.util;

import io.trino.spi.TrinoException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/**
 * @apiNote This mirrors {@code io.trino.util.Reflection}.
 */
public final class Reflection
{
    private Reflection() {}

    /**
     * Returns a MethodHandle corresponding to the specified method.
     * <p>
     * Warning: The way Oracle JVM implements producing MethodHandle for a method involves creating
     * JNI global weak references. G1 processes such references serially. As a result, calling this
     * method in a tight loop can create significant GC pressure and significantly increase
     * application pause time.
     */
    public static MethodHandle methodHandle(Class<?> clazz, String name, Class<?>... parameterTypes)
    {
        try {
            return MethodHandles.lookup().unreflect(clazz.getMethod(name, parameterTypes));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
