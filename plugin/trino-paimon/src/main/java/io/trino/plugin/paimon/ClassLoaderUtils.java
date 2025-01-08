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
package io.trino.plugin.paimon;

import java.util.function.Supplier;

/**
 * Utils for {@link ClassLoader}.
 */
public final class ClassLoaderUtils
{
    private ClassLoaderUtils() {}

    public static <T> T runWithContextClassLoader(Supplier<T> supplier, ClassLoader classLoader)
    {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            return supplier.get();
        }
        finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
