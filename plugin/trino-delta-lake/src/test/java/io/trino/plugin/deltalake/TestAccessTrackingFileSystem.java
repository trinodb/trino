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
package io.trino.plugin.deltalake;

import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class TestAccessTrackingFileSystem
{
    @Test
    public void testAllMethodsOverridden()
    {
        Set<Method> objectMethods = Arrays.stream(Object.class.getMethods()).collect(toImmutableSet());

        Set<Method> excludeMethods = Arrays.stream(FileSystem.class.getMethods())
                .filter(method -> Modifier.isProtected(method.getModifiers()) || Modifier.isFinal(method.getModifiers()))
                .filter(method -> !objectMethods.contains(method))
                .collect(toImmutableSet());

        assertAllMethodsOverridden(FileSystem.class, AccessTrackingFileSystem.class, excludeMethods);
    }
}
