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
package io.trino.spi;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import io.trino.spi.connector.ConnectorContext;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.difference;
import static java.lang.ClassLoader.getPlatformClassLoader;
import static java.lang.ClassLoader.getSystemClassLoader;
import static java.lang.reflect.Modifier.isPublic;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSpiBackwardCompatibility
{
    private static final Map<String, Set<String>> BACKWARD_INCOMPATIBLE_CHANGES = ImmutableMap.<String, Set<String>>builder()
            // When updating this map, please try to remove backward incompatible changes for old versions.
            // Also consider mentioning backward incompatible changes in release notes.
            // We try to be backward compatible with at least the last released version.
            .put("123", ImmutableSet.of(// example
                    "Class: public static class io.trino.spi.predicate.BenchmarkSortedRangeSet$Data",
                    "Constructor: public io.trino.spi.predicate.BenchmarkSortedRangeSet$Data()",
                    "Method: public void io.trino.spi.predicate.BenchmarkSortedRangeSet$Data.init()",
                    "Field: public java.util.List<io.trino.spi.predicate.Range> io.trino.spi.predicate.BenchmarkSortedRangeSet$Data.ranges"))
            .put("377", ImmutableSet.of(
                    "Constructor: public io.trino.spi.memory.MemoryPoolInfo(long,long,long,java.util.Map<io.trino.spi.QueryId, java.lang.Long>,java.util.Map<io.trino.spi.QueryId, java.util.List<io.trino.spi.memory.MemoryAllocation>>,java.util.Map<io.trino.spi.QueryId, java.lang.Long>)"))
            .buildOrThrow();

    @Test
    public void testSpiSingleVersionBackwardCompatibility()
            throws Exception
    {
        assertThat(getCurrentSpi()).containsAll(difference(getPreviousSpi(), getBackwardIncompatibleChanges()));
    }

    @Test
    public void testBackwardIncompatibleEntitiesAreInPreviousSpi()
            throws Exception
    {
        assertThat(getPreviousSpi()).containsAll(getBackwardIncompatibleChanges());
    }

    private static Set<String> getBackwardIncompatibleChanges()
    {
        String version = new ConnectorContext() {}.getSpiVersion().replace("-SNAPSHOT", "");
        return BACKWARD_INCOMPATIBLE_CHANGES.getOrDefault(version, ImmutableSet.of());
    }

    private static Set<String> getCurrentSpi()
            throws IOException
    {
        return getSpiEntities(getSystemClassLoader(), true);
    }

    private static Set<String> getPreviousSpi()
            throws Exception
    {
        try (Stream<Path> list = Files.list(Path.of("target", "released-artifacts"))) {
            URL[] jars = list.map(TestSpiBackwardCompatibility::getUrl)
                    .toArray(URL[]::new);
            try (URLClassLoader urlClassLoader = new URLClassLoader(jars, getPlatformClassLoader())) {
                return getSpiEntities(urlClassLoader, false);
            }
        }
    }

    private static URL getUrl(Path path)
    {
        try {
            return path.toUri().toURL();
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<String> getSpiEntities(ClassLoader classLoader, boolean includeDeprecated)
            throws IOException
    {
        ImmutableSet.Builder<String> entities = ImmutableSet.builder();
        for (ClassInfo classInfo : ClassPath.from(classLoader).getTopLevelClassesRecursive("io.trino.spi")) {
            Class<?> clazz = classInfo.load();
            addClassEntities(entities, clazz, includeDeprecated);
        }
        return entities.build();
    }

    private static void addClassEntities(ImmutableSet.Builder<String> entities, Class<?> clazz, boolean includeDeprecated)
    {
        if (!isPublic(clazz.getModifiers())) {
            return;
        }
        entities.add("Class: " + clazz.toGenericString());
        for (Class<?> nestedClass : clazz.getDeclaredClasses()) {
            addClassEntities(entities, nestedClass, includeDeprecated);
        }
        if (!includeDeprecated && clazz.isAnnotationPresent(Deprecated.class)) {
            return;
        }
        for (Constructor<?> constructor : clazz.getConstructors()) {
            if (!includeDeprecated && constructor.isAnnotationPresent(Deprecated.class)) {
                continue;
            }
            entities.add("Constructor: " + constructor.toGenericString());
        }
        for (Method method : clazz.getDeclaredMethods()) {
            if (!isPublic(method.getModifiers())) {
                continue;
            }
            if (!includeDeprecated && method.isAnnotationPresent(Deprecated.class)) {
                continue;
            }
            entities.add("Method: " + method.toGenericString());
        }
        for (Field field : clazz.getDeclaredFields()) {
            if (!isPublic(field.getModifiers())) {
                continue;
            }
            if (!includeDeprecated && field.isAnnotationPresent(Deprecated.class)) {
                continue;
            }
            entities.add("Field: " + field.toGenericString());
        }
    }
}
