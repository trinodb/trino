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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
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
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static java.lang.ClassLoader.getPlatformClassLoader;
import static java.lang.ClassLoader.getSystemClassLoader;
import static java.lang.reflect.Modifier.isPublic;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSpiBackwardCompatibility
{
    private static final SetMultimap<String, String> BACKWARD_INCOMPATIBLE_CHANGES = ImmutableSetMultimap.<String, String>builder()
            // When updating this map, please try to remove backward incompatible changes for old versions.
            // Also consider mentioning backward incompatible changes in release notes.
            // We try to be backward compatible with at least the last released version.
            // example
            .put("123", "Class: public static class io.trino.spi.predicate.BenchmarkSortedRangeSet$Data")
            // example
            .put("123", "Constructor: public io.trino.spi.predicate.BenchmarkSortedRangeSet$Data()")
            // example
            .put("123", "Method: public void io.trino.spi.predicate.BenchmarkSortedRangeSet$Data.init()")
            // example
            .put("123", "Field: public java.util.List<io.trino.spi.predicate.Range> io.trino.spi.predicate.BenchmarkSortedRangeSet$Data.ranges")
            .put("377", "Constructor: public io.trino.spi.memory.MemoryPoolInfo(long,long,long,java.util.Map<io.trino.spi.QueryId, java.lang.Long>,java.util.Map<io.trino.spi.QueryId, java.util.List<io.trino.spi.memory.MemoryAllocation>>,java.util.Map<io.trino.spi.QueryId, java.lang.Long>)")
            .put("382", "Method: public io.trino.spi.ptf.TableArgumentSpecification$Builder io.trino.spi.ptf.TableArgumentSpecification$Builder.rowSemantics(boolean)")
            .put("382", "Method: public io.trino.spi.ptf.TableArgumentSpecification$Builder io.trino.spi.ptf.TableArgumentSpecification$Builder.pruneWhenEmpty(boolean)")
            .put("382", "Method: public io.trino.spi.ptf.TableArgumentSpecification$Builder io.trino.spi.ptf.TableArgumentSpecification$Builder.passThroughColumns(boolean)")
            .put("382", "Class: public abstract class io.trino.spi.ptf.ConnectorTableFunction")
            .put("382", "Constructor: public io.trino.spi.ptf.ConnectorTableFunction(java.lang.String,java.lang.String,java.util.List<io.trino.spi.ptf.ArgumentSpecification>,io.trino.spi.ptf.ReturnTypeSpecification)")
            .put("382", "Method: public java.util.List<io.trino.spi.ptf.ArgumentSpecification> io.trino.spi.ptf.ConnectorTableFunction.getArguments()")
            .put("382", "Method: public io.trino.spi.ptf.ReturnTypeSpecification io.trino.spi.ptf.ConnectorTableFunction.getReturnTypeSpecification()")
            .put("382", "Method: public java.lang.String io.trino.spi.ptf.ConnectorTableFunction.getName()")
            .put("382", "Method: public java.lang.String io.trino.spi.ptf.ConnectorTableFunction.getSchema()")
            .put("383", "Method: public abstract java.lang.String io.trino.spi.function.AggregationState.value()")
            .put("383", "Method: public default void io.trino.spi.security.SystemAccessControl.checkCanExecuteFunction(io.trino.spi.security.SystemSecurityContext,io.trino.spi.connector.CatalogSchemaRoutineName)")
            .put("383", "Method: public default void io.trino.spi.connector.ConnectorAccessControl.checkCanExecuteFunction(io.trino.spi.connector.ConnectorSecurityContext,io.trino.spi.connector.SchemaRoutineName)")
            .put("384", "Constructor: public io.trino.spi.eventlistener.QueryInputMetadata(java.lang.String,java.lang.String,java.lang.String,java.util.List<java.lang.String>,java.util.Optional<java.lang.Object>,java.util.OptionalLong,java.util.OptionalLong)")
            .put("386", "Method: public default java.util.stream.Stream<io.trino.spi.connector.TableColumnsMetadata> io.trino.spi.connector.ConnectorMetadata.streamTableColumns(io.trino.spi.connector.ConnectorSession,io.trino.spi.connector.SchemaTablePrefix)")
            .put("386", "Method: public default boolean io.trino.spi.connector.ConnectorMetadata.isSupportedVersionType(io.trino.spi.connector.ConnectorSession,io.trino.spi.connector.SchemaTableName,io.trino.spi.connector.PointerType,io.trino.spi.type.Type)")
            .put("386", "Method: public static io.trino.spi.ptf.TableArgumentSpecification$Builder io.trino.spi.ptf.TableArgumentSpecification.builder(java.lang.String)")
            .put("387", "Constructor: public io.trino.spi.eventlistener.QueryContext(java.lang.String,java.util.Optional<java.lang.String>,java.util.Set<java.lang.String>,java.util.Optional<java.lang.String>,java.util.Optional<java.lang.String>,java.util.Optional<java.lang.String>,java.util.Optional<java.lang.String>,java.util.Set<java.lang.String>,java.util.Set<java.lang.String>,java.util.Optional<java.lang.String>,java.util.Optional<java.lang.String>,java.util.Optional<java.lang.String>,java.util.Optional<io.trino.spi.resourcegroups.ResourceGroupId>,java.util.Map<java.lang.String, java.lang.String>,io.trino.spi.session.ResourceEstimates,java.lang.String,java.lang.String,java.lang.String,java.util.Optional<io.trino.spi.resourcegroups.QueryType>)")
            .build();

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
        return BACKWARD_INCOMPATIBLE_CHANGES.get(version);
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
        return entities.build().stream()
                // Ignore `final` so that we can e.g. remove final from a SPI method.
                // While adding `final` can be a breaking change, we currently ignore such breakages.
                .map(entity -> entity.replace(" final ", " "))
                .collect(toImmutableSet());
    }

    private static void addClassEntities(ImmutableSet.Builder<String> entities, Class<?> clazz, boolean includeDeprecated)
    {
        if (!isPublic(clazz.getModifiers())) {
            return;
        }
        for (Class<?> nestedClass : clazz.getDeclaredClasses()) {
            addClassEntities(entities, nestedClass, includeDeprecated);
        }
        if (!includeDeprecated && clazz.isAnnotationPresent(Deprecated.class)) {
            return;
        }
        entities.add("Class: " + clazz.toGenericString());
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
