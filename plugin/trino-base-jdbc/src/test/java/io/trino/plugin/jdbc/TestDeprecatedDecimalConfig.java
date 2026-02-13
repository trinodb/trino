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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.STRICT;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeprecatedDecimalConfig
{
    @Test
    @SuppressWarnings("deprecation")
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DeprecatedDecimalConfig.class)
                .setDecimalMapping(STRICT)
                .setDecimalDefaultScale(0)
                .setDecimalRoundingMode(UNNECESSARY));
    }

    @Test
    @SuppressWarnings("deprecation")
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("decimal-mapping", "allow_overflow")
                .put("decimal-default-scale", "16")
                .put("decimal-rounding-mode", "HALF_UP")
                .buildOrThrow();

        DeprecatedDecimalConfig expected = new DeprecatedDecimalConfig()
                .setDecimalMapping(ALLOW_OVERFLOW)
                .setDecimalDefaultScale(16)
                .setDecimalRoundingMode(HALF_UP);

        assertFullMapping(properties, expected);
    }

    @Test
    void testDeprecations()
    {
        Stream.of(DeprecatedDecimalConfig.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .forEachOrdered(method -> {
                    assertThat(method.isAnnotationPresent(Deprecated.class)).describedAs("Method %s should be deprecated".formatted(method))
                            .isTrue();
                });
    }

    @Test
    void testConfigsAnnotationsConsistency()
    {
        Set<String> legacy = inspectConfigs(LegacyDecimalConfig.class);
        // Test self-test
        assertThat(legacy.toString())
                .contains("@io.airlift.configuration.Config(\"decimal-rounding-mode\")")
                .contains("@jakarta.validation.constraints.Max(message=\"{jakarta.validation.constraints.Max.message}\", payload={}, groups={}, value=38L)");

        Set<String> deprecated = inspectConfigs(DeprecatedDecimalConfig.class);
        assertThat(deprecated)
                // contains() might produce better error message when things go wrong ...
                .containsExactlyInAnyOrderElementsOf(legacy)
                // ... but equality is what we really want to assert
                .isEqualTo(legacy);
    }

    private static Set<String> inspectConfigs(Class<?> configClass)
    {
        return Stream.of(configClass.getMethods())
                .map(method -> {
                    String annotations = Stream.of(method.getAnnotations())
                            .filter(annotation -> !(annotation instanceof Deprecated))
                            .map(Annotation::toString)
                            .sorted()
                            .collect(joining("\n", "", "\n"));
                    return "%s%s".formatted(annotations, method.getName());
                })
                .collect(toImmutableSet());
    }

    @Test
    void testConfigsDefaultsConsistency()
    {
        Map<String, Object> legacy = inspectFields(LegacyDecimalConfig.class);
        // Test self-test
        assertThat(legacy)
                .containsEntry("decimalRoundingMode", UNNECESSARY);

        Map<String, Object> deprecated = inspectFields(DeprecatedDecimalConfig.class);
        assertThat(deprecated)
                // contains() might produce better error message when things go wrong ...
                .containsExactlyInAnyOrderEntriesOf(legacy)
                // ... but equality is what we really want to assert
                .isEqualTo(legacy);
    }

    private static Map<String, Object> inspectFields(Class<?> configClass)
    {
        try {
            Map<String, Object> values = new HashMap<>();
            Object configInstance = configClass.getConstructor().newInstance();
            for (Field field : configClass.getDeclaredFields()) {
                field.setAccessible(true);
                values.put(field.getName(), field.get(configInstance));
            }
            return values;
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
