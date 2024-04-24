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
package io.trino.tests.product;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.errorprone.annotations.FormatMethod;
import io.trino.tempto.internal.convention.ConventionBasedTest;
import io.trino.tempto.internal.convention.ConventionBasedTestFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static io.trino.tests.product.SuiteGroups.SUITE1_EXCLUSIONS;
import static io.trino.tests.product.TestGroups.CONFIGURED_FEATURES;
import static io.trino.tests.product.TestGroups.GROUP_BY;
import static io.trino.tests.product.TestGroups.HIVE_FILE_HEADER;
import static io.trino.tests.product.TestGroups.JOIN;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.SMOKE;
import static io.trino.tests.product.TestProductTestGroups.ProductTestInfo.Type.CONVENTION_BASED;
import static io.trino.tests.product.TestProductTestGroups.ProductTestInfo.Type.JAVA;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // ConventionBasedTestFactory is not thread-safe
public class TestProductTestGroups
{
    // TODO these are test groups used in convention tests; we should probably remove these groups and also these tests too
    @Deprecated
    private static final Set<String> IGNORED_CONVENTION_BASED_TEST_GROUPS = ImmutableSet.<String>builder()
            .add("varchar")
            .add("base_sql")
            .add("insert")
            .add("conditional")
            .add("limit")
            .add("distinct")
            .add("aggregate")
            .add("window")
            .add("union")
            .add("orderby")
            .add("set_operation")
            .add("with_clause")
            .add("qe")
            .add("hive")
            .add("array_functions")
            .add("binary_functions")
            .add("conversion_functions")
            .add("horology_functions")
            .add("json_functions")
            .add("map_functions")
            .add("math_functions")
            .add("ml_functions")
            .add("regex_functions")
            .add("string_functions")
            .add("url_functions")
            .add("color")
            .add("empty")
            .add("system")
            .add("jmx")
            .add("tpch_connector") // TODO replace with tpch
            .build();

    // TODO these are tests run (not excluded) in Suite1, and also included in other tests
    @Deprecated
    private static final Set<String> KNOWN_DUPLICATE_GROUPS = ImmutableSet.<String>builder()
            .add(SMOKE)
            .add(JOIN)
            .add(GROUP_BY)
            .add(HIVE_FILE_HEADER)
            .build();

    @Test
    public void selfTest()
    {
        Set<String> definedGroups = definedTestGroups();

        Set<String> conventionBasedGroups = conventionBasedProductTests()
                .flatMap(test -> test.groups().stream())
                .collect(toImmutableSet());

        assertThat(intersection(definedGroups, IGNORED_CONVENTION_BASED_TEST_GROUPS)).as("IGNORED_CONVENTION_TEST_GROUPS must not contain any valid groups")
                .isEmpty();

        assertThat(difference(IGNORED_CONVENTION_BASED_TEST_GROUPS, conventionBasedGroups)).as("IGNORED_CONVENTION_BASED_TEST_GROUPS must not contain any groups test are not in use")
                .isEmpty();

        assertThat(intersection(KNOWN_DUPLICATE_GROUPS, SUITE1_EXCLUSIONS)).as("KNOWN_DUPLICATE_GROUPS must not contain any groups from SUITE1_EXCLUSIONS")
                .isEmpty();

        assertThat(difference(KNOWN_DUPLICATE_GROUPS, definedGroups)).as("KNOWN_DUPLICATE_GROUPS must contain only valid TestGroups")
                .isEmpty();
    }

    @Test
    public void testAllGroupsUsed()
            throws Exception
    {
        Set<String> definedGroups = definedTestGroups();
        Set<String> usedGroups = productTests()
                .flatMap(test -> test.groups().stream())
                .collect(toImmutableSet());

        assertThat(difference(definedGroups, usedGroups)).as("All groups defined by TestGroups should be used in product tests")
                .isEmpty();
    }

    @Test
    public void testGroupsReasonable()
            throws Exception
    {
        Set<String> definedGroups = definedTestGroups();
        List<String> errors = new ArrayList<>();

        AtomicInteger testMethods = new AtomicInteger();
        productTests().forEach(test -> {
            testMethods.incrementAndGet();

            Set<String> groups = ImmutableSet.copyOf(test.groups());
            assertThat(groups).as("Duplicate test groups in %s", test.name())
                    .hasSize(test.groups().size());

            if (test.type() == CONVENTION_BASED) {
                groups = difference(groups, IGNORED_CONVENTION_BASED_TEST_GROUPS);
            }
            Set<String> invalidGroups = difference(groups, definedGroups);
            check(errors::add, invalidGroups.isEmpty(), "Invalid groups in %s: %s", test.name(), invalidGroups);

            // Avoid running as part of Suite1 unintentionally
            if (groups.isEmpty()) {
                // OK, Suite1
            }
            else if (groups.size() == 1 && KNOWN_DUPLICATE_GROUPS.contains(getOnlyElement(groups))) {
                // TODO some of these tests are run in Suite1 and some other suites, probably not intentionally
            }
            else if (groups.equals(Set.of(CONFIGURED_FEATURES)) && test.name().equals("io.trino.tests.product.TestConfiguredFeatures.selectConfiguredConnectors")) {
                // This is a special test, self-test of CONFIGURED_FEATURES
            }
            else {
                check(
                        errors::add,
                        !intersection(groups, SUITE1_EXCLUSIONS).isEmpty(),
                        "The test should probably have %s (or any other from %s), besides %s, to avoid being run more times than desired: %s",
                        PROFILE_SPECIFIC_TESTS,
                        SUITE1_EXCLUSIONS,
                        groups,
                        test.name());
            }
        });

        if (!errors.isEmpty()) {
            fail("%s Errors:".formatted(errors.size()) +
                    errors.stream()
                            .map(message -> "\n\t- " + message)
                            .collect(joining()));
        }

        // Self-test of this test. If we found too few tests, it might be because our assumptions
        // on how product tests look like are no longer valid.
        assertThat(testMethods).hasValueGreaterThan(500);
    }

    private Stream<ProductTestInfo> productTests()
            throws Exception
    {
        return Stream.concat(
                javaProductTests(),
                conventionBasedProductTests());
    }

    private Stream<ProductTestInfo> javaProductTests()
            throws Exception
    {
        return ClassPath.from(getClass().getClassLoader())
                .getTopLevelClassesRecursive("io.trino.tests.product").stream()
                .flatMap(classInfo -> Stream.of(classInfo.load().getMethods()))
                .filter(method -> method.isAnnotationPresent(org.testng.annotations.Test.class))
                .map(method -> new ProductTestInfo(
                        JAVA,
                        "%s.%s".formatted(method.getDeclaringClass().getName(), method.getName()),
                        ImmutableList.copyOf(method.getAnnotation(org.testng.annotations.Test.class).groups())));
    }

    private Stream<ProductTestInfo> conventionBasedProductTests()
    {
        return Stream.of(new ConventionBasedTestFactory().createTestCases())
                .map(ConventionBasedTest.class::cast)
                .map(conventionTest -> new ProductTestInfo(
                        CONVENTION_BASED,
                        conventionTest.getTestName(),
                        ImmutableList.copyOf(conventionTest.getTestGroups())));
    }

    private Set<String> definedTestGroups()
    {
        return Stream.of(TestGroups.class.getFields())
                .map(field -> {
                    try {
                        return (String) field.get(null);
                    }
                    catch (ReflectiveOperationException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(toImmutableSet());
    }

    record ProductTestInfo(Type type, String name, List<String> groups)
    {
        enum Type
        {
            JAVA,
            CONVENTION_BASED,
        }

        ProductTestInfo
        {
            requireNonNull(type, "type is null");
            requireNonNull(name, "name is null");
            groups = ImmutableList.copyOf(requireNonNull(groups, "groups is null"));
        }
    }

    @FormatMethod
    private static void check(Consumer<String> errorConsumer, boolean condition, String messagePattern, Object... args)
    {
        if (!condition) {
            errorConsumer.accept(messagePattern.formatted(args));
        }
    }
}
