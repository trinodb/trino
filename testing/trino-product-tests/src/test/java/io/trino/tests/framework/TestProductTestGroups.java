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
package io.trino.tests.framework;

import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectPackage;

class TestProductTestGroups
{
    private static final String PRODUCT_TEST_PACKAGE = "io.trino.tests.product";

    @Test
    void testAllProductTestsHaveGroup()
    {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(selectPackage(PRODUCT_TEST_PACKAGE))
                .build();
        TestPlan testPlan = LauncherFactory.create().discover(request);
        Set<String> productTestGroups = Arrays.stream(TestGroup.class.getDeclaredClasses())
                .map(annotation -> annotation.getAnnotation(Tag.class))
                .filter(tag -> tag != null)
                .map(Tag::value)
                .collect(Collectors.toUnmodifiableSet());

        List<String> ungroupedTests = testPlan.getRoots().stream()
                .flatMap(root -> Stream.concat(Stream.of(root), testPlan.getDescendants(root).stream()))
                .filter(identifier -> isProductTestMethod(identifier.getSource().orElse(null)))
                .filter(identifier -> identifier.getTags().stream()
                        .noneMatch(tag -> productTestGroups.contains(tag.getName())))
                .map(identifier -> (MethodSource) identifier.getSource().orElseThrow())
                .map(source -> source.getClassName() + "#" + source.getMethodName())
                .distinct()
                .sorted()
                .toList();

        assertThat(ungroupedTests)
                .as("Product tests must declare at least one TestGroup")
                .isEmpty();
    }

    private static boolean isProductTestMethod(TestSource source)
    {
        return source instanceof MethodSource methodSource &&
                methodSource.getClassName().startsWith(PRODUCT_TEST_PACKAGE + ".");
    }
}
