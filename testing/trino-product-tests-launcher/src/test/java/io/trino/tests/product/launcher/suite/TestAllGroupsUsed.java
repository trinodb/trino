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
package io.trino.tests.product.launcher.suite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.trino.tests.product.TestGroups;
import io.trino.tests.product.launcher.env.configs.ConfigDefault;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAllGroupsUsed
{
    @Test
    public void testAllTestGroupsUsedInSuites()
    {
        Bootstrap app = new Bootstrap(
                ImmutableList.<Module>builder()
                        .add(new SuiteModule(EMPTY_MODULE /* TODO support extensions */))
                        .build());

        Injector injector = app
                .initialize();

        SuiteFactory suiteFactory = injector.getInstance(SuiteFactory.class);

        Set<String> usedGroups = suiteFactory.listSuites().stream()
                .map(suiteFactory::getSuite)
                .flatMap(suite -> suite.getTestRuns(new ConfigDefault()).stream())
                .flatMap(testRun -> Stream.concat(
                        testRun.getGroups().stream(),
                        testRun.getExcludedGroups().stream()))
                .collect(toImmutableSet());

        Set<String> definedGroups = TestGroups.Introspection.getAllGroups();

        assertThat(Sets.difference(usedGroups, definedGroups)).as("Only groups defined by TestGroups should be used in test suites")
                .isEmpty();

        assertThat(Sets.difference(definedGroups, usedGroups)).as("All groups defined by TestGroups should be used in test suites")
                .isEmpty();
    }
}
