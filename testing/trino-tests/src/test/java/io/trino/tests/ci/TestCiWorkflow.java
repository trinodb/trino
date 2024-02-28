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
package io.trino.tests.ci;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCiWorkflow
{
    private static final Path CI_YML = Paths.get("../../.github/workflows/ci.yml");

    @Test
    public void testBuildSuccessDependencies()
            throws Exception
    {
        String buildSuccessJob = "build-success";

        Yaml yaml = new Yaml();
        Map<?, ?> workflow = yaml.load(new StringReader(Files.readString(CI_YML)));
        Map<?, ?> jobs = (Map<?, ?>) workflow.get("jobs");

        Set<String> jobNames = jobs.keySet().stream()
                .map(String.class::cast)
                .filter(jobName -> !jobName.equals(buildSuccessJob))
                .collect(toImmutableSet());
        List<String> dependencies = ((List<?>) ((Map<?, ?>) jobs.get(buildSuccessJob)).get("needs")).stream()
                .map(String.class::cast)
                .collect(toImmutableList());
        assertThat(dependencies).as("dependencies for %s", buildSuccessJob)
                .isSorted()
                .doesNotHaveDuplicates()
                .containsExactlyInAnyOrderElementsOf(jobNames);
    }
}
