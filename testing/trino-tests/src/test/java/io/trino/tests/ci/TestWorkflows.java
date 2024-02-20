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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.yaml.snakeyaml.Yaml;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWorkflows
{
    @ParameterizedTest
    @MethodSource("listWorkflows")
    public void testUseStarburstRunners(Path path)
    {
        try {
            Yaml yaml = new Yaml();
            Map<?, ?> workflow = yaml.load(new StringReader(Files.readString(path)));
            Map<?, ?> jobs = (Map<?, ?>) workflow.get("jobs");
            jobs.forEach((jobName, jobDefinition) -> {
                Map<?, ?> job = (Map<?, ?>) jobDefinition;

                String condition = (String) job.get("if");
                if (condition != null && (condition.equals("github.repository == 'trinodb/trino'") ||
                        condition.equals("github.repository_owner == 'trinodb'") ||
                        condition.equals("${{ github.event.issue.pull_request }} && github.repository_owner == 'trinodb'"))) {
                    // Workflows with such conditions don't run in a fork.
                    return;
                }

                Object runsOn = job.get("runs-on");
                verifyNotNull(runsOn, "No runs-on for job %s".formatted(jobName));
                assertThat(runsOn.toString()).as("runs-on for job %s", jobName)
                        // TODO enforce "self-hosted" in runs-on? or is it redundant?
                        .contains("sep-cicd");
            });
        }
        catch (AssertionError | Exception e) {
            throw new AssertionError("Failed when checking %s: %s".formatted(path, e), e);
        }
    }

    public static List<Path> listWorkflows()
            throws Exception
    {
        try (Stream<Path> walk = Files.walk(Paths.get("../../.github/workflows"))) {
            return walk
                    .filter(path -> path.toString().endsWith(".yml"))
                    .collect(toImmutableList());
        }
    }
}
